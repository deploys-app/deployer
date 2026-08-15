package k8s

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	spotRebalanceMaxStarting = 3
	spotRebalanceMinAge      = 5 * time.Minute
)

func (c *Client) RebalanceSpot(ctx context.Context) error {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pods, err := c.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	deps, err := c.client.AppsV1().Deployments(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	rss, err := c.client.AppsV1().ReplicaSets(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	pick := pickSpotRebalance(c.namespace, time.Now(), nodes.Items, pods.Items, deps.Items, rss.Items)
	if pick.StartingOnSpot >= spotRebalanceMaxStarting {
		slog.Info("spot rebalance: waiting, many pods starting on spot", "count", pick.StartingOnSpot)
		return nil
	}
	if pick.Pod == nil {
		return nil
	}

	slog.Info("spot rebalance: evicting pod to spot",
		"pod", pick.Pod.Name,
		"deployment", pick.Deployment,
		"fromNode", pick.Pod.Spec.NodeName,
	)
	return c.client.CoreV1().Pods(pick.Pod.Namespace).EvictV1(ctx, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pick.Pod.Name,
			Namespace: pick.Pod.Namespace,
		},
	})
}

type spotPick struct {
	Pod            *v1.Pod
	Deployment     string
	StartingOnSpot int
}

func pickSpotRebalance(ns string, now time.Time, nodes []v1.Node, pods []v1.Pod, deps []appsv1.Deployment, rss []appsv1.ReplicaSet) spotPick {
	nodeByName := make(map[string]*v1.Node, len(nodes))
	for i := range nodes {
		nodeByName[nodes[i].Name] = &nodes[i]
	}

	podsByNode := make(map[string][]*v1.Pod)
	startingOnSpot := 0
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName == "" {
			continue
		}
		podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
		if n := nodeByName[p.Spec.NodeName]; n != nil && isSpotNode(n) && isStarting(p) {
			startingOnSpot++
		}
	}
	if startingOnSpot >= spotRebalanceMaxStarting {
		return spotPick{StartingOnSpot: startingOnSpot}
	}

	deployByName := make(map[string]*appsv1.Deployment, len(deps))
	for i := range deps {
		deployByName[deps[i].Name] = &deps[i]
	}
	rsToDeploy := make(map[string]string, len(rss))
	for i := range rss {
		kind, name := controllerOf(&rss[i].ObjectMeta)
		if kind == "Deployment" {
			rsToDeploy[rss[i].Name] = name
		}
	}

	var spotNodes []*v1.Node
	for i := range nodes {
		n := &nodes[i]
		if isSpotNode(n) {
			spotNodes = append(spotNodes, n)
		}
	}

	type cand struct {
		pod  *v1.Pod
		name string
	}
	var candidates []cand
	for i := range pods {
		p := &pods[i]
		if p.Namespace != ns {
			continue
		}
		depName := owningDeployment(p, rsToDeploy)
		if depName == "" {
			continue
		}
		d := deployByName[depName]
		if d == nil || !deploymentEvictable(d) {
			continue
		}
		if !candidatePod(p, now, nodeByName) {
			continue
		}
		if !spotNodeFits(p, spotNodes, podsByNode) {
			continue
		}
		candidates = append(candidates, cand{pod: p, name: depName})
	}
	if len(candidates) == 0 {
		return spotPick{StartingOnSpot: startingOnSpot}
	}

	slices.SortFunc(candidates, func(a, b cand) int {
		if c := a.pod.CreationTimestamp.Time.Compare(b.pod.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.pod.Name, b.pod.Name)
	})
	return spotPick{
		Pod:            candidates[0].pod,
		Deployment:     candidates[0].name,
		StartingOnSpot: startingOnSpot,
	}
}

func candidatePod(p *v1.Pod, now time.Time, nodeByName map[string]*v1.Node) bool {
	if p.Labels["id"] == "" || p.Labels["projectId"] == "" {
		return false
	}
	if p.DeletionTimestamp != nil {
		return false
	}
	if p.Status.Phase != v1.PodRunning || !isPodReady(p) {
		return false
	}
	if now.Sub(p.CreationTimestamp.Time) < spotRebalanceMinAge {
		return false
	}
	if hasPVC(p) {
		return false
	}
	var aff *v1.NodeAffinity
	if p.Spec.Affinity != nil {
		aff = p.Spec.Affinity.NodeAffinity
	}
	if !prefersSpot(aff) {
		return false
	}
	n := nodeByName[p.Spec.NodeName]
	if n == nil || isSpotNode(n) {
		return false
	}
	return true
}

func deploymentEvictable(d *appsv1.Deployment) bool {
	if d.Spec.Replicas == nil || *d.Spec.Replicas <= 1 {
		return false
	}
	want := *d.Spec.Replicas
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.UpdatedReplicas >= want &&
		d.Status.ReadyReplicas >= want &&
		d.Status.UnavailableReplicas == 0
}

func owningDeployment(p *v1.Pod, rsToDeploy map[string]string) string {
	kind, name := controllerOf(&p.ObjectMeta)
	switch kind {
	case "Deployment":
		return name
	case "ReplicaSet":
		return rsToDeploy[name]
	default:
		return ""
	}
}

func controllerOf(obj *metav1.ObjectMeta) (kind, name string) {
	for _, o := range obj.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			return o.Kind, o.Name
		}
	}
	return "", ""
}

func prefersSpot(affinity *v1.NodeAffinity) bool {
	if affinity == nil {
		return false
	}
	if req := affinity.RequiredDuringSchedulingIgnoredDuringExecution; req != nil && requiresNonSpot(req) {
		return false
	}
	for _, term := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
		if nodeSelectorTermHas(term.Preference, gkeSpotLabel, v1.NodeSelectorOpExists) {
			return true
		}
	}
	return false
}

func requiresNonSpot(sel *v1.NodeSelector) bool {
	if sel == nil || len(sel.NodeSelectorTerms) == 0 {
		return false
	}
	for _, term := range sel.NodeSelectorTerms {
		if !nodeSelectorTermHas(term, gkeSpotLabel, v1.NodeSelectorOpDoesNotExist) {
			return false
		}
	}
	return true
}

func nodeSelectorTermHas(term v1.NodeSelectorTerm, key string, op v1.NodeSelectorOperator) bool {
	for _, expr := range term.MatchExpressions {
		if expr.Key == key && expr.Operator == op {
			return true
		}
	}
	return false
}

func isSpotNode(n *v1.Node) bool {
	if n == nil {
		return false
	}
	_, ok := n.Labels[gkeSpotLabel]
	return ok
}

func isStarting(p *v1.Pod) bool {
	if p.DeletionTimestamp != nil || p.Spec.NodeName == "" {
		return false
	}
	if p.Status.Phase == v1.PodPending {
		return true
	}
	restartable := restartableInitNames(p)
	for _, cs := range p.Status.InitContainerStatuses {
		if waitingStart(cs) {
			return true
		}
		if !restartable[cs.Name] && cs.State.Terminated == nil {
			return true
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		if waitingStart(cs) {
			return true
		}
	}
	return false
}

func restartableInitNames(p *v1.Pod) map[string]bool {
	out := make(map[string]bool)
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			out[c.Name] = true
		}
	}
	return out
}

func waitingStart(cs v1.ContainerStatus) bool {
	w := cs.State.Waiting
	if w == nil {
		return false
	}
	switch w.Reason {
	case "ContainerCreating", "PodInitializing", "ErrImagePull", "ImagePullBackOff":
		return true
	default:
		return false
	}
}

func isPodReady(p *v1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func hasPVC(p *v1.Pod) bool {
	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			return true
		}
	}
	return false
}

func spotNodeFits(p *v1.Pod, spotNodes []*v1.Node, podsByNode map[string][]*v1.Pod) bool {
	for _, n := range spotNodes {
		if nodeFits(n, p, podsByNode[n.Name]) {
			return true
		}
	}
	return false
}

func nodeFits(n *v1.Node, p *v1.Pod, bound []*v1.Pod) bool {
	if n.Spec.Unschedulable || !nodeReady(n) {
		return false
	}
	if !nodeSelectorMatches(p.Spec.NodeSelector, n.Labels) {
		return false
	}
	if !toleratesNoSchedule(p.Spec.Tolerations, n.Spec.Taints) {
		return false
	}
	if !requiredNodeAffinityMatches(p.Spec.Affinity, n) {
		return false
	}
	if hasSibling(p.Labels["id"], bound) {
		return false
	}
	leftCPU, leftMem := leftoverOn(n, bound)
	reqCPU, reqMem := podRequests(p)
	return leftCPU.Cmp(reqCPU) >= 0 && leftMem.Cmp(reqMem) >= 0
}

func nodeReady(n *v1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}

func nodeSelectorMatches(sel, labels map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func toleratesNoSchedule(tols []v1.Toleration, taints []v1.Taint) bool {
	for _, t := range taints {
		if t.Effect != v1.TaintEffectNoSchedule && t.Effect != v1.TaintEffectNoExecute {
			continue
		}
		if !tolerationMatchesAny(t, tols) {
			return false
		}
	}
	return true
}

func tolerationMatchesAny(taint v1.Taint, tols []v1.Toleration) bool {
	return slices.ContainsFunc(tols, func(tol v1.Toleration) bool {
		return tolerationMatches(tol, taint)
	})
}

func tolerationMatches(tol v1.Toleration, taint v1.Taint) bool {
	if tol.Effect != "" && tol.Effect != taint.Effect {
		return false
	}
	op := tol.Operator
	if op == "" {
		op = v1.TolerationOpEqual
	}
	switch op {
	case v1.TolerationOpExists:
		return tol.Key == "" || tol.Key == taint.Key
	case v1.TolerationOpEqual:
		return tol.Key == taint.Key && tol.Value == taint.Value
	default:
		return false
	}
}

func requiredNodeAffinityMatches(aff *v1.Affinity, n *v1.Node) bool {
	if aff == nil || aff.NodeAffinity == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	req := aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(req.NodeSelectorTerms) == 0 {
		return true
	}
	for _, term := range req.NodeSelectorTerms {
		if nodeSelectorTermMatches(term, n) {
			return true
		}
	}
	return false
}

func nodeSelectorTermMatches(term v1.NodeSelectorTerm, n *v1.Node) bool {
	for _, expr := range term.MatchExpressions {
		if !labelExprMatches(expr, n.Labels) {
			return false
		}
	}
	for _, expr := range term.MatchFields {
		if !fieldExprMatches(expr, n) {
			return false
		}
	}
	return true
}

func labelExprMatches(expr v1.NodeSelectorRequirement, labels map[string]string) bool {
	val, exists := labels[expr.Key]
	return selectorOpMatches(expr, val, exists)
}

func fieldExprMatches(expr v1.NodeSelectorRequirement, n *v1.Node) bool {
	var val string
	var exists bool
	if expr.Key == "metadata.name" {
		val, exists = n.Name, true
	}
	return selectorOpMatches(expr, val, exists)
}

func selectorOpMatches(expr v1.NodeSelectorRequirement, val string, exists bool) bool {
	switch expr.Operator {
	case v1.NodeSelectorOpIn:
		return exists && slices.Contains(expr.Values, val)
	case v1.NodeSelectorOpNotIn:
		return !exists || !slices.Contains(expr.Values, val)
	case v1.NodeSelectorOpExists:
		return exists
	case v1.NodeSelectorOpDoesNotExist:
		return !exists
	case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
		if !exists || len(expr.Values) != 1 {
			return false
		}
		have, err1 := strconv.ParseInt(val, 10, 64)
		want, err2 := strconv.ParseInt(expr.Values[0], 10, 64)
		if err1 != nil || err2 != nil {
			return false
		}
		if expr.Operator == v1.NodeSelectorOpGt {
			return have > want
		}
		return have < want
	default:
		return false
	}
}

func hasSibling(id string, bound []*v1.Pod) bool {
	if id == "" {
		return false
	}
	for _, p := range bound {
		if p.Labels["id"] == id {
			return true
		}
	}
	return false
}

func leftoverOn(n *v1.Node, bound []*v1.Pod) (cpu, mem resource.Quantity) {
	if a, ok := n.Status.Allocatable[v1.ResourceCPU]; ok {
		cpu = a.DeepCopy()
	}
	if a, ok := n.Status.Allocatable[v1.ResourceMemory]; ok {
		mem = a.DeepCopy()
	}
	for _, p := range bound {
		rc, rm := podRequests(p)
		cpu.Sub(rc)
		mem.Sub(rm)
	}
	return cpu, mem
}

func podRequests(p *v1.Pod) (cpu, mem resource.Quantity) {
	add := func(c v1.Container) {
		if r, ok := c.Resources.Requests[v1.ResourceCPU]; ok {
			cpu.Add(r)
		}
		if r, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
			mem.Add(r)
		}
	}
	for _, c := range p.Spec.Containers {
		add(c)
	}
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			add(c)
		}
	}
	return cpu, mem
}
