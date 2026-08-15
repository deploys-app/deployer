package k8s

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPrefersSpot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		aff  *v1.NodeAffinity
		want bool
	}{
		{"nil", nil, false},
		{"defaultSpot", defaultSpotNodeAffinity(), true},
		{"preferSpot", preferSpotNodeAffinity(), true},
		{"preferNonSpot", preferNonSpotNodeAffinity(), false},
		{"requiredNonSpot", nonSpotNodeAffinity(), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := prefersSpot(tc.aff); got != tc.want {
				t.Fatalf("prefersSpot = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsSpotNode(t *testing.T) {
	t.Parallel()

	if isSpotNode(nil) {
		t.Fatal("nil node is not spot")
	}
	if isSpotNode(&v1.Node{}) {
		t.Fatal("unlabeled node is not spot")
	}
	n := &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{gkeSpotLabel: "true"}}}
	if !isSpotNode(n) {
		t.Fatal("labeled node should be spot")
	}
}

func TestIsStarting(t *testing.T) {
	t.Parallel()

	bound := func(phase v1.PodPhase, wait string, crash bool) *v1.Pod {
		p := &v1.Pod{
			Spec:   v1.PodSpec{NodeName: "n1"},
			Status: v1.PodStatus{Phase: phase},
		}
		if wait != "" {
			p.Status.ContainerStatuses = []v1.ContainerStatus{{
				State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: wait}},
			}}
		}
		if crash {
			p.Status.Phase = v1.PodRunning
			p.Status.ContainerStatuses = []v1.ContainerStatus{{
				State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}}
		}
		return p
	}

	if !isStarting(bound(v1.PodPending, "", false)) {
		t.Error("Pending+bound should be starting")
	}
	if !isStarting(bound(v1.PodRunning, "ContainerCreating", false)) {
		t.Error("ContainerCreating should be starting")
	}
	if !isStarting(bound(v1.PodRunning, "PodInitializing", false)) {
		t.Error("PodInitializing should be starting")
	}
	initPod := bound(v1.PodRunning, "", false)
	initPod.Spec.InitContainers = []v1.Container{{Name: "init"}}
	initPod.Status.InitContainerStatuses = []v1.ContainerStatus{{
		Name:  "init",
		State: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
	}}
	if !isStarting(initPod) {
		t.Error("unfinished init should be starting")
	}
	ready := bound(v1.PodRunning, "", false)
	ready.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	if isStarting(ready) {
		t.Error("Running+Ready should not be starting")
	}
	if isStarting(bound(v1.PodRunning, "", true)) {
		t.Error("CrashLoopBackOff should not be starting")
	}
	unbound := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodPending}}
	if isStarting(unbound) {
		t.Error("unbound Pending is not on a spot VM")
	}

	sidecar := bound(v1.PodRunning, "", false)
	sidecar.Spec.InitContainers = []v1.Container{{
		Name:          "proxy",
		RestartPolicy: new(v1.ContainerRestartPolicyAlways),
	}}
	sidecar.Status.InitContainerStatuses = []v1.ContainerStatus{{
		Name:  "proxy",
		State: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
	}}
	sidecar.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	if isStarting(sidecar) {
		t.Error("running native sidecar is not starting")
	}
}

func TestPodRequestsIncludesSidecar(t *testing.T) {
	t.Parallel()

	p := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("128Mi"),
				}},
			}},
			InitContainers: []v1.Container{
				{
					Name: "setup",
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					}},
				},
				{
					Name:          "proxy",
					RestartPolicy: new(v1.ContainerRestartPolicyAlways),
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("10m"),
						v1.ResourceMemory: resource.MustParse("16Mi"),
					}},
				},
			},
		},
	}
	cpu, mem := podRequests(p)
	if cpu.Cmp(resource.MustParse("110m")) != 0 {
		t.Errorf("cpu = %s, want 110m (app+sidecar, not one-shot init)", cpu.String())
	}
	if mem.Cmp(resource.MustParse("144Mi")) != 0 {
		t.Errorf("mem = %s, want 144Mi", mem.String())
	}
}

func TestNodeFits(t *testing.T) {
	t.Parallel()

	spot := readySpotNode("spot-a", "2", "4Gi", false)
	pod := preferSpotPod("web-0", "web", "ondemand-a", resource.MustParse("500m"), resource.MustParse("512Mi"))

	if !nodeFits(spot, pod, nil) {
		t.Fatal("empty Ready spot node should fit")
	}

	cordoned := readySpotNode("spot-a", "2", "4Gi", true)
	if nodeFits(cordoned, pod, nil) {
		t.Error("cordoned node should not fit")
	}

	notReady := readySpotNode("spot-a", "2", "4Gi", false)
	notReady.Status.Conditions = []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}}
	if nodeFits(notReady, pod, nil) {
		t.Error("not-Ready node should not fit")
	}

	small := readySpotNode("spot-a", "100m", "4Gi", false)
	if nodeFits(small, pod, nil) {
		t.Error("CPU short should not fit")
	}
	smallMem := readySpotNode("spot-a", "2", "100Mi", false)
	if nodeFits(smallMem, pod, nil) {
		t.Error("memory short should not fit")
	}

	occupant := preferSpotPod("other-0", "other", "spot-a", resource.MustParse("1800m"), resource.MustParse("3Gi"))
	if nodeFits(spot, pod, []*v1.Pod{occupant}) {
		t.Error("leftover after occupant should not fit")
	}

	sel := preferSpotPod("web-0", "web", "ondemand-a", resource.MustParse("500m"), resource.MustParse("512Mi"))
	sel.Spec.NodeSelector = map[string]string{"pool": "gold"}
	if nodeFits(spot, sel, nil) {
		t.Error("nodeSelector miss should not fit")
	}
	gold := readySpotNode("spot-a", "2", "4Gi", false)
	gold.Labels["pool"] = "gold"
	if !nodeFits(gold, sel, nil) {
		t.Error("matching nodeSelector should fit")
	}

	tainted := readySpotNode("spot-a", "2", "4Gi", false)
	tainted.Spec.Taints = []v1.Taint{{
		Key:    gkeSpotLabel,
		Value:  "true",
		Effect: v1.TaintEffectNoSchedule,
	}}
	if nodeFits(tainted, pod, nil) {
		t.Error("untolerated taint should not fit")
	}
	tol := preferSpotPod("web-0", "web", "ondemand-a", resource.MustParse("500m"), resource.MustParse("512Mi"))
	tol.Spec.Tolerations = []v1.Toleration{{
		Key:      gkeSpotLabel,
		Operator: v1.TolerationOpEqual,
		Value:    "true",
		Effect:   v1.TaintEffectNoSchedule,
	}}
	if !nodeFits(tainted, tol, nil) {
		t.Error("matching toleration should fit")
	}

	sib := preferSpotPod("web-1", "web", "spot-a", resource.MustParse("100m"), resource.MustParse("64Mi"))
	if nodeFits(spot, pod, []*v1.Pod{sib}) {
		t.Error("sibling on node should not fit")
	}
}

func TestPickSpotRebalance(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	old := now.Add(-10 * time.Minute)
	young := now.Add(-time.Minute)

	ondemand := readyOnDemandNode("ondemand-a", "4", "8Gi")
	spot := readySpotNode("spot-a", "4", "8Gi", false)
	spot.Spec.Taints = []v1.Taint{{
		Key:    gkeSpotLabel,
		Value:  "true",
		Effect: v1.TaintEffectNoSchedule,
	}}

	base := func() (nodes []v1.Node, pods []v1.Pod, deps []appsv1.Deployment, rss []appsv1.ReplicaSet) {
		p := preferSpotPod("web-0", "web", "ondemand-a", resource.MustParse("100m"), resource.MustParse("64Mi"))
		p.CreationTimestamp = metav1.NewTime(old)
		p.Spec.Tolerations = spotToleration()
		return []v1.Node{*ondemand, *spot}, []v1.Pod{*p}, []appsv1.Deployment{settledDeploy("web", 2)}, []appsv1.ReplicaSet{ownedRS("web-rs", "web")}
	}

	t.Run("picks oldest eligible", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		older := preferSpotPod("web-old", "web", "ondemand-a", resource.MustParse("100m"), resource.MustParse("64Mi"))
		older.CreationTimestamp = metav1.NewTime(old.Add(-time.Hour))
		older.Spec.Tolerations = spotToleration()
		pods = append(pods, *older)
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod == nil || got.Pod.Name != "web-old" {
			t.Fatalf("got %+v, want web-old", podName(got.Pod))
		}
	})

	t.Run("skip replicas==1", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		deps[0].Spec.Replicas = new(int32(1))
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip already on spot", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		pods[0].Spec.NodeName = "spot-a"
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip prefer non-spot", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		pods[0].Spec.Affinity.NodeAffinity = preferNonSpotNodeAffinity()
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip starting>=3", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		for _, name := range []string{"c1", "c2", "c3"} {
			s := preferSpotPod(name, "other", "spot-a", resource.MustParse("1m"), resource.MustParse("1Mi"))
			s.Status.Phase = v1.PodPending
			s.Status.Conditions = nil
			pods = append(pods, *s)
		}
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
		if got.StartingOnSpot != 3 {
			t.Fatalf("StartingOnSpot = %d, want 3", got.StartingOnSpot)
		}
	})

	t.Run("skip no capacity", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		nodes[1].Status.Allocatable = v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("10m"),
			v1.ResourceMemory: resource.MustParse("8Mi"),
		}
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip young pod", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		pods[0].CreationTimestamp = metav1.NewTime(young)
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip updatedReplicas < replicas", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		deps[0].Status.UpdatedReplicas = 1
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("skip sibling on only spot node", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		sib := preferSpotPod("web-1", "web", "spot-a", resource.MustParse("100m"), resource.MustParse("64Mi"))
		pods = append(pods, *sib)
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod != nil {
			t.Fatalf("picked %s, want none", got.Pod.Name)
		}
	})

	t.Run("crashloop on spot does not freeze", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		for _, name := range []string{"x1", "x2", "x3"} {
			c := preferSpotPod(name, "other", "spot-a", resource.MustParse("1m"), resource.MustParse("1Mi"))
			c.Status.Phase = v1.PodRunning
			c.Status.Conditions = nil
			c.Status.ContainerStatuses = []v1.ContainerStatus{{
				State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}}
			pods = append(pods, *c)
		}
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod == nil {
			t.Fatal("crashloops should not block pick")
		}
	})

	t.Run("at most one", func(t *testing.T) {
		nodes, pods, deps, rss := base()
		p2 := preferSpotPod("other-0", "other", "ondemand-a", resource.MustParse("100m"), resource.MustParse("64Mi"))
		p2.CreationTimestamp = metav1.NewTime(old)
		p2.Spec.Tolerations = spotToleration()
		pods = append(pods, *p2)
		deps = append(deps, settledDeploy("other", 2))
		rss = append(rss, ownedRS("other-rs", "other"))
		got := pickSpotRebalance("default", now, nodes, pods, deps, rss)
		if got.Pod == nil {
			t.Fatal("expected one pick")
		}
	})
}

func readySpotNode(name, cpu, mem string, unschedulable bool) *v1.Node {
	n := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{gkeSpotLabel: "true"},
		},
		Spec: v1.NodeSpec{Unschedulable: unschedulable},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(cpu),
				v1.ResourceMemory: resource.MustParse(mem),
			},
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
	return n
}

func readyOnDemandNode(name, cpu, mem string) *v1.Node {
	n := readySpotNode(name, cpu, mem, false)
	delete(n.Labels, gkeSpotLabel)
	return n
}

func preferSpotPod(name, id, node string, cpu, mem resource.Quantity) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{"id": id, "projectId": "1"},
			OwnerReferences: []metav1.OwnerReference{{
				Kind:       "ReplicaSet",
				Name:       id + "-rs",
				Controller: new(true),
			}},
		},
		Spec: v1.PodSpec{
			NodeName: node,
			Affinity: &v1.Affinity{NodeAffinity: defaultSpotNodeAffinity()},
			Containers: []v1.Container{{
				Name: "app",
				Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    cpu,
					v1.ResourceMemory: mem,
				}},
			}},
		},
		Status: v1.PodStatus{
			Phase:      v1.PodRunning,
			Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
		},
	}
}

func spotToleration() []v1.Toleration {
	return []v1.Toleration{{
		Key:      gkeSpotLabel,
		Operator: v1.TolerationOpEqual,
		Value:    "true",
		Effect:   v1.TaintEffectNoSchedule,
	}}
}

func settledDeploy(name string, replicas int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: new(replicas)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration:  1,
			UpdatedReplicas:     replicas,
			ReadyReplicas:       replicas,
			UnavailableReplicas: 0,
		},
	}
}

func ownedRS(name, deploy string) appsv1.ReplicaSet {
	return appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			OwnerReferences: []metav1.OwnerReference{{
				Kind:       "Deployment",
				Name:       deploy,
				Controller: new(true),
			}},
		},
	}
}

func podName(p *v1.Pod) string {
	if p == nil {
		return "<nil>"
	}
	return p.Name
}
