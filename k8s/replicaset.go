package k8s

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

type ReplicaSet struct {
	ID        string
	ProjectID string
	// Name is the k8s resource name prefix (the apiserver's KubeName).
	Name string
	// DisplayName is the user-facing deployment name, surfaced via K_SERVICE/
	// K_CONFIGURATION and the deploys.app/name annotation. Empty on commands
	// from apiservers that predate it — displayName() falls back to Name.
	DisplayName   string
	Revision      int64
	Image         string
	Env           Env
	Command       []string
	Args          []string
	SA            string
	Replicas      int
	ExposePort    int
	Annotations   map[string]string // pod's annotations
	RequestCPU    string
	RequestMemory string
	LimitCPU      string
	LimitMemory   string
	PullSecret    string
	Disk          Disk
}

func (c *Client) CreateReplicaSet(ctx context.Context, obj ReplicaSet) error {
	s := c.client.AppsV1().ReplicaSets(c.namespace)

	id := fmt.Sprintf("%s-%d", obj.ID, obj.Revision)

	label := map[string]string{
		"id":        obj.ID,
		"revision":  strconv.FormatInt(obj.Revision, 10),
		"projectId": obj.ProjectID,
	}
	annotations := map[string]string{
		"deploys.app/id":        obj.ID,
		"deploys.app/projectId": obj.ProjectID,
		"deploys.app/name":      obj.displayName(),
		"deploys.app/revision":  strconv.FormatInt(obj.Revision, 10),
		"deploys.app/at":        time.Now().Format(time.RFC3339),
	}
	// merge annotations to object
	if obj.Annotations == nil {
		obj.Annotations = map[string]string{}
	}
	for k, v := range annotations {
		obj.Annotations[k] = v
	}

	if obj.Env == nil {
		obj.Env = make(Env)
	}
	obj.Env["K_SERVICE"] = obj.displayName()
	obj.Env["K_REVISION"] = fmt.Sprintf("%s.%d", obj.displayName(), obj.Revision)
	obj.Env["K_CONFIGURATION"] = obj.displayName()

	var (
		livenessProbe  *v1.Probe
		readinessProbe *v1.Probe
	)
	if obj.ExposePort > 0 {
		obj.Env["PORT"] = strconv.Itoa(obj.ExposePort)

		livenessProbe = &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.FromInt(obj.ExposePort),
					Host: "",
				},
			},
			InitialDelaySeconds: 5,
			TimeoutSeconds:      10,
			PeriodSeconds:       30,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}

		readinessProbe = &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.FromInt(obj.ExposePort),
					Host: "",
				},
			},
			InitialDelaySeconds: 3,
			TimeoutSeconds:      5,
			PeriodSeconds:       5,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}
	}

	limits := v1.ResourceList{}
	if obj.LimitCPU != "" {
		q, err := resource.ParseQuantity(obj.LimitCPU)
		if err != nil {
			return err
		}
		limits["cpu"] = q
	}
	if obj.LimitMemory != "" {
		q, err := resource.ParseQuantity(obj.LimitMemory)
		if err != nil {
			return err
		}
		limits["memory"] = q
	}

	rs := &appsv1.ReplicaSet{}
	rs.ObjectMeta.Name = id
	rs.ObjectMeta.Labels = label
	rs.ObjectMeta.Annotations = annotations
	rs.Spec = appsv1.ReplicaSetSpec{} // reset
	rs.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: label,
	}

	rs.Spec.Replicas = pointer.Int32(int32(obj.Replicas))
	rs.Spec.Template.ObjectMeta = metav1.ObjectMeta{
		Labels:      label,
		Annotations: obj.Annotations,
	}
	if obj.PullSecret != "" {
		rs.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{Name: obj.PullSecret},
		}
	}

	rs.Spec.Template.Spec.Affinity = &v1.Affinity{}

	// try to spread stateless workload to difference node
	if obj.Disk.Name == "" {
		rs.Spec.Template.Spec.Affinity.PodAntiAffinity = &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: v1.PodAffinityTerm{
						TopologyKey: "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: label,
						},
					},
				},
			},
		}
	}

	if obj.RequestMemory != "0" {
		// reserved memory
		rs.Spec.Template.Spec.Affinity.NodeAffinity = nonSpotNodeAffinity()
	} else {
		// non-reserved memory
		rs.Spec.Template.Spec.Affinity.NodeAffinity = defaultSpotNodeAffinity()
	}

	env := obj.Env.envVars()
	env = append(env, v1.EnvVar{
		Name: "K_IP",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	})

	requestCPU, err := resource.ParseQuantity(obj.RequestCPU)
	if err != nil {
		return err
	}
	requestMemory, err := resource.ParseQuantity(obj.RequestMemory)
	if err != nil {
		return err
	}

	app := v1.Container{
		Name:            "app",
		Image:           obj.Image,
		ImagePullPolicy: imagePullPolicy(obj.Image),
		Env:             env,
		Command:         obj.Command,
		Args:            obj.Args,
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":               requestCPU,
				"memory":            requestMemory,
				"ephemeral-storage": defaultEphemeralStorage,
			},
			Limits: limits,
		},
		LivenessProbe:  livenessProbe,
		ReadinessProbe: readinessProbe,
	}
	if obj.Disk.Name != "" {
		rs.Spec.Template.Spec.Volumes = []v1.Volume{
			{
				Name: "data",
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: obj.Disk.Name,
					},
				},
			},
		}
		app.VolumeMounts = []v1.VolumeMount{
			{
				Name:      "data",
				MountPath: obj.Disk.MountPath,
				SubPath:   obj.Disk.SubPath,
			},
		}
	}
	rs.Spec.Template.Spec.Containers = []v1.Container{app}
	rs.Spec.Template.Spec.ServiceAccountName = obj.SA
	rs.Spec.Template.Spec.TerminationGracePeriodSeconds = pointer.Int64(terminationGracePeriodSeconds)
	// rs.Spec.Template.Spec.SecurityContext = securityContext()

	_, err = s.Create(ctx, rs, metav1.CreateOptions{})
	return err
}

func (c *Client) GetReplicaSet(ctx context.Context, id string) (*appsv1.ReplicaSet, error) {
	list, err := c.client.AppsV1().ReplicaSets(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("id=%s", id),
	})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

func (c *Client) DeleteReplicaSet(ctx context.Context, idWithRevision string) error {
	err := c.client.AppsV1().ReplicaSets(c.namespace).Delete(ctx, idWithRevision, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *Client) WaitReplicaSetReady(ctx context.Context, id string, revision int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}

		list, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("id=%s,revision=%d", id, revision),
		})
		if err != nil {
			return err
		}
		if len(list.Items) == 0 {
			continue
		}

		ready := true
		for _, pod := range list.Items {
			if len(pod.Status.ContainerStatuses) == 0 {
				ready = false
			}
			ready = ready && pod.Status.ContainerStatuses[0].Ready
		}

		if ready {
			break
		}
	}
	return nil
}

func (c *Client) GetReplicaSetPodIP(ctx context.Context, id string, revision int64) (string, error) {
	list, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("id=%s,revision=%d", id, revision),
	})
	if err != nil {
		return "", err
	}
	if len(list.Items) == 0 {
		return "", nil
	}

	return list.Items[0].Status.PodIP, nil
}

func (obj *ReplicaSet) displayName() string {
	if obj.DisplayName != "" {
		return obj.DisplayName
	}
	return obj.Name
}
