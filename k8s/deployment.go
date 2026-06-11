package k8s

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deploys-app/api"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

const (
	h2cpImage                     = "gcr.io/moonrhythm-containers/h2cp@sha256:d999aac31aec82224cb9e432a9ccc3e212f744ab644d6cc277c43dceed4cca16"
	terminationGracePeriodSeconds = 20
	nonSpotReplicaThreshold       = 1
)

type PoolConfig struct {
	Name  string
	Share bool
}

func (c *Client) GetDeployment(ctx context.Context, name string) (*appsv1.Deployment, error) {
	return c.client.AppsV1().Deployments(c.namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *Client) GetDeploymentsForProject(ctx context.Context, projectID string) ([]appsv1.Deployment, error) {
	res, err := c.client.AppsV1().Deployments(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "projectId=" + projectID,
	})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

func (c *Client) DeleteDeployment(ctx context.Context, name string) error {
	err := c.client.AppsV1().Deployments(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

type Deployment struct {
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
	RuntimeClass  string
	Pool          PoolConfig
	BindConfigMap map[string]string // key => file path
	H2CP          bool
	Protocol      string
	Sidecars      []*api.SidecarConfig
	ForceSpot     bool
	HealthCheck   api.DeploymentHealthCheck
}

func (c *Client) CreateDeployment(ctx context.Context, obj Deployment) error {
	s := c.client.AppsV1().Deployments(c.namespace)

	deploy, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	label := map[string]string{
		"id":        obj.ID,
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
		startupProbe   *v1.Probe
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
			FailureThreshold:    5,
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
		if obj.HealthCheck.HTTPGet != nil {
			readinessProbe.ProbeHandler.TCPSocket = nil
			readinessProbe.ProbeHandler.HTTPGet = &v1.HTTPGetAction{
				Path: obj.HealthCheck.HTTPGet.Path,
			}
		}

		startupProbe = &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.FromInt(obj.ExposePort),
					Host: "",
				},
			},
			InitialDelaySeconds: 0,
			TimeoutSeconds:      2,
			PeriodSeconds:       5,
			SuccessThreshold:    1,
			FailureThreshold:    30,
		}
	}

	limits := v1.ResourceList{
		"ephemeral-storage": resource.MustParse("10Gi"),
	}
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

	if deploy == nil {
		deploy = &appsv1.Deployment{}
	}
	deploy.ObjectMeta.Name = obj.ID
	deploy.ObjectMeta.Labels = label
	deploy.ObjectMeta.Annotations = annotations
	deploy.Spec = appsv1.DeploymentSpec{} // reset
	deploy.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: label,
	}
	deploy.Spec.Strategy = appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxSurge:       intstrIntPtr(1),
			MaxUnavailable: intstrIntPtr(0),
		},
	}

	// stateful single replica
	if obj.Disk.Name != "" && obj.Replicas <= 1 {
		deploy.Spec.Strategy = appsv1.DeploymentStrategy{
			Type: appsv1.RecreateDeploymentStrategyType,
		}
	}

	deploy.Spec.Replicas = pointer.Int32(int32(obj.Replicas))
	deploy.Spec.RevisionHistoryLimit = pointer.Int32(1)
	deploy.Spec.Template.ObjectMeta = metav1.ObjectMeta{
		Labels:      label,
		Annotations: obj.Annotations,
	}
	deploy.Spec.Template.Spec.AutomountServiceAccountToken = pointer.Bool(false)

	if obj.PullSecret != "" {
		deploy.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{Name: obj.PullSecret},
		}
	}

	deploy.Spec.Template.Spec.Affinity = &v1.Affinity{}

	// try to spread stateless workload to difference node
	if obj.Disk.Name == "" {
		deploy.Spec.Template.Spec.Affinity.PodAntiAffinity = &v1.PodAntiAffinity{
			// RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
			// 	{
			// 		TopologyKey: "kubernetes.io/hostname",
			// 		LabelSelector: &metav1.LabelSelector{
			// 			MatchLabels: label,
			// 		},
			// 	},
			// },
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

	isReservedMemory := obj.RequestMemory != "0"
	switch {
	case isReservedMemory && obj.Replicas <= nonSpotReplicaThreshold:
		deploy.Spec.Template.Spec.Affinity.NodeAffinity = nonSpotNodeAffinity()
	case isReservedMemory:
		deploy.Spec.Template.Spec.Affinity.NodeAffinity = preferNonSpotNodeAffinity()
	default:
		deploy.Spec.Template.Spec.Affinity.NodeAffinity = defaultSpotNodeAffinity()
	}

	if obj.ForceSpot {
		deploy.Spec.Template.Spec.Affinity.NodeAffinity = preferSpotNodeAffinity()
	}

	if obj.Pool.Name != "" {
		// dedicate pool

		deploy.Spec.Template.Spec.Tolerations = append(deploy.Spec.Template.Spec.Tolerations, v1.Toleration{
			Key:      "pool",
			Operator: v1.TolerationOpEqual,
			Value:    obj.Pool.Name,
			Effect:   v1.TaintEffectNoSchedule,
		})

		// rev share select
		// T   T     T
		// T   F     T
		// F   T     F
		// F   F     T
		// rev || !share === select
		if isReservedMemory || !obj.Pool.Share {
			deploy.Spec.Template.Spec.NodeSelector = map[string]string{
				"pool": obj.Pool.Name,
			}
		}
	}

	if obj.RuntimeClass != "" {
		deploy.Spec.Template.Spec.RuntimeClassName = &obj.RuntimeClass
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
		StartupProbe:   startupProbe,
	}

	deploy.Spec.Template.Spec.Volumes = append(deploy.Spec.Template.Spec.Volumes, v1.Volume{
		Name: "config",
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: obj.ID,
				},
			},
		},
	})
	for key, path := range obj.BindConfigMap {
		if strings.HasPrefix(path, "/sidecar") {
			continue
		}
		app.VolumeMounts = append(app.VolumeMounts, v1.VolumeMount{
			Name:      "config",
			MountPath: path,
			SubPath:   key,
		})
	}
	if obj.Disk.Name != "" {
		deploy.Spec.Template.Spec.Volumes = append(deploy.Spec.Template.Spec.Volumes, v1.Volume{
			Name: "data",
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: obj.Disk.Name,
				},
			},
		})
		app.VolumeMounts = append(app.VolumeMounts, v1.VolumeMount{
			Name:      "data",
			MountPath: obj.Disk.MountPath,
			SubPath:   obj.Disk.SubPath,
		})
		deploy.Spec.Template.Spec.SecurityContext = securityContext()
	}
	deploy.Spec.Template.Spec.Containers = []v1.Container{app}
	deploy.Spec.Template.Spec.ServiceAccountName = obj.SA
	deploy.Spec.Template.Spec.TerminationGracePeriodSeconds = pointer.Int64(terminationGracePeriodSeconds)
	// deploy.Spec.Template.Spec.SecurityContext = securityContext()

	if obj.H2CP {
		target := "http"
		if obj.Protocol == "https" {
			target = "https"
		}
		target += "://127.0.0.1:" + strconv.Itoa(obj.ExposePort)
		args := []string{
			"-addr=:1",
			"-target=" + target,
		}

		deploy.Spec.Template.Spec.Containers = append(deploy.Spec.Template.Spec.Containers, v1.Container{
			Name:            "h2cp",
			Image:           h2cpImage,
			ImagePullPolicy: v1.PullIfNotPresent,
			Args:            args,
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": resource.MustParse("0.001"),
				},
			},
		})
	}

	for _, s := range obj.Sidecars {
		container := v1.Container{
			Name:            s.Name,
			Image:           s.Image,
			ImagePullPolicy: v1.PullIfNotPresent,
			Env:             Env(s.Env).envVars(),
			Command:         s.Command,
			Args:            s.Args,
			Ports: []v1.ContainerPort{
				{
					ContainerPort: int32(*s.Port),
				},
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": resource.MustParse("0.001"),
				},
			},
		}
		for key, path := range obj.BindConfigMap {
			if !strings.HasPrefix(path, "/sidecar") {
				continue
			}
			container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
				Name:      "config",
				MountPath: path,
				SubPath:   key,
			})
		}
		deploy.Spec.Template.Spec.Containers = append(deploy.Spec.Template.Spec.Containers, container)
	}

	_, err = s.Update(ctx, deploy, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, deploy, metav1.CreateOptions{})
	}
	return err
}

func (obj *Deployment) displayName() string {
	if obj.DisplayName != "" {
		return obj.DisplayName
	}
	return obj.Name
}
