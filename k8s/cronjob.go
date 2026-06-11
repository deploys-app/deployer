package k8s

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deploys-app/api"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

type CronJob struct {
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
	Schedule      string
	RequestCPU    string
	RequestMemory string
	LimitCPU      string
	LimitMemory   string
	PullSecret    string
	Disk          Disk
	RuntimeClass  string
	Pool          PoolConfig
	BindConfigMap map[string]string // key => file path
	Sidecars      []*api.SidecarConfig
}

func (c *Client) CreateCronJob(ctx context.Context, obj CronJob) error {
	s := c.client.BatchV1().CronJobs(c.namespace)

	cj, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
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

	if obj.Env == nil {
		obj.Env = make(Env)
	}
	obj.Env["K_SERVICE"] = obj.displayName()
	obj.Env["K_REVISION"] = fmt.Sprintf("%s.%d", obj.displayName(), obj.Revision)
	obj.Env["K_CONFIGURATION"] = obj.displayName()

	limits := v1.ResourceList{
		"ephemeral-storage": resource.MustParse("30Gi"),
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

	if cj == nil {
		cj = &batchv1.CronJob{}
	}
	cj.ObjectMeta.Name = obj.ID
	cj.ObjectMeta.Labels = label
	cj.ObjectMeta.Annotations = annotations
	cj.Spec = batchv1.CronJobSpec{} // reset
	cj.Spec.Schedule = obj.Schedule
	cj.Spec.StartingDeadlineSeconds = pointer.Int64(600)
	cj.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
	cj.Spec.SuccessfulJobsHistoryLimit = pointer.Int32(1)
	cj.Spec.FailedJobsHistoryLimit = pointer.Int32(1)
	cj.Spec.JobTemplate.ObjectMeta = metav1.ObjectMeta{
		Labels: label,
	}
	cj.Spec.JobTemplate.Spec.ActiveDeadlineSeconds = pointer.Int64(int64(12 * time.Hour / time.Second))
	cj.Spec.JobTemplate.Spec.TTLSecondsAfterFinished = pointer.Int32(int32(24 * time.Hour / time.Second))
	cj.Spec.JobTemplate.Spec.Template.ObjectMeta = metav1.ObjectMeta{
		Labels: label,
	}
	cj.Spec.JobTemplate.Spec.Template.Spec.AutomountServiceAccountToken = pointer.Bool(false)

	cj.Spec.JobTemplate.Spec.Template.Spec.Affinity = &v1.Affinity{
		NodeAffinity: preferSpotNodeAffinity(),
	}
	if obj.Pool.Name != "" {
		// dedicate pool
		cj.Spec.JobTemplate.Spec.Template.Spec.Tolerations = append(cj.Spec.JobTemplate.Spec.Template.Spec.Tolerations, v1.Toleration{
			Key:      "pool",
			Operator: v1.TolerationOpEqual,
			Value:    obj.Pool.Name,
			Effect:   v1.TaintEffectNoSchedule,
		})

		if !obj.Pool.Share {
			cj.Spec.JobTemplate.Spec.Template.Spec.NodeSelector = map[string]string{
				"pool": obj.Pool.Name,
			}
		}
	}

	if obj.RuntimeClass != "" {
		cj.Spec.JobTemplate.Spec.Template.Spec.RuntimeClassName = &obj.RuntimeClass
	}

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
		Env:             obj.Env.envVars(),
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
	}

	cj.Spec.JobTemplate.Spec.Template.Spec.Volumes = append(cj.Spec.JobTemplate.Spec.Template.Spec.Volumes, v1.Volume{
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
		cj.Spec.JobTemplate.Spec.Template.Spec.Volumes = append(cj.Spec.JobTemplate.Spec.Template.Spec.Volumes, v1.Volume{
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
	}
	cj.Spec.JobTemplate.Spec.Template.Spec.Containers = []v1.Container{app}

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
		cj.Spec.JobTemplate.Spec.Template.Spec.Containers = append(cj.Spec.JobTemplate.Spec.Template.Spec.Containers, container)
	}

	cj.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName = obj.SA
	if obj.PullSecret != "" {
		cj.Spec.JobTemplate.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{Name: obj.PullSecret},
		}
	}
	cj.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = v1.RestartPolicyOnFailure
	cj.Spec.JobTemplate.Spec.Template.Spec.TerminationGracePeriodSeconds = pointer.Int64(terminationGracePeriodSeconds)
	// cj.Spec.JobTemplate.Spec.Template.Spec.SecurityContext = securityContext()

	_, err = s.Update(ctx, cj, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, cj, metav1.CreateOptions{})
	}
	return err
}

func (c *Client) DeleteCronJob(ctx context.Context, id string) error {
	err := c.client.BatchV1().CronJobs(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (obj *CronJob) displayName() string {
	if obj.DisplayName != "" {
		return obj.DisplayName
	}
	return obj.Name
}
