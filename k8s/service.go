package k8s

import (
	"context"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

type Service struct {
	ID         string
	ProjectID  string
	Port       int
	Protocol   string
	ExposeNode bool
	H2CP       bool
}

func (c *Client) CreateService(ctx context.Context, obj Service) error {
	s := c.client.CoreV1().Services(c.namespace)

	svc, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	labels := map[string]string{
		"id":        obj.ID,
		"projectId": obj.ProjectID,
	}

	if svc == nil {
		svc = &v1.Service{}
	}

	if !obj.ExposeNode {
		// self-healing
		if ip := svc.Spec.ClusterIP; ip != "" && ip != "None" {
			s.Delete(ctx, obj.ID, metav1.DeleteOptions{})
			svc = &v1.Service{}
		}
	}

	svc.ObjectMeta.Name = obj.ID
	svc.ObjectMeta.Labels = labels
	svc.Spec.Selector = labels

	if !obj.ExposeNode {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "None"
		svc.Spec.Ports = []v1.ServicePort{
			{
				Name:       "http",
				Protocol:   v1.ProtocolTCP,
				Port:       int32(obj.Port),
				TargetPort: intstr.FromInt(obj.Port),
			},
		}

		if obj.Protocol != "" {
			svc.Spec.Ports[0].AppProtocol = pointer.String(obj.Protocol)
		}

		if obj.H2CP {
			svc.Spec.Ports[0].Port = 1
			svc.Spec.Ports[0].TargetPort = intstr.FromInt(1)
			svc.Spec.Ports[0].AppProtocol = pointer.String("h2c")
		}
	} else {
		svc.Spec.Type = v1.ServiceTypeNodePort
		if len(svc.Spec.Ports) == 0 {
			svc.Spec.Ports = append(svc.Spec.Ports, v1.ServicePort{})
		}
		svc.Spec.Ports[0].Protocol = v1.ProtocolTCP
		svc.Spec.Ports[0].Port = int32(obj.Port)
		svc.Spec.Ports[0].TargetPort = intstr.FromInt(obj.Port)
	}

	_, err = s.Update(ctx, svc, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, svc, metav1.CreateOptions{})
	}
	return err
}

// EnsureExternalNameService creates or updates an ExternalName Service named
// `name` in the deployer's namespace, aliasing it (via DNS CNAME) to
// externalName — an in-cluster FQDN like "static-gateway.deployer.svc.cluster.local".
//
// It exists to reach a shared backend that lives in a DIFFERENT namespace: the
// parapet controller only resolves an Ingress backend within the Ingress's own
// namespace and dials <svc>.<ns>.svc.cluster.local, so a bare backend name
// can't cross namespaces. An ExternalName Service of the same name in this
// namespace makes that dial CNAME to the real Service elsewhere. The declared
// port is what the controller dials, so it must match the target Service's port.
//
// Idempotent. It will not clobber a real (non-ExternalName) Service of the same
// name — e.g. the gateway itself running in this namespace.
func (c *Client) EnsureExternalNameService(ctx context.Context, name, externalName string, port int) error {
	s := c.client.CoreV1().Services(c.namespace)

	svc, err := s.Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}
	if svc != nil && svc.Spec.Type != "" && svc.Spec.Type != v1.ServiceTypeExternalName {
		// A real Service already owns this name; don't turn it into an alias.
		return nil
	}
	if svc == nil {
		svc = &v1.Service{}
	}

	svc.ObjectMeta.Name = name
	svc.Spec.Type = v1.ServiceTypeExternalName
	svc.Spec.ExternalName = externalName
	svc.Spec.Selector = nil
	svc.Spec.Ports = []v1.ServicePort{
		{
			Name:       "http",
			Protocol:   v1.ProtocolTCP,
			Port:       int32(port),
			TargetPort: intstr.FromInt(port),
		},
	}

	_, err = s.Update(ctx, svc, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, svc, metav1.CreateOptions{})
	}
	return err
}

func (c *Client) DeleteService(ctx context.Context, id string) error {
	err := c.client.CoreV1().Services(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *Client) GetNodePort(ctx context.Context, id string) (int, error) {
	s := c.client.CoreV1().Services(c.namespace)

	svc, err := s.Get(ctx, id, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	if len(svc.Spec.Ports) == 0 {
		return 0, nil
	}
	return int(svc.Spec.Ports[0].NodePort), nil
}

type ServiceForReplicaSet struct {
	ID         string
	Revision   int64
	ProjectID  string
	Port       int
	ExposeNode bool
}

func (c *Client) CreateServiceForReplicaSet(ctx context.Context, obj ServiceForReplicaSet) error {
	s := c.client.CoreV1().Services(c.namespace)

	svc, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	labels := map[string]string{
		"id":        obj.ID,
		"revision":  strconv.FormatInt(obj.Revision, 10),
		"projectId": obj.ProjectID,
	}

	if svc == nil {
		svc = &v1.Service{}
	}

	if !obj.ExposeNode {
		// self-healing
		if ip := svc.Spec.ClusterIP; ip != "" && ip != "None" {
			s.Delete(ctx, obj.ID, metav1.DeleteOptions{})
			svc = &v1.Service{}
		}
	}

	svc.ObjectMeta.Name = obj.ID
	svc.ObjectMeta.Labels = labels
	svc.Spec.Selector = labels

	if !obj.ExposeNode {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.Ports = []v1.ServicePort{
			{
				Name:       "app",
				Protocol:   v1.ProtocolTCP,
				Port:       int32(obj.Port),
				TargetPort: intstr.FromInt(obj.Port),
			},
		}
	} else {
		svc.Spec.Type = v1.ServiceTypeNodePort
		if len(svc.Spec.Ports) == 0 {
			svc.Spec.Ports = append(svc.Spec.Ports, v1.ServicePort{})
		}
		svc.Spec.Ports[0].Protocol = v1.ProtocolTCP
		svc.Spec.Ports[0].Port = int32(obj.Port)
		svc.Spec.Ports[0].TargetPort = intstr.FromInt(obj.Port)
	}

	_, err = s.Update(ctx, svc, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, svc, metav1.CreateOptions{})
	}
	return err
}
