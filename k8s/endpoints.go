package k8s

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ExternalUpstream is a route backend that points at a customer-owned server by
// IP rather than an in-cluster deployment ("bring your own server" + our WAF).
//
// It is materialized as a selector-less headless Service plus a hand-managed
// Endpoints object of the same name. The parapet ingress controller then
// load-balances to the external IP exactly as it does to a deployment's pods,
// and per-backend bandwidth metering (parapet_backend_network_*{service_name})
// attributes to the project for free because the name ends in -<projectID>.
//
// We use the classic core/v1 Endpoints object (not discovery/v1 EndpointSlice)
// because parapet does not consume EndpointSlice yet.
type ExternalUpstream struct {
	ID        string // Service + Endpoints name, e.g. ext-<routeID>-<projectID>
	ProjectID string
	IP        string
	Port      int
}

func (c *Client) CreateExternalUpstream(ctx context.Context, obj ExternalUpstream) error {
	if err := c.upsertExternalService(ctx, obj); err != nil {
		return err
	}
	return c.upsertExternalEndpoints(ctx, obj)
}

func (c *Client) upsertExternalService(ctx context.Context, obj ExternalUpstream) error {
	s := c.client.CoreV1().Services(c.namespace)

	labels := map[string]string{
		"id":        obj.ID,
		"projectId": obj.ProjectID,
	}

	svc, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		svc, err = nil, nil
	}
	if err != nil {
		return err
	}
	if svc == nil {
		svc = &v1.Service{}
	}

	svc.ObjectMeta.Name = obj.ID
	svc.ObjectMeta.Labels = labels
	// Selector-less + headless: we own the Endpoints object, so kube-proxy must
	// not try to manage endpoints for this Service.
	svc.Spec.Selector = nil
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

	_, err = s.Update(ctx, svc, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, svc, metav1.CreateOptions{})
	}
	return err
}

func (c *Client) upsertExternalEndpoints(ctx context.Context, obj ExternalUpstream) error {
	e := c.client.CoreV1().Endpoints(c.namespace)

	labels := map[string]string{
		"id":        obj.ID,
		"projectId": obj.ProjectID,
	}

	ep, err := e.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		ep, err = nil, nil
	}
	if err != nil {
		return err
	}
	if ep == nil {
		ep = &v1.Endpoints{}
	}

	ep.ObjectMeta.Name = obj.ID
	ep.ObjectMeta.Labels = labels
	// The port name ("http") must match the Service port name so parapet binds
	// them. Addresses (not NotReadyAddresses) marks the endpoint ready.
	ep.Subsets = []v1.EndpointSubset{
		{
			Addresses: []v1.EndpointAddress{
				{IP: obj.IP},
			},
			Ports: []v1.EndpointPort{
				{
					Name:     "http",
					Port:     int32(obj.Port),
					Protocol: v1.ProtocolTCP,
				},
			},
		},
	}

	_, err = e.Update(ctx, ep, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = e.Create(ctx, ep, metav1.CreateOptions{})
	}
	return err
}

// DeleteExternalUpstream removes the Service and Endpoints created by
// CreateExternalUpstream. Both deletes are idempotent (a missing object is not
// an error), so it is safe to call for any route on teardown — non-external
// routes simply have nothing to delete.
func (c *Client) DeleteExternalUpstream(ctx context.Context, id string) error {
	errEp := c.client.CoreV1().Endpoints(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(errEp) {
		errEp = nil
	}
	errSvc := c.client.CoreV1().Services(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(errSvc) {
		errSvc = nil
	}
	if errEp != nil {
		return errEp
	}
	return errSvc
}
