package k8s

import (
	"context"
	"net"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// managedByExternalUpstream is the endpointslice.kubernetes.io/managed-by value
// we stamp on the EndpointSlice we hand-manage, so it's clear the slice is owned
// by the deployer and not by a core controller.
const managedByExternalUpstream = "deployer"

// ExternalUpstream is a route backend that points at a customer-owned server by
// IP rather than an in-cluster deployment ("bring your own server" + our WAF).
//
// It is materialized as a selector-less headless Service plus a hand-managed
// core/v1 Endpoints object AND a matching discovery/v1 EndpointSlice, all of the
// same name. The parapet ingress controller then load-balances to the external
// IP exactly as it does to a deployment's pods, and per-backend bandwidth
// metering (parapet_backend_network_*{service_name}) attributes to the project
// for free because the name ends in -<projectID>.
//
// We publish both endpoint shapes: the classic core/v1 Endpoints (what parapet
// reads today) and an explicit discovery/v1 EndpointSlice (its modern,
// non-deprecated replacement), so a consumer on either API resolves the same
// backend. The Endpoints object carries the
// endpointslice.kubernetes.io/skip-mirror=true label so the EndpointSlice
// mirroring controller does not ALSO mirror it into a second, duplicate slice —
// we own the slice ourselves.
type ExternalUpstream struct {
	ID        string // Service + Endpoints + EndpointSlice name, e.g. ext-<routeID>-<projectID>
	ProjectID string
	IP        string
	Port      int
}

func (c *Client) CreateExternalUpstream(ctx context.Context, obj ExternalUpstream) error {
	if err := c.upsertExternalService(ctx, obj); err != nil {
		return err
	}
	if err := c.upsertExternalEndpoints(ctx, obj); err != nil {
		return err
	}
	return c.upsertExternalEndpointSlice(ctx, obj)
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
	// Selector-less + headless: we own the Endpoints/EndpointSlice objects, so
	// kube-proxy must not try to manage endpoints for this Service.
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
		// We publish our own EndpointSlice (see upsertExternalEndpointSlice), so
		// tell the EndpointSlice mirroring controller to leave this Endpoints
		// alone — otherwise it would mirror it into a second, duplicate slice.
		discoveryv1.LabelSkipMirror: "true",
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

func (c *Client) upsertExternalEndpointSlice(ctx context.Context, obj ExternalUpstream) error {
	e := c.client.DiscoveryV1().EndpointSlices(c.namespace)

	desired := buildExternalEndpointSlice(obj)

	es, err := e.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		es, err = nil, nil
	}
	if err != nil {
		return err
	}
	// AddressType is immutable. If the upstream switched IP family (e.g. the
	// route target changed from an IPv4 to an IPv6 literal), the existing slice
	// can't be updated in place — delete it so it's recreated below.
	if es != nil && es.AddressType != desired.AddressType {
		if err := e.Delete(ctx, obj.ID, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			return err
		}
		es = nil
	}
	if es == nil {
		es = &discoveryv1.EndpointSlice{}
	}

	es.ObjectMeta.Name = desired.ObjectMeta.Name
	es.ObjectMeta.Labels = desired.ObjectMeta.Labels
	es.AddressType = desired.AddressType
	es.Endpoints = desired.Endpoints
	es.Ports = desired.Ports

	_, err = e.Update(ctx, es, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = e.Create(ctx, es, metav1.CreateOptions{})
	}
	return err
}

// buildExternalEndpointSlice renders the discovery/v1 EndpointSlice that mirrors
// the hand-managed Endpoints for an external upstream. It is a pure function so
// the slice shape (labels, address type, ready conditions, port) can be unit
// tested without a cluster.
func buildExternalEndpointSlice(obj ExternalUpstream) *discoveryv1.EndpointSlice {
	// EndpointSlice validation is stricter than Endpoints: the address string
	// must be the canonical form of its AddressType (e.g. an IPv4-typed slice
	// can't carry an "::ffff:a.b.c.d" mapped form, and IPv6 must be lowercase).
	// The caller already validated the IP, so canonicalize it here.
	addr := obj.IP
	if parsed := net.ParseIP(obj.IP); parsed != nil {
		addr = parsed.String()
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: obj.ID,
			Labels: map[string]string{
				"id":        obj.ID,
				"projectId": obj.ProjectID,
				// Associates this slice with the selector-less Service of the same
				// name; consumers find a Service's slices by this label.
				discoveryv1.LabelServiceName: obj.ID,
				// We own this slice — the EndpointSlice controller only manages
				// slices for Services with a selector, which this Service is not.
				discoveryv1.LabelManagedBy: managedByExternalUpstream,
			},
		},
		AddressType: externalAddressType(obj.IP),
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{addr},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			},
		},
		// The port name ("http") must match the Service port name so parapet
		// binds them, mirroring the Endpoints object above.
		Ports: []discoveryv1.EndpointPort{
			{
				Name:     ptr.To("http"),
				Port:     ptr.To(int32(obj.Port)),
				Protocol: ptr.To(v1.ProtocolTCP),
			},
		},
	}
}

// externalAddressType reports the EndpointSlice address family for an upstream
// IP. The caller has already validated the IP (net.ParseIP), so anything that
// isn't a recognizable IPv6 address is treated as IPv4.
func externalAddressType(ip string) discoveryv1.AddressType {
	if parsed := net.ParseIP(ip); parsed != nil && parsed.To4() == nil {
		return discoveryv1.AddressTypeIPv6
	}
	return discoveryv1.AddressTypeIPv4
}

// DeleteExternalUpstream removes the Service, Endpoints, and EndpointSlice
// created by CreateExternalUpstream. Every delete is idempotent (a missing
// object is not an error), so it is safe to call for any route on teardown —
// non-external routes simply have nothing to delete.
func (c *Client) DeleteExternalUpstream(ctx context.Context, id string) error {
	errSlice := c.client.DiscoveryV1().EndpointSlices(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(errSlice) {
		errSlice = nil
	}
	errEp := c.client.CoreV1().Endpoints(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(errEp) {
		errEp = nil
	}
	errSvc := c.client.CoreV1().Services(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(errSvc) {
		errSvc = nil
	}
	if errSlice != nil {
		return errSlice
	}
	if errEp != nil {
		return errEp
	}
	return errSvc
}
