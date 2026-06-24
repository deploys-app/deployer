package k8s

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

func TestBuildExternalEndpointSlice(t *testing.T) {
	t.Parallel()

	obj := ExternalUpstream{
		ID:        "ext-7-42",
		ProjectID: "42",
		IP:        "203.0.113.5",
		Port:      8080,
	}
	es := buildExternalEndpointSlice(obj)

	if es.Name != "ext-7-42" {
		t.Errorf("name = %q, want ext-7-42", es.Name)
	}

	// The slice must be associated with the same-named Service and marked as
	// ours, and keep the id/projectId labels used for metering attribution.
	wantLabels := map[string]string{
		"id":                         "ext-7-42",
		"projectId":                  "42",
		discoveryv1.LabelServiceName: "ext-7-42",
		discoveryv1.LabelManagedBy:   managedByExternalUpstream,
	}
	for k, want := range wantLabels {
		if got := es.Labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}

	if es.AddressType != discoveryv1.AddressTypeIPv4 {
		t.Errorf("addressType = %q, want IPv4", es.AddressType)
	}

	if len(es.Endpoints) != 1 {
		t.Fatalf("endpoints = %d, want 1", len(es.Endpoints))
	}
	ep := es.Endpoints[0]
	if len(ep.Addresses) != 1 || ep.Addresses[0] != obj.IP {
		t.Errorf("addresses = %v, want [%s]", ep.Addresses, obj.IP)
	}
	if ep.Conditions.Ready == nil || !*ep.Conditions.Ready {
		t.Errorf("ready = %v, want true", ep.Conditions.Ready)
	}
	if ep.Conditions.Serving == nil || !*ep.Conditions.Serving {
		t.Errorf("serving = %v, want true", ep.Conditions.Serving)
	}
	if ep.Conditions.Terminating == nil || *ep.Conditions.Terminating {
		t.Errorf("terminating = %v, want false", ep.Conditions.Terminating)
	}

	if len(es.Ports) != 1 {
		t.Fatalf("ports = %d, want 1", len(es.Ports))
	}
	port := es.Ports[0]
	if port.Name == nil || *port.Name != "http" {
		t.Errorf("port name = %v, want http", port.Name)
	}
	if port.Port == nil || *port.Port != int32(obj.Port) {
		t.Errorf("port = %v, want %d", port.Port, obj.Port)
	}
	if port.Protocol == nil || *port.Protocol != v1.ProtocolTCP {
		t.Errorf("protocol = %v, want TCP", port.Protocol)
	}
}

func TestBuildExternalEndpointSliceIPv6(t *testing.T) {
	t.Parallel()

	es := buildExternalEndpointSlice(ExternalUpstream{
		ID:        "ext-9-1",
		ProjectID: "1",
		IP:        "2001:db8::1",
		Port:      443,
	})
	if es.AddressType != discoveryv1.AddressTypeIPv6 {
		t.Errorf("addressType = %q, want IPv6", es.AddressType)
	}
	if es.Endpoints[0].Addresses[0] != "2001:db8::1" {
		t.Errorf("address = %q, want 2001:db8::1", es.Endpoints[0].Addresses[0])
	}
}

// EndpointSlice validation requires the address string to be the canonical form
// of its declared AddressType. Non-canonical IPv6 must be lowered, and an
// IPv4-mapped address declared IPv4 must render as a plain IPv4 string.
func TestBuildExternalEndpointSliceCanonicalizesAddress(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ip       string
		wantType discoveryv1.AddressType
		wantAddr string
	}{
		{"2001:DB8::1", discoveryv1.AddressTypeIPv6, "2001:db8::1"},
		{"::ffff:203.0.113.5", discoveryv1.AddressTypeIPv4, "203.0.113.5"},
	}
	for _, tc := range cases {
		es := buildExternalEndpointSlice(ExternalUpstream{ID: "ext-1-1", ProjectID: "1", IP: tc.ip, Port: 80})
		if es.AddressType != tc.wantType {
			t.Errorf("ip %q: addressType = %q, want %q", tc.ip, es.AddressType, tc.wantType)
		}
		if got := es.Endpoints[0].Addresses[0]; got != tc.wantAddr {
			t.Errorf("ip %q: address = %q, want %q", tc.ip, got, tc.wantAddr)
		}
	}
}

func TestExternalAddressType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ip   string
		want discoveryv1.AddressType
	}{
		{"203.0.113.5", discoveryv1.AddressTypeIPv4},
		{"10.0.0.1", discoveryv1.AddressTypeIPv4},
		{"2001:db8::1", discoveryv1.AddressTypeIPv6},
		{"::1", discoveryv1.AddressTypeIPv6},
		{"fe80::1", discoveryv1.AddressTypeIPv6},
		// IPv4-mapped IPv6 still resolves to a 4-byte address → IPv4.
		{"::ffff:203.0.113.5", discoveryv1.AddressTypeIPv4},
		// Defensive: a non-IP falls back to IPv4 rather than panicking.
		{"not-an-ip", discoveryv1.AddressTypeIPv4},
		{"", discoveryv1.AddressTypeIPv4},
	}
	for _, tc := range cases {
		if got := externalAddressType(tc.ip); got != tc.want {
			t.Errorf("externalAddressType(%q) = %q, want %q", tc.ip, got, tc.want)
		}
	}
}

// guard against an accidental change to the skip-mirror sentinel: the mirroring
// controller keys off this exact label/value to avoid creating a duplicate slice.
func TestSkipMirrorLabelConstant(t *testing.T) {
	t.Parallel()

	if discoveryv1.LabelSkipMirror != "endpointslice.kubernetes.io/skip-mirror" {
		t.Errorf("LabelSkipMirror = %q, want endpointslice.kubernetes.io/skip-mirror", discoveryv1.LabelSkipMirror)
	}
}
