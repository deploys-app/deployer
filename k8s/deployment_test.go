package k8s

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

// TestBuildSidecarContainerNativeOrdering guards the fix for sidecars racing the
// app container: a sidecar (e.g. cloud-sql-proxy) must render as a native
// sidecar — an init container with an Always restart policy and a TCP startup
// probe on its port — so Kubernetes holds the app container until the sidecar's
// port accepts connections. Without this the app dials localhost before the
// proxy listens and crash-loops.
func TestBuildSidecarContainerNativeOrdering(t *testing.T) {
	t.Parallel()

	port := 5432
	c := buildSidecarContainer(Sidecar{
		Name:  "cloudsql-proxy",
		Image: "proxy:latest",
		Port:  &port,
		BindConfigMap: map[string]string{
			"cred-key": "/sidecar/cloudsqlproxy/credentials.json",
		},
	})

	// Always restart policy is what makes this a native sidecar.
	if c.RestartPolicy == nil || *c.RestartPolicy != v1.ContainerRestartPolicyAlways {
		t.Fatalf("RestartPolicy = %v, want Always", c.RestartPolicy)
	}

	// Startup probe must gate the app on the sidecar's port, capped at ~60s.
	sp := c.StartupProbe
	if sp == nil || sp.TCPSocket == nil {
		t.Fatalf("StartupProbe TCPSocket missing: %+v", sp)
	}
	if got := sp.TCPSocket.Port.IntValue(); got != port {
		t.Errorf("startup probe port = %d, want %d", got, port)
	}
	if sp.PeriodSeconds != 1 || sp.FailureThreshold != 60 {
		t.Errorf("startup probe cap = %ds*%d, want 1s*60", sp.PeriodSeconds, sp.FailureThreshold)
	}

	// Config-map credentials still mount into the sidecar.
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/sidecar/cloudsqlproxy/credentials.json" {
		t.Errorf("VolumeMounts = %+v, want single credentials mount", c.VolumeMounts)
	}
}
