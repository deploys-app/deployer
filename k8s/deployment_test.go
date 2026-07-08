package k8s

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

// TestBuildSidecarContainerHealthGate guards the redo of the reverted native-
// sidecar fix: a sidecar with a health check must render as a native sidecar
// (init container + Always restart policy) with an httpGet startup probe on the
// health port/path, so Kubernetes holds the app container until the proxy
// reports ready. The earlier attempt used a TCP probe that dialed the pod IP
// while the proxy listened only on 127.0.0.1, so it never passed and pods hung
// in Init — hence the probe must target the 0.0.0.0-bound HTTP health port.
func TestBuildSidecarContainerHealthGate(t *testing.T) {
	t.Parallel()

	port := 5432
	c := buildSidecarContainer(Sidecar{
		Name:  "cloudsql-proxy",
		Image: "proxy:latest",
		Port:  &port,
		HealthCheck: &SidecarHealthCheck{
			Port: 9090,
			Path: "/startup",
		},
		BindConfigMap: map[string]string{
			"cred-key": "/sidecar/cloudsqlproxy/credentials.json",
		},
	})

	// Always restart policy is what makes this a native sidecar.
	if c.RestartPolicy == nil || *c.RestartPolicy != v1.ContainerRestartPolicyAlways {
		t.Fatalf("RestartPolicy = %v, want Always", c.RestartPolicy)
	}

	// httpGet startup probe on the health port/path, capped at ~60s.
	sp := c.StartupProbe
	if sp == nil || sp.HTTPGet == nil {
		t.Fatalf("StartupProbe HTTPGet missing: %+v", sp)
	}
	if got := sp.HTTPGet.Port.IntValue(); got != 9090 {
		t.Errorf("startup probe port = %d, want 9090", got)
	}
	if sp.HTTPGet.Path != "/startup" {
		t.Errorf("startup probe path = %q, want /startup", sp.HTTPGet.Path)
	}
	if sp.PeriodSeconds != 1 || sp.FailureThreshold != 60 {
		t.Errorf("startup probe cap = %ds*%d, want 1s*60", sp.PeriodSeconds, sp.FailureThreshold)
	}

	// The health port is declared alongside the DB port, both unnamed so two
	// sidecars can't collide on a port name.
	var haveDB, haveHealth bool
	for _, p := range c.Ports {
		if p.Name != "" {
			t.Errorf("port %+v should be unnamed", p)
		}
		if p.ContainerPort == int32(port) {
			haveDB = true
		}
		if p.ContainerPort == 9090 {
			haveHealth = true
		}
	}
	if !haveDB || !haveHealth {
		t.Errorf("ports = %+v, want both DB %d and health 9090", c.Ports, port)
	}

	// Config-map credentials still mount into the sidecar.
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/sidecar/cloudsqlproxy/credentials.json" {
		t.Errorf("VolumeMounts = %+v, want single credentials mount", c.VolumeMounts)
	}
}

// TestBuildSidecarContainerNoHealthCheck verifies a sidecar without a health
// check still becomes a native sidecar (ordering) but gets no startup probe, so
// it can never wedge the pod in Init waiting on an unsatisfiable probe.
func TestBuildSidecarContainerNoHealthCheck(t *testing.T) {
	t.Parallel()

	port := 6379
	c := buildSidecarContainer(Sidecar{
		Name:  "cache",
		Image: "redis:latest",
		Port:  &port,
	})

	if c.RestartPolicy == nil || *c.RestartPolicy != v1.ContainerRestartPolicyAlways {
		t.Fatalf("RestartPolicy = %v, want Always", c.RestartPolicy)
	}
	if c.StartupProbe != nil {
		t.Errorf("StartupProbe = %+v, want nil without a health check", c.StartupProbe)
	}
}
