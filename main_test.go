package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/deploys-app/api"

	"github.com/deploys-app/deployer/k8s"
)

// TestStaticSitePrefix asserts the release prefix the static-gateway serves a
// Static deployment from — used verbatim as the Ingress upstream-path (with a
// leading "/" added by the caller in deploymentDeploy). The apiserver sends
// Spec.SitePrefix = `<project>/<name>/<release-sha>`; the fallback derives the
// same from Spec.Site = `site://<bucket>/<project>/<name>@<release-sha>`.
//
// NOTE: deploymentDeploy/reconcile itself is not unit-testable here — the k8s
// Client wraps a concrete *kubernetes.Clientset (not kubernetes.Interface) and
// Worker.Client is *k8s.Client, so there is no fake-clientset seam without an
// out-of-scope client refactor. This test covers the one pure, load-bearing
// piece of the Static branch: how the upstream-path is derived.
func TestStaticSitePrefix(t *testing.T) {
	t.Parallel()

	const release = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	cases := []struct {
		name string
		spec api.DeployerCommandDeploymentDeploySpec
		want string
	}{
		{
			name: "prefers SitePrefix",
			spec: api.DeployerCommandDeploymentDeploySpec{
				SitePrefix: "deploys/website/" + release,
				Site:       "site://deploysapp-sites-x/deploys/website@" + release,
			},
			want: "deploys/website/" + release,
		},
		{
			name: "trims surrounding slashes on SitePrefix",
			spec: api.DeployerCommandDeploymentDeploySpec{
				SitePrefix: "/deploys/website/" + release + "/",
			},
			want: "deploys/website/" + release,
		},
		{
			name: "falls back to parsing Site ref",
			spec: api.DeployerCommandDeploymentDeploySpec{
				Site: "site://deploysapp-sites-x/deploys/docs@" + release,
			},
			want: "deploys/docs/" + release,
		},
		{
			name: "empty when neither set",
			spec: api.DeployerCommandDeploymentDeploySpec{},
			want: "",
		},
		{
			name: "empty when Site ref has no release",
			spec: api.DeployerCommandDeploymentDeploySpec{
				Site: "site://deploysapp-sites-x/deploys/docs",
			},
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			it := &api.DeployerCommandDeploymentDeploy{Spec: tc.spec}
			got := staticSitePrefix(it)
			if got != tc.want {
				t.Fatalf("staticSitePrefix() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReleaseHost asserts the immutable per-release URL host label (B2). The
// first four cases MUST match the apiserver's TestReleaseHost byte-for-byte
// (server/deployment_test.go): the apiserver reports the per-release URL and the
// deployer creates the matching pinned-ingress host, so the two helpers must
// agree exactly. The last case covers the deployer-only legacy fallback (empty
// DisplayName → resource name).
func TestReleaseHost(t *testing.T) {
	t.Parallel()

	const projectID = 486418960667672577 // 18 digits
	const sha = "abcdef0123456789abcdef0123456789"
	n35 := strings.Repeat("a", 35)
	n36 := strings.Repeat("a", 36)

	cases := []struct {
		name        string
		displayName string
		kubeName    string
		releaseSHA  string
		want        string
	}{
		{"short display name", "myapp", "d123", sha, "myapp-abcdef01-486418960667672577"},
		{"35-char name still fits (63)", n35, "d123", sha, n35 + "-abcdef01-486418960667672577"},
		{"36-char name overflows to id host", n36, "d123", sha, "d123-abcdef01-486418960667672577"},
		{"sha shorter than 8 used verbatim", "myapp", "d123", "abc", "myapp-abc-486418960667672577"},
		{"empty display name falls back to name", "", "d123", sha, "d123-abcdef01-486418960667672577"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := releaseHost(tc.displayName, tc.kubeName, projectID, tc.releaseSHA)
			if got != tc.want {
				t.Fatalf("releaseHost(%q, %q, %d, %q) = %q, want %q",
					tc.displayName, tc.kubeName, projectID, tc.releaseSHA, got, tc.want)
			}
		})
	}
}

// TestAccessForwardAuth covers the pure helper that synthesizes the forward-auth
// config gating a host with Deployment Access — used for a deployment's PUBLIC
// default-URL ingress and for custom-domain routes targeting it. The
// reconcile/CreateIngress path is not unit-testable here (no fake clientset seam
// — see TestStaticSitePrefix), so this is the unit under test: nil when access
// is off/absent, correct Target/headers when on.
func TestAccessForwardAuth(t *testing.T) {
	t.Parallel()

	w := &Worker{AccessVerifyURL: "https://access.deploys.app/verify"}

	t.Run("nil when access absent", func(t *testing.T) {
		t.Parallel()
		if got := w.accessForwardAuth(42, nil); got != nil {
			t.Fatalf("accessForwardAuth() = %+v, want nil", got)
		}
	})

	t.Run("nil when require login off", func(t *testing.T) {
		t.Parallel()
		access := &api.DeploymentAccessConfig{
			RequireGoogleLogin: false,
			AllowedDomains:     []string{"acme.com"},
		}
		if got := w.accessForwardAuth(42, access); got != nil {
			t.Fatalf("accessForwardAuth() = %+v, want nil", got)
		}
	})

	t.Run("forward-auth when require login on", func(t *testing.T) {
		t.Parallel()
		access := &api.DeploymentAccessConfig{
			RequireGoogleLogin: true,
			AllowedEmails:      []string{"alice@acme.com"},
		}

		got := w.accessForwardAuth(12345, access)
		if got == nil {
			t.Fatal("accessForwardAuth() = nil, want forward-auth config")
		}
		if want := "https://access.deploys.app/verify?d=12345"; got.Target != want {
			t.Fatalf("Target = %q, want %q", got.Target, want)
		}
		if want := []string{"Cookie"}; !reflect.DeepEqual(got.AuthRequestHeaders, want) {
			t.Fatalf("AuthRequestHeaders = %v, want %v", got.AuthRequestHeaders, want)
		}
		if want := []string{"X-Auth-Email", "X-Auth-User"}; !reflect.DeepEqual(got.AuthResponseHeaders, want) {
			t.Fatalf("AuthResponseHeaders = %v, want %v", got.AuthResponseHeaders, want)
		}
	})
}

// TestSidecarsTwoCloudSQLProxy guards against the apiserver rejecting a
// Deployment that has two cloud-sql-proxy sidecars. Both sidecars share the
// container name "cloudsql-proxy" and mount their credentials at the same path
// (/sidecar/cloudsqlproxy/credentials.json), so without per-sidecar resolution
// the spec ends up with a duplicate container name and, worse, every sidecar
// mounting every sidecar's credentials file at the same path within one
// container. This verifies names are made unique and each sidecar is bound only
// to its own file.
func TestSidecarsTwoCloudSQLProxy(t *testing.T) {
	t.Parallel()

	specSidecars := []*api.Sidecar{
		{CloudSQLProxy: &api.CloudSQLProxySidecar{Instance: "proj:region:db1", Port: 3300, Credentials: "cred-1"}},
		{CloudSQLProxy: &api.CloudSQLProxySidecar{Instance: "proj:region:db2", Port: 3301, Credentials: "cred-2"}},
	}

	configs := make([]*api.SidecarConfig, len(specSidecars))
	for i, s := range specSidecars {
		configs[i] = s.Config()
	}

	configMapData, bindData, sidecarBinds := prepareMountData(nil, configs)
	sidecars := buildSidecars(configs, sidecarBinds)

	if len(sidecars) != 2 {
		t.Fatalf("len(sidecars) = %d, want 2", len(sidecars))
	}

	// Container names must be unique within the pod.
	names := map[string]bool{}
	for _, s := range sidecars {
		if names[s.Name] {
			t.Fatalf("duplicate sidecar name %q", s.Name)
		}
		names[s.Name] = true
	}
	if !names["cloudsql-proxy"] || !names["cloudsql-proxy-1"] {
		t.Fatalf("unexpected sidecar names %v", names)
	}

	// No /sidecar files should leak into the application container's binds.
	for _, path := range bindData {
		if strings.HasPrefix(path, "/sidecar") {
			t.Fatalf("application container bound to sidecar path %q", path)
		}
	}

	// Each sidecar mounts exactly its own credentials file: a single mount at
	// the shared path, pointing at a distinct config map key with this
	// sidecar's data.
	seenKeys := map[string]bool{}
	for i, s := range sidecars {
		if len(s.BindConfigMap) != 1 {
			t.Fatalf("sidecar[%d] %q has %d binds, want 1: %v", i, s.Name, len(s.BindConfigMap), s.BindConfigMap)
		}
		mountPaths := map[string]bool{}
		for key, path := range s.BindConfigMap {
			if mountPaths[path] {
				t.Fatalf("sidecar[%d] %q has duplicate mount path %q", i, s.Name, path)
			}
			mountPaths[path] = true
			if path != "/sidecar/cloudsqlproxy/credentials.json" {
				t.Fatalf("sidecar[%d] %q unexpected mount path %q", i, s.Name, path)
			}
			if seenKeys[key] {
				t.Fatalf("config map key %q shared across sidecars", key)
			}
			seenKeys[key] = true
			if want := "cred-" + map[int]string{0: "1", 1: "2"}[i]; configMapData[key] != want {
				t.Fatalf("sidecar[%d] key %q data = %q, want %q", i, key, configMapData[key], want)
			}
		}
	}

	// The k8s field type carries through unchanged.
	_ = []k8s.Sidecar(sidecars)
}
