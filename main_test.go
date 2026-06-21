package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/deploys-app/api"
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
