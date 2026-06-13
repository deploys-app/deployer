package main

import (
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
