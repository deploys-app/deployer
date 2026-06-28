package k8s

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

// TestImagePullPolicy pins the pull-policy heuristic used for every workload:
// an image pinned to an immutable sha256 digest is pulled only if absent
// (PullIfNotPresent); anything else (a mutable tag, or no tag at all) is always
// pulled so a redeploy of the same tag picks up a new image.
func TestImagePullPolicy(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  v1.PullPolicy
	}{
		{"digest pinned", "nginx@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", v1.PullIfNotPresent},
		{"registry + digest", "registry.deploys.app/acme/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", v1.PullIfNotPresent},
		{"tag + digest", "nginx:1.27@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", v1.PullIfNotPresent},
		{"tag only", "nginx:1.27", v1.PullAlways},
		{"latest tag", "registry.deploys.app/acme/web:latest", v1.PullAlways},
		{"no tag", "nginx", v1.PullAlways},
		{"empty", "", v1.PullAlways},
		// Only sha256 digests are recognised; another algorithm falls through to
		// PullAlways (current behaviour — sha256 is the de-facto standard).
		{"sha512 digest not recognised", "nginx@sha512:abc", v1.PullAlways},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := imagePullPolicy(tc.image); got != tc.want {
				t.Errorf("imagePullPolicy(%q) = %v, want %v", tc.image, got, tc.want)
			}
		})
	}
}
