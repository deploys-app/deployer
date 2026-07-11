package k8s

import (
	"context"
	"testing"

	"github.com/deploys-app/api"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const testNamespace = "deploys"

func newTestClient(objs ...runtime.Object) *Client {
	return &Client{
		client:    fake.NewClientset(objs...),
		namespace: testNamespace,
	}
}

func testIngress(name, projectID string, annotations map[string]string) *networking.Ingress {
	return &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Labels:      map[string]string{"projectId": projectID},
			Annotations: annotations,
		},
	}
}

func testCorazaConfigMap(name, projectID string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels: map[string]string{
				"id":        name,
				"projectId": projectID,
				corazaLabel: "zone",
			},
		},
		Data: map[string]string{"crs.conf": "SecRuleEngine On\n"},
	}
}

// TestWAFZoneCorazaTransitions covers the Coraza half of
// CreateWAFZone/DeleteWAFZone against the fake clientset: materialize
// (ConfigMap upsert + annotation stamp), teardown (ConfigMap delete +
// annotation strip), and the mixed-version guard (an empty corazaZoneID must
// leave all Coraza state untouched — a regression that strips the annotation
// on pre-managed-rules commands would silently unbind live zones).
func TestWAFZoneCorazaTransitions(t *testing.T) {
	t.Parallel()

	enabled := &api.WAFManagedRules{Enabled: true}

	cases := []struct {
		name string
		// initial cluster state
		objs []runtime.Object
		// action
		run func(c *Client) error
		// expectations
		wantConfigMap  bool
		wantAnnotation string // "" = annotation must be absent
	}{
		{
			name: "materialize stamps configmap and annotation",
			objs: []runtime.Object{
				testIngress("web", "42", nil),
			},
			run: func(c *Client) error {
				return c.CreateWAFZone(context.Background(), "42", "waf-42", "", "coraza-42", nil, nil, enabled)
			},
			wantConfigMap:  true,
			wantAnnotation: "coraza-42",
		},
		{
			name: "disable deletes configmap and strips annotation",
			objs: []runtime.Object{
				testIngress("web", "42", map[string]string{corazaZoneAnnotation: "coraza-42"}),
				testCorazaConfigMap("coraza-42", "42"),
			},
			run: func(c *Client) error {
				return c.CreateWAFZone(context.Background(), "42", "waf-42", "", "coraza-42", nil, nil, &api.WAFManagedRules{Enabled: false})
			},
			wantConfigMap:  false,
			wantAnnotation: "",
		},
		{
			name: "nil managed rules tears down like disabled",
			objs: []runtime.Object{
				testIngress("web", "42", map[string]string{corazaZoneAnnotation: "coraza-42"}),
				testCorazaConfigMap("coraza-42", "42"),
			},
			run: func(c *Client) error {
				return c.CreateWAFZone(context.Background(), "42", "waf-42", "", "coraza-42", nil, nil, nil)
			},
			wantConfigMap:  false,
			wantAnnotation: "",
		},
		{
			name: "empty corazaZoneID on create leaves live zone untouched",
			objs: []runtime.Object{
				testIngress("web", "42", map[string]string{corazaZoneAnnotation: "coraza-42"}),
				testCorazaConfigMap("coraza-42", "42"),
			},
			run: func(c *Client) error {
				return c.CreateWAFZone(context.Background(), "42", "waf-42", "", "", nil, nil, nil)
			},
			wantConfigMap:  true,
			wantAnnotation: "coraza-42",
		},
		{
			name: "delete removes configmap and strips annotation",
			objs: []runtime.Object{
				testIngress("web", "42", map[string]string{corazaZoneAnnotation: "coraza-42"}),
				testCorazaConfigMap("coraza-42", "42"),
			},
			run: func(c *Client) error {
				return c.DeleteWAFZone(context.Background(), "42", "waf-42", "", "coraza-42")
			},
			wantConfigMap:  false,
			wantAnnotation: "",
		},
		{
			name: "empty corazaZoneID on delete leaves live zone untouched",
			objs: []runtime.Object{
				testIngress("web", "42", map[string]string{corazaZoneAnnotation: "coraza-42"}),
				testCorazaConfigMap("coraza-42", "42"),
			},
			run: func(c *Client) error {
				return c.DeleteWAFZone(context.Background(), "42", "waf-42", "", "")
			},
			wantConfigMap:  true,
			wantAnnotation: "coraza-42",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestClient(tc.objs...)
			if err := tc.run(c); err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			_, err := c.client.CoreV1().ConfigMaps(testNamespace).Get(ctx, "coraza-42", metav1.GetOptions{})
			switch {
			case tc.wantConfigMap && err != nil:
				t.Errorf("coraza-42 ConfigMap must exist: %v", err)
			case !tc.wantConfigMap && !errors.IsNotFound(err):
				t.Errorf("coraza-42 ConfigMap must be gone, got err = %v", err)
			}

			ing, err := c.client.NetworkingV1().Ingresses(testNamespace).Get(ctx, "web", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, ok := ing.Annotations[corazaZoneAnnotation]
			if tc.wantAnnotation == "" {
				if ok {
					t.Errorf("annotation %s must be absent, got %q", corazaZoneAnnotation, got)
				}
			} else if got != tc.wantAnnotation {
				t.Errorf("annotation %s = %q, want %q", corazaZoneAnnotation, got, tc.wantAnnotation)
			}
		})
	}
}

// TestWAFZoneCorazaMaterializedConf pins the materialized ConfigMap shape:
// data key, generated conf bytes, and the labels parapet's Coraza watch
// selects on (parapet.moonrhythm.io/coraza=zone + projectId).
func TestWAFZoneCorazaMaterializedConf(t *testing.T) {
	t.Parallel()

	c := newTestClient(testIngress("web", "42", nil))
	managed := &api.WAFManagedRules{
		Enabled:       true,
		ParanoiaLevel: 1,
		ExcludedRules: []int{942100},
	}
	err := c.CreateWAFZone(context.Background(), "42", "waf-42", "", "coraza-42", nil, nil, managed)
	if err != nil {
		t.Fatal(err)
	}

	cm, err := c.client.CoreV1().ConfigMaps(testNamespace).Get(context.Background(), "coraza-42", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cm.Labels[corazaLabel], "zone"; got != want {
		t.Errorf("label %s = %q, want %q", corazaLabel, got, want)
	}
	if got, want := cm.Labels["projectId"], "42"; got != want {
		t.Errorf("label projectId = %q, want %q", got, want)
	}
	if got, want := cm.Data["crs.conf"], generateCorazaConf(managed); got != want {
		t.Errorf("crs.conf =\n%s\nwant:\n%s", got, want)
	}
}
