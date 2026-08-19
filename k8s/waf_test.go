package k8s

import (
	"strings"
	"testing"

	"github.com/deploys-app/api"
	"gopkg.in/yaml.v3"
)

// controllerLimit mirrors parapet-ingress-controller's ratelimitrule.Limit
// yaml shape — the consumer contract for the rendered ConfigMap document.
type controllerLimit struct {
	ID        string   `yaml:"id"`
	Key       []string `yaml:"key"`
	Rate      int      `yaml:"rate"`
	Window    string   `yaml:"window"`
	Algorithm string   `yaml:"algorithm"`
	Mode      string   `yaml:"mode"`
	Status    int      `yaml:"status"`
	Message   string   `yaml:"message"`
	Filter    string   `yaml:"filter"`
}

func TestMarshalLimitsYAML(t *testing.T) {
	t.Parallel()

	const filter = `request.method == "POST" && request.path.startsWith("/api/")`
	out, err := marshalLimitsYAML([]api.WAFLimit{
		{
			ID:     "1-abc",
			Key:    []string{"ip"},
			Rate:   100,
			Window: "1m",
			Filter: filter,
		},
		{
			ID:     "1-def",
			Key:    []string{"ip"},
			Rate:   10,
			Window: "10s",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var doc struct {
		Limits []controllerLimit `yaml:"limits"`
	}
	if err := yaml.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("rendered document must parse: %v", err)
	}
	if len(doc.Limits) != 2 {
		t.Fatalf("limits = %d, want 2", len(doc.Limits))
	}
	if got := doc.Limits[0].Filter; got != filter {
		t.Errorf("filter = %q, want %q", got, filter)
	}
	if got := doc.Limits[0].Rate; got != 100 {
		t.Errorf("rate = %d, want 100", got)
	}

	// A limit without a filter must not render the key at all (omitempty), so
	// zones that don't use filters produce the same document older controllers
	// already accept.
	if n := strings.Count(out, "filter:"); n != 1 {
		t.Errorf("filter key rendered %d times, want exactly 1 (omitempty for the empty filter):\n%s", n, out)
	}
}

// generateCorazaConfCases are the managed-rules goldens. Every case's output is
// also fed to the real Coraza engine by TestGenerateCorazaConfCompiles
// (coraza_test.go) — text goldens alone cannot catch a well-formed-looking
// document the engine rejects, and a compile rejection in production is a
// silent pass-through (controller-log-only, last-good for a new zone is
// nothing).
var generateCorazaConfCases = []struct {
	name    string
	managed api.WAFManagedRules
	want    string
}{
	{
		// The spec's worked example: exclusions render sorted ascending
		// regardless of input order, so repeated Sets are byte-identical.
		name: "enforce tuned with exclusions",
		managed: api.WAFManagedRules{
			Enabled:          true,
			Mode:             "enforce",
			ParanoiaLevel:    2,
			AnomalyThreshold: 5,
			ExcludedRules:    []int{942100, 920420},
		},
		want: `SecRuleEngine On
SecRequestBodyAccess On
SecAction "id:900000,phase:1,pass,t:none,nolog,setvar:tx.blocking_paranoia_level=2"
SecAction "id:900110,phase:1,pass,t:none,nolog,setvar:tx.inbound_anomaly_score_threshold=5"
Include @crs-setup.conf.example
Include @owasp_crs/*.conf
SecRuleRemoveById 920420
SecRuleRemoveById 942100
`,
	},
	{
		// Zero knobs render the defaults explicitly (paranoia 1, threshold 5)
		// so the conf fingerprint doesn't depend on CRS-setup defaults.
		name:    "defaults",
		managed: api.WAFManagedRules{Enabled: true},
		want: `SecRuleEngine On
SecRequestBodyAccess On
SecAction "id:900000,phase:1,pass,t:none,nolog,setvar:tx.blocking_paranoia_level=1"
SecAction "id:900110,phase:1,pass,t:none,nolog,setvar:tx.inbound_anomaly_score_threshold=5"
Include @crs-setup.conf.example
Include @owasp_crs/*.conf
`,
	},
	{
		name: "detect mode",
		managed: api.WAFManagedRules{
			Enabled:          true,
			Mode:             "detect",
			ParanoiaLevel:    4,
			AnomalyThreshold: 10,
			ExcludedRules:    []int{941100},
		},
		want: `SecRuleEngine DetectionOnly
SecRequestBodyAccess On
SecAction "id:900000,phase:1,pass,t:none,nolog,setvar:tx.blocking_paranoia_level=4"
SecAction "id:900110,phase:1,pass,t:none,nolog,setvar:tx.inbound_anomaly_score_threshold=10"
Include @crs-setup.conf.example
Include @owasp_crs/*.conf
SecRuleRemoveById 941100
`,
	},
}

func TestGenerateCorazaConf(t *testing.T) {
	t.Parallel()

	for _, tc := range generateCorazaConfCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := generateCorazaConf(&tc.managed); got != tc.want {
				t.Errorf("generateCorazaConf() =\n%s\nwant:\n%s", got, tc.want)
			}
		})
	}
}

func TestGenerateCorazaConfDeterministic(t *testing.T) {
	t.Parallel()

	m := api.WAFManagedRules{Enabled: true, ExcludedRules: []int{942100, 920420, 933100}}
	first := generateCorazaConf(&m)
	if got := generateCorazaConf(&m); got != first {
		t.Error("repeated generation must be byte-identical")
	}
	// Sorting must not mutate the caller's slice (the command payload).
	if m.ExcludedRules[0] != 942100 {
		t.Error("generateCorazaConf must not reorder the input slice")
	}
}
