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
