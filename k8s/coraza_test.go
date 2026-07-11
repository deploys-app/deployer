package k8s

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	coreruleset "github.com/corazawaf/coraza-coreruleset/v4"
	"github.com/deploys-app/api"
	"github.com/moonrhythm/parapet-ingress-controller/corazawaf"
)

// These tests import the exact engine that consumes the generated document in
// production (parapet-ingress-controller's corazawaf over the embedded OWASP
// CRS). The module pins must track the deployed parapet image's coraza /
// coraza-coreruleset versions (currently coraza v3.7.0 + coreruleset v4.25.0,
// parapet-ingress-controller#181) and bump together with image rollouts, so a
// green CI compile is the production compile.

// TestGenerateCorazaConfCompiles is the compile gate: SetDirectives must accept
// every document the generator can emit. A rejection in production is
// controller-log-only while the zone reports Success, and last-good for a
// brand-new zone is pass-through — a non-compiling document would be a silent
// no-op WAF.
func TestGenerateCorazaConfCompiles(t *testing.T) {
	t.Parallel()

	for _, tc := range generateCorazaConfCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := corazawaf.New(corazawaf.Options{RootFS: coreruleset.FS})
			if err := in.SetDirectives(generateCorazaConf(&tc.managed)); err != nil {
				t.Fatalf("engine rejected generated conf: %v\n%s", err, tc.want)
			}
			if !in.Loaded() {
				t.Fatal("ruleset must be loaded after SetDirectives")
			}
		})
	}
}

// canaryRequest builds a browser-shaped GET so the canary exercises the attack
// signature, not CRS's missing-header hygiene rules.
func canaryRequest(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) canary")
	r.Header.Set("Accept", "text/html")
	return r
}

func serveCanary(t *testing.T, m *api.WAFManagedRules, target string) int {
	t.Helper()
	in := corazawaf.New(corazawaf.Options{RootFS: coreruleset.FS})
	if err := in.SetDirectives(generateCorazaConf(m)); err != nil {
		t.Fatalf("engine rejected generated conf: %v", err)
	}
	h := in.ServeHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, canaryRequest(target))
	return rec.Code
}

// TestCorazaCanaryGETXSS is the behavior canary: with the default-knob
// document, a GET reflected XSS must be denied 403 by CRS anomaly blocking at
// paranoia level 1 with no request body. This is exactly the unconditional
// phase-2 case (949110 and most CRS detections are phase 2) — it goes red if
// an engine/CRS bump reintroduces body-gated phase-2 evaluation or changes the
// include layout.
func TestCorazaCanaryGETXSS(t *testing.T) {
	t.Parallel()

	const xss = "/?q=%3Cscript%3Ealert(1)%3C%2Fscript%3E"

	enforce := api.WAFManagedRules{Enabled: true}
	if code := serveCanary(t, &enforce, xss); code != http.StatusForbidden {
		t.Errorf("GET reflected XSS = %d, want 403", code)
	}
	if code := serveCanary(t, &enforce, "/?q=hello"); code != http.StatusOK {
		t.Errorf("clean GET = %d, want 200", code)
	}

	detect := api.WAFManagedRules{Enabled: true, Mode: "detect"}
	if code := serveCanary(t, &detect, xss); code != http.StatusOK {
		t.Errorf("detect mode GET reflected XSS = %d, want 200 (DetectionOnly must never block)", code)
	}
}

// TestCorazaIncludeFormsPinned pins the two include lines to the only forms the
// embedded FS resolves — coraza's Include is a plain fs.ReadFile that globs
// only on '*', so the bare @crs-setup / @owasp_crs forms fail to compile.
func TestCorazaIncludeFormsPinned(t *testing.T) {
	t.Parallel()

	conf := generateCorazaConf(&api.WAFManagedRules{Enabled: true})
	for _, line := range []string{
		"Include @crs-setup.conf.example\n",
		"Include @owasp_crs/*.conf\n",
	} {
		if !strings.Contains(conf, line) {
			t.Errorf("generated conf missing %q", strings.TrimSpace(line))
		}
	}
}
