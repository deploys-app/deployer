package k8s

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/deploys-app/api"
)

const (
	// corazaLabel marks a ConfigMap as a parapet Coraza (managed rules /
	// OWASP CRS) zone. Separate label key and watch from the WAF's, same
	// global+zone model.
	corazaLabel = "parapet.moonrhythm.io/coraza"
	// corazaZoneAnnotation binds an Ingress to a Coraza zone by its id
	// (= the zone ConfigMap name). parapet resolves it namespace-locally.
	corazaZoneAnnotation = "parapet.moonrhythm.io/coraza-zone"
)

// generateCorazaConf renders the managed-rules SecLang document from the typed
// knobs. Deterministic — fixed line order, defaults rendered explicitly (so the
// conf fingerprint is stable across CRS bumps), exclusions sorted — so repeated
// Sets are byte-identical and parapet's fingerprint skip works.
//
// The include forms are the only ones the embedded coreruleset.FS resolves:
// coraza's Include is a plain fs.ReadFile that globs only when the path
// contains '*', and the FS holds @crs-setup.conf.example (a file) and
// @owasp_crs/ (a directory) — the bare @crs-setup / @owasp_crs forms fail to
// compile. A compile failure is controller-log-only and last-good for a new
// zone is pass-through, so TestGenerateCorazaConfCompiles keeps "the engine
// accepts every conf we can emit" a CI invariant.
//
// Exclusions render after the includes (SecRuleRemoveById removes
// already-loaded rules); the api-side id bounds (911100..948999) keep the two
// platform SecActions, the CRS setup, and the anomaly-scoring machinery out of
// reach.
func generateCorazaConf(m *api.WAFManagedRules) string {
	engine := "On"
	if m.Mode == "detect" {
		engine = "DetectionOnly"
	}
	paranoia := m.ParanoiaLevel
	if paranoia == 0 {
		paranoia = 1
	}
	threshold := m.AnomalyThreshold
	if threshold == 0 {
		threshold = 5
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SecRuleEngine %s\n", engine)
	b.WriteString("SecRequestBodyAccess On\n")
	fmt.Fprintf(&b, "SecAction \"id:900000,phase:1,pass,t:none,nolog,setvar:tx.blocking_paranoia_level=%d\"\n", paranoia)
	fmt.Fprintf(&b, "SecAction \"id:900110,phase:1,pass,t:none,nolog,setvar:tx.inbound_anomaly_score_threshold=%d\"\n", threshold)
	b.WriteString("Include @crs-setup.conf.example\n")
	b.WriteString("Include @owasp_crs/*.conf\n")
	excluded := slices.Clone(m.ExcludedRules)
	slices.Sort(excluded)
	for _, id := range excluded {
		fmt.Fprintf(&b, "SecRuleRemoveById %d\n", id)
	}
	return b.String()
}

// corazaZoneForProject is wafZoneForProject for the project's Coraza (managed
// rules) zone ConfigMap. The ConfigMap exists only while managed rules are
// enabled, so a disabled zone naturally stamps nothing on new ingresses.
func (c *Client) corazaZoneForProject(ctx context.Context, projectID string) (string, error) {
	return c.zoneForProject(ctx, corazaLabel, projectID)
}
