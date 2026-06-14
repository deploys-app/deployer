package k8s

import (
	"context"

	"github.com/deploys-app/api"
	"gopkg.in/yaml.v2"
)

const (
	// cacheLabel marks a ConfigMap as a parapet cache-override zone. The edge
	// control plane watches with an existence selector on this key and treats the
	// ConfigMap name as the zone id. It is a separate label key from the WAF and
	// ratelimit ones — parapet refuses a ConfigMap carrying two feature labels.
	cacheLabel = "parapet.moonrhythm.io/cache"
	// cacheZoneAnnotation binds an Ingress to a cache zone by its id (= the zone
	// ConfigMap name). Cache overrides are edge-only, so only the edge control
	// plane consumes this annotation; it resolves cross-namespace (the WAF model).
	cacheZoneAnnotation = "parapet.moonrhythm.io/cache-zone"
)

// CreateCacheZone upserts the project's cache-override zone ConfigMap (name =
// zoneID, labeled parapet.moonrhythm.io/cache: zone) holding the overrides as a
// parapet `overrides:` YAML document, then binds every one of the project's
// Ingresses in this namespace via the parapet.moonrhythm.io/cache-zone
// annotation. Unlike WAF there is no sibling ConfigMap and no empty-set special
// case: an empty override set writes a valid (empty) document — parapet treats
// it as "match nothing / honor origin", an inert zone — so re-adding overrides
// is a single Set rather than a re-create.
func (c *Client) CreateCacheZone(ctx context.Context, projectID string, zoneID string, overrides []api.CacheOverride) error {
	if overrides == nil {
		overrides = []api.CacheOverride{}
	}
	overridesYAML, err := marshalOverridesYAML(overrides)
	if err != nil {
		return err
	}
	err = c.upsertZoneConfigMap(ctx, projectID, zoneID, cacheLabel, map[string]string{
		"overrides.yaml": overridesYAML,
	})
	if err != nil {
		return err
	}

	return c.syncZoneAnnotations(ctx, projectID, map[string]string{
		cacheZoneAnnotation: zoneID,
	})
}

// DeleteCacheZone removes the project's cache zone ConfigMap and strips the
// parapet.moonrhythm.io/cache-zone annotation from every one of the project's
// Ingresses in this namespace.
func (c *Client) DeleteCacheZone(ctx context.Context, projectID string, zoneID string) error {
	err := c.deleteConfigMap(ctx, zoneID)
	if err != nil {
		return err
	}
	return c.syncZoneAnnotations(ctx, projectID, map[string]string{
		cacheZoneAnnotation: "",
	})
}

// marshalOverridesYAML renders the cache-override ConfigMap document consumed by
// parapet-ingress-controller's cacherule.Parse. api.CacheOverride's yaml tags
// are that contract (id/action/filter/ttl/policy/stale_*/status/mode/priority);
// the deploys.app-only description field is harmless (cacherule.Parse ignores
// unknown keys). A single document — never `---`-joined (cacherule reads one
// document per ConfigMap data value).
func marshalOverridesYAML(overrides []api.CacheOverride) (string, error) {
	b, err := yaml.Marshal(struct {
		Overrides []api.CacheOverride `yaml:"overrides"`
	}{Overrides: overrides})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// cacheZoneForProject returns the name of the project's cache zone ConfigMap in
// this namespace, or "" if the project has no zone. Used by ingress creation so
// routes added after a zone exists still get covered.
func (c *Client) cacheZoneForProject(ctx context.Context, projectID string) (string, error) {
	return c.zoneForProject(ctx, cacheLabel, projectID)
}
