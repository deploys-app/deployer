package k8s

import (
	"context"

	"github.com/deploys-app/api"
	"gopkg.in/yaml.v2"
)

const (
	// transformLabel marks a ConfigMap as a parapet transform zone. The
	// in-cluster parapet-ingress-controller watches with an existence selector on
	// this key and treats the ConfigMap name as the zone id. It is a separate
	// label key from the WAF, ratelimit, and cache ones — parapet refuses a
	// ConfigMap carrying two feature labels.
	transformLabel = "parapet.moonrhythm.io/transform"
	// transformZoneAnnotation binds an Ingress to a transform zone by its id (= the
	// zone ConfigMap name). v1 transform runs in-cluster, so the in-cluster
	// controller consumes this annotation; it resolves namespace-locally (the WAF
	// model).
	transformZoneAnnotation = "parapet.moonrhythm.io/transform-zone"
)

// CreateTransformZone upserts the project's transform zone ConfigMap (name =
// zoneID, labeled parapet.moonrhythm.io/transform: zone) holding the rules as a
// parapet `transforms:` YAML document, then binds every one of the project's
// Ingresses in this namespace via the parapet.moonrhythm.io/transform-zone
// annotation. Like cache there is no sibling ConfigMap and no empty-set special
// case: an empty rule set writes a valid (empty) document — parapet treats it as
// an inert zone — so re-adding rules is a single Set rather than a re-create.
func (c *Client) CreateTransformZone(ctx context.Context, projectID string, zoneID string, transforms []api.TransformRule) error {
	if transforms == nil {
		transforms = []api.TransformRule{}
	}
	transformsYAML, err := marshalTransformsYAML(transforms)
	if err != nil {
		return err
	}
	err = c.upsertZoneConfigMap(ctx, projectID, zoneID, transformLabel, map[string]string{
		"transforms.yaml": transformsYAML,
	})
	if err != nil {
		return err
	}

	return c.syncZoneAnnotations(ctx, projectID, map[string]string{
		transformZoneAnnotation: zoneID,
	})
}

// DeleteTransformZone removes the project's transform zone ConfigMap and strips
// the parapet.moonrhythm.io/transform-zone annotation from every one of the
// project's Ingresses in this namespace.
func (c *Client) DeleteTransformZone(ctx context.Context, projectID string, zoneID string) error {
	err := c.deleteConfigMap(ctx, zoneID)
	if err != nil {
		return err
	}
	return c.syncZoneAnnotations(ctx, projectID, map[string]string{
		transformZoneAnnotation: "",
	})
}

// marshalTransformsYAML renders the transform ConfigMap document consumed by
// parapet-ingress-controller's transformrule.Parse. api.TransformRule's yaml
// tags are that contract; the api field, the command field, and the wire
// document root key are all `transforms` (the DB JSONB column is `rules`, an
// internal name only). A single document — never `---`-joined (transformrule
// reads one document per ConfigMap data value).
func marshalTransformsYAML(transforms []api.TransformRule) (string, error) {
	b, err := yaml.Marshal(struct {
		Transforms []api.TransformRule `yaml:"transforms"`
	}{Transforms: transforms})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// transformZoneForProject returns the name of the project's transform zone
// ConfigMap in this namespace, or "" if the project has no zone. Used by ingress
// creation so routes added after a zone exists still get covered.
func (c *Client) transformZoneForProject(ctx context.Context, projectID string) (string, error) {
	return c.zoneForProject(ctx, transformLabel, projectID)
}
