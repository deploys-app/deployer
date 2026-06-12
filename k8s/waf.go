package k8s

import (
	"context"

	"github.com/deploys-app/api"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// wafLabel marks a ConfigMap as a parapet WAF zone. parapet watches with
	// an existence selector on this key and treats the ConfigMap name as the
	// zone id.
	wafLabel = "parapet.moonrhythm.io/waf"
	// wafZoneAnnotation binds an Ingress to a WAF zone by its id (= the zone
	// ConfigMap name).
	wafZoneAnnotation = "parapet.moonrhythm.io/waf-zone"
	// rateLimitLabel marks a ConfigMap as a parapet ratelimit zone. It is a
	// separate label key (and a separate parapet watch) from the WAF's, so the
	// project's limits live in their own ConfigMap next to the WAF one.
	rateLimitLabel = "parapet.moonrhythm.io/ratelimit"
	// rateLimitZoneAnnotation binds an Ingress to a ratelimit zone by its id
	// (= the zone ConfigMap name). parapet resolves it namespace-locally.
	rateLimitZoneAnnotation = "parapet.moonrhythm.io/ratelimit-zone"
)

// CreateWAFZone upserts the project's WAF zone ConfigMap (name = zoneID,
// labeled parapet.moonrhythm.io/waf: zone) holding the rules as a parapet
// `rules:` YAML document, plus — when the zone has limits — the project's
// ratelimit zone ConfigMap (name = rateLimitZoneID, labeled
// parapet.moonrhythm.io/ratelimit: zone) holding them as a parapet `limits:`
// YAML document. It then binds every one of the project's Ingresses in this
// namespace via the parapet.moonrhythm.io/waf-zone and ratelimit-zone
// annotations. An empty limit set removes the ratelimit ConfigMap and
// annotation so parapet drops the zone instead of keeping an empty set.
func (c *Client) CreateWAFZone(ctx context.Context, projectID string, zoneID, rateLimitZoneID string, rules []api.WAFRule, limits []api.WAFLimit) error {
	if rules == nil {
		rules = []api.WAFRule{}
	}
	rulesYAML, err := yaml.Marshal(struct {
		Rules []api.WAFRule `yaml:"rules"`
	}{Rules: rules})
	if err != nil {
		return err
	}
	err = c.upsertZoneConfigMap(ctx, projectID, zoneID, wafLabel, map[string]string{
		"rules.yaml": string(rulesYAML),
	})
	if err != nil {
		return err
	}

	// auto-apply the zones to all of the project's routes. An empty
	// rateLimitZoneID means the command came from an apiserver that predates
	// rate limits — leave everything ratelimit-related untouched so a mixed
	// version rollout (or apiserver rollback) can't wedge WAF zone work.
	annotations := map[string]string{
		wafZoneAnnotation: zoneID,
	}
	if rateLimitZoneID != "" {
		if len(limits) > 0 {
			limitsYAML, err := marshalLimitsYAML(limits)
			if err != nil {
				return err
			}
			err = c.upsertZoneConfigMap(ctx, projectID, rateLimitZoneID, rateLimitLabel, map[string]string{
				"limits.yaml": limitsYAML,
			})
			if err != nil {
				return err
			}
			annotations[rateLimitZoneAnnotation] = rateLimitZoneID
		} else {
			err = c.deleteConfigMap(ctx, rateLimitZoneID)
			if err != nil {
				return err
			}
			annotations[rateLimitZoneAnnotation] = ""
		}
	}

	return c.syncZoneAnnotations(ctx, projectID, annotations)
}

// DeleteWAFZone removes the project's WAF zone and ratelimit zone ConfigMaps
// and strips the parapet.moonrhythm.io/waf-zone and ratelimit-zone annotations
// from every one of the project's Ingresses in this namespace.
func (c *Client) DeleteWAFZone(ctx context.Context, projectID string, zoneID, rateLimitZoneID string) error {
	err := c.deleteConfigMap(ctx, zoneID)
	if err != nil {
		return err
	}
	// Empty rateLimitZoneID = pre-ratelimit apiserver (see CreateWAFZone);
	// nothing ratelimit-related exists to tear down.
	if rateLimitZoneID != "" {
		err = c.deleteConfigMap(ctx, rateLimitZoneID)
		if err != nil {
			return err
		}
	}

	annotations := map[string]string{
		wafZoneAnnotation: "",
	}
	if rateLimitZoneID != "" {
		annotations[rateLimitZoneAnnotation] = ""
	}
	return c.syncZoneAnnotations(ctx, projectID, annotations)
}

// marshalLimitsYAML renders the rate-limit ConfigMap document consumed by
// parapet-ingress-controller's ratelimitrule.Parse. api.WAFLimit's yaml tags
// are that contract (id/key/rate/window/algorithm/mode/status/message/filter);
// optional fields use omitempty so a zone without them renders byte-identical
// to what older controllers already accept.
func marshalLimitsYAML(limits []api.WAFLimit) (string, error) {
	b, err := yaml.Marshal(struct {
		Limits []api.WAFLimit `yaml:"limits"`
	}{Limits: limits})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// upsertZoneConfigMap creates or replaces a parapet zone ConfigMap: name =
// zoneID, labeled <label>: zone and projectId, with the given data.
func (c *Client) upsertZoneConfigMap(ctx context.Context, projectID, zoneID, label string, data map[string]string) error {
	s := c.client.CoreV1().ConfigMaps(c.namespace)

	configMap, err := s.Get(ctx, zoneID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	if configMap == nil {
		configMap = &v1.ConfigMap{}
	}
	configMap.ObjectMeta.Name = zoneID
	configMap.ObjectMeta.Labels = map[string]string{
		"id":        zoneID,
		"projectId": projectID,
		label:       "zone",
	}
	configMap.ObjectMeta.Annotations = map[string]string{}
	configMap.Data = data

	_, err = s.Update(ctx, configMap, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, configMap, metav1.CreateOptions{})
	}
	return err
}

func (c *Client) deleteConfigMap(ctx context.Context, name string) error {
	err := c.client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	return err
}

// syncZoneAnnotations reconciles the given annotations on every one of the
// project's Ingresses in this namespace: a non-empty value is set, an empty
// value removes the annotation. Ingresses already in the desired state are
// left untouched.
func (c *Client) syncZoneAnnotations(ctx context.Context, projectID string, annotations map[string]string) error {
	ingresses, err := c.ingressesForProject(ctx, projectID)
	if err != nil {
		return err
	}
	ings := c.client.NetworkingV1().Ingresses(c.namespace)
	for i := range ingresses {
		ing := &ingresses[i]
		changed := false
		for k, v := range annotations {
			switch {
			case v == "":
				if _, ok := ing.Annotations[k]; ok {
					delete(ing.Annotations, k)
					changed = true
				}
			case ing.Annotations[k] != v:
				if ing.Annotations == nil {
					ing.Annotations = map[string]string{}
				}
				ing.Annotations[k] = v
				changed = true
			}
		}
		if !changed {
			continue
		}
		_, err = ings.Update(ctx, ing, metav1.UpdateOptions{})
		if errors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// ingressesForProject lists all Ingresses owned by the project in this
// namespace.
func (c *Client) ingressesForProject(ctx context.Context, projectID string) ([]networking.Ingress, error) {
	res, err := c.client.NetworkingV1().Ingresses(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "projectId=" + projectID,
	})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// wafZoneForProject returns the name of the project's WAF zone ConfigMap in
// this namespace, or "" if the project has no zone. Used by ingress creation
// so routes added after a zone exists still get covered.
func (c *Client) wafZoneForProject(ctx context.Context, projectID string) (string, error) {
	return c.zoneForProject(ctx, wafLabel, projectID)
}

// rateLimitZoneForProject is wafZoneForProject for the project's ratelimit
// zone ConfigMap.
func (c *Client) rateLimitZoneForProject(ctx context.Context, projectID string) (string, error) {
	return c.zoneForProject(ctx, rateLimitLabel, projectID)
}

func (c *Client) zoneForProject(ctx context.Context, label, projectID string) (string, error) {
	res, err := c.client.CoreV1().ConfigMaps(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: label + "=zone,projectId=" + projectID,
	})
	if err != nil {
		return "", err
	}
	if len(res.Items) == 0 {
		return "", nil
	}
	return res.Items[0].Name, nil
}
