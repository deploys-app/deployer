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
)

// CreateWAFZone upserts the project's WAF zone ConfigMap (name = zoneID,
// labeled parapet.moonrhythm.io/waf: zone) holding the rules as a parapet
// `rules:` YAML document, then binds every one of the project's Ingresses in
// this namespace to the zone via the parapet.moonrhythm.io/waf-zone annotation.
func (c *Client) CreateWAFZone(ctx context.Context, projectID string, zoneID string, rules []api.WAFRule) error {
	s := c.client.CoreV1().ConfigMaps(c.namespace)

	configMap, err := s.Get(ctx, zoneID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	if rules == nil {
		rules = []api.WAFRule{}
	}
	rulesYAML, err := yaml.Marshal(struct {
		Rules []api.WAFRule `yaml:"rules"`
	}{Rules: rules})
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
		wafLabel:    "zone",
	}
	configMap.ObjectMeta.Annotations = map[string]string{}
	configMap.Data = map[string]string{
		"rules.yaml": string(rulesYAML),
	}

	_, err = s.Update(ctx, configMap, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, configMap, metav1.CreateOptions{})
	}
	if err != nil {
		return err
	}

	// auto-apply the zone to all of the project's routes
	ingresses, err := c.ingressesForProject(ctx, projectID)
	if err != nil {
		return err
	}
	ings := c.client.NetworkingV1().Ingresses(c.namespace)
	for i := range ingresses {
		ing := &ingresses[i]
		if ing.Annotations == nil {
			ing.Annotations = map[string]string{}
		}
		if ing.Annotations[wafZoneAnnotation] == zoneID {
			continue
		}
		ing.Annotations[wafZoneAnnotation] = zoneID
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

// DeleteWAFZone removes the project's WAF zone ConfigMap and strips the
// parapet.moonrhythm.io/waf-zone annotation from every one of the project's
// Ingresses in this namespace.
func (c *Client) DeleteWAFZone(ctx context.Context, projectID string, zoneID string) error {
	err := c.client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, zoneID, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	ingresses, err := c.ingressesForProject(ctx, projectID)
	if err != nil {
		return err
	}
	ings := c.client.NetworkingV1().Ingresses(c.namespace)
	for i := range ingresses {
		ing := &ingresses[i]
		if ing.Annotations == nil {
			continue
		}
		if _, ok := ing.Annotations[wafZoneAnnotation]; !ok {
			continue
		}
		delete(ing.Annotations, wafZoneAnnotation)
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
	res, err := c.client.CoreV1().ConfigMaps(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: wafLabel + "=zone,projectId=" + projectID,
	})
	if err != nil {
		return "", err
	}
	if len(res.Items) == 0 {
		return "", nil
	}
	return res.Items[0].Name, nil
}
