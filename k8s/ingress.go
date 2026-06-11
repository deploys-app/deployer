package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/deploys-app/api"
	"gopkg.in/yaml.v2"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

type Ingress struct {
	ID           string
	Service      string
	ProjectID    string
	Domain       string
	Path         string
	Secret       string
	UpstreamHost string
	UpstreamPath string
	Internal     bool
	Config       api.RouteConfig
}

func (c *Client) CreateIngress(ctx context.Context, x Ingress) error {
	if x.Domain == "" || x.Path == "" {
		return fmt.Errorf("empty domain or path")
	}

	x.Domain = strings.ToLower(x.Domain)

	pathType := networking.PathTypeImplementationSpecific
	s := c.client.NetworkingV1().Ingresses(c.namespace)

	backend := networking.IngressBackend{
		Service: &networking.IngressServiceBackend{
			Name: x.Service,
			Port: networking.ServiceBackendPort{
				Name: "http",
			},
		},
	}

	x.Path = strings.TrimSuffix(x.Path, "/")
	if !strings.HasPrefix(x.Path, "/") {
		x.Path = "/" + x.Path
	}
	stripPath := ""
	if x.Path != "/" {
		stripPath = x.Path
	}

	rule := networking.IngressRule{
		Host: x.Domain,
		IngressRuleValue: networking.IngressRuleValue{
			HTTP: &networking.HTTPIngressRuleValue{
				Paths: []networking.HTTPIngressPath{
					{
						Path:     x.Path,
						Backend:  backend,
						PathType: &pathType,
					},
				},
			},
		},
	}
	if x.Path != "/" {
		rule.IngressRuleValue.HTTP.Paths = append(rule.IngressRuleValue.HTTP.Paths, networking.HTTPIngressPath{
			Path:     x.Path + "/",
			Backend:  backend,
			PathType: &pathType,
		})
	}

	annotation := make(map[string]string, 10)
	if stripPath != "" {
		annotation["parapet.moonrhythm.io/strip-prefix"] = stripPath
	}
	if !x.Internal {
		annotation["parapet.moonrhythm.io/hsts"] = "default"
		annotation["parapet.moonrhythm.io/redirect-https"] = "true"
	}
	if x.UpstreamHost != "" {
		annotation["parapet.moonrhythm.io/upstream-host"] = x.UpstreamHost
	}
	if x.UpstreamPath != "" {
		annotation["parapet.moonrhythm.io/upstream-path"] = x.UpstreamPath
	}
	if a := x.Config.BasicAuth; a != nil {
		annotation["parapet.moonrhythm.io/basic-auth"] = a.User + ":" + a.Password
	}
	if a := x.Config.ForwardAuth; a != nil {
		b, _ := yaml.Marshal(struct {
			URL                 string   `yaml:"url"`
			AuthRequestHeaders  []string `yaml:"authRequestHeaders"`
			AuthResponseHeaders []string `yaml:"authResponseHeaders"`
		}{
			URL:                 a.Target,
			AuthRequestHeaders:  a.AuthRequestHeaders,
			AuthResponseHeaders: a.AuthResponseHeaders,
		})
		annotation["parapet.moonrhythm.io/forward-auth"] = string(b)
	}

	// Bind to the project's WAF and ratelimit zones if they exist, so routes
	// added after the zones were created are still covered. Best-effort: a
	// lookup error must not fail ingress creation since WAF is best-effort
	// relative to routing.
	if zoneID, err := c.wafZoneForProject(ctx, x.ProjectID); err != nil {
		slog.Error("ingress: looking up waf zone error", "id", x.ID, "projectId", x.ProjectID, "error", err)
	} else if zoneID != "" {
		annotation[wafZoneAnnotation] = zoneID
	}
	if zoneID, err := c.rateLimitZoneForProject(ctx, x.ProjectID); err != nil {
		slog.Error("ingress: looking up ratelimit zone error", "id", x.ID, "projectId", x.ProjectID, "error", err)
	} else if zoneID != "" {
		annotation[rateLimitZoneAnnotation] = zoneID
	}

	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name: x.ID,
			Labels: map[string]string{
				"id":        x.ID,
				"projectId": x.ProjectID,
			},
			Annotations: annotation,
		},
		Spec: networking.IngressSpec{
			IngressClassName: pointer.String("parapet"),
			Rules:            []networking.IngressRule{rule},
		},
	}
	if x.Internal {
		ing.Spec.IngressClassName = pointer.String("parapet-internal")
	}

	if x.Secret != "" {
		ing.Spec.TLS = []networking.IngressTLS{
			{
				SecretName: x.Secret,
			},
		}
	}

	_, err := s.Update(ctx, ing, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, ing, metav1.CreateOptions{})
	}
	return err
}

type RedirectIngress struct {
	ID        string
	ProjectID string
	Domain    string
	Path      string
	Target    string
	Secret    string
	Config    api.RouteConfig
}

func (c *Client) CreateRedirectIngress(ctx context.Context, x RedirectIngress) error {
	if x.Domain == "" || x.Path == "" {
		return fmt.Errorf("empty domain or path")
	}

	x.Domain = strings.ToLower(x.Domain)

	s := c.client.NetworkingV1().Ingresses(c.namespace)

	x.Path = strings.TrimSuffix(x.Path, "/")
	if !strings.HasPrefix(x.Path, "/") {
		x.Path = "/" + x.Path
	}
	stripPath := ""
	if x.Path != "/" {
		stripPath = x.Path
	}

	redirectRule := x.Domain + x.Path + ": " + x.Target

	annotation := make(map[string]string, 5)
	annotation["parapet.moonrhythm.io/redirect"] = redirectRule
	if stripPath != "" {
		annotation["parapet.moonrhythm.io/strip-prefix"] = stripPath
	}
	if a := x.Config.BasicAuth; a != nil {
		annotation["parapet.moonrhythm.io/basic-auth"] = a.User + ":" + a.Password
	}
	if a := x.Config.ForwardAuth; a != nil {
		b, _ := yaml.Marshal(struct {
			URL                 string   `yaml:"url"`
			AuthRequestHeaders  []string `yaml:"authRequestHeaders"`
			AuthResponseHeaders []string `yaml:"authResponseHeaders"`
		}{
			URL:                 a.Target,
			AuthRequestHeaders:  a.AuthRequestHeaders,
			AuthResponseHeaders: a.AuthResponseHeaders,
		})
		annotation["parapet.moonrhythm.io/forward-auth"] = string(b)
	}

	// Bind to the project's WAF and ratelimit zones if they exist, so routes
	// added after the zones were created are still covered. Best-effort: a
	// lookup error must not fail ingress creation since WAF is best-effort
	// relative to routing.
	if zoneID, err := c.wafZoneForProject(ctx, x.ProjectID); err != nil {
		slog.Error("ingress: looking up waf zone error", "id", x.ID, "projectId", x.ProjectID, "error", err)
	} else if zoneID != "" {
		annotation[wafZoneAnnotation] = zoneID
	}
	if zoneID, err := c.rateLimitZoneForProject(ctx, x.ProjectID); err != nil {
		slog.Error("ingress: looking up ratelimit zone error", "id", x.ID, "projectId", x.ProjectID, "error", err)
	} else if zoneID != "" {
		annotation[rateLimitZoneAnnotation] = zoneID
	}

	ing := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name: x.ID,
			Labels: map[string]string{
				"id":        x.ID,
				"projectId": x.ProjectID,
			},
			Annotations: annotation,
		},
		Spec: networking.IngressSpec{
			IngressClassName: pointer.String("parapet"),
			DefaultBackend: &networking.IngressBackend{
				Service: &networking.IngressServiceBackend{
					Name: "default",
					Port: networking.ServiceBackendPort{
						Name: "http",
					},
				},
			},
		},
	}

	if x.Secret != "" {
		ing.Spec.TLS = []networking.IngressTLS{
			{
				SecretName: x.Secret,
			},
		}
	}

	_, err := s.Update(ctx, ing, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, ing, metav1.CreateOptions{})
	}
	return err
}

func (c *Client) DeleteIngress(ctx context.Context, id string) error {
	err := c.client.NetworkingV1().Ingresses(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}
