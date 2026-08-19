package k8s

import (
	"context"

	v1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Certificate struct {
	ID        string
	ProjectID string
	Domain    string
	// Wildcard adds a `*.<Domain>` SAN alongside the apex and routes the whole
	// cert through the DNS-01-only ClusterIssuer letsencrypt-dns01.
	//
	// The shared `letsencrypt` Issuer carries both an HTTP-01 (parapet ingress)
	// and a DNS-01 solver. cert-manager only auto-filters HTTP-01 for the
	// `*.<Domain>` SAN (Let's Encrypt rejects HTTP-01 for wildcards); the bare
	// apex SAN still selects the HTTP-01 solver. For a Cloudflare/CDN-proxied
	// domain that challenge can never complete (the request is redirected/404'd
	// at the edge and never reaches the parapet solver), so cert-manager retries
	// it forever. Pinning wildcard rows to the DNS-01-only issuer makes both the
	// apex and the wildcard SAN validate via DNS-01. See deploys-app/deployer#38.
	Wildcard bool
}

// CreateCertificate ensures the cert-manager Certificate exists (create or
// idempotent update) and reports whether it has actually issued — its Ready
// condition is True. A no-op spec re-apply doesn't reset cert-manager's status,
// so the pre-apply Get reflects current issuance; a freshly-created cert is not
// yet ready and reports false until cert-manager completes the order.
func (c *Client) CreateCertificate(ctx context.Context, obj Certificate) (ready bool, err error) {
	s := c.certManagerClient.CertmanagerV1().Certificates(c.namespace)

	cert, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return false, err
	}

	ready = certificateReady(cert)

	labels := map[string]string{
		"projectId": obj.ProjectID,
	}

	if cert == nil {
		cert = &v1.Certificate{}
	}

	dnsNames := []string{obj.Domain}
	issuerRef := cmmeta.ObjectReference{
		Name: "letsencrypt",
		Kind: v1.IssuerKind,
	}
	if obj.Wildcard {
		dnsNames = append(dnsNames, "*."+obj.Domain)
		// DNS-01 for every SAN (apex included) — avoids the HTTP-01 solver that
		// never validates behind a CDN. See the Wildcard field comment above.
		issuerRef = cmmeta.ObjectReference{
			Name: "letsencrypt-dns01",
			Kind: v1.ClusterIssuerKind,
		}
	}

	cert.ObjectMeta.Name = obj.ID
	cert.ObjectMeta.Labels = labels
	cert.Spec = v1.CertificateSpec{
		CommonName: obj.Domain,
		DNSNames:   dnsNames,
		IssuerRef:  issuerRef,
		PrivateKey: &v1.CertificatePrivateKey{
			Algorithm: v1.ECDSAKeyAlgorithm,
			Size:      256,
		},
		SecretName: "tls-" + obj.ID,
	}

	_, err = s.Update(ctx, cert, metav1.UpdateOptions{})
	if errors.IsNotFound(err) {
		_, err = s.Create(ctx, cert, metav1.CreateOptions{})
	}
	return ready, err
}

// certificateReady reports whether the cert-manager Certificate's Ready
// condition is True. A nil cert (not yet created) is not ready.
func certificateReady(cert *v1.Certificate) bool {
	if cert == nil {
		return false
	}
	for _, cond := range cert.Status.Conditions {
		if cond.Type == v1.CertificateConditionReady {
			return cond.Status == cmmeta.ConditionTrue
		}
	}
	return false
}

func (c *Client) DeleteCertificate(ctx context.Context, id string) error {
	err := c.certManagerClient.CertmanagerV1().Certificates(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}
