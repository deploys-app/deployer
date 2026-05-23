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
	// Wildcard requests a `*.<Domain>` SAN alongside the apex. Wildcard certs
	// must use DNS-01 (Let's Encrypt rejects HTTP-01 for wildcards), so we
	// also point IssuerRef at the cluster's DNS-01 issuer instead of the
	// default HTTP-01 one.
	Wildcard bool
}

func (c *Client) CreateCertificate(ctx context.Context, obj Certificate) error {
	s := c.certManagerClient.CertmanagerV1().Certificates(c.namespace)

	cert, err := s.Get(ctx, obj.ID, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		return err
	}

	labels := map[string]string{
		"projectId": obj.ProjectID,
	}

	if cert == nil {
		cert = &v1.Certificate{}
	}

	dnsNames := []string{obj.Domain}
	issuer := cmmeta.ObjectReference{
		Name: "letsencrypt",
		Kind: v1.IssuerKind,
	}
	if obj.Wildcard {
		dnsNames = append(dnsNames, "*."+obj.Domain)
		issuer = cmmeta.ObjectReference{
			Name: "letsencrypt-dns01",
			Kind: v1.ClusterIssuerKind,
		}
	}

	cert.ObjectMeta.Name = obj.ID
	cert.ObjectMeta.Labels = labels
	cert.Spec = v1.CertificateSpec{
		CommonName: obj.Domain,
		DNSNames:   dnsNames,
		IssuerRef:  issuer,
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
	return err
}

func (c *Client) DeleteCertificate(ctx context.Context, id string) error {
	err := c.certManagerClient.CertmanagerV1().Certificates(c.namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}
