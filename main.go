package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/acoshift/configfile"
	"github.com/deploys-app/api"
	"github.com/deploys-app/api/client"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/resource"
	knet "k8s.io/apimachinery/pkg/util/net"

	"github.com/deploys-app/deployer/k8s"
	// "github.com/deploys-app/deploys/logs"
)

func main() {
	cfg := configfile.NewEnvReader()

	locationID := cfg.MustString("location")
	projectID := cfg.String("project_id")
	namespace := cfg.String("namespace")
	deployerNamespace := cfg.String("deployer_namespace")
	apiEndpoint := cfg.String("api_endpoint")

	token := cfg.String("token")
	if token == "" {
		slog.Error("token required")
		os.Exit(1)
	}

	var k8sClient *k8s.Client
	if cfg.Bool("local") {
		var err error
		k8sClient, err = k8s.NewLocalClient(namespace)
		if err != nil {
			slog.Error("can not create k8s client", "error", err)
			os.Exit(1)
		}
	} else {
		var err error
		k8sClient, err = k8s.NewClient(namespace)
		if err != nil {
			slog.Error("can not create k8s client", "error", err)
			os.Exit(1)
		}
	}

	slog.Info("start deployer")
	slog.Info("config",
		"location", locationID,
		"project_id", projectID,
		"namespace", namespace,
		"api_endpoint", apiEndpoint,
	)

	ctx := context.Background()

	chEvent := make(chan struct{})

	eventTopic := cfg.String("event_topic")
	if eventTopic != "" {
		pubSubClient, err := pubsub.NewClient(ctx, projectID)
		if err != nil {
			slog.Error("can not create pubsub client", "error", err)
			os.Exit(1)
		}

		if pubSubClient != nil {
			defer pubSubClient.Close()

			subscription := locationID + "." + eventTopic

			_, err = pubSubClient.CreateSubscription(ctx, subscription, pubsub.SubscriptionConfig{
				Topic:             pubSubClient.Topic(eventTopic),
				AckDeadline:       10 * time.Second,
				RetentionDuration: time.Hour,
				ExpirationPolicy:  24 * time.Hour,
			})
			if err != nil {
				slog.Info("creating subscription error", "error", err)
			}

			go func() {
				err := pubSubClient.Subscription(subscription).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
					slog.Info("received event", "data", string(msg.Data))

					msg.Ack()

					select {
					case chEvent <- struct{}{}:
					default:
					}
				})
				if err != nil {
					slog.Error("can't subscribe", "error", err)
					if !cfg.Bool("local") {
						os.Exit(1)
					}
				}
			}()
		}
	}

	deployer := (&client.Client{
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Endpoint: apiEndpoint,
		Auth: func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+token)
		},
	}).Deployer()

	w := Worker{
		Deployer:          deployer,
		Client:            k8sClient,
		Namespace:         namespace,
		DeployerNamespace: deployerNamespace,
		RuntimeClass:      cfg.String("runtime_class"),
		H2CP:              cfg.Bool("h2cp"),
		Cert:              cfg.Bool("cert"),
		CPULimit:          cfg.StringDefault("cpu_limit", defaultLimitCPU),
		MemoryLimit:       cfg.StringDefault("memory_limit", defaultMemoryLimit),
		AccessVerifyURL:   cfg.StringDefault("access_verify_url", defaultAccessVerifyURL),
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	for {
		w.Run()

		select {
		case <-stop:
			return
		case <-time.After(10 * time.Second):
		case <-chEvent:
		}
	}
}

const (
	defaultRequestCPU  = "0.01"
	defaultLimitCPU    = "2"
	defaultMemoryLimit = "2Gi"

	// defaultAccessVerifyURL is the forward-auth verifier the public default-URL
	// ingress is pointed at when a deployment enables Require Google login. The
	// per-deployment numeric id is appended as ?d=<id> by accessForwardAuth.
	defaultAccessVerifyURL = "https://access.deploys.app/verify"

	// staticGatewayService is the shared Service name a Static deployment's
	// ingress points at; staticGatewayPort is the port the static-gateway
	// listens on (its PORT default). When DEPLOYER_NAMESPACE names a different
	// namespace, the deployer ensures a same-named ExternalName Service aliasing
	// to that namespace so the bare backend still resolves.
	staticGatewayService = "static-gateway"
	staticGatewayPort    = 8080
)

type Worker struct {
	Deployer  api.Deployer
	Client    *k8s.Client
	Namespace string // namespace deployments are created in (k8s client namespace)
	// DeployerNamespace (env DEPLOYER_NAMESPACE) is the namespace the shared
	// static-gateway runs in. Empty or equal to Namespace means the gateway is
	// co-located with deployments and the bare Service name resolves directly;
	// when it differs, a Static deploy ensures a `static-gateway` ExternalName
	// Service in Namespace aliasing to the gateway in this namespace.
	DeployerNamespace string
	RuntimeClass      string
	H2CP              bool
	Cert              bool // manage cert using cert manager
	CPULimit          string
	MemoryLimit       string
	// AccessVerifyURL is the base forward-auth target for Deployment Access
	// (env ACCESS_VERIFY_URL, default defaultAccessVerifyURL). It gates the
	// public default URL when Spec.Access.RequireGoogleLogin is set.
	AccessVerifyURL string

	// state
	location *api.LocationItem
	results  []*api.DeployerSetResultItem
}

// accessForwardAuth synthesizes the forward-auth config that gates a host with
// Deployment Access. It returns nil when access is off or absent, so the host
// stays ungated and public.
//
// When enabled it points parapet's in-cluster ForwardAuth at the access
// verifier, scoped to the gated deployment by its numeric id (deploymentID),
// which is exactly what the verifier's /verify?d= resolves. The resulting
// *api.RouteConfigForwardAuth is set on k8s.Ingress.Config.ForwardAuth so
// CreateIngress emits the parapet.moonrhythm.io/forward-auth annotation it
// already emits for user route forward-auth.
//
// It gates a deployment's PUBLIC default URL (WebService external + Static
// branch) and any user-created custom-domain route that targets the deployment
// (routeCreate passes the target deployment's id + access policy). The internal
// (parapet-internal) ingress and redirect routes are left ungated.
func (w *Worker) accessForwardAuth(deploymentID int64, access *api.DeploymentAccessConfig) *api.RouteConfigForwardAuth {
	if access == nil || !access.RequireGoogleLogin {
		return nil
	}
	return &api.RouteConfigForwardAuth{
		Target:              w.AccessVerifyURL + "?d=" + strconv.FormatInt(deploymentID, 10),
		AuthRequestHeaders:  []string{"Cookie"},
		AuthResponseHeaders: []string{"X-Auth-Email", "X-Auth-User"},
	}
}

// ensureStaticGatewayAlias makes the shared `static-gateway` Service resolvable
// from the deployment namespace when the gateway runs in a different namespace
// (DEPLOYER_NAMESPACE). A Static deployment's ingress backend references
// `static-gateway` by bare name, which the parapet controller only resolves
// within the ingress's own namespace; the ExternalName alias CNAMEs that bare
// name to static-gateway.<DeployerNamespace>.svc.cluster.local.
//
// It is a no-op when DEPLOYER_NAMESPACE is empty or equals the deployment
// namespace (the gateway is co-located, so the bare name already resolves — and
// aliasing it to itself would be a DNS loop).
func (w *Worker) ensureStaticGatewayAlias(ctx context.Context) error {
	if w.DeployerNamespace == "" || w.DeployerNamespace == w.Namespace {
		return nil
	}
	target := staticGatewayService + "." + w.DeployerNamespace + ".svc.cluster.local"
	return w.Client.EnsureExternalNameService(ctx, staticGatewayService, target, staticGatewayPort)
}

func (w *Worker) cpuLimit(limit string) string {
	if limit == "" || limit == "0" {
		return w.CPULimit
	}
	return limit
}

func (w *Worker) memoryLimit(memory string) string {
	if memory == "" || memory == "0" {
		return w.MemoryLimit
	}
	m, _ := resource.ParseQuantity(memory)
	if m.IsZero() {
		return "2Gi"
	}
	return m.String()
}

func (w *Worker) normalizeRequestCPU(request string) string {
	if request == "" {
		return defaultRequestCPU
	}
	if request == "0" {
		return defaultRequestCPU
	}
	_, err := resource.ParseQuantity(request)
	if err != nil {
		return defaultRequestCPU
	}
	return request
}

func (w *Worker) normalizeLimitCPU(limit string) string {
	// preserve old behavior when not setting limit, to support single-thread app
	if limit == "" || limit == "0" {
		return "1"
	}
	_, err := resource.ParseQuantity(w.cpuLimit(limit))
	if err != nil {
		return "1"
	}
	return limit
}

// target for 1 limit cpu (for single thread application)
func (w *Worker) targetCPUPercent(request, limit string) int {
	reqQuantity := resource.MustParse(w.normalizeRequestCPU(request))
	limQuantity := resource.MustParse(w.normalizeLimitCPU(limit))
	req := float64(reqQuantity.MilliValue()) / 1000
	lim := float64(limQuantity.MilliValue()) / 1000

	// 80 * limit / request
	return int(80 * lim / req)
}

func (w *Worker) Run() {
	ctx := context.Background()

	if w.location == nil {
		var err error
		w.location, err = w.Deployer.GetLocation(ctx, &api.Empty{})
		if err != nil {
			slog.Error("can not get location from api", "error", err)
			return
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	w.results = nil

	commands, err := w.Deployer.GetCommands(ctx, &api.Empty{})
	if err != nil {
		slog.Error("can not get commands from api", "error", err)
		return
	}

	if l := len(*commands); l > 0 {
		slog.Info("got commands", "count", l)
	}

	for _, x := range *commands {
		forceFlush := false

		switch {
		case x.PullSecretCreate != nil:
			x := x.PullSecretCreate
			w.pullSecretCreate(ctx, x)
		case x.PullSecretDelete != nil:
			x := x.PullSecretDelete
			w.pullSecretDelete(ctx, x)
		case x.WorkloadIdentityCreate != nil:
			x := x.WorkloadIdentityCreate
			w.workloadIdentityCreate(ctx, x)
		case x.WorkloadIdentityDelete != nil:
			x := x.WorkloadIdentityDelete
			w.workloadIdentityDelete(ctx, x)
		case x.DiskCreate != nil:
			x := x.DiskCreate
			w.diskCreate(ctx, x)
		case x.DiskDelete != nil:
			x := x.DiskDelete
			w.diskDelete(ctx, x)
		case x.DeploymentDeploy != nil:
			x := x.DeploymentDeploy
			w.deploymentDeploy(ctx, x)
		case x.DeploymentDelete != nil:
			x := x.DeploymentDelete
			w.deploymentDelete(ctx, x)
		case x.DeploymentPause != nil:
			x := x.DeploymentPause
			w.deploymentPause(ctx, x)
		case x.DeploymentCleanup != nil:
			x := x.DeploymentCleanup
			w.deploymentCleanupResource(ctx, x)
		case x.RouteCreate != nil:
			x := x.RouteCreate
			w.routeCreate(ctx, x)
			forceFlush = true
		case x.RouteDelete != nil:
			x := x.RouteDelete
			w.routeDelete(ctx, x)
			forceFlush = true
		case x.WAFSet != nil:
			x := x.WAFSet
			w.wafSet(ctx, x)
			forceFlush = true
		case x.WAFDelete != nil:
			x := x.WAFDelete
			w.wafDelete(ctx, x)
			forceFlush = true
		case x.CacheSet != nil:
			x := x.CacheSet
			w.cacheSet(ctx, x)
			forceFlush = true
		case x.CacheDelete != nil:
			x := x.CacheDelete
			w.cacheDelete(ctx, x)
			forceFlush = true
		case x.DomainCertCreate != nil:
			x := x.DomainCertCreate
			w.domainCertCreate(ctx, x)
			forceFlush = true
		case x.DomainCertDelete != nil:
			x := x.DomainCertDelete
			w.domainCertDelete(ctx, x)
			forceFlush = true
		}

		if forceFlush || len(w.results) > 3 {
			w.flushResults()
		}
	}

	w.flushResults()
}

func (w *Worker) flushResults() {
	if len(w.results) == 0 {
		return
	}
	slog.Info("flushing results", "count", len(w.results))

	results := api.DeployerSetResult(w.results)
	_, err := w.Deployer.SetResults(context.Background(), &results)
	if err != nil {
		slog.Error("can not set results", "error", err)
		return
	}
	w.results = nil
}

func (w *Worker) pullSecretCreate(ctx context.Context, it *api.DeployerCommandPullSecretCreate) {
	slog.Info("pullsecret: creating", "id", it.ID)

	id := pullSecretResourceID(it.ProjectID, it.Name)
	projectID := idString(it.ProjectID)

	jsonData, _ := base64.StdEncoding.DecodeString(it.Value)

	err := w.Client.CreateSecretDockerConfigJSON(ctx, k8s.SecretDockerConfigJSON{
		ID:        id,
		ProjectID: projectID,
		JSON:      jsonData,
	})
	if err != nil {
		slog.Error("pullsecret: creating error", "id", it.ID, "error", err)
		return
	}

	slog.Info("pullsecret: created", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		PullSecretCreate: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) pullSecretDelete(ctx context.Context, it *api.DeployerCommandMetadata) {
	slog.Info("pullsecret: deleting", "id", it.ID)

	err := w.Client.DeleteSecret(ctx, pullSecretResourceID(it.ProjectID, it.Name))
	if err != nil {
		slog.Error("pullsecret: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("pullsecret: deleted", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		PullSecretDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) deploymentDeploy(ctx context.Context, it *api.DeployerCommandDeploymentDeploy) {
	slog.Info("deployment: deploying", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)
	host := deploymentHost(it.DisplayName, it.Name, it.ProjectID)
	projectID := idString(it.ProjectID)

	var result api.DeployerSetResultItemDeploy

	f := func() error {
		// reset each retry
		result = api.DeployerSetResultItemDeploy{
			ID:       it.ID,
			Revision: it.Revision,
		}

		// Static deployments are served by the shared in-cluster static-gateway
		// reading the release directly from object storage. They produce NO
		// workload at all: no Deployment, Service, HPA, PVC, ConfigMap, or
		// pull-secret. The only k8s objects are the two default-URL Ingresses
		// (public + internal), both backed by the shared static-gateway Service
		// with the release prefix carried in upstream-path — the exact
		// shared-backend shape ipfs:// uses (see routeCreate / main.go ~1020).
		// Handle it here, before the ConfigMap creation and the container-type
		// branches, and return early so it never falls through into workload
		// logic.
		if it.Type == api.DeploymentTypeStatic {
			upstreamPath := "/" + staticSitePrefix(it)

			// When the static-gateway runs in another namespace
			// (DEPLOYER_NAMESPACE), make its bare Service name resolve from the
			// deployment namespace before the ingress references it. No-op when
			// the gateway is co-located.
			if err := w.ensureStaticGatewayAlias(ctx); err != nil {
				slog.Error("deployment: ensuring static-gateway alias error", "id", it.ID, "error", err)
				return err
			}

			// Static sites have no pod/Service — they are served by the shared
			// static-gateway — so there is no internal (parapet-internal)
			// ingress, only the public default URL.
			err := w.Client.CreateIngress(ctx, k8s.Ingress{
				ID:           id,
				Service:      staticGatewayService,
				ProjectID:    projectID,
				Domain:       fmt.Sprintf("%s%s", host, w.location.DomainSuffix),
				Path:         "/",
				UpstreamHost: staticGatewayService,
				UpstreamPath: upstreamPath,
				// Deployment Access: gate the public default URL when Require
				// Google login is on. Static has no internal ingress, so this
				// public ingress is the only surface to gate.
				Config: api.RouteConfig{ForwardAuth: w.accessForwardAuth(it.ID, it.Spec.Access)},
			})
			if err != nil {
				slog.Error("deployment: creating external ingress error", "id", it.ID, "error", err)
				return err
			}

			// Reconcile the immutable per-release Ingresses (B2) alongside the
			// always-current default URL above. Best-effort: the per-release URLs
			// are a secondary surface (the default URL above IS the user's
			// deployment), so a reconcile failure — logged with specifics inside —
			// must not fail the deploy. The set is reconciled again on every
			// deploy, so it self-heals on the next one.
			if err := w.reconcilePinnedReleaseIngresses(ctx, it, id, projectID); err != nil {
				slog.Error("deployment: reconciling pinned ingresses error", "id", it.ID, "error", err)
			}

			result.Success = true
			return nil
		}

		sidecarConfigs := lo.Map(it.Spec.Sidecars, func(x *api.Sidecar, _ int) *api.SidecarConfig {
			return x.Config()
		})

		configMapData, bindData, sidecarBinds := prepareMountData(it.Spec.MountData, sidecarConfigs)
		sidecars := buildSidecars(sidecarConfigs, sidecarBinds)
		cm := k8s.ConfigMap{
			ID:        id,
			ProjectID: projectID,
			Data:      configMapData,
		}

		err := w.Client.CreateConfigMap(ctx, cm)
		if err != nil {
			slog.Error("deployment: creating config map error", "id", it.ID, "error", err)
			return err
		}

		switch it.Type {
		case api.DeploymentTypeWebService:
			h2cp := w.H2CP && (it.Spec.Protocol == "http" || it.Spec.Protocol == "https")

			deploy := k8s.Deployment{
				ID:            id,
				ProjectID:     projectID,
				Name:          it.Name,
				DisplayName:   it.DisplayName,
				Revision:      it.Revision,
				Image:         it.Spec.Image,
				Env:           it.Spec.Env,
				Command:       it.Spec.Command,
				Args:          it.Spec.Args,
				Replicas:      it.Spec.MinReplicas,
				ExposePort:    it.Spec.Port,
				Annotations:   it.Spec.Annotations,
				RequestCPU:    w.normalizeRequestCPU(it.Spec.CPU),
				RequestMemory: it.Spec.Memory,
				LimitCPU:      w.cpuLimit(it.Spec.CPULimit),
				LimitMemory:   w.memoryLimit(it.Spec.Memory),
				RuntimeClass:  w.RuntimeClass,
				Pool: k8s.PoolConfig{
					Name:  it.BillingConfig.Pool,
					Share: it.BillingConfig.SharePool,
				},
				SA:         resourceID(it.ProjectID, it.Spec.WorkloadIdentityName),
				PullSecret: pullSecretResourceID(it.ProjectID, it.Spec.PullSecretName),
				Disk: k8s.Disk{
					Name:      resourceID(it.ProjectID, it.Spec.DiskName),
					MountPath: it.Spec.DiskMountPath,
					SubPath:   it.Spec.DiskSubPath,
				},
				BindConfigMap: bindData,
				H2CP:          h2cp,
				Protocol:      string(it.Spec.Protocol),
				Sidecars:      sidecars,
				ForceSpot:     it.BillingConfig.ForceSpot,
				HealthCheck:   it.Spec.HealthCheck,
			}

			err = w.Client.CreateDeployment(ctx, deploy)
			if err != nil {
				slog.Error("deployment: creating deployment error", "id", it.ID, "error", err)
				return err
			}

			err = w.Client.CreateService(ctx, k8s.Service{
				ID:        id,
				ProjectID: projectID,
				Port:      it.Spec.Port,
				Protocol:  string(it.Spec.Protocol),
				H2CP:      h2cp,
			})
			if err != nil {
				slog.Error("deployment: creating service error", "id", it.ID, "error", err)
				return err
			}

			// internal ingress
			err = w.Client.CreateIngress(ctx, k8s.Ingress{
				ID:        id + "-internal",
				Service:   id,
				ProjectID: projectID,
				Domain:    fmt.Sprintf("%s.internal%s", host, w.location.DomainSuffix),
				Path:      "/",
				Internal:  true,
			})
			if err != nil {
				slog.Error("deployment: creating internal ingress error", "id", it.ID, "error", err)
				return err
			}

			if it.Spec.Internal {
				// delete external ingress
				err = w.Client.DeleteIngress(ctx, id)
				if err != nil {
					slog.Error("deployment: deleting external ingress error", "id", it.ID, "error", err)
					return err
				}
			} else {
				// external ingress
				err = w.Client.CreateIngress(ctx, k8s.Ingress{
					ID:        id,
					Service:   id,
					ProjectID: projectID,
					Domain:    fmt.Sprintf("%s%s", host, w.location.DomainSuffix),
					Path:      "/",
					// Deployment Access: gate the public default URL when
					// Require Google login is on. The internal ingress above is
					// left ungated per spec §10.
					Config: api.RouteConfig{ForwardAuth: w.accessForwardAuth(it.ID, it.Spec.Access)},
				})
				if err != nil {
					slog.Error("deployment: creating external ingress error", "id", it.ID, "error", err)
					return err
				}
			}

			if it.Spec.MinReplicas != it.Spec.MaxReplicas {
				err = w.Client.CreateHorizontalPodAutoscaler(ctx, k8s.HorizontalPodAutoscaler{
					ID:            id,
					ProjectID:     projectID,
					MinReplicas:   it.Spec.MinReplicas,
					MaxReplicas:   it.Spec.MaxReplicas,
					TargetPercent: w.targetCPUPercent(it.Spec.CPU, it.Spec.CPULimit),
				})
				if err != nil {
					slog.Error("deployment: creating hpa error", "id", it.ID, "error", err)
					return err
				}
			} else {
				err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
				if err != nil {
					slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
					return err
				}
			}
		case api.DeploymentTypeWorker:
			deploy := k8s.Deployment{
				ID:            id,
				ProjectID:     projectID,
				Name:          it.Name,
				DisplayName:   it.DisplayName,
				Revision:      it.Revision,
				Image:         it.Spec.Image,
				Env:           it.Spec.Env,
				Command:       it.Spec.Command,
				Args:          it.Spec.Args,
				Replicas:      it.Spec.MinReplicas,
				Annotations:   it.Spec.Annotations,
				RequestCPU:    w.normalizeRequestCPU(it.Spec.CPU),
				RequestMemory: it.Spec.Memory,
				LimitCPU:      w.cpuLimit(it.Spec.CPULimit),
				LimitMemory:   w.memoryLimit(it.Spec.Memory),
				RuntimeClass:  w.RuntimeClass,
				Pool: k8s.PoolConfig{
					Name:  it.BillingConfig.Pool,
					Share: it.BillingConfig.SharePool,
				},
				SA:         resourceID(it.ProjectID, it.Spec.WorkloadIdentityName),
				PullSecret: pullSecretResourceID(it.ProjectID, it.Spec.PullSecretName),
				Disk: k8s.Disk{
					Name:      resourceID(it.ProjectID, it.Spec.DiskName),
					MountPath: it.Spec.DiskMountPath,
					SubPath:   it.Spec.DiskSubPath,
				},
				BindConfigMap: bindData,
				Sidecars:      sidecars,
				ForceSpot:     it.BillingConfig.ForceSpot,
				HealthCheck:   it.Spec.HealthCheck,
			}

			err = w.Client.CreateDeployment(ctx, deploy)
			if err != nil {
				slog.Error("deployment: creating deployment error", "id", it.ID, "error", err)
				return err
			}

			if it.Spec.MinReplicas != it.Spec.MaxReplicas {
				err = w.Client.CreateHorizontalPodAutoscaler(ctx, k8s.HorizontalPodAutoscaler{
					ID:            id,
					ProjectID:     projectID,
					MinReplicas:   it.Spec.MinReplicas,
					MaxReplicas:   it.Spec.MaxReplicas,
					TargetPercent: w.targetCPUPercent(it.Spec.CPU, it.Spec.CPULimit),
				})
				if err != nil {
					slog.Error("deployment: creating hpa error", "id", it.ID, "error", err)
					return err
				}
			} else {
				err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
				if err != nil {
					slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
					return err
				}
			}
		case api.DeploymentTypeCronJob:
			cj := k8s.CronJob{
				ID:            id,
				ProjectID:     projectID,
				Name:          it.Name,
				DisplayName:   it.DisplayName,
				Revision:      it.Revision,
				Image:         it.Spec.Image,
				Env:           it.Spec.Env,
				Command:       it.Spec.Command,
				Args:          it.Spec.Args,
				Schedule:      it.Spec.Schedule,
				RequestCPU:    w.normalizeRequestCPU(it.Spec.CPU),
				RequestMemory: it.Spec.Memory,
				LimitCPU:      w.cpuLimit(it.Spec.CPULimit),
				LimitMemory:   w.memoryLimit(it.Spec.Memory),
				RuntimeClass:  w.RuntimeClass,
				Pool: k8s.PoolConfig{
					Name:  it.BillingConfig.Pool,
					Share: it.BillingConfig.SharePool,
				},
				SA:         resourceID(it.ProjectID, it.Spec.WorkloadIdentityName),
				PullSecret: pullSecretResourceID(it.ProjectID, it.Spec.PullSecretName),
				Disk: k8s.Disk{
					Name:      resourceID(it.ProjectID, it.Spec.DiskName),
					MountPath: it.Spec.DiskMountPath,
					SubPath:   it.Spec.DiskSubPath,
				},
				BindConfigMap: bindData,
				Sidecars:      sidecars,
			}

			err = w.Client.CreateCronJob(ctx, cj)
			if err != nil {
				slog.Error("deployment: creating cronjob error", "id", it.ID, "error", err)
				return err
			}
		case api.DeploymentTypeTCPService:
			deploy := k8s.Deployment{
				ID:            id,
				ProjectID:     projectID,
				Name:          it.Name,
				DisplayName:   it.DisplayName,
				Revision:      it.Revision,
				Image:         it.Spec.Image,
				Env:           it.Spec.Env,
				Command:       it.Spec.Command,
				Args:          it.Spec.Args,
				Replicas:      1,
				ExposePort:    it.Spec.Port,
				Annotations:   it.Spec.Annotations,
				RequestCPU:    w.normalizeRequestCPU(it.Spec.CPU),
				RequestMemory: it.Spec.Memory,
				LimitCPU:      w.cpuLimit(it.Spec.CPULimit),
				LimitMemory:   w.memoryLimit(it.Spec.Memory),
				RuntimeClass:  w.RuntimeClass,
				Pool: k8s.PoolConfig{
					Name:  it.BillingConfig.Pool,
					Share: it.BillingConfig.SharePool,
				},
				SA:         resourceID(it.ProjectID, it.Spec.WorkloadIdentityName),
				PullSecret: pullSecretResourceID(it.ProjectID, it.Spec.PullSecretName),
				Disk: k8s.Disk{
					Name:      resourceID(it.ProjectID, it.Spec.DiskName),
					MountPath: it.Spec.DiskMountPath,
					SubPath:   it.Spec.DiskSubPath,
				},
				BindConfigMap: bindData,
				Sidecars:      sidecars,
				ForceSpot:     it.BillingConfig.ForceSpot,
				HealthCheck:   it.Spec.HealthCheck,
			}

			err = w.Client.CreateDeployment(ctx, deploy)
			if err != nil {
				slog.Error("deployment: creating deployment error", "id", it.ID, "error", err)
				return err
			}

			err = w.Client.CreateService(ctx, k8s.Service{
				ID:         id,
				ProjectID:  projectID,
				Port:       it.Spec.Port,
				Protocol:   string(it.Spec.Protocol),
				ExposeNode: true,
			})
			if err != nil {
				slog.Error("deployment: creating service error", "id", it.ID, "error", err)
				return err
			}

			time.Sleep(time.Second)

			nodePort, err := w.Client.GetNodePort(ctx, id)
			if err != nil {
				slog.Error("deployment: getting service node port error", "id", it.ID, "error", err)
				return err
			}

			result.NodePort = &nodePort
		case api.DeploymentTypeInternalTCPService:
			deploy := k8s.Deployment{
				ID:            id,
				ProjectID:     projectID,
				Name:          it.Name,
				DisplayName:   it.DisplayName,
				Revision:      it.Revision,
				Image:         it.Spec.Image,
				Env:           it.Spec.Env,
				Command:       it.Spec.Command,
				Args:          it.Spec.Args,
				Replicas:      it.Spec.MinReplicas,
				ExposePort:    it.Spec.Port,
				Annotations:   it.Spec.Annotations,
				RequestCPU:    w.normalizeRequestCPU(it.Spec.CPU),
				RequestMemory: it.Spec.Memory,
				LimitCPU:      w.cpuLimit(it.Spec.CPULimit),
				LimitMemory:   w.memoryLimit(it.Spec.Memory),
				RuntimeClass:  w.RuntimeClass,
				Pool: k8s.PoolConfig{
					Name:  it.BillingConfig.Pool,
					Share: it.BillingConfig.SharePool,
				},
				SA:         resourceID(it.ProjectID, it.Spec.WorkloadIdentityName),
				PullSecret: pullSecretResourceID(it.ProjectID, it.Spec.PullSecretName),
				Disk: k8s.Disk{
					Name:      resourceID(it.ProjectID, it.Spec.DiskName),
					MountPath: it.Spec.DiskMountPath,
					SubPath:   it.Spec.DiskSubPath,
				},
				BindConfigMap: bindData,
				Sidecars:      sidecars,
				ForceSpot:     it.BillingConfig.ForceSpot,
				HealthCheck:   it.Spec.HealthCheck,
			}

			err = w.Client.CreateDeployment(ctx, deploy)
			if err != nil {
				slog.Error("deployment: creating deployment error", "id", it.ID, "error", err)
				return err
			}

			err = w.Client.CreateService(ctx, k8s.Service{
				ID:        id,
				ProjectID: projectID,
				Port:      it.Spec.Port,
				Protocol:  string(it.Spec.Protocol),
			})
			if err != nil {
				slog.Error("deployment: creating service error", "id", it.ID, "error", err)
				return err
			}

			if it.Spec.MinReplicas != it.Spec.MaxReplicas {
				err = w.Client.CreateHorizontalPodAutoscaler(ctx, k8s.HorizontalPodAutoscaler{
					ID:            id,
					ProjectID:     projectID,
					MinReplicas:   it.Spec.MinReplicas,
					MaxReplicas:   it.Spec.MaxReplicas,
					TargetPercent: w.targetCPUPercent(it.Spec.CPU, it.Spec.CPULimit),
				})
				if err != nil {
					slog.Error("deployment: creating hpa error", "id", it.ID, "error", err)
					return err
				}
			} else {
				err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
				if err != nil {
					slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
					return err
				}
			}
		default:
			return fmt.Errorf("unknown type")
		}

		result.Success = true

		return nil
	}

	err := f()
	if isRetryable(err) {
		slog.Error("deployment: got retryable error", "id", it.ID, "error", err)
		return
	}
	if err != nil {
		slog.Error("deployment: error", "id", it.ID, "error", err)

		result.Success = false
		result.NodePort = nil
		w.results = append(w.results, &api.DeployerSetResultItem{
			DeploymentDeploy: &result,
		})
		return
	}

	slog.Info("deployment: deployed", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DeploymentDeploy: &result,
	})
}

func (w *Worker) deploymentDelete(ctx context.Context, it *api.DeployerCommandDeploymentMetadata) {
	slog.Info("deployment: deleting", "id", it.ID)

	err := w.deploymentRemoveK8SResource(ctx, it)
	if err != nil {
		slog.Error("deployment: k8s remove resource error", "id", it.ID, "error", err)
		return
	}

	slog.Info("deployment: deleted", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DeploymentDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) deploymentPause(ctx context.Context, it *api.DeployerCommandDeploymentMetadata) {
	slog.Info("deployment: pausing", "id", it.ID)

	err := w.deploymentRemoveK8SResource(ctx, it)
	if err != nil {
		slog.Error("deployment: k8s remove resource error", "id", it.ID, "error", err)
		return
	}

	slog.Info("deployment: paused", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DeploymentPause: &api.DeployerSetResultItemDeployment{
			ID:       it.ID,
			Revision: it.Revision,
		},
	})
}

func (w *Worker) deploymentCleanupResource(ctx context.Context, it *api.DeployerCommandDeploymentMetadata) {
	slog.Info("deployment: cleanup resource", "id", it.ID)

	err := w.deploymentRemoveK8SResource(ctx, it)
	if err != nil {
		slog.Error("deployment: k8s remove resource error", "id", it.ID, "error", err)
		return
	}

	slog.Info("deployment: cleanup resource", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DeploymentCleanup: &api.DeployerSetResultItemDeployment{
			ID:       it.ID,
			Revision: it.Revision,
		},
	})
}

func (w *Worker) deploymentRemoveK8SResource(ctx context.Context, it *api.DeployerCommandDeploymentMetadata) error {
	slog.Info("deployment: removing k8s resource", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)

	var err error
	switch it.Type {
	case api.DeploymentTypeStatic:
		// Static deployments create only the public default-URL Ingress (no
		// Deployment/Service/HPA/PVC/ConfigMap). Remove it, plus — defensively
		// — the "-internal" ingress an earlier deployer version used to create,
		// so a static site deployed before this change is torn down cleanly.
		// Both deletes are delete-if-exists (no-op on NotFound), so this can't
		// error.
		err = w.Client.DeleteIngress(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting ingress error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteIngress(ctx, id+"-internal")
		if err != nil {
			slog.Error("deployment: deleting internal ingress error", "id", it.ID, "error", err)
			return err
		}

		// Drop every release-pinned Ingress (<id>-rel-*) so TTL expiry and
		// explicit delete tear down the immutable per-release URLs with the
		// deployment (SPEC-preview-environments.md §B.3).
		projectID := idString(it.ProjectID)
		pinned, err := w.Client.ListPinnedIngressIDs(ctx, id, projectID)
		if err != nil {
			slog.Error("deployment: listing pinned ingresses error", "id", it.ID, "error", err)
			return err
		}
		for _, pinID := range pinned {
			if err = w.Client.DeleteIngress(ctx, pinID); err != nil {
				slog.Error("deployment: deleting pinned ingress error", "id", it.ID, "ingress", pinID, "error", err)
				return err
			}
		}

		return nil
	case api.DeploymentTypeWebService:
		err = w.Client.DeleteDeployment(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting deployment error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteIngress(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting ingress error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteIngress(ctx, id+"-internal")
		if err != nil {
			slog.Error("deployment: deleting internal ingress error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteService(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting service error", "id", it.ID, "error", err)
			return err
		}
	case api.DeploymentTypeWorker:
		err = w.Client.DeleteDeployment(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting deployment error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
			return err
		}
	case api.DeploymentTypeCronJob:
		err = w.Client.DeleteCronJob(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting cronjob error", "id", it.ID, "error", err)
			return err
		}
	case api.DeploymentTypeTCPService:
		err = w.Client.DeleteDeployment(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting deployment error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteService(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting service error", "id", it.ID, "error", err)
			return err
		}
	case api.DeploymentTypeInternalTCPService:
		err = w.Client.DeleteDeployment(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting deployment error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteHorizontalPodAutoscaler(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting hpa error", "id", it.ID, "error", err)
			return err
		}

		err = w.Client.DeleteService(ctx, id)
		if err != nil {
			slog.Error("deployment: deleting service error", "id", it.ID, "error", err)
			return err
		}
	default:
		return fmt.Errorf("unknown type")
	}

	err = w.Client.DeleteConfigMap(ctx, id)
	if err != nil {
		slog.Error("deployment: deleting config map error", "id", it.ID, "error", err)
		return err
	}

	return nil
}

func (w *Worker) diskCreate(ctx context.Context, it *api.DeployerCommandDiskCreate) {
	slog.Info("disk: creating", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)
	projectID := idString(it.ProjectID)

	err := w.Client.CreatePersistentVolumeClaim(ctx, k8s.PersistentVolumeClaim{
		ID:        id,
		ProjectID: projectID,
		Size:      it.Size,
		// StorageClass: "ssd",
	})
	if err != nil {
		slog.Error("disk: creating error", "id", it.ID, "error", err)
		return
	}

	slog.Info("disk: created", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DiskCreate: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) diskDelete(ctx context.Context, it *api.DeployerCommandMetadata) {
	slog.Info("disk: deleting", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)

	err := w.Client.DeletePersistentVolumeClaim(ctx, id)
	if err != nil {
		slog.Error("disk: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("disk: deleted", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		DiskDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) routeCreate(ctx context.Context, it *api.DeployerCommandRouteCreate) {
	slog.Info("route: creating", "id", it.ID)

	ingID := fmt.Sprintf("domain-%d", it.ID)
	domainID := normalizeDomain(it.Domain)

	projectID := idString(it.ProjectID)
	var secret string
	if w.Cert {
		secret = "tls-" + domainID
	}

	switch {
	default:
		ing := k8s.Ingress{
			ID:        ingID,
			Service:   resourceID(it.ProjectID, it.Target), // TODO: unsupport when remove non-prefix target
			ProjectID: projectID,
			Domain:    it.Domain,
			Path:      it.Path,
			Secret:    secret,
			Config:    it.Config,
		}
		switch {
		case strings.HasPrefix(it.Target, "deployment://"):
			// Deployment Access: when the target deployment is gated, stamp the
			// same forward-auth gate as its default-URL ingress (.../verify?d=<id>).
			// parapet allows only one forward-auth per ingress, so the access gate
			// takes precedence over a user-supplied route forward-auth — it is a
			// security control, not an app-level one. When access is off this is
			// nil and the route's own Config (set above) stands, so toggling access
			// off restores any user forward-auth and clears the gate annotation.
			if fa := w.accessForwardAuth(it.TargetDeploymentID, it.Access); fa != nil {
				if it.Config.ForwardAuth != nil {
					slog.Warn("route: deployment access overrides route forward-auth",
						"id", it.ID, "domain", it.Domain)
				}
				ing.Config.ForwardAuth = fa
			}
			if it.TargetType == api.DeploymentTypeStatic {
				// A Static deployment has no per-deployment Service — it is
				// served by the shared static-gateway reading the release from
				// object storage. Back the ingress with static-gateway and put
				// the release prefix in upstream-path, mirroring the static
				// deployment's own default-URL ingress (see the
				// DeploymentTypeStatic branch in deploymentDeploy). The apiserver
				// supplies the prefix as SitePrefix on the route-create command.
				ing.Service = "static-gateway"
				ing.UpstreamHost = "static-gateway"
				ing.UpstreamPath = "/" + strings.Trim(it.SitePrefix, "/")
				break
			}
			ing.Service = resourceID(it.ProjectID, strings.TrimPrefix(it.Target, "deployment://"))
		case strings.HasPrefix(it.Target, "ipfs://"):
			ing.Service = "ipfs-gateway"
			ing.UpstreamHost = "ipfs-gateway"
			ing.UpstreamPath = "/ipfs/" + strings.TrimPrefix(it.Target, "ipfs://")
		case strings.HasPrefix(it.Target, "ipns://"):
			ing.Service = "ipfs-gateway"
			ing.UpstreamHost = "ipfs-gateway"
			ing.UpstreamPath = "/ipns/" + strings.TrimPrefix(it.Target, "ipns://")
		case strings.HasPrefix(it.Target, "dnslink://"):
			ing.Service = "ipfs-gateway"
		case strings.HasPrefix(it.Target, "http://"):
			// External upstream: customer-owned server by IP, fronted by our
			// edge + WAF. Back it with a selector-less Service + Endpoints so
			// parapet routes to the IP like any other backend.
			host, port, ok := parseExternalHTTPTarget(it.Target)
			if !ok {
				slog.Error("route: invalid external target", "id", it.ID, "target", it.Target)
				return
			}
			extID := resourceID(it.ProjectID, "ext-"+idString(it.ID))
			err := w.Client.CreateExternalUpstream(ctx, k8s.ExternalUpstream{
				ID:        extID,
				ProjectID: projectID,
				IP:        host,
				Port:      port,
			})
			if err != nil {
				slog.Error("route: creating external upstream error", "id", it.ID, "error", err)
				return
			}
			ing.Service = extID
			// Optional Host-header override (RouteConfig.Host). External backends
			// commonly virtual-host on Host; default is the original request Host.
			if it.Config.Host != "" {
				ing.UpstreamHost = it.Config.Host
			}
		}

		err := w.Client.CreateIngress(ctx, ing)
		if err != nil {
			slog.Error("route: creating ingress error", "id", it.ID, "error", err)
			return
		}
	case strings.HasPrefix(it.Target, "redirect://"):
		target := strings.TrimPrefix(it.Target, "redirect://")

		err := w.Client.CreateRedirectIngress(ctx, k8s.RedirectIngress{
			ID:        ingID,
			ProjectID: projectID,
			Domain:    it.Domain,
			Path:      it.Path,
			Target:    target,
			Secret:    secret,
			Config:    it.Config,
		})
		if err != nil {
			slog.Error("route: creating redirect ingress error", "id", it.ID, "error", err)
			return
		}
	}

	// Cert lifecycle now lives on the Domain row (see domainCertCreate /
	// domainCertDelete) so we only ever request a Let's Encrypt cert after
	// DNS has been verified. The ingress above references
	// `tls-<normalizeDomain(it.Domain)>` regardless; k8s tolerates a
	// not-yet-existent secret, and cert-manager populates it once
	// verification + issuance complete.

	slog.Info("route: created", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		RouteCreate: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) routeDelete(ctx context.Context, it *api.DeployerCommandRouteDelete) {
	slog.Info("route: deleting", "id", it.ID)

	ingID := fmt.Sprintf("domain-%d", it.ID)

	err := w.Client.DeleteIngress(ctx, ingID)
	if err != nil {
		slog.Error("route: deleting ingress error", "id", it.ID, "error", err)
		return
	}

	// External routes (http://<ip>) also create a backing Service + Endpoints
	// named ext-<id>-<projectID>. The delete is unconditional and idempotent:
	// for non-external routes nothing exists, so this is a no-op.
	extID := resourceID(it.ProjectID, "ext-"+idString(it.ID))
	if err := w.Client.DeleteExternalUpstream(ctx, extID); err != nil {
		slog.Error("route: deleting external upstream error", "id", it.ID, "error", err)
		return
	}

	// Cert is no longer torn down with the route — it now follows the
	// Domain row (see domainCertDelete). Deleting the last route on a domain
	// used to also delete the cert, which produced LE-quota burn cycles when
	// users rapidly re-created routes; keeping the cert tied to the domain
	// avoids that.

	slog.Info("route: deleted", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		RouteDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) wafSet(ctx context.Context, it *api.DeployerCommandWAFSet) {
	slog.Info("waf: setting", "id", it.ID, "zone", it.ZoneID)

	projectID := idString(it.ProjectID)

	err := w.Client.CreateWAFZone(ctx, projectID, it.ZoneID, it.RateLimitZoneID, it.Rules, it.Limits)
	if err != nil {
		slog.Error("waf: setting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("waf: set", "id", it.ID, "zone", it.ZoneID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		WAFSet: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) wafDelete(ctx context.Context, it *api.DeployerCommandWAFDelete) {
	slog.Info("waf: deleting", "id", it.ID, "zone", it.ZoneID)

	projectID := idString(it.ProjectID)

	err := w.Client.DeleteWAFZone(ctx, projectID, it.ZoneID, it.RateLimitZoneID)
	if err != nil {
		slog.Error("waf: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("waf: deleted", "id", it.ID, "zone", it.ZoneID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		WAFDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) cacheSet(ctx context.Context, it *api.DeployerCommandCacheSet) {
	slog.Info("cache: setting", "id", it.ID, "zone", it.ZoneID)

	projectID := idString(it.ProjectID)

	err := w.Client.CreateCacheZone(ctx, projectID, it.ZoneID, it.Overrides)
	if err != nil {
		slog.Error("cache: setting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("cache: set", "id", it.ID, "zone", it.ZoneID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		CacheSet: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) cacheDelete(ctx context.Context, it *api.DeployerCommandCacheDelete) {
	slog.Info("cache: deleting", "id", it.ID, "zone", it.ZoneID)

	projectID := idString(it.ProjectID)

	err := w.Client.DeleteCacheZone(ctx, projectID, it.ZoneID)
	if err != nil {
		slog.Error("cache: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("cache: deleted", "id", it.ID, "zone", it.ZoneID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		CacheDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

// domainCertCreate issues the cert-manager Certificate for a non-CDN domain
// that's just passed DNS verification. The cert ID is normalizeDomain(domain)
// so it matches the secret name (`tls-<normalizeDomain>`) that route ingresses
// already reference — no rename / migration of existing certs needed.
func (w *Worker) domainCertCreate(ctx context.Context, it *api.DeployerCommandDomainCertCreate) {
	slog.Info("domain cert: creating", "id", it.ID, "domain", it.Domain)

	if !w.Cert {
		slog.Info("domain cert: skip (disabled)", "id", it.ID, "domain", it.Domain)
		// Report ready so the apiserver advances cert_status — a clean cluster
		// (no cert-manager) has nothing to issue, so treat it as issued;
		// otherwise the deployer-poll loops forever.
		ready := true
		w.results = append(w.results, &api.DeployerSetResultItem{
			DomainCertCreate: &api.DeployerSetResultItemDomainCert{ID: it.ID, Ready: &ready},
		})
		return
	}

	certID := normalizeDomain(it.Domain)
	projectID := idString(it.ProjectID)
	ready, err := w.Client.CreateCertificate(ctx, k8s.Certificate{
		ID:        certID,
		ProjectID: projectID,
		Domain:    it.Domain,
		Wildcard:  it.Wildcard,
	})
	if err != nil {
		slog.Error("domain cert: creating error", "id", it.ID, "error", err)
		return
	}

	slog.Info("domain cert: created", "id", it.ID, "ready", ready)
	w.results = append(w.results, &api.DeployerSetResultItem{
		DomainCertCreate: &api.DeployerSetResultItemDomainCert{ID: it.ID, Ready: &ready},
	})
}

// domainCertDelete tears down the cert-manager Certificate for a domain
// whose DNS no longer points at us (or that's being deleted entirely).
func (w *Worker) domainCertDelete(ctx context.Context, it *api.DeployerCommandDomainCertDelete) {
	slog.Info("domain cert: deleting", "id", it.ID, "domain", it.Domain)

	if !w.Cert {
		w.results = append(w.results, &api.DeployerSetResultItem{
			DomainCertDelete: &api.DeployerSetResultItemGeneral{ID: it.ID},
		})
		return
	}

	certID := normalizeDomain(it.Domain)
	err := w.Client.DeleteCertificate(ctx, certID)
	if err != nil {
		slog.Error("domain cert: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("domain cert: deleted", "id", it.ID)
	w.results = append(w.results, &api.DeployerSetResultItem{
		DomainCertDelete: &api.DeployerSetResultItemGeneral{ID: it.ID},
	})
}

func (w *Worker) workloadIdentityCreate(ctx context.Context, it *api.DeployerCommandWorkloadIdentityCreate) {
	slog.Info("workloadidentity: creating", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)
	projectID := idString(it.ProjectID)

	err := w.Client.CreateServiceAccount(ctx, k8s.ServiceAccount{
		ID:        id,
		ProjectID: projectID,
		GSA:       it.GSA,
	})
	if err != nil {
		slog.Error("workloadidentity: creating error", "id", it.ID, "error", err)
		return
	}

	slog.Info("workloadidentity: created", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		WorkloadIdentityCreate: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

func (w *Worker) workloadIdentityDelete(ctx context.Context, it *api.DeployerCommandMetadata) {
	slog.Info("workloadidentity: deleting", "id", it.ID)

	id := resourceID(it.ProjectID, it.Name)

	err := w.Client.DeleteServiceAccount(ctx, id)
	if err != nil {
		slog.Error("workloadidentity: deleting error", "id", it.ID, "error", err)
		return
	}

	slog.Info("workloadidentity: deleted", "id", it.ID)

	w.results = append(w.results, &api.DeployerSetResultItem{
		WorkloadIdentityDelete: &api.DeployerSetResultItemGeneral{
			ID: it.ID,
		},
	})
}

// func (w *Worker) database(ctx context.Context) {
// 	list, err := server.ListDatabases(ctx,
// 		server.Status(api.Pending),
// 		server.InLocation(w.Location.ID),
// 		server.OrderByCreatedAtAsc(),
// 	)
// 	if err != nil {
// 		logs.Errorf("database: can not list; %v", err)
// 		logs.Report(err)
// 		return
// 	}
//
// 	for _, it := range list {
// 		// processName := "database/" + it.ResourceID()
// 		// _, ok := w.processList.Load(processName)
// 		// if ok {
// 		// 	continue
// 		// }
// 		//
// 		// w.incrProcess()
// 		// w.processList.Store(processName, struct{}{})
//
// 		// it := it
//
// 		// go func() {
// 		// defer func() {
// 		// 	w.processList.Delete(processName)
// 		// 	w.decrProcess()
// 		// }()
//
// 		switch it.Action {
// 		case api.Create:
// 			w.databaseCreate(ctx, it)
// 		case api.Delete:
// 			w.databaseDelete(ctx, it)
// 		}
// 		// }()
// 	}
// }

// func (w *Worker) databaseCreate(ctx context.Context, it *server.Database) {
// 	logs.Infof("database: %d, creating...", it.ID)
//
// 	id := it.ResourceID()
// 	projectID := idString(it.ProjectID)
//
// 	f := func() error {
// 		status, err := server.GetDatabaseStatus(ctx, it.ID)
// 		if err != nil {
// 			return err
// 		}
// 		if status != api.Pending {
// 			return nil
// 		}
//
// 		switch it.DBName {
// 		case "redis", "redislabs/redisearch":
// 			d := redis.Deployer{Client: w.Client}
// 			err = d.Deploy(ctx, redis.Config{
// 				ID:         id,
// 				Name:       it.Name,
// 				ProjectID:  projectID,
// 				Image:      it.DBName + ":" + latestIfEmpty(it.DBVersion),
// 				Args:       it.Args,
// 				DBSize:     parseInt64(it.Config["db_size"]),
// 				Databases:  parseInt64(it.Config["databases"]),
// 				RequestCPU: normalizeRequestCPU(it.CPU),
// 				LimitCPU:   defaultLimitCPU,
// 				Password:   it.Config["password"],
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		default:
// 			return fmt.Errorf("invalid db name")
// 		}
//
// 		err = server.SetDatabaseStatus(ctx, it.ID, api.Success)
// 		if err != nil {
// 			return err
// 		}
// 		err = server.StampDatabaseSuccess(ctx, it.ID)
// 		if err != nil {
// 			return err
// 		}
//
// 		return nil
// 	}
//
// 	err := f()
// 	if err != nil {
// 		if err := server.SetDatabaseStatus(ctx, it.ID, api.Error); err != nil {
// 			logs.Errorf("database: can not set error status for %d; %v", it.ID, err)
// 		}
// 		return
// 	}
//
// 	logs.Infof("database: %d, created...", it.ID)
// }
//
// func (w *Worker) databaseDelete(ctx context.Context, it *server.Database) {
// 	logs.Infof("database: %d, deleting...", it.ID)
//
// 	id := it.ResourceID()
//
// 	err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
// 		status, err := server.GetDatabaseStatus(ctx, it.ID)
// 		if err != nil {
// 			return err
// 		}
// 		if status != api.Pending {
// 			return nil
// 		}
//
// 		err = server.RemoveDatabase(ctx, it.ID)
// 		if err != nil {
// 			return err
// 		}
//
// 		switch it.DBName {
// 		case "redis":
// 			d := redis.Deployer{Client: w.Client}
// 			err = d.Delete(ctx, id)
// 			if err != nil {
// 				return err
// 			}
// 		default:
// 			return fmt.Errorf("invalid db name")
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		if err := server.SetDatabaseStatus(ctx, it.ID, api.Error); err != nil {
// 			logs.Errorf("database: can not set error status for %d; %v", it.ID, err)
// 		}
// 		return
// 	}
//
// 	logs.Infof("database: %d, deleted", it.ID)
// }

func idString(id int64) string {
	return strconv.FormatInt(id, 10)
}

func parseFloat64(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func parseInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

// func targetCPUPercent(limit float64) int {
// 	request, _ := strconv.ParseFloat(requestCPU, 64)
// 	if request <= 0 {
// 		request = 0.01
// 	}
//
// 	// floor limit in-case of limit can be fraction
// 	limit = math.Floor(limit)
// 	if limit <= 0 {
// 		limit = 1
// 	}
//
// 	// return 80% of limit but relative to request
// 	return int(80 * limit / request)
// }

func latestIfEmpty(version string) string {
	if version == "" {
		return "latest"
	}
	return version
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	if knet.IsConnectionRefused(err) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if strings.Contains(err.Error(), "would exceed context deadline") {
		return true
	}

	return false
}

func normalizeDomain(domain string) string {
	domain = strings.ReplaceAll(domain, "-", "--")
	domain = strings.ReplaceAll(domain, ".", "-")
	domain = strings.ToLower(domain)
	return domain
}

func resourceID(projectID int64, name string) string {
	if projectID <= 0 || name == "" {
		return ""
	}
	return fmt.Sprintf("%s-%d", name, projectID)
}

// deploymentHost is the first DNS label of a deployment's default public /
// internal URL. It prefers the friendly display name (`<displayName>-<projectID>`)
// and falls back to the id-based k8s name (`<name>-<projectID>`, where Name is
// the resource prefix) only when the display-name host would overflow the
// 63-char DNS label limit. Mirrors the apiserver's deploymentHost
// (server/deployment_model.go) so the shown URL and the ingress host agree;
// keep the two in sync. DisplayName is empty on legacy/older commands — then the
// resource name (== display name for legacy) is used.
func deploymentHost(displayName, name string, projectID int64) string {
	if displayName == "" {
		displayName = name
	}
	if h := resourceID(projectID, displayName); len(h) <= 63 {
		return h
	}
	return resourceID(projectID, name)
}

// releaseHostShaLen is the number of leading hex chars of a release-sha embedded
// in a Static deployment's immutable per-release URL host label. MUST match the
// apiserver's release8Len (server/deployment_model.go).
const releaseHostShaLen = 8

// releaseHost is the first DNS label of a Static deployment's immutable
// per-release URL (`<host>-<release8>.<region>.deploys.app`). Like deploymentHost
// it prefers the friendly display name (`<displayName>-<release8>-<projectID>`)
// and falls back to the id-based resource name when the display-name host would
// overflow the 63-char DNS label limit. Mirrors the apiserver's releaseHost
// (server/deployment_model.go) byte-for-byte so the URL the apiserver reports and
// the pinned-ingress host the deployer creates agree; keep the two in sync
// (SPEC-preview-environments.md §B.1).
func releaseHost(displayName, name string, projectID int64, releaseSHA string) string {
	if displayName == "" {
		displayName = name
	}
	r := releaseSHA
	if len(r) > releaseHostShaLen {
		r = r[:releaseHostShaLen]
	}
	if h := fmt.Sprintf("%s-%s-%d", displayName, r, projectID); len(h) <= 63 {
		return h
	}
	return fmt.Sprintf("%s-%s-%d", name, r, projectID)
}

// staticSitePrefix returns the release prefix the static-gateway serves a
// Static deployment from, used as the Ingress upstream-path (with a leading
// "/" added by the caller). The apiserver already computes this as
// `<project>/<name>/<release-sha>` and sends it in Spec.SitePrefix, so we use
// that verbatim. As a defensive fallback (older apiservers that send only
// Spec.Site), we derive the same `<project>/<name>/<release-sha>` from the
// site ref `site://<bucket>/<project>/<name>@<release-sha>`.
func staticSitePrefix(it *api.DeployerCommandDeploymentDeploy) string {
	if it.Spec.SitePrefix != "" {
		return strings.Trim(it.Spec.SitePrefix, "/")
	}

	// fallback: parse `site://<bucket>/<project>/<name>@<release-sha>`
	ref := strings.TrimPrefix(it.Spec.Site, "site://")
	at := strings.LastIndex(ref, "@")
	if at < 0 {
		return ""
	}
	release := ref[at+1:]
	pathPart := ref[:at] // <bucket>/<project>/<name>
	segs := strings.Split(strings.Trim(pathPart, "/"), "/")
	if len(segs) < 3 || release == "" {
		return ""
	}
	// drop the bucket; keep <project>/<name>
	project := segs[len(segs)-2]
	name := segs[len(segs)-1]
	return project + "/" + name + "/" + release
}

// reconcilePinnedReleaseIngresses brings the Static deployment's set of immutable
// per-release Ingresses in line with it.Spec.RetainedSiteRefs (B2): it creates
// one Ingress per retained release-sha — named `<id>-rel-<release8>`, host via
// releaseHost, upstream-path pinned to that exact content-addressed release — and
// deletes any existing `<id>-rel-*` Ingress whose sha has dropped out of the
// retained set. Pruning therefore happens on the next deploy, when a new revision
// pushes the oldest sha past the retention bound; there is no GC→deployer signal,
// so this reconcile is the only channel that removes a stale pinned Ingress.
//
// Because the retained set == the apiserver/siteGC live set, a pinned URL's
// release is always live while its Ingress exists, and vice versa
// (SPEC-preview-environments.md §B.3). Each pinned Ingress inherits the
// deployment's access policy: public for a public deployment, but gated by the
// same "Require Google login" forward-auth as the default URL when the
// deployment opted into Deployment Access — a per-release URL must never be an
// ungated mirror of gated content.
func (w *Worker) reconcilePinnedReleaseIngresses(ctx context.Context, it *api.DeployerCommandDeploymentDeploy, id, projectID string) error {
	// "<project>/<name>" — the release-prefix base shared by every revision.
	base := staticSitePrefix(it)
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[:i]
	}
	if base == "" {
		// Malformed/empty site prefix: can't form valid upstream-paths, so leave
		// any existing pinned Ingresses untouched rather than reconcile blindly.
		return nil
	}

	want := make(map[string]string, len(it.Spec.RetainedSiteRefs)) // pinned id -> full sha
	for _, sha := range it.Spec.RetainedSiteRefs {
		if sha == "" {
			continue
		}
		r8 := sha
		if len(r8) > releaseHostShaLen {
			r8 = r8[:releaseHostShaLen]
		}
		want[id+"-rel-"+r8] = sha
	}

	for pinID, sha := range want {
		relHost := releaseHost(it.DisplayName, it.Name, it.ProjectID, sha)
		err := w.Client.CreateIngress(ctx, k8s.Ingress{
			ID:           pinID,
			Service:      staticGatewayService,
			ProjectID:    projectID,
			Domain:       fmt.Sprintf("%s%s", relHost, w.location.DomainSuffix),
			Path:         "/",
			UpstreamHost: staticGatewayService,
			UpstreamPath: "/" + base + "/" + sha,
			// Tag with the parent deployment id so the pinned set is selected by
			// an exact label (ListPinnedIngressIDs), never a name-prefix scan.
			Labels: map[string]string{k8s.PinnedIngressDeploymentLabel: id},
			// Inherit the deployment's access policy, exactly like the default URL
			// above. A public deployment (Access nil / RequireGoogleLogin false)
			// yields no gate, so the per-release URL is public — but a deployment
			// that opted into "Require Google login" to keep its content private
			// MUST gate its per-release URLs too, or they'd be an ungated mirror of
			// gated content. The verifier is scoped by deployment id (?d=<id>), so
			// the same gate applies to every release pin of this deployment.
			Config: api.RouteConfig{ForwardAuth: w.accessForwardAuth(it.ID, it.Spec.Access)},
		})
		if err != nil {
			slog.Error("deployment: creating pinned ingress error", "id", it.ID, "ingress", pinID, "error", err)
			return err
		}
	}

	// Delete pinned Ingresses whose sha is no longer retained.
	existing, err := w.Client.ListPinnedIngressIDs(ctx, id, projectID)
	if err != nil {
		slog.Error("deployment: listing pinned ingresses error", "id", it.ID, "error", err)
		return err
	}
	for _, pinID := range existing {
		if _, ok := want[pinID]; ok {
			continue
		}
		if err := w.Client.DeleteIngress(ctx, pinID); err != nil {
			slog.Error("deployment: deleting stale pinned ingress error", "id", it.ID, "ingress", pinID, "error", err)
			return err
		}
	}
	return nil
}

// parseExternalHTTPTarget parses an http://<ip>[:port] external route target
// into the upstream IP and port (defaulting to 80). It mirrors the apiserver's
// validExternalTarget guard; the apiserver has already rejected non-public IPs
// before the command reaches here, so this is a defensive re-parse.
func parseExternalHTTPTarget(target string) (ip string, port int, ok bool) {
	hostport := strings.TrimPrefix(target, "http://")
	if hostport == "" {
		return "", 0, false
	}

	host := hostport
	port = 80
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return "", 0, false
		}
		host, port = h, n
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		// Bare bracketed IPv6 literal without a port.
		host = host[1 : len(host)-1]
	}

	if net.ParseIP(host) == nil {
		return "", 0, false
	}
	return host, port, true
}

func pullSecretResourceID(projectID int64, name string) string {
	if projectID <= 0 || name == "" {
		return ""
	}
	return fmt.Sprintf("pull-%s-%d", name, projectID)
}

// prepareMountData lays out every mounted file into a single config map with
// globally unique keys, and splits the resulting key => path bindings between
// the application container (bindData) and each sidecar (sidecarBinds, indexed
// to match sidecars). Keeping each sidecar's bindings separate is required
// because two sidecars may mount different files at the same path (for example
// two cloud-sql-proxy sidecars both using /sidecar/cloudsqlproxy/credentials.json):
// a mount path only has to be unique within a single container, but it must not
// be mounted into a container it does not belong to.
func prepareMountData(mountData map[string]string, sidecars []*api.SidecarConfig) (configMapData map[string]string, bindData map[string]string, sidecarBinds []map[string]string) {
	type item struct {
		key     string
		path    string
		data    string
		sidecar int // index into sidecars, or -1 for the application container
	}

	var list []item
	for path, data := range mountData {
		list = append(list, item{path: path, data: data, sidecar: -1})
	}
	for i, s := range sidecars {
		for path, data := range s.MountData {
			list = append(list, item{path: path, data: data, sidecar: i})
		}
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].sidecar != list[j].sidecar {
			return list[i].sidecar < list[j].sidecar
		}
		return list[i].path < list[j].path
	})

	for i := range list {
		list[i].key = fmt.Sprintf("file-%d", i)
	}

	configMapData = make(map[string]string)
	bindData = make(map[string]string)
	sidecarBinds = make([]map[string]string, len(sidecars))
	for i := range sidecarBinds {
		sidecarBinds[i] = make(map[string]string)
	}
	for _, t := range list {
		configMapData[t.key] = t.data
		if t.sidecar < 0 {
			bindData[t.key] = t.path
		} else {
			sidecarBinds[t.sidecar][t.key] = t.path
		}
	}
	return
}

// buildSidecars resolves sidecar configs into k8s sidecar specs, ensuring each
// container has a unique name (multiple sidecars of the same kind, e.g. two
// cloud-sql-proxy instances, otherwise share the name "cloudsql-proxy") and is
// bound only to its own mounted files.
func buildSidecars(configs []*api.SidecarConfig, binds []map[string]string) []k8s.Sidecar {
	used := make(map[string]bool, len(configs))
	out := make([]k8s.Sidecar, len(configs))
	for i, c := range configs {
		name := c.Name
		for n := 1; used[name]; n++ {
			name = fmt.Sprintf("%s-%d", c.Name, n)
		}
		used[name] = true

		out[i] = k8s.Sidecar{
			Name:          name,
			Image:         c.Image,
			Env:           c.Env,
			Command:       c.Command,
			Args:          c.Args,
			Port:          c.Port,
			BindConfigMap: binds[i],
		}
	}
	return out
}
