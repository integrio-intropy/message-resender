package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	messagingv1 "github.com/integrio-intropy/message-resender/api/v1"
	"github.com/integrio-intropy/message-resender/internal/broker"
	"github.com/integrio-intropy/message-resender/internal/broker/rabbitmq"
	"github.com/integrio-intropy/message-resender/internal/broker/servicebus"
	"github.com/integrio-intropy/message-resender/internal/controller"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(messagingv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics server")

	// Broker flags
	var brokerType string
	var rabbitmqDSN string
	var dlqPrefix string
	var drainInterval time.Duration
	var maxConcurrentDrains int
	var watchNamespaces string
	var asbConnectionString string
	var asbNamespace string
	flag.StringVar(&watchNamespaces, "watch-namespaces", "",
		"Comma-separated list of namespaces to watch for ResendPolicy resources. "+
			"Empty means all namespaces (cluster-wide).")
	flag.StringVar(&brokerType, "broker-type", "rabbitmq", "Message broker type (rabbitmq, azureservicebus).")
	flag.StringVar(&rabbitmqDSN, "rabbitmq-dsn", "",
		"RabbitMQ connection string (amqp://user:pass@host:port/vhost). Falls back to RABBITMQ_DSN env var.")
	flag.StringVar(&dlqPrefix, "dlq-prefix", "dlq-", "Default prefix for DLQ names (RabbitMQ only).")
	flag.StringVar(&asbConnectionString, "asb-connection-string", "",
		"Azure Service Bus connection string. Falls back to ASB_CONNECTION_STRING env var.")
	flag.StringVar(&asbNamespace, "asb-namespace", "",
		"Azure Service Bus fully-qualified namespace (for managed identity). Falls back to ASB_NAMESPACE env var.")
	flag.DurationVar(&drainInterval, "drain-interval", 5*time.Minute, "Interval between drain runs per queue.")
	flag.IntVar(&maxConcurrentDrains, "max-concurrent-drains", 10, "Maximum concurrent drain operations.")

	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Create the message broker.
	msgBroker, brokerHost, err := createBroker(
		brokerType, rabbitmqDSN, dlqPrefix, asbConnectionString, asbNamespace,
	)
	if err != nil {
		setupLog.Error(err, "Failed to create broker")
		os.Exit(1)
	}
	defer func() { _ = msgBroker.Close() }()

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	var cacheOpts cache.Options
	if watchNamespaces != "" {
		nsList := strings.Split(watchNamespaces, ",")
		cacheOpts.DefaultNamespaces = make(map[string]cache.Config, len(nsList))
		for _, n := range nsList {
			if n = strings.TrimSpace(n); n != "" {
				cacheOpts.DefaultNamespaces[n] = cache.Config{}
			}
		}
		setupLog.Info("Watching namespaces", "namespaces", nsList)
	} else {
		setupLog.Info("Watching all namespaces")
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cacheOpts,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "c0b8f70a.intropy.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	if err := (&controller.ResendPolicyReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		Broker:              msgBroker,
		DrainInterval:       drainInterval,
		MaxConcurrentDrains: maxConcurrentDrains,
		Metrics:             controller.NewMetrics(brokerHost),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "ResendPolicy")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

// createBroker initialises the message broker based on the given type and flags.
// It returns the broker, a host label for metrics, and any error.
func createBroker(
	brokerType, rabbitmqDSN, dlqPrefix, asbConnectionString, asbNamespace string,
) (broker.Broker, string, error) {
	switch brokerType {
	case "rabbitmq":
		setupLog.Info("creating RabbitMQ broker")
		if rabbitmqDSN == "" {
			rabbitmqDSN = os.Getenv("RABBITMQ_DSN")
		}
		if rabbitmqDSN == "" {
			return nil, "", fmt.Errorf("--rabbitmq-dsn or RABBITMQ_DSN is required")
		}
		b, err := rabbitmq.New(rabbitmqDSN, dlqPrefix, ctrl.Log.WithName("rabbitmq"))
		if err != nil {
			return nil, "", fmt.Errorf("create RabbitMQ broker: %w", err)
		}
		return b, rabbitmqHost(rabbitmqDSN), nil
	case "azureservicebus":
		setupLog.Info("creating Azure Service Bus broker")
		if asbConnectionString == "" {
			asbConnectionString = os.Getenv("ASB_CONNECTION_STRING")
		}
		if asbNamespace == "" {
			asbNamespace = os.Getenv("ASB_NAMESPACE")
		}
		if asbConnectionString == "" && asbNamespace == "" {
			return nil, "", fmt.Errorf(
				"--asb-connection-string or --asb-namespace is required")
		}
		if asbConnectionString != "" && asbNamespace != "" {
			return nil, "", fmt.Errorf(
				"only one of --asb-connection-string or --asb-namespace may be set")
		}
		b, err := servicebus.New(asbConnectionString, asbNamespace, ctrl.Log.WithName("servicebus"))
		if err != nil {
			return nil, "", fmt.Errorf("create Azure Service Bus broker: %w", err)
		}
		return b, asbHost(asbConnectionString, asbNamespace), nil
	default:
		return nil, "", fmt.Errorf("unknown broker type: %s", brokerType)
	}
}

// rabbitmqHost derives a host label from an AMQP DSN: "hostname/vhost".
func rabbitmqHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	vhost := strings.TrimPrefix(u.Path, "/")
	return u.Hostname() + "/" + vhost
}

// asbHost derives a host label for Azure Service Bus.
// Uses namespace directly, or parses it from a connection string
// (Endpoint=sb://mynamespace.servicebus.windows.net/;...).
func asbHost(connStr, namespace string) string {
	if namespace != "" {
		return namespace
	}
	for part := range strings.SplitSeq(connStr, ";") {
		part = strings.TrimSpace(part)
		if after, ok := strings.CutPrefix(part, "Endpoint="); ok {
			endpoint := after
			if u, err := url.Parse(endpoint); err == nil {
				return u.Hostname()
			}
		}
	}
	return "unknown"
}
