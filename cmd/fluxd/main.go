package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/client/restclient"
	"k8s.io/kubernetes/pkg/client/unversioned/clientcmd"

	"github.com/weaveworks/fluxy"
	"github.com/weaveworks/fluxy/automator"
	"github.com/weaveworks/fluxy/git"
	"github.com/weaveworks/fluxy/history"
	"github.com/weaveworks/fluxy/platform/kubernetes"
	"github.com/weaveworks/fluxy/registry"
	"github.com/weaveworks/fluxy/release"
)

func main() {
	// Flag domain.
	fs := pflag.NewFlagSet("default", pflag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "DESCRIPTION\n")
		fmt.Fprintf(os.Stderr, "  fluxd is a deployment daemon.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		fs.PrintDefaults()
	}
	// This mirrors how kubectl extracts information from the environment.
	var (
		listenAddr                = fs.StringP("listen", "l", ":3030", "Listen address for Flux API clients")
		registryCredentials       = fs.String("registry-credentials", "", "Path to image registry credentials file, in the format of ~/.docker/config.json")
		kubernetesMinikube        = fs.Bool("kubernetes-minikube", false, "Parse Kubernetes access information from standard minikube files")
		kubernetesKubectl         = fs.String("kubernetes-kubectl", "", "Optional, explicit path to kubectl tool")
		kubernetesHost            = fs.String("kubernetes-host", "", "Kubernetes host, e.g. http://10.11.12.13:8080")
		kubernetesUsername        = fs.String("kubernetes-username", "", "Kubernetes HTTP basic auth username")
		kubernetesPassword        = fs.String("kubernetes-password", "", "Kubernetes HTTP basic auth password")
		kubernetesClientCert      = fs.String("kubernetes-client-certificate", "", "Path to Kubernetes client certification file for TLS")
		kubernetesClientKey       = fs.String("kubernetes-client-key", "", "Path to Kubernetes client key file for TLS")
		kubernetesCertAuthority   = fs.String("kubernetes-certificate-authority", "", "Path to Kubernetes cert file for certificate authority")
		kubernetesBearerTokenFile = fs.String("kubernetes-bearer-token-file", "", "Path to file containing Kubernetes Bearer Token file")
		databaseDriver            = fs.String("database-driver", "ql-mem", `Database driver name, e.g., "postgres"; the default is an in-memory DB`)
		databaseSource            = fs.String("database-source", "history.db", `Database source name; specific to the database driver (--database-driver) used. The default is an arbitrary, in-memory DB name`)
		repoURL                   = fs.String("repo-url", "", "Config repo URL, e.g. git@github.com:myorg/conf (required)")
		repoKey                   = fs.String("repo-key", "", "SSH key file with commit rights to config repo")
		repoPath                  = fs.String("repo-path", "", "Path within config repo to look for resource definition files")
		slackWebhookURL           = fs.String("slack-webhook-url", "", "Slack webhook URL for release notifications (optional)")
		slackUsername             = fs.String("slack-username", "fluxy-deploy", "Slack username for release notifications")
	)
	fs.Parse(os.Args)

	// Logger component.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC)
		logger = log.NewContext(logger).With("caller", log.DefaultCaller)
	}

	// Instrumentation
	var (
		httpDuration   metrics.Histogram
		serverMetrics  flux.Metrics
		releaseMetrics release.Metrics
		helperDuration metrics.Histogram
	)
	{
		httpDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
		}, []string{"method", "status_code"})
		serverMetrics.ListServicesDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "list_services_duration_seconds",
			Help:      "ListServices method duration in seconds.",
		}, []string{"namespace", "success"})
		serverMetrics.ListImagesDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "list_images_duration_seconds",
			Help:      "ListImages method duration in seconds.",
		}, []string{"service_spec", "success"})
		serverMetrics.HistoryDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "history_duration_seconds",
			Help:      "History method duration in seconds.",
		}, []string{"service_spec", "success"})
		releaseMetrics.ReleaseDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "release_duration_seconds",
			Help:      "Release method duration in seconds.",
		}, []string{"release_type", "release_kind", "success"})
		releaseMetrics.ActionDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "release_action_duration_seconds",
			Help:      "Duration in seconds of each sub-action invoked as part of a non-dry-run release.",
		}, []string{"action", "success"})
		releaseMetrics.StageDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "release_stage_duration_seconds",
			Help:      "Duration in seconds of each stage of a release, including dry-runs.",
		}, []string{"method", "stage"})
		helperDuration = prometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "fluxy",
			Subsystem: "fluxd",
			Name:      "release_helper_duration_seconds",
			Help:      "Duration in seconds of a variety of release helper methods.",
		}, []string{"method", "success"})
	}

	// Registry component.
	var reg *registry.Client
	{
		logger := log.NewContext(logger).With("component", "registry")
		creds := registry.NoCredentials()
		if *registryCredentials != "" {
			logger.Log("credentials", *registryCredentials)
			c, err := registry.CredentialsFromFile(*registryCredentials)
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}
			creds = c
		} else {
			logger.Log("credentials", "none")
		}
		reg = &registry.Client{
			Credentials: creds,
			Logger:      logger,
		}
	}

	// Platform component.
	var k8s *kubernetes.Cluster
	{
		var restClientConfig *restclient.Config

		if *kubernetesMinikube {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			configOverrides := &clientcmd.ConfigOverrides{}

			// TODO: handle the filename for kubeconfig here, as well.
			kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
			var err error
			restClientConfig, err = kubeConfig.ClientConfig()
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}
		}

		if restClientConfig == nil {
			var bearerToken string
			if *kubernetesBearerTokenFile != "" {
				buf, err := ioutil.ReadFile(*kubernetesBearerTokenFile)
				if err != nil {
					logger.Log("err", err)
					os.Exit(1)
				}
				bearerToken = string(buf)
			}
			restClientConfig = &restclient.Config{
				Host:        *kubernetesHost,
				Username:    *kubernetesUsername,
				Password:    *kubernetesPassword,
				BearerToken: bearerToken,
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: *kubernetesClientCert,
					KeyFile:  *kubernetesClientKey,
					CAFile:   *kubernetesCertAuthority,
				},
			}
		}

		// When adding a new platform, don't just bash it in. Create a Platform
		// or Cluster interface in package platform, and have kubernetes.Cluster
		// and your new platform implement that interface.
		logger := log.NewContext(logger).With("component", "platform")
		logger.Log("host", restClientConfig.Host)

		var err error
		k8s, err = kubernetes.NewCluster(restClientConfig, *kubernetesKubectl, logger)
		if err != nil {
			logger.Log("err", err)
			os.Exit(1)
		}

		if services, err := k8s.Services("default"); err != nil {
			logger.Log("services", err)
		} else {
			logger.Log("services", len(services))
		}
	}

	// History component.
	var eventWriter history.EventWriter
	var eventReader history.EventReader
	{
		db, err := history.NewSQL(*databaseDriver, *databaseSource)
		if err != nil {
			logger.Log("component", "history", "err", err)
			os.Exit(1)
		}

		eventWriter, eventReader = db, db

		if *slackWebhookURL != "" {
			eventWriter = history.TeeWriter(eventWriter, history.NewSlackEventWriter(
				http.DefaultClient,
				*slackWebhookURL,
				*slackUsername,
				`Release`, // only catch the final message
			))
			logger.Log("Slack", "enabled", "username", *slackUsername)
		} else {
			logger.Log("Slack", "disabled")
		}
	}

	// Configuration, i.e., whether services are automated or not.
	var config config.DB
	{
		db, err := configSQL.New(*databaseDriver, *databaseSource)
		if err != nil {
			logger.Log("component", "config", "err", err)
			os.Exit(1)
		}
		config = db
	}

	// Repo and releaser.
	var rel flux.Releaser
	{
		repo := git.Repo{
			URL:  *repoURL,
			Key:  *repoKey,
			Path: *repoPath,
		}
		rel = release.New(k8s, reg, logger, repo, eventWriter, releaseMetrics, helperDuration)
	}

	// Automator component.
	var auto *automator.Automator
	{
		var err error
		auto, err = automator.New(automator.Config{
			Releaser: rel,
			History:  eventWriter,
			Config:   config,
		})
		if err == nil {
			logger.Log("automator", "enabled", "repo", *repoURL)
		} else {
			// Service can handle a nil automator pointer.
			logger.Log("automator", "disabled", "reason", err)
		}
	}

	// The server.
	server := flux.NewServer(k8s, reg, rel, auto, eventReader, logger, serverMetrics, helperDuration)

	// Mechanical components.
	errc := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	// HTTP transport component.
	go func() {
		logger.Log("addr", *listenAddr)
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.Handle("/", flux.NewHandler(server, flux.NewRouter(), logger, httpDuration))
		errc <- http.ListenAndServe(*listenAddr, mux)
	}()

	// Go!
	logger.Log("exit", <-errc)
}
