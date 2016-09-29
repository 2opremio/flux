// Package kubernetes provides abstractions for the Kubernetes platform. At the
// moment, Kubernetes is the only supported platform, so we are directly
// returning Kubernetes objects. As we add more platforms, we will create
// abstractions and common data types in package platform.
package kubernetes

import (
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"k8s.io/kubernetes/pkg/api"
	k8serrors "k8s.io/kubernetes/pkg/api/errors"
	apiext "k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/client/restclient"
	k8sclient "k8s.io/kubernetes/pkg/client/unversioned"

	"github.com/weaveworks/fluxy"
	"github.com/weaveworks/fluxy/platform"
)

type extendedClient struct {
	*k8sclient.Client
	*k8sclient.ExtensionsClient
}

type apiObject struct {
	bytes    []byte
	Version  string `yaml:"apiVersion"`
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
}

type regradeExecFunc func(*Cluster, log.Logger) error

type regrade struct {
	exec    regradeExecFunc
	summary string
}

// Cluster is a handle to a Kubernetes API server.
// (Typically, this code is deployed into the same cluster.)
type Cluster struct {
	config  *restclient.Config
	client  extendedClient
	kubectl string
	status  *statusMap
	actionc chan func()
	logger  log.Logger
}

// NewCluster returns a usable cluster. Host should be of the form
// "http://hostname:8080".
func NewCluster(config *restclient.Config, kubectl string, logger log.Logger) (*Cluster, error) {
	client, err := k8sclient.New(config)
	if err != nil {
		return nil, err
	}
	extclient, err := k8sclient.NewExtensions(config)
	if err != nil {
		return nil, err
	}

	if kubectl == "" {
		kubectl, err = exec.LookPath("kubectl")
		if err != nil {
			return nil, err
		}
	} else {
		if _, err := os.Stat(kubectl); err != nil {
			return nil, err
		}
	}
	logger.Log("kubectl", kubectl)

	c := &Cluster{
		config:  config,
		client:  extendedClient{client, extclient},
		kubectl: kubectl,
		status:  newStatusMap(),
		actionc: make(chan func()),
		logger:  logger,
	}
	go c.loop()
	return c, nil
}

// Stop terminates the goroutine that serializes and executes requests against
// the cluster. A stopped cluster cannot be restarted.
func (c *Cluster) Stop() {
	close(c.actionc)
}

func (c *Cluster) loop() {
	for f := range c.actionc {
		f()
	}
}

// --- platform API

// SomeServices returns the services named, missing out any that don't
// exist in the cluster.
func (c *Cluster) SomeServices(ids []flux.ServiceID) ([]platform.Service, error) {
	return nil, errors.New("Not implemented")
}

// AllServices returns all services matching the criteria; that is, in the namespace (or any namespace if that argument is empty), and not in the `ignore` set given.
func (c *Cluster) AllServices(namespace string, ignore flux.ServiceIDSet) ([]platform.Service, error) {
	return nil, errors.New("Not implemented")
}

func definitionObj(bytes []byte) (*apiObject, error) {
	obj := apiObject{bytes: bytes}
	return &obj, yaml.Unmarshal(bytes, &obj)
}

// Regrade performs service regrades as specified by the RegradeSpecs. If all
// regrades succeed, Regrade returns a nil error. If any regrade fails, Regrade
// returns an error of type RegradeError, which can be inspected for more
// detailed information. Regrades are serialized per cluster.
//
// Regrade assumes there is a one-to-one mapping between services and
// replication controllers or deployments; this can be improved. Regrade blocks
// until an update is complete; this can be improved. Regrade invokes `kubectl
// rolling-update` or `kubectl apply` in a seperate process, and assumes kubectl
// is in the PATH; this can be improved.
func (c *Cluster) Regrade(specs []platform.RegradeSpec) error {
	errc := make(chan error)
	c.actionc <- func() {
		regradeErr := platform.RegradeError{}
		for _, spec := range specs {
			newDef, err := definitionObj(spec.NewDefinition)
			if err != nil {
				regradeErr[spec.NamespacedService] = errors.Wrap(err, "reading definition")
				continue
			}

			pc, err := c.podControllerFor(spec.Namespace, spec.Service)
			if err != nil {
				regradeErr[spec.NamespacedService] = errors.Wrap(err, "getting pod controller")
				continue
			}

			plan, err := pc.newRegrade(newDef)
			if err != nil {
				regradeErr[spec.NamespacedService] = errors.Wrap(err, "creating regrade")
				continue
			}

			c.status.startRegrade(spec.NamespacedService, plan)
			defer c.status.endRegrade(spec.NamespacedService)

			logger := log.NewContext(c.logger).With("method", "Release", "namespace", spec.Namespace, "service", spec.Service)
			if err = plan.exec(c, logger); err != nil {
				regradeErr[spec.NamespacedService] = errors.Wrapf(err, "releasing %s/%s", spec.Namespace, spec.Service)
				continue
			}
		}
		if len(regradeErr) > 0 {
			errc <- regradeErr
			return
		}
		errc <- nil
	}
	return <-errc
}

// --- end platform API

type statusMap struct {
	inProgress map[platform.NamespacedService]*regrade
	mx         sync.RWMutex
}

func newStatusMap() *statusMap {
	return &statusMap{
		inProgress: make(map[platform.NamespacedService]*regrade),
	}
}

func (m *statusMap) startRegrade(ns platform.NamespacedService, r *regrade) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.inProgress[ns] = r
}

func (m *statusMap) getRegradeProgress(ns platform.NamespacedService) (string, bool) {
	m.mx.RLock()
	defer m.mx.RUnlock()
	if r, ok := m.inProgress[ns]; ok {
		return r.summary, true
	}
	return "", false
}

func (m *statusMap) endRegrade(ns platform.NamespacedService) {
	m.mx.Lock()
	defer m.mx.Unlock()
	delete(m.inProgress, ns)
}

// Either a replication controller, a deployment, or neither (both nils).
type podController struct {
	ReplicationController *api.ReplicationController
	Deployment            *apiext.Deployment
}

func (p podController) name() string {
	if p.Deployment != nil {
		return p.Deployment.Name
	} else if p.ReplicationController != nil {
		return p.ReplicationController.Name
	}
	return ""
}

func (p podController) kind() string {
	if p.Deployment != nil {
		return "Deployment"
	} else if p.ReplicationController != nil {
		return "ReplicationController"
	}
	return "unknown"
}

func (p podController) templateContainers() []api.Container {
	if p.Deployment != nil {
		return p.Deployment.Spec.Template.Spec.Containers
	} else if p.ReplicationController != nil {
		return p.ReplicationController.Spec.Template.Spec.Containers
	}
	return nil
}

func (c *Cluster) podControllerFor(namespace, serviceName string) (res podController, err error) {
	res = podController{}

	service, err := c.service(namespace, serviceName)
	if err != nil {
		return res, errors.Wrap(err, "fetching service "+namespace+"/"+serviceName)
	}

	selector := service.Spec.Selector
	if len(selector) <= 0 {
		return res, platform.ErrServiceHasNoSelector
	}

	// Now, try to find a deployment or replication controller that matches the
	// selector given in the service. The simplifying assumption for the time
	// being is that there's just one of these -- we return an error otherwise.

	// Find a replication controller which produces pods that match that
	// selector. We have to match all of the criteria in the selector, but we
	// don't need a perfect match of all of the replication controller's pod
	// properties.
	rclist, err := c.client.ReplicationControllers(namespace).List(api.ListOptions{})
	if err != nil {
		return res, errors.Wrap(err, "fetching replication controllers for ns "+namespace)
	}
	var rcs []api.ReplicationController
	for _, rc := range rclist.Items {
		match := func() bool {
			// For each key=value pair in the service spec, check if the RC
			// annotates its pods in the same way. If any rule fails, the RC is
			// not a match. If all rules pass, the RC is a match.
			for k, v := range selector {
				labels := rc.Spec.Template.Labels
				if labels[k] != v {
					return false
				}
			}
			return true
		}()
		if match {
			rcs = append(rcs, rc)
		}
	}
	switch len(rcs) {
	case 0:
		break // we can hope to find a deployment
	case 1:
		res.ReplicationController = &rcs[0]
	default:
		return res, platform.ErrMultipleMatching
	}

	// Now do the same work for deployments.
	deplist, err := c.client.Deployments(namespace).List(api.ListOptions{})
	if err != nil {
		return res, errors.Wrap(err, "fetching deployments for ns "+namespace)
	}
	var deps []apiext.Deployment
	for _, d := range deplist.Items {
		match := func() bool {
			// For each key=value pair in the service spec, check if the
			// deployment annotates its pods in the same way. If any rule fails,
			// the deployment is not a match. If all rules pass, the deployment
			// is a match.
			for k, v := range selector {
				labels := d.Spec.Template.Labels
				if labels[k] != v {
					return false
				}
			}
			return true
		}()
		if match {
			deps = append(deps, d)
		}
	}
	switch len(deps) {
	case 0:
		break
	case 1:
		res.Deployment = &deps[0]
	default:
		return res, platform.ErrMultipleMatching
	}

	if res.ReplicationController != nil && res.Deployment != nil {
		return res, platform.ErrMultipleMatching
	}
	if res.ReplicationController == nil && res.Deployment == nil {
		return res, platform.ErrNoMatching
	}
	return res, nil
}
