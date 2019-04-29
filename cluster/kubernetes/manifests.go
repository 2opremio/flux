package kubernetes

import (
	"fmt"
	"strings"

	jsonyaml "github.com/ghodss/yaml"
	"github.com/go-kit/kit/log"
	"gopkg.in/yaml.v2"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/cluster"
	kresource "github.com/weaveworks/flux/cluster/kubernetes/resource"
	"github.com/weaveworks/flux/image"
	fluxscheme "github.com/weaveworks/flux/integrations/client/clientset/versioned/scheme"
	"github.com/weaveworks/flux/resource"
)

var fullScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(k8sscheme.AddToScheme(fullScheme))
	utilruntime.Must(fluxscheme.AddToScheme(fullScheme))
}

// ResourceScopes maps resource definitions (GroupVersionKind) to whether they are namespaced or not
type ResourceScopes map[schema.GroupVersionKind]v1beta1.ResourceScope

// namespacer assigns namespaces to manifests that need it (or "" if
// the manifest should not have a namespace.
type namespacer interface {
	// EffectiveNamespace gives the namespace that would be used were
	// the manifest to be applied. This may be "", indicating that it
	// should not have a namespace (i.e., it's a cluster-level
	// resource).
	EffectiveNamespace(manifest kresource.KubeManifest, knownScopes ResourceScopes) (string, error)
}

// manifests is an implementation of cluster.Manifests, particular to
// Kubernetes. Aside from loading manifests from files, it does some
// "post-processsing" to make sure the view of the manifests is what
// would be applied; in particular, it fills in the namespace of
// manifests that would be given a default namespace when applied.
type manifests struct {
	namespacer       namespacer
	logger           log.Logger
	resourceWarnings map[string]struct{}
}

func NewManifests(ns namespacer, logger log.Logger) *manifests {
	return &manifests{
		namespacer:       ns,
		logger:           logger,
		resourceWarnings: map[string]struct{}{},
	}
}

var _ cluster.Manifests = &manifests{}

func getCRDScopes(manifests map[string]kresource.KubeManifest) ResourceScopes {
	result := ResourceScopes{}
	for _, km := range manifests {
		if km.GetKind() == "CustomResourceDefinition" {
			var crd v1beta1.CustomResourceDefinition
			if err := yaml.Unmarshal(km.Bytes(), &crd); err != nil {
				// The CRD can't be parsed, so we (intentionally) ignore it and
				// just hope for EffectiveNamespace() to find its scope in the cluster if needed.
				continue
			}
			crdVersions := crd.Spec.Versions
			if len(crdVersions) == 0 {
				crdVersions = []v1beta1.CustomResourceDefinitionVersion{{Name: crd.Spec.Version}}
			}
			for _, crdVersion := range crdVersions {
				gvk := schema.GroupVersionKind{
					Group:   crd.Spec.Group,
					Version: crdVersion.Name,
					Kind:    crd.Spec.Names.Kind,
				}
				result[gvk] = crd.Spec.Scope
			}
		}
	}
	return result
}

func (m *manifests) setEffectiveNamespaces(manifests map[string]kresource.KubeManifest) (map[string]resource.Resource, error) {
	knownScopes := getCRDScopes(manifests)
	result := map[string]resource.Resource{}
	for _, km := range manifests {
		resID := km.ResourceID()
		resIDStr := resID.String()
		ns, err := m.namespacer.EffectiveNamespace(km, knownScopes)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				// discard the resource and keep going after making sure we logged about it
				if _, warningLogged := m.resourceWarnings[resIDStr]; !warningLogged {
					_, kind, name := resID.Components()
					partialResIDStr := kind + "/" + name
					m.logger.Log(
						"warn", fmt.Sprintf("cannot find scope of resource %s: %s", partialResIDStr, err),
						"impact", fmt.Sprintf("resource %s will be excluded until its scope is available", partialResIDStr))
					m.resourceWarnings[resIDStr] = struct{}{}
				}
				continue
			}
			return nil, err
		}
		km.SetNamespace(ns)
		if _, warningLogged := m.resourceWarnings[resIDStr]; warningLogged {
			// indicate that we found the resource's scope and allow logging a warning again
			m.logger.Log("info", fmt.Sprintf("found scope of resource %s, back in bussiness!", km.ResourceID().String()))
			delete(m.resourceWarnings, resIDStr)
		}
		result[km.ResourceID().String()] = km
	}
	return result, nil
}

func (m *manifests) LoadManifests(baseDir string, paths []string) (map[string]resource.Resource, error) {
	manifests, err := kresource.Load(baseDir, paths)
	if err != nil {
		return nil, err
	}
	return m.setEffectiveNamespaces(manifests)
}

func (m *manifests) CreateManifestPatch(original, updated []byte) ([]byte, error) {
	resources, err := kresource.ParseMultidoc(original, "patch_creation")
	if err != nil {
		return nil, err
	}
	if len(resources) != 1 {
		return nil, fmt.Errorf("expected one workload, got %d", len(resources))
	}
	var manifest kresource.KubeManifest
	for _, v := range resources {
		manifest = v
	}
	groupVersion, err := schema.ParseGroupVersion(manifest.GroupVersion())
	if err != nil {
		return nil, fmt.Errorf("cannot parse groupVersion %q: %s", manifest.GroupVersion(), err)
	}
	gvk := groupVersion.WithKind(manifest.GetKind())
	obj, err := fullScheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain scheme for gvk %q: %s", gvk, err)
	}
	originalJSON, err := jsonyaml.YAMLToJSON(original)
	if err != nil {
		return nil, fmt.Errorf("cannot transform original resource to JSON: %s", err)
	}
	updatedJSON, err := jsonyaml.YAMLToJSON(updated)
	if err != nil {
		return nil, fmt.Errorf("cannot transform updated resource to JSON: %s", err)
	}
	smpJSON, err := strategicpatch.CreateTwoWayMergePatch(originalJSON, updatedJSON, obj)
	if err != nil {
		return nil, err
	}
	smp, err := jsonyaml.JSONToYAML(smpJSON)
	if err != nil {
		return nil, fmt.Errorf("cannot transform updated patch to YAML: %s", err)
	}
	return smp, nil
}

func (m *manifests) ParseManifest(def []byte, source string) (map[string]resource.Resource, error) {
	resources, err := kresource.ParseMultidoc(def, source)
	if err != nil {
		return nil, err
	}
	// Note: setEffectiveNamespaces() won't work for CRD instances whose CRD is yet to be created
	// (due to the CRD not being present in kresources).
	// We could get out of our way to fix this (or give a better error) but:
	// 1. With the exception of HelmReleases CRD instances are not workloads anyways.
	// 2. The problem is eventually fixed by the first successful sync.
	result, err := m.setEffectiveNamespaces(resources)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *manifests) SetWorkloadContainerImage(def []byte, id flux.ResourceID, container string, image image.Ref) ([]byte, error) {
	return updateWorkload(def, id, container, image)
}

// UpdateWorkloadPolicies in policies.go
