package kubernetes

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/weaveworks/flux/cluster/kubernetes/testfiles"
)

func TestLocalCRDScope(t *testing.T) {
	coreClient := makeFakeClient()

	nser, err := NewNamespacer(coreClient.Discovery())
	assert.NoError(t, err)
	manifests := NewManifests(nser, log.NewLogfmtLogger(os.Stdout))

	dir, cleanup := testfiles.TempDir(t)
	defer cleanup()
	const defs = `---
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: foo
spec:
  group: foo.example.com
  names:
    kind: Foo
    listKind: FooList
    plural: foos
    shortNames:
    - foo
  scope: Namespaced
  version: v1beta1
  versions:
    - name: v1beta1
      served: true
      storage: true
---
apiVersion: foo.example.com/v1beta1
kind: Foo
metadata:
  name: fooinstance
  namespace: bar
`

	err = ioutil.WriteFile(filepath.Join(dir, "test.yaml"), []byte(defs), 0600)
	assert.NoError(t, err)

	resources, err := manifests.LoadManifests(dir, []string{dir})
	if err != nil {
		t.Fatal(err)
	}

	assert.Contains(t, resources, "bar:foo/fooinstance")
}

func TestUnKnownCRDScope(t *testing.T) {
	coreClient := makeFakeClient()

	nser, err := NewNamespacer(coreClient.Discovery())
	assert.NoError(t, err)
	logBuffer := bytes.NewBuffer(nil)
	manifests := NewManifests(nser, log.NewLogfmtLogger(logBuffer))

	dir, cleanup := testfiles.TempDir(t)
	defer cleanup()
	const defs = `---
apiVersion: v1
kind: Namespace
metadata:
  name: mynamespace
---
apiVersion: foo.example.com/v1beta1
kind: Foo
metadata:
  name: fooinstance
  namespace: bar
`

	err = ioutil.WriteFile(filepath.Join(dir, "test.yaml"), []byte(defs), 0600)
	assert.NoError(t, err)

	resources, err := manifests.LoadManifests(dir, []string{dir})
	assert.NoError(t, err)

	// can't contain the CRD since we cannot figure out its scope
	assert.NotContains(t, resources, "bar:foo/fooinstance")

	// however, it should contain the namespace
	assert.Contains(t, resources, "<cluster>:namespace/mynamespace")

	savedLog := logBuffer.String()
	// and we should had logged a warning about it
	assert.Contains(t, savedLog, "cannot find scope of resource foo/fooinstance")

	// loading again shouldn't result in more warnings
	resources, err = manifests.LoadManifests(dir, []string{dir})
	assert.NoError(t, err)
	assert.Equal(t, logBuffer.String(), savedLog)

	// But getting the scope of the CRD should result in a log saying we found the scope
	apiResourcesWithoutFoo := coreClient.Resources
	apiResource := &metav1.APIResourceList{
		GroupVersion: "foo.example.com/v1beta1",
		APIResources: []metav1.APIResource{
			{Name: "foos", SingularName: "foo", Namespaced: true, Kind: "Foo"},
		},
	}
	coreClient.Resources = append(coreClient.Resources, apiResource)

	logBuffer.Reset()
	resources, err = manifests.LoadManifests(dir, []string{dir})
	assert.NoError(t, err)
	assert.Len(t, resources, 2)
	assert.Contains(t, logBuffer.String(), "found scope of resource bar:foo/fooinstance")

	// and missing the scope information again should result in another warning
	coreClient.Resources = apiResourcesWithoutFoo
	logBuffer.Reset()
	resources, err = manifests.LoadManifests(dir, []string{dir})
	assert.NoError(t, err)
	assert.Contains(t, savedLog, "cannot find scope of resource foo/fooinstance")
}

func TestCreateManifestPatchHelmRelease(t *testing.T) {
	const (
		originalManifest = `apiVersion: flux.weave.works/v1beta1
kind: HelmRelease
metadata:
  name: ghost
  namespace: demo
  annotations:
    flux.weave.works/automated: "false"
    flux.weave.works/tag.chart-image: glob:1.21.*
spec:
  values:
    image: bitnami/ghost
    tag: 1.21.5-r0
`
		updatedManifest = `apiVersion: flux.weave.works/v1beta1
kind: HelmRelease
metadata:
  name: ghost
  namespace: demo
  annotations:
    flux.weave.works/automated: "false"
    flux.weave.works/tag.chart-image: glob:1.21.*
spec:
  values:
    image: bitnami/ghost
    tag: 1.21.6
`
	)

	coreClient := makeFakeClient()

	nser, err := NewNamespacer(coreClient.Discovery())
	assert.NoError(t, err)
	manifests := NewManifests(nser, log.NewLogfmtLogger(os.Stderr))

	_, err = manifests.CreateManifestPatch([]byte(originalManifest), []byte(updatedManifest))
	assert.NoError(t, err)
}

func TestCreateManifestPatchDeployment(t *testing.T) {
	const (
		originalManifest = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: name
spec:
  template:
    spec:
      containers:
      - name: one
        image: one:one
      - name: two
        image: two:two
      initContainers:
      - name: one
        image: one:one
`
		updatedManifest = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: name
spec:
  template:
    spec:
      containers:
      - name: one
        image: oneplus:oneplus
      - name: two
        image: two:two
      initContainers:
      - name: one
        image: one:one
`
	)

	coreClient := makeFakeClient()

	nser, err := NewNamespacer(coreClient.Discovery())
	assert.NoError(t, err)
	manifests := NewManifests(nser, log.NewLogfmtLogger(os.Stderr))

	_, err = manifests.CreateManifestPatch([]byte(originalManifest), []byte(updatedManifest))
	assert.NoError(t, err)
}
