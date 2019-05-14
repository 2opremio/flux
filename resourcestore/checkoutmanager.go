package resourcestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/cluster"
	"github.com/weaveworks/flux/git"
	"github.com/weaveworks/flux/image"
	"github.com/weaveworks/flux/policy"
	"github.com/weaveworks/flux/resource"
)

type resourceWithOrigin struct {
	resource   resource.Resource
	configFile *ConfigFile // only set if the resource came from a configuration file
}

type checkoutManager struct {
	ctx              context.Context
	checkout         *git.Checkout
	manifests        cluster.Manifests
	policyTranslator cluster.PolicyTranslator
	rawManifestDirs  []string
	configFiles      []*ConfigFile
	resourcesByID    map[string]resourceWithOrigin
	sync.RWMutex
}

var _ ResourceStore = &checkoutManager{}

func NewCheckoutManager(ctx context.Context, enableManifestGeneration bool,
	manifests cluster.Manifests, policyTranslator cluster.PolicyTranslator, checkout *git.Checkout) (*checkoutManager, error) {
	var (
		err             error
		configFiles     []*ConfigFile
		rawManifestDirs []string
	)

	rawManifestDirs = checkout.ManifestDirs()
	if enableManifestGeneration {
		configFiles, rawManifestDirs, err = splitConfigFilesAndRawManifestPaths(checkout.Dir(), checkout.ManifestDirs())
		if err != nil {
			return nil, err
		}
	}

	result := &checkoutManager{
		ctx:              ctx,
		checkout:         checkout,
		manifests:        manifests,
		policyTranslator: policyTranslator,
		rawManifestDirs:  rawManifestDirs,
		configFiles:      configFiles,
	}
	return result, nil
}

func splitConfigFilesAndRawManifestPaths(baseDir string, paths []string) ([]*ConfigFile, []string, error) {
	var (
		configFiles      []*ConfigFile
		rawManifestPaths []string
	)

	for _, path := range paths {
		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return nil, nil, err
		}
		configFilePath, workingDirPath, err := findConfigFilePaths(baseDir, path)
		if err != nil {
			if err == configFileNotFoundErr {
				rawManifestPaths = append(rawManifestPaths, path)
				continue
			}
			return nil, nil, fmt.Errorf("error when searching config files for path %q: %s", relPath, err)
		}
		cf, err := NewConfigFile(configFilePath, workingDirPath)
		if err != nil {
			relConfigFilePath, relErr := filepath.Rel(baseDir, configFilePath)
			if err != nil {
				return nil, nil, relErr
			}
			return nil, nil, fmt.Errorf("cannot parse config file %q: %s", relConfigFilePath, err)
		}
		configFiles = append(configFiles, cf)
	}

	return configFiles, rawManifestPaths, nil
}

var configFileNotFoundErr = fmt.Errorf("config file not found")

func findConfigFilePaths(baseDir string, initialPath string) (string, string, error) {
	// The path can directly be a .flux.yaml config file
	fileStat, err := os.Stat(initialPath)
	if err != nil {
		return "", "", err
	}
	if !fileStat.IsDir() {
		workingDir, filename := filepath.Split(initialPath)
		if filename == ConfigFilename {
			return initialPath, filepath.Clean(workingDir), nil
		}
		return "", "", configFileNotFoundErr
	}

	// Make paths canonical and remove potential ending slash,
	// for filepath.Dir() to work as we expect.
	// Also, the initial path must be contained in baseDir
	// (to make sure we don't escape the git checkout when
	// moving upwards in the directory hierarchy)
	_, cleanInitialPath, err := cleanAndEnsurePaternity(baseDir, initialPath)
	if err != nil {
		return "", "", err
	}

	for path := cleanInitialPath; ; {
		potentialConfigFilePath := filepath.Join(path, ConfigFilename)
		if _, err := os.Stat(potentialConfigFilePath); err == nil {
			return potentialConfigFilePath, initialPath, nil
		}
		if path == baseDir {
			break
		}
		// check the parent directory
		path = filepath.Dir(path)
	}

	return "", "", configFileNotFoundErr
}

func (cm *checkoutManager) SetWorkloadContainerImage(id flux.ResourceID, container string, newImageID image.Ref) error {
	resourcesByID, err := cm.getResourcesByID()
	if err != nil {
		return err
	}
	resWithOrigin, ok := resourcesByID[id.String()]
	if !ok {
		return ErrResourceNotFound(id.String())
	}
	if resWithOrigin.configFile != nil {
		if err := cm.setManifestWorkloadContainerImage(resWithOrigin.resource, container, newImageID); err != nil {
			return err
		}
	}
	if err := cm.setConfigFileWorkloadContainerImage(resWithOrigin.configFile, resWithOrigin.resource, container, newImageID); err != nil {
		return err
	}
	// Reset resources, since we have modified one
	cm.resetResources()
	return nil
}

func (cm *checkoutManager) setManifestWorkloadContainerImage(r resource.Resource, container string, newImageID image.Ref) error {
	fullFilePath := filepath.Join(cm.checkout.Dir(), r.Source())
	def, err := ioutil.ReadFile(fullFilePath)
	if err != nil {
		return err
	}
	newDef, err := cm.manifests.SetWorkloadContainerImage(def, r.ResourceID(), container, newImageID)
	if err != nil {
		return err
	}
	fi, err := os.Stat(fullFilePath)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fullFilePath, newDef, fi.Mode())
}

func (cm *checkoutManager) setConfigFileWorkloadContainerImage(cf *ConfigFile, r resource.Resource,
	container string, newImageID image.Ref) error {
	if cf.PatchUpdated != nil {
		return cm.updatePatchFile(cf, func(previousManifests []byte) ([]byte, error) {
			return cm.manifests.SetWorkloadContainerImage(previousManifests, r.ResourceID(), container, newImageID)
		})
	}

	// Command-updated
	result := cf.ExecContainerImageUpdaters(cm.ctx,
		r.ResourceID(),
		container,
		newImageID.Name.String(), newImageID.Tag,
	)
	if len(result) > 0 && result[len(result)-1].Error != nil {
		updaters := cf.CommandUpdated.Updaters
		return fmt.Errorf("error executing image updater command %q from file %q: %s\noutput:\n%s",
			updaters[len(result)-1].ContainerImage.Command,
			result[len(result)-1].Error,
			r.Source(),
			result[len(result)-1].Output,
		)
	}
	return nil
}

func (cm *checkoutManager) updatePatchFile(cf *ConfigFile, updateF func(previousManifests []byte) ([]byte, error)) error {
	patchUpdated := *cf.PatchUpdated
	generatedManifests, patchedManifests, patchFilePath, err := cm.getGeneratedAndPatchedManifests(cf, patchUpdated)
	if err != nil {
		relConfigFilePath, err := filepath.Rel(cm.checkout.Dir(), cf.Path)
		if err != nil {
			return err
		}
		return fmt.Errorf("error parsing generated, patched output from file %s: %s", relConfigFilePath, err)
	}
	finalManifests, err := updateF(patchedManifests)
	newPatch, err := cm.manifests.CreateManifestPatch(generatedManifests, finalManifests,
		"generated manifests", "patched and updated manifests")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(patchFilePath, newPatch, 0600)
}

func (cm *checkoutManager) getGeneratedAndPatchedManifests(cf *ConfigFile, patchUpdated PatchUpdated) ([]byte, []byte, string, error) {
	generatedManifests, err := cm.getGeneratedManifests(cf, patchUpdated.Generators)
	if err != nil {
		return nil, nil, "", err
	}

	// The patch file is expressed relatively to the configuration file's working directory
	explicitPatchFilePath := patchUpdated.PatchFile
	patchFilePath := filepath.Join(cf.WorkingDir, explicitPatchFilePath)

	// Make sure that the patch file doesn't fall out of the Git repository checkout
	_, _, err = cleanAndEnsurePaternity(cm.checkout.Dir(), patchFilePath)
	if err != nil {
		return nil, nil, "", err
	}
	patch, err := ioutil.ReadFile(patchFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, "", err
		}
		// Tolerate a missing patch file, since it may not have been created yet.
		// However, its base path must exist.
		patchBaseDir := filepath.Dir(patchFilePath)
		if stat, err := os.Stat(patchBaseDir); err != nil || !stat.IsDir() {
			err := fmt.Errorf("base directory (%q) of patchFile (%q) does not exist",
				filepath.Dir(explicitPatchFilePath), explicitPatchFilePath)
			return nil, nil, "", err
		}
		patch = nil
	}
	relConfigFilePath, err := filepath.Rel(cm.checkout.Dir(), cf.Path)
	if err != nil {
		return nil, nil, "", err
	}
	patchedManifests, err := cm.manifests.ApplyManifestPatch(generatedManifests, patch, relConfigFilePath, explicitPatchFilePath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("cannot patch generated resources: %s", err)
	}
	return generatedManifests, patchedManifests, patchFilePath, nil
}

func (cm *checkoutManager) getGeneratedManifests(cf *ConfigFile, generators []Generator) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	for i, cmdResult := range cf.ExecGenerators(cm.ctx, generators) {
		relConfigFilePath, err := filepath.Rel(cm.checkout.Dir(), cf.Path)
		if err != nil {
			return nil, err
		}
		if cmdResult.Error != nil {
			err := fmt.Errorf("error executing generator command %q from file %q: %s\nerror output:\n%s\ngenerated output:\n%s",
				generators[i].Command,
				relConfigFilePath,
				cmdResult.Error,
				string(cmdResult.Stderr),
				string(cmdResult.Stderr),
			)
			return nil, err
		}
		if err := cluster.AppendManifestToBuffer(cmdResult.Stdout, buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (cm *checkoutManager) UpdateWorkloadPolicies(id flux.ResourceID, update policy.Update) (bool, error) {
	resourcesByID, err := cm.getResourcesByID()
	if err != nil {
		return false, err
	}
	resWithOrigin, ok := resourcesByID[id.String()]
	if !ok {
		return false, ErrResourceNotFound(id.String())
	}
	var changed bool
	if resWithOrigin.configFile != nil {
		changed, err = cm.updateManifestWorkloadPolicies(resWithOrigin.resource, update)
	} else {
		changed, err = cm.updateConfigFileWorkloadPolicies(resWithOrigin.configFile, resWithOrigin.resource, update)
	}
	if err != nil {
		return false, err
	}
	// Reset resources, since we have modified one
	cm.resetResources()
	return changed, nil
}

func (cm *checkoutManager) updateManifestWorkloadPolicies(r resource.Resource, update policy.Update) (bool, error) {
	fullFilePath := filepath.Join(cm.checkout.Dir(), r.Source())
	def, err := ioutil.ReadFile(fullFilePath)
	if err != nil {
		return false, err
	}
	newDef, err := cm.manifests.UpdateWorkloadPolicies(def, r.ResourceID(), update)
	if err != nil {
		return false, err
	}
	fi, err := os.Stat(fullFilePath)
	if err != nil {
		return false, err
	}
	if err := ioutil.WriteFile(fullFilePath, newDef, fi.Mode()); err != nil {
		return false, err
	}
	return bytes.Compare(def, newDef) != 0, nil
}

func (cm *checkoutManager) updateConfigFileWorkloadPolicies(cf *ConfigFile, r resource.Resource, update policy.Update) (bool, error) {
	if cf.PatchUpdated != nil {
		var changed bool
		err := cm.updatePatchFile(cf, func(previousManifests []byte) ([]byte, error) {
			updatedManifests, err := cm.manifests.UpdateWorkloadPolicies(previousManifests, r.ResourceID(), update)
			if err != nil {
				changed = bytes.Compare(previousManifests, updatedManifests) != 0
			}
			return updatedManifests, err
		})
		return changed, err
	}

	// Command-updated
	workload, ok := r.(resource.Workload)
	if !ok {
		return false, errors.New("resource " + r.ResourceID().String() + " does not have containers")
	}
	changes, err := cm.policyTranslator.GetAnnotationChangesForPolicyUpdate(workload, update)
	if err != nil {
		return false, err
	}
	for _, change := range changes {
		result := cf.ExecAnnotationUpdaters(cm.ctx,
			r.ResourceID(),
			change.AnnotationKey,
			change.AnnotationValue,
		)
		if len(result) > 0 && result[len(result)-1].Error != nil {
			updaters := cf.CommandUpdated.Updaters
			err := fmt.Errorf("error executing annotation updater command %q from file %q: %s\noutput:\n%s",
				updaters[len(result)-1].Annotation.Command,
				result[len(result)-1].Error,
				r.Source(),
				result[len(result)-1].Output,
			)
			return false, err
		}
	}
	// We assume that the update changed the resource. Alternatively, we could generate the resources
	// again and compare the output, but that's expensive.
	return true, nil
}

func (cm *checkoutManager) GetAllResourcesByID() (map[string]resource.Resource, error) {
	resourcesByID, err := cm.getResourcesByID()
	if err != nil {
		return nil, err
	}
	result := make(map[string]resource.Resource, len(resourcesByID))
	for id, resourceWithOrigin := range resourcesByID {
		result[id] = resourceWithOrigin.resource
	}
	return result, nil
}

func (cm *checkoutManager) getResourcesByID() (map[string]resourceWithOrigin, error) {
	cm.RLock()
	if cm.resourcesByID != nil {
		cm.RUnlock()
		return cm.resourcesByID, nil
	}
	cm.RUnlock()
	resourcesByID := map[string]resourceWithOrigin{}
	if len(cm.rawManifestDirs) > 0 {
		resources, err := cm.manifests.LoadManifests(cm.checkout.Dir(), cm.rawManifestDirs)
		if err != nil {
			return nil, err
		}
		for id, r := range resources {
			resourcesByID[id] = resourceWithOrigin{resource: r, configFile: nil}
		}
	}
	for _, cf := range cm.configFiles {
		var (
			resourceManifests []byte
			err               error
			originPath        string
		)
		if cf.CommandUpdated != nil {
			var err error
			resourceManifests, err = cm.getGeneratedManifests(cf, cf.CommandUpdated.Generators)
			if err != nil {
				return nil, err
			}
		} else {
			_, resourceManifests, err = cm.getGeneratedAndPatchedManifests(cf, *cf.PatchUpdated)
		}
		if err != nil {
			return nil, err
		}
		cluster.AppendManifestToBuffer(resourceManifests, manifestBuf)
	}
	cm.Lock()
	cm.resourcesByID = resourcesByID
	cm.Unlock()
	return resourcesByID, nil
}

func (cm *checkoutManager) resetResources() {
	cm.Lock()
	cm.resourcesByID = nil
	cm.Unlock()
}

func cleanAndEnsurePaternity(basePath string, childPath string) (string, string, error) {
	// Make paths canonical and remove potential ending slash,
	// for filepath.Dir() to work as we expect
	cleanBasePath, err := filepath.Abs(basePath)
	if err != nil {
		return "", "", err
	}
	cleanChildPath, err := filepath.Abs(childPath)
	if err != nil {
		return "", "", err
	}
	cleanBasePath = filepath.Clean(cleanBasePath)
	cleanChildPath = filepath.Clean(cleanChildPath)

	// The initial path must be relative to baseDir
	// (to make sure we don't escape the git checkout when
	// moving upwards in the directory hierarchy)
	if !strings.HasPrefix(cleanChildPath, cleanBasePath) {
		return "", "", fmt.Errorf("path %q is outside of base directory %s", childPath, basePath)
	}
	return cleanBasePath, cleanChildPath, nil
}
