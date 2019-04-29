package resourcestore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/weaveworks/flux"
)

const (
	ConfigFilename = ".flux.yaml"
	CommandTimeout = time.Minute
)

type ConfigFile struct {
	WorkingDir string
	Version    string
	Generators []Generator
	Updaters   []Updater
}

type Generator struct {
	Command string
}

type Updater struct {
	ContainerImage ContainerImageUpdater `yaml:"containerImage"`
	Annotation     AnnotationUpdater
}

type ContainerImageUpdater struct {
	Command string
}

type AnnotationUpdater struct {
	Command string
}

func NewConfigFile(path, workingDir string) (*ConfigFile, error) {
	var result ConfigFile
	fileBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read: %s", err)
	}
	if err := yaml.Unmarshal(fileBytes, &result); err != nil {
		return nil, fmt.Errorf("cannot parse: %s", err)
	}
	result.WorkingDir = workingDir
	return &result, nil
}

type ConfigFileExecResult struct {
	Error  error
	Stderr []byte
	Stdout []byte
}

type ConfigFileCombinedExecResult struct {
	Error  error
	Output []byte
}

func (cf *ConfigFile) ExecGenerators(ctx context.Context) []ConfigFileExecResult {
	result := []ConfigFileExecResult{}
	for _, g := range cf.Generators {
		stdErr := bytes.NewBuffer(nil)
		stdOut := bytes.NewBuffer(nil)
		err := cf.execCommand(ctx, nil, stdOut, stdErr, g.Command)
		r := ConfigFileExecResult{
			Stdout: stdOut.Bytes(),
			Stderr: stdErr.Bytes(),
			Error:  err,
		}
		result = append(result, r)
		// Stop exectuing on the first command error
		if err != nil {
			break
		}
	}
	return result
}

type ManifestUpdate struct {
	OriginalManifest, UpdatedManifest, StrategicMergePatch []byte
}

// ExecContainerImageUpdaters executes all the image updates in the configuration file.
// It will stop at the first error, in which case the returned error will be non-nil
func (cf *ConfigFile) ExecContainerImageUpdaters(ctx context.Context,
	workload flux.ResourceID, container string, image, imageTag string, mu ManifestUpdate) ([]ConfigFileCombinedExecResult, error) {
	env := makeEnvFromResourceID(workload)
	env = append(env,
		"FLUX_CONTAINER="+container,
		"FLUX_IMG="+image,
		"FLUX_TAG="+imageTag,
	)
	updateEnv, cleanup, err := makeEnvFromManifestUpdate(mu)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	env = append(env, updateEnv...)
	commands := []string{}
	for _, u := range cf.Updaters {
		commands = append(commands, u.ContainerImage.Command)
	}
	return cf.execCommandsWithCombinedOutput(ctx, env, commands), nil
}

// ExecAnnotationUpdaters executes all the annotation updates in the configuration file.
// It will stop at the first error, in which case the returned error will be non-nil
func (cf *ConfigFile) ExecAnnotationUpdaters(ctx context.Context,
	workload flux.ResourceID, annotationKey string, annotationValue *string, mu ManifestUpdate) ([]ConfigFileCombinedExecResult, error) {
	env := makeEnvFromResourceID(workload)
	env = append(env, "FLUX_ANNOTATION_KEY="+annotationKey)
	if annotationValue != nil {
		env = append(env, "FLUX_ANNOTATION_VALUE="+*annotationValue)
	}
	updateEnv, cleanup, err := makeEnvFromManifestUpdate(mu)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	env = append(env, updateEnv...)
	commands := []string{}
	for _, u := range cf.Updaters {
		commands = append(commands, u.Annotation.Command)
	}
	return cf.execCommandsWithCombinedOutput(ctx, env, commands), nil
}

func (cf *ConfigFile) execCommandsWithCombinedOutput(ctx context.Context, env []string, commands []string) []ConfigFileCombinedExecResult {
	env = append(env, "PATH="+os.Getenv("PATH"))
	result := []ConfigFileCombinedExecResult{}
	for _, c := range commands {
		stdOutAndErr := bytes.NewBuffer(nil)
		err := cf.execCommand(ctx, env, stdOutAndErr, stdOutAndErr, c)
		r := ConfigFileCombinedExecResult{
			Output: stdOutAndErr.Bytes(),
			Error:  err,
		}
		result = append(result, r)
		// Stop executing on the first command error
		if err != nil {
			break
		}
	}
	return result
}

func (cf *ConfigFile) execCommand(ctx context.Context, env []string, stdOut, stdErr io.Writer, command string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, CommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Env = env
	cmd.Dir = cf.WorkingDir
	cmd.Stdout = stdOut
	cmd.Stderr = stdErr
	err := cmd.Run()
	if cmdCtx.Err() == context.DeadlineExceeded {
		err = cmdCtx.Err()
	} else if cmdCtx.Err() == context.Canceled {
		err = errors.Wrap(ctx.Err(), fmt.Sprintf("context was unexpectedly cancelled"))
	}
	return err
}

func makeEnvFromResourceID(id flux.ResourceID) []string {
	ns, kind, name := id.Components()
	return []string{
		"FLUX_WORKLOAD=" + id.String(),
		"FLUX_WL_NS=" + ns,
		"FLUX_WL_KIND=" + kind,
		"FLUX_WL_NAME=" + name,
	}
}

func makeEnvFromManifestUpdate(mu ManifestUpdate) ([]string, func(), error) {
	entries := []struct {
		envPrefix string
		content   []byte
	}{

		{
			envPrefix: "FLUX_ORIGINAL_MANIFEST",
			content:   mu.OriginalManifest,
		},
		{
			envPrefix: "FLUX_UPDATED_MANIFEST",
			content:   mu.UpdatedManifest,
		},
		{
			envPrefix: "FLUX_MANIFEST_PATCH",
			content:   mu.StrategicMergePatch,
		},
	}

	var envs []string
	var paths []string
	for _, entry := range entries {
		env := entry.envPrefix + "_PATH"
		pattern := entry.envPrefix + "*" + ".yaml"
		f, err := ioutil.TempFile(os.TempDir(), pattern)
		if err != nil {
			return nil, nil, fmt.Errorf("creating file for %s: %s", env, err)
		}
		if _, err := f.Write(entry.content); err != nil {
			return nil, nil, fmt.Errorf("writing content to %s (%q): %s", env, f.Name(), err)
		}
		if err := f.Close(); err != nil {
			return nil, nil, fmt.Errorf("closing file in %s (%q): %s", env, f.Name(), err)
		}
		paths = append(paths, f.Name())
		envs = append(envs, env+"="+f.Name())
	}

	cleanup := func() {
		for _, p := range paths {
			os.Remove(p)
		}
	}

	return envs, cleanup, nil
}
