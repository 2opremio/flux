package release

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/pkg/errors"

	"github.com/weaveworks/fluxy"
	"github.com/weaveworks/fluxy/git"
	"github.com/weaveworks/fluxy/helper"
	"github.com/weaveworks/fluxy/history"
	"github.com/weaveworks/fluxy/platform"
	"github.com/weaveworks/fluxy/platform/kubernetes"
	"github.com/weaveworks/fluxy/registry"
)

type releaser struct {
	helper    *helper.Helper
	repo      git.Repo
	history   history.EventWriter
	metrics   Metrics
	semaphore chan struct{}
}

const maxSimultaneousReleases = 1

type Metrics struct {
	ReleaseDuration metrics.Histogram
	ActionDuration  metrics.Histogram
	StageDuration   metrics.Histogram
}

func newReleaser(
	platform *kubernetes.Cluster,
	registry *registry.Client,
	logger log.Logger,
	repo git.Repo,
	history history.EventWriter,
	metrics Metrics,
	helperDuration metrics.Histogram,
) *releaser {
	return &releaser{
		helper:    helper.New(platform, registry, logger, helperDuration),
		repo:      repo,
		history:   history,
		metrics:   metrics,
		semaphore: make(chan struct{}, maxSimultaneousReleases),
	}
}

func (r *releaser) Release(job *flux.ReleaseJob, updater flux.ReleaseJobUpdater) (err error) {
	releaseType := "unknown"
	defer func(begin time.Time) {
		r.metrics.ReleaseDuration.With(
			"release_type", releaseType,
			"release_kind", fmt.Sprint(job.Spec.Kind),
			"success", fmt.Sprint(err == nil),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	updateJob := func(format string, args ...interface{}) {
		status := fmt.Sprintf(format, args...)
		job.Status = status
		job.Log = append(job.Log, status)
		updater.UpdateJob(*job)
	}

	exclude := flux.ServiceIDSet{}
	exclude.Add(job.Spec.Excludes)

	select {
	case r.semaphore <- struct{}{}:
		break // we've acquired the lock
	default:
		return errors.New("a release is already in progress; please try again later")
	}
	defer func() { <-r.semaphore }()

	updateJob("Calculating release actions.")

	switch {
	case job.Spec.ServiceSpec == flux.ServiceSpecAll && job.Spec.ImageSpec == flux.ImageSpecLatest:
		releaseType = "release_all_to_latest"
		return r.releaseAllToLatest(job.Spec.Kind, exclude, updateJob)

	case job.Spec.ServiceSpec == flux.ServiceSpecAll && job.Spec.ImageSpec == flux.ImageSpecNone:
		releaseType = "release_all_without_update"
		return r.releaseAllWithoutUpdate(job.Spec.Kind, exclude, updateJob)

	case job.Spec.ServiceSpec == flux.ServiceSpecAll:
		releaseType = "release_all_for_image"
		imageID := flux.ParseImageID(string(job.Spec.ImageSpec))
		return r.releaseAllForImage(imageID, job.Spec.Kind, exclude, updateJob)

	case job.Spec.ImageSpec == flux.ImageSpecLatest:
		releaseType = "release_one_to_latest"
		serviceID, err := flux.ParseServiceID(string(job.Spec.ServiceSpec))
		if err != nil {
			return errors.Wrapf(err, "parsing service ID from spec %s", job.Spec.ServiceSpec)
		}
		return r.releaseOneToLatest(serviceID, job.Spec.Kind, exclude, updateJob)

	case job.Spec.ImageSpec == flux.ImageSpecNone:
		releaseType = "release_one_without_update"
		serviceID, err := flux.ParseServiceID(string(job.Spec.ServiceSpec))
		if err != nil {
			return errors.Wrapf(err, "parsing service ID from spec %s", job.Spec.ServiceSpec)
		}
		return r.releaseOneWithoutUpdate(serviceID, job.Spec.Kind, exclude, updateJob)

	default:
		releaseType = "release_one"
		serviceID, err := flux.ParseServiceID(string(job.Spec.ServiceSpec))
		if err != nil {
			return errors.Wrapf(err, "parsing service ID from spec %s", job.Spec.ServiceSpec)
		}
		imageID := flux.ParseImageID(string(job.Spec.ImageSpec))
		return r.releaseOne(serviceID, imageID, job.Spec.Kind, exclude, updateJob)
	}
}

func (r *releaser) releaseAllToLatest(kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	res = append(res, r.releaseActionPrintf("I'm going to release all services to their latest images."))

	var (
		base  = r.metrics.StageDuration.With("method", "release_all_to_latest")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "fetch_all_platform_services"))

	services, err := r.helper.GetAllServices("")
	if err != nil {
		return errors.Wrap(err, "fetching all platform services")
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "calculate_regrades"))

	// Each service is running multiple images.
	// Each image may need to be upgraded, and trigger a release.
	images, err := r.helper.CollectAvailableImages(services)
	if err != nil {
		return errors.Wrap(err, "collecting available images to calculate regrades")
	}

	regradeMap := map[flux.ServiceID][]containerRegrade{}
	for _, service := range services {
		containers, err := service.ContainersOrError()
		if err != nil {
			res = append(res, r.releaseActionPrintf("service %s does not have images associated: %s", service.ID, err))
			continue
		}
		for _, container := range containers {
			currentImageID := flux.ParseImageID(container.Image)
			latestImage, err := images.LatestImage(currentImageID.Repository())
			if err != nil {
				return errors.Wrapf(err, "getting latest image from %s", currentImageID.Repository())
			}

			if currentImageID == latestImage.ID {
				res = append(res, r.releaseActionPrintf("Service image %s is already the latest one; skipping.", currentImageID))
				continue
			}

			regradeMap[service.ID] = append(regradeMap[service.ID], containerRegrade{
				container: container.Name,
				current:   currentImageID,
				target:    latestImage.ID,
			})
		}
	}
	if len(regradeMap) <= 0 {
		res = append(res, r.releaseActionPrintf("All services are running the latest images. Nothing to do."))
		return nil
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	// We have identified at least 1 release that needs to occur. Releasing
	// means cloning the repo, changing the resource file(s), committing and
	// pushing, and then making the release(s) to the platform.

	res = append(res, r.releaseActionClone())
	for service, regrades := range regradeMap {
		res = append(res, r.releaseActionUpdatePodController(service, regrades))
	}
	res = append(res, r.releaseActionCommitAndPush("Release latest images to all services"))
	var servicesToRegrade []flux.ServiceID
	for service := range regradeMap {
		servicesToRegrade = append(servicesToRegrade, service)
	}
	res = append(res, r.releaseActionRegradeServices(servicesToRegrade, "latest images (to all services)"))

	return nil
}

func (r *releaser) releaseAllForImage(target flux.ImageID, kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	res = append(res, r.releaseActionPrintf("I'm going to release image %s to all services that would use it.", target))

	var (
		base  = r.metrics.StageDuration.With("method", "release_all_for_image")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "fetch_all_platform_services"))

	services, err := r.helper.GetAllServicesExcept("", exclude)
	if err != nil {
		return errors.Wrap(err, "fetching all platform services")
	}

	// Each service is running multiple images.
	// Each image may need to be modified, and trigger a release.

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "calculate_regrades"))

	regradeMap := map[flux.ServiceID][]containerRegrade{}
	for _, service := range services {
		containers, err := service.ContainersOrError()
		if err != nil {
			res = append(res, r.releaseActionPrintf("service %s does not have images associated: %s", service.ID, err))
			continue
		}
		for _, container := range containers {
			candidate := flux.ParseImageID(container.Image)
			if candidate.Repository() != target.Repository() {
				continue
			}
			if candidate == target {
				res = append(res, r.releaseActionPrintf("Service %s image %s matches the target image exactly. Skipping.", service.ID, candidate))
				continue
			}
			regradeMap[service.ID] = append(regradeMap[service.ID], containerRegrade{
				container: container.Name,
				current:   candidate,
				target:    target,
			})
		}
	}
	if len(regradeMap) <= 0 {
		res = append(res, r.releaseActionPrintf("All matching services are already running image %s. Nothing to do.", target))
		return nil
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	// We have identified at least 1 release that needs to occur. Releasing
	// means cloning the repo, changing the resource file(s), committing and
	// pushing, and then making the release(s) to the platform.

	res = append(res, r.releaseActionClone())
	for service, imageReleases := range regradeMap {
		res = append(res, r.releaseActionUpdatePodController(service, imageReleases))
	}
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release %s to all services", target)))
	var servicesToRegrade []flux.ServiceID
	for service := range regradeMap {
		servicesToRegrade = append(servicesToRegrade, service)
	}
	res = append(res, r.releaseActionRegradeServices(servicesToRegrade, string(target)+" (to all services)"))

	return nil
}

func (r *releaser) releaseOneToLatest(serviceID flux.ServiceID, kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	res = append(res, r.releaseActionPrintf("I'm going to release the latest images(s) for service %s.", serviceID))

	var (
		base  = r.metrics.StageDuration.With("method", "release_one_to_latest")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "fetch_images_for_service"))

	if exclude.Contains(serviceID) {
		res = append(res, r.releaseActionPrintf("Specified service %s is excluded; ignoring.", serviceID))
		return nil
	}

	services, err := r.helper.GetServices([]flux.ServiceID{serviceID})
	if err != nil {
		return errors.Wrapf(err, "fetching service %s from platform", serviceID)
	}
	if len(services) != 1 {
		return errors.New(fmt.Sprintf("expected exactly 1 service, found %d", len(services)))
	}

	service := services[0]

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "calculate_regrades"))

	containers, err := service.ContainersOrError()
	if err != nil {
		return errors.Wrapf(err, "service %s has no associated images", serviceID)
	}

	images, err := r.helper.CollectAvailableImages(services)
	if err != nil {
		return errors.Wrapf(err, "fetching images for service %s", serviceID)
	}

	var regrades []containerRegrade
	for _, container := range containers {
		currentImageID := flux.ParseImageID(container.Image)
		latestImage, err := images.LatestImage(currentImageID.Repository())
		if err != nil {
			return errors.Wrapf(err, "getting latest image from %s", currentImageID.Repository())
		}

		if currentImageID == latestImage.ID {
			res = append(res, r.releaseActionPrintf("Service image %s is already the latest one; skipping.", currentImageID))
			continue
		}

		regrades = append(regrades, containerRegrade{
			container: container.Name,
			current:   currentImageID,
			target:    latestImage.ID,
		})
	}
	if len(regrades) <= 0 {
		res = append(res, r.releaseActionPrintf("The service is already running the latest version of all its images. Nothing to do."))
		return nil
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	// We need to make 1 release. Releasing means cloning the repo, changing the
	// resource file(s), committing and pushing, and then making the release(s)
	// to the platform.

	res = append(res, r.releaseActionClone())
	res = append(res, r.releaseActionUpdatePodController(serviceID, regrades))
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release latest images to %s", serviceID)))
	res = append(res, r.releaseActionRegradeServices([]flux.ServiceID{serviceID}, "latest images"))

	return nil
}

func (r *releaser) releaseOne(serviceID flux.ServiceID, target flux.ImageID, kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	res = append(res, r.releaseActionPrintf("I'm going to release image %s to service %s.", target, serviceID))

	var (
		base  = r.metrics.StageDuration.With("method", "release_one")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "fetch_images_for_service"))

	if exclude.Contains(serviceID) {
		res = append(res, r.releaseActionPrintf("Specified service %s is excluded; ignoring.", serviceID))
		return nil
	}

	services, err := r.helper.GetServices([]flux.ServiceID{serviceID})
	if err != nil {
		return errors.Wrapf(err, "fetching service %s from platform", serviceID)
	}
	if len(services) != 1 {
		return errors.New(fmt.Sprintf("expected exactly 1 service, found %d", len(services)))
	}

	service := services[0]

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "calculate_regrades"))

	var regrades []containerRegrade
	containers, err := service.ContainersOrError()
	if err != nil {
		return errors.Wrapf(err, "service %s has no associated images", serviceID)
	}
	for _, container := range containers {
		candidate := flux.ParseImageID(container.Image)
		if candidate.Repository() != target.Repository() {
			continue
		}
		if candidate == target {
			res = append(res, r.releaseActionPrintf("Service %s image %s matches the target image exactly. Skipping.", service.ID, candidate))
			continue
		}
		regrades = append(regrades, containerRegrade{
			container: container.Name,
			current:   candidate,
			target:    target,
		})
	}

	if len(regrades) <= 0 {
		res = append(res, r.releaseActionPrintf("All matching services are already running image %s. Nothing to do.", target))
		return nil
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	res = append(res, r.releaseActionClone())
	res = append(res, r.releaseActionUpdatePodController(serviceID, regrades))
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release %s to %s", target, serviceID)))
	res = append(res, r.releaseActionRegradeServices([]flux.ServiceID{serviceID}, string(target)))

	return nil
}

// Release whatever is in the cloned configuration, without changing anything
func (r *releaser) releaseOneWithoutUpdate(serviceID flux.ServiceID, kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	var (
		base  = r.metrics.StageDuration.With("method", "release_one_without_update")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	if exclude.Contains(serviceID) {
		res = append(res, r.releaseActionPrintf("Specified service %s is excluded; ignoring.", serviceID))
		return nil
	}

	res = append(res, r.releaseActionPrintf("I'm going to release service %s using the config from the git repo, without updating it", serviceID))
	res = append(res, r.releaseActionClone())
	res = append(res, r.releaseActionFindPodController(serviceID))
	res = append(res, r.releaseActionRegradeServices([]flux.ServiceID{serviceID}, "without update"))

	return nil
}

// Release whatever is in the cloned configuration, without changing anything
func (r *releaser) releaseAllWithoutUpdate(kind flux.ReleaseKind, exclude flux.ServiceIDSet, updateJob func(string, ...interface{})) (err error) {
	var res []flux.ReleaseAction
	defer func() {
		if err == nil {
			err = r.execute(res, kind, updateJob)
		}
	}()

	var (
		base  = r.metrics.StageDuration.With("method", "release_all_without_update")
		stage *metrics.Timer
	)

	defer func() { stage.ObserveDuration() }()
	stage = metrics.NewTimer(base.With("stage", "fetch_all_platform_services"))

	services, err := r.helper.GetAllServicesExcept("", exclude)
	if err != nil {
		return errors.Wrap(err, "fetching all platform services")
	}

	stage.ObserveDuration()
	stage = metrics.NewTimer(base.With("stage", "finalize"))

	res = append(res, r.releaseActionPrintf("I'm going to release all services using the config from the git repo, without updating it."))
	res = append(res, r.releaseActionClone())

	ids := []flux.ServiceID{}
	for _, service := range services {
		res = append(res, r.releaseActionFindPodController(service.ID))
		ids = append(ids, service.ID)
	}
	res = append(res, r.releaseActionRegradeServices(ids, "without update (all services)"))

	return nil
}

func (r *releaser) execute(actions []flux.ReleaseAction, kind flux.ReleaseKind, updateJob func(string, ...interface{})) error {
	rc := flux.NewReleaseContext()
	defer rc.Clean()

	for i, action := range actions {
		updateJob(action.Description)
		r.helper.Log("description", action.Description)
		if action.Do == nil {
			continue
		}

		if kind == flux.ReleaseKindExecute {
			result, err := action.Do(rc)
			if err != nil {
				updateJob(err.Error())
				r.helper.Log("err", err)
				actions[i].Result = "Failed: " + err.Error()
				return err
			}
			if result != "" {
				updateJob(result)
			}
			actions[i].Result = result
		}
	}

	return nil
}

// Release helpers.

type containerRegrade struct {
	container string
	current   flux.ImageID
	target    flux.ImageID
}

// ReleaseAction Do funcs

func (r *releaser) releaseActionPrintf(format string, args ...interface{}) flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: fmt.Sprintf(format, args...),
		Do: func(_ *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "printf",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			return "", nil
		},
	}
}

func (r *releaser) releaseActionClone() flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: "Clone the config repo.",
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "clone",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			path, keyFile, err := r.repo.Clone()
			if err != nil {
				return "", errors.Wrap(err, "clone the config repo")
			}
			rc.RepoPath = path
			rc.RepoKey = keyFile
			return "Clone OK.", nil
		},
	}
}

func (r *releaser) releaseActionFindPodController(service flux.ServiceID) flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: fmt.Sprintf("Load the resource definition file for service %s", service),
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "find_pod_controller",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			resourcePath := filepath.Join(rc.RepoPath, r.repo.Path)
			if fi, err := os.Stat(resourcePath); err != nil || !fi.IsDir() {
				return "", fmt.Errorf("the resource path (%s) is not valid", resourcePath)
			}

			namespace, serviceName := service.Components()
			files, err := kubernetes.FilesFor(resourcePath, namespace, serviceName)

			if err != nil {
				return "", errors.Wrapf(err, "finding resource definition file for %s", service)
			}
			if len(files) <= 0 { // fine; we'll just skip it
				return fmt.Sprintf("no resource definition file found for %s; skipping", service), nil
			}
			if len(files) > 1 {
				return "", fmt.Errorf("multiple resource definition files found for %s: %s", service, strings.Join(files, ", "))
			}

			def, err := ioutil.ReadFile(files[0]) // TODO(mb) not multi-doc safe
			if err != nil {
				return "", err
			}
			rc.PodControllers[service] = def
			return "Found pod controller OK.", nil
		},
	}
}

func (r *releaser) releaseActionUpdatePodController(service flux.ServiceID, regrades []containerRegrade) flux.ReleaseAction {
	var actions []string
	for _, regrade := range regrades {
		actions = append(actions, fmt.Sprintf("%s (%s -> %s)", regrade.container, regrade.current, regrade.target))
	}
	actionList := strings.Join(actions, ", ")

	return flux.ReleaseAction{
		Description: fmt.Sprintf("Update %d images(s) in the resource definition file for %s: %s.", len(regrades), service, actionList),
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "update_pod_controller",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			resourcePath := filepath.Join(rc.RepoPath, r.repo.Path)
			if fi, err := os.Stat(resourcePath); err != nil || !fi.IsDir() {
				return "", fmt.Errorf("the resource path (%s) is not valid", resourcePath)
			}

			namespace, serviceName := service.Components()
			files, err := kubernetes.FilesFor(resourcePath, namespace, serviceName)
			if err != nil {
				return "", errors.Wrapf(err, "finding resource definition file for %s", service)
			}
			if len(files) <= 0 {
				return fmt.Sprintf("no resource definition file found for %s; skipping", service), nil
			}
			if len(files) > 1 {
				return "", fmt.Errorf("multiple resource definition files found for %s: %s", service, strings.Join(files, ", "))
			}

			def, err := ioutil.ReadFile(files[0])
			if err != nil {
				return "", err
			}
			fi, err := os.Stat(files[0])
			if err != nil {
				return "", err
			}

			for _, regrade := range regrades {
				// Note 1: UpdatePodController parses the target (new) image
				// name, extracts the repository, and only mutates the line(s)
				// in the definition that match it. So for the time being we
				// ignore the current image. UpdatePodController could be
				// updated, if necessary.
				//
				// Note 2: we keep overwriting the same def, to handle multiple
				// images in a single file.
				def, err = kubernetes.UpdatePodController(def, string(regrade.target), ioutil.Discard)
				if err != nil {
					return "", errors.Wrapf(err, "updating pod controller for %s", regrade.target)
				}
			}

			// Write the file back, so commit/push works.
			if err := ioutil.WriteFile(files[0], def, fi.Mode()); err != nil {
				return "", err
			}

			// Put the def in the map, so release works.
			rc.PodControllers[service] = def
			return "Update pod controller OK.", nil
		},
	}
}

func (r *releaser) releaseActionCommitAndPush(msg string) flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: "Commit and push the config repo.",
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "commit_and_push",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			if fi, err := os.Stat(rc.RepoPath); err != nil || !fi.IsDir() {
				return "", fmt.Errorf("the repo path (%s) is not valid", rc.RepoPath)
			}
			if _, err := os.Stat(rc.RepoKey); err != nil {
				return "", fmt.Errorf("the repo key (%s) is not valid: %v", rc.RepoKey, err)
			}
			result, err := r.repo.CommitAndPush(rc.RepoPath, rc.RepoKey, msg)
			if err == nil && result == "" {
				return "Pushed commit: " + msg, nil
			}
			return result, err
		},
	}
}

func service2string(a []flux.ServiceID) []string {
	s := make([]string, len(a))
	for i := range a {
		s[i] = string(a[i])
	}
	return s
}

func (r *releaser) releaseActionRegradeServices(services []flux.ServiceID, cause string) flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: fmt.Sprintf("Regrade %d service(s): %s.", len(services), strings.Join(service2string(services), ", ")),
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "regrade_services",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			// We'll collect results for each service regrade.
			results := map[flux.ServiceID]error{}

			// Collect specs for each service regrade.
			var specs []platform.RegradeSpec
			for _, service := range services {
				def, ok := rc.PodControllers[service]
				if !ok {
					results[service] = errors.New("no pod controller in release context; skipping regrade")
					continue
				}

				namespace, serviceName := service.Components()
				r.history.LogEvent(namespace, serviceName, "Starting regrade "+cause)
				specs = append(specs, platform.RegradeSpec{
					NamespacedService: platform.NamespacedService{
						Namespace: namespace,
						Service:   serviceName,
					},
					NewDefinition: def,
				})
			}

			// Execute the regrades as a single transaction.
			// Splat any errors into our results map.
			transactionErr := r.helper.PlatformRegrade(specs)
			if transactionErr != nil {
				for ns, regradeErr := range transactionErr.(platform.RegradeError) {
					id, _ := flux.ParseServiceID(fmt.Sprintf("%s/%s", ns.Namespace, ns.Service))
					results[id] = regradeErr
				}
			}

			// Report individual service regrade results.
			for _, service := range services {
				namespace, serviceName := service.Components()
				if err := results[service]; err == nil { // no entry = nil error
					r.history.LogEvent(namespace, serviceName, "Regrade "+cause+": done")
				} else {
					r.history.LogEvent(namespace, serviceName, "Regrade "+cause+": failed: "+err.Error())
				}
			}

			return "", transactionErr
		},
	}
}
