package release

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/pkg/errors"

	"github.com/weaveworks/fluxy"
	"github.com/weaveworks/fluxy/git"
	"github.com/weaveworks/fluxy/history"
	"github.com/weaveworks/fluxy/platform/kubernetes"
)

type Releaser struct {
	helper    *flux.Helper
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

func New(
	helper *flux.Helper,
	repo git.Repo,
	history history.EventWriter,
	metrics Metrics,
) *Releaser {
	return &Releaser{
		helper:    helper,
		repo:      repo,
		history:   history,
		metrics:   metrics,
		semaphore: make(chan struct{}, maxSimultaneousReleases),
	}
}

func (r *Releaser) Release(serviceSpec flux.ServiceSpec, imageSpec flux.ImageSpec, kind flux.ReleaseKind) (res []flux.ReleaseAction, err error) {
	releaseType := "unknown"
	defer func(begin time.Time) {
		r.metrics.ReleaseDuration.With(
			"release_type", releaseType,
			"release_kind", fmt.Sprint(kind),
			"success", fmt.Sprint(err == nil),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	select {
	case r.semaphore <- struct{}{}:
		break // we've acquired the lock
	default:
		return nil, errors.New("a release is already in progress; please try again later")
	}
	defer func() { <-r.semaphore }()

	var actions []flux.ReleaseAction

	switch {
	case serviceSpec == flux.ServiceSpecAll && imageSpec == flux.ImageSpecLatest:
		releaseType = "release_all_to_latest"
		actions, err = r.releaseAllToLatest()

	case serviceSpec == flux.ServiceSpecAll && imageSpec == flux.ImageSpecNone:
		releaseType = "release_all_without_update"
		actions, err = r.releaseAllWithoutUpdate()

	case serviceSpec == flux.ServiceSpecAll:
		releaseType = "release_all_for_image"
		imageID := flux.ParseImageID(string(imageSpec))
		actions, err = r.releaseAllForImage(imageID)

	case imageSpec == flux.ImageSpecLatest:
		releaseType = "release_one_to_latest"
		serviceID, err := flux.ParseServiceID(string(serviceSpec))
		if err != nil {
			return nil, errors.Wrapf(err, "parsing service ID from spec %s", serviceSpec)
		}
		actions, err = r.releaseOneToLatest(serviceID)

	case imageSpec == flux.ImageSpecNone:
		releaseType = "release_one_without_update"
		serviceID, err := flux.ParseServiceID(string(serviceSpec))
		if err != nil {
			return nil, errors.Wrapf(err, "parsing service ID from spec %s", serviceSpec)
		}
		actions, err = r.releaseOneWithoutUpdate(serviceID)

	default:
		releaseType = "release_one"
		serviceID, err := flux.ParseServiceID(string(serviceSpec))
		if err != nil {
			return nil, errors.Wrapf(err, "parsing service ID from spec %s", serviceSpec)
		}
		imageID := flux.ParseImageID(string(imageSpec))
		actions, err = r.releaseOne(serviceID, imageID)
	}

	if kind == flux.ReleaseKindExecute {
		if err = r.execute(actions); err != nil {
			return actions, err
		}
	}

	return actions, nil
}

type stageTimer struct {
	base  metrics.Histogram
	stage *metrics.Timer
}

func (r *Releaser) newStageTimer(method string) *stageTimer {
	return &stageTimer{
		base: r.metrics.StageDuration.With("method", method),
	}
}

func (t *stageTimer) next(stageName string) {
	if t.stage != nil {
		t.stage.ObserveDuration()
	}
	t.stage = metrics.NewTimer(t.base.With("stage", stageName))
}

func (t *stageTimer) finish() {
	if t.stage != nil {
		t.stage.ObserveDuration()
	}
}

// Specific releaseX functions. The general idea:
// - Walk the platform and collect things to do;
// - If ReleaseKindExecute, execute those things; and then
// - Return the things we did (or didn't) do.

func (r *Releaser) releaseAllToLatest() (res []flux.ReleaseAction, err error) {
	res = append(res, r.releaseActionPrintf("I'm going to release all services to their latest images."))

	timer := r.newStageTimer("release_all_to_latest")
	defer timer.finish()

	timer.next("fetch_all_platform_services")

	serviceIDs, err := r.helper.AllServices()
	if err != nil {
		return nil, errors.Wrap(err, "fetching all platform services")
	}

	timer.next("all_releasable_images_for")

	containerMap, err := r.helper.AllReleasableImagesFor(serviceIDs)
	if err != nil {
		return nil, errors.Wrap(err, "fetching images for services")
	}

	timer.next("calculate_regrades")

	// Each service is running multiple images.
	// Each image may need to be upgraded, and trigger a release.

	regradeMap := map[flux.ServiceID][]containerRegrade{}
	for serviceID, containers := range containerMap {
		for _, container := range containers {
			currentImageID := flux.ParseImageID(container.Image)
			imageRepo, err := r.helper.RegistryGetRepository(currentImageID.Repository())
			if err != nil {
				return nil, errors.Wrapf(err, "fetching image repo for %s", currentImageID)
			}
			latestImage, err := imageRepo.LatestImage()
			if err != nil {
				return nil, errors.Wrapf(err, "getting latest image from %s", imageRepo.Name)
			}
			latestImageID := flux.ParseImageID(latestImage.String())
			if currentImageID == latestImageID {
				res = append(res, r.releaseActionPrintf("Service image %s is already the latest one; skipping.", currentImageID))
				continue
			}
			regradeMap[serviceID] = append(regradeMap[serviceID], containerRegrade{
				container: container.Name,
				current:   currentImageID,
				target:    latestImageID,
			})
		}
	}
	if len(regradeMap) <= 0 {
		res = append(res, r.releaseActionPrintf("All services are running the latest images. Nothing to do."))
		return res, nil
	}

	timer.next("finalize")

	// We have identified at least 1 release that needs to occur. Releasing
	// means cloning the repo, changing the resource file(s), committing and
	// pushing, and then making the release(s) to the platform.

	res = append(res, r.releaseActionClone())
	for service, regrades := range regradeMap {
		res = append(res, r.releaseActionUpdatePodController(service, regrades))
	}
	res = append(res, r.releaseActionCommitAndPush("Release latest images to all services"))
	for service := range regradeMap {
		res = append(res, r.releaseActionReleaseService(service, "latest images (to all services)"))
	}
	return res, nil
}

func (r *Releaser) releaseAllForImage(target flux.ImageID) (res []flux.ReleaseAction, err error) {
	res = append(res, r.releaseActionPrintf("I'm going to release image %s to all services that would use it.", target))

	timer := r.newStageTimer("release_all_for_image")
	defer timer.finish()

	timer.next("fetch_all_platform_services")

	serviceIDs, err := r.helper.AllServices()
	if err != nil {
		return nil, errors.Wrap(err, "fetching all platform services")
	}

	timer.next("all_releasable_images_for")

	containerMap, err := r.helper.AllReleasableImagesFor(serviceIDs)
	if err != nil {
		return nil, errors.Wrap(err, "fetching images for services")
	}

	// Each service is running multiple images.
	// Each image may need to be modified, and trigger a release.

	timer.next("calculate_regrades")

	regradeMap := map[flux.ServiceID][]containerRegrade{}
	for serviceID, containers := range containerMap {
		for _, container := range containers {
			candidate := flux.ParseImageID(container.Image)
			if candidate.Repository() != target.Repository() {
				continue
			}
			if candidate == target {
				res = append(res, r.releaseActionPrintf("Service %s image %s matches the target image exactly. Skipping.", serviceID, candidate))
				continue
			}
			regradeMap[serviceID] = append(regradeMap[serviceID], containerRegrade{
				container: container.Name,
				current:   candidate,
				target:    target,
			})
		}
	}
	if len(regradeMap) <= 0 {
		res = append(res, r.releaseActionPrintf("All matching services are already running image %s. Nothing to do.", target))
		return res, nil
	}

	timer.next("finalize")

	// We have identified at least 1 release that needs to occur. Releasing
	// means cloning the repo, changing the resource file(s), committing and
	// pushing, and then making the release(s) to the platform.

	res = append(res, r.releaseActionClone())
	for service, imageReleases := range regradeMap {
		res = append(res, r.releaseActionUpdatePodController(service, imageReleases))
	}
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release %s to all services", target)))
	for service := range regradeMap {
		res = append(res, r.releaseActionReleaseService(service, string(target)+" (to all services)"))
	}

	return res, nil
}

func (r *Releaser) releaseOneToLatest(id flux.ServiceID) (res []flux.ReleaseAction, err error) {
	res = append(res, r.releaseActionPrintf("I'm going to release the latest images(s) for service %s.", id))

	timer := r.newStageTimer("release_one_to_latest")
	defer timer.finish()
	timer.next("fetch_images_for_service")

	namespace, service := id.Components()
	containers, err := r.helper.PlatformContainersFor(namespace, service)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching images for service %s", id)
	}

	timer.next("calculate_regrades")

	// Each service is running multiple images.
	// Each image may need to be modified, and trigger a release.

	var regrades []containerRegrade
	for _, container := range containers {
		imageID := flux.ParseImageID(container.Image)
		imageRepo, err := r.helper.RegistryGetRepository(imageID.Repository())
		if err != nil {
			return nil, errors.Wrapf(err, "fetching repository for %s", imageID)
		}
		if len(imageRepo.Images) <= 0 {
			res = append(res, r.releaseActionPrintf("The service image %s had no images available in its repository; very strange!", imageID))
			continue
		}

		latestImage, err := imageRepo.LatestImage()
		if err != nil {
			return nil, errors.Wrapf(err, "getting latest image from %s", imageRepo.Name)
		}
		latestID := flux.ParseImageID(latestImage.String())
		if imageID == latestID {
			res = append(res, r.releaseActionPrintf("The service image %s is already at latest; skipping.", imageID))
			continue
		}
		regrades = append(regrades, containerRegrade{
			container: container.Name,
			current:   imageID,
			target:    latestID,
		})
	}
	if len(regrades) <= 0 {
		res = append(res, r.releaseActionPrintf("The service is already running the latest version of all its images. Nothing to do."))
		return res, nil
	}

	timer.next("finalize")

	// We need to make 1 release. Releasing means cloning the repo, changing the
	// resource file(s), committing and pushing, and then making the release(s)
	// to the platform.

	res = append(res, r.releaseActionClone())
	res = append(res, r.releaseActionUpdatePodController(id, regrades))
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release latest images to %s", id)))
	res = append(res, r.releaseActionReleaseService(id, "latest images"))

	return res, nil
}

func (r *Releaser) releaseOne(serviceID flux.ServiceID, target flux.ImageID) (res []flux.ReleaseAction, err error) {
	res = append(res, r.releaseActionPrintf("I'm going to release image %s to service %s.", target, serviceID))

	timer := r.newStageTimer("release_one")
	defer timer.finish()
	timer.next("fetch_images_for_service")

	namespace, service := serviceID.Components()
	containers, err := r.helper.PlatformContainersFor(namespace, service)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching images for service %s", serviceID)
	}

	timer.next("calculate_regrades")

	// Each service is running multiple images.
	// Each image may need to be modified, and trigger a release.

	var regrades []containerRegrade
	for _, container := range containers {
		candidate := flux.ParseImageID(container.Image)
		if candidate.Repository() != target.Repository() {
			res = append(res, r.releaseActionPrintf("Image %s is different than %s. Ignoring.", candidate, target))
			continue
		}
		if candidate == target {
			res = append(res, r.releaseActionPrintf("Image %s is already released. Skipping.", candidate))
			continue
		}
		regrades = append(regrades, containerRegrade{
			container: container.Name,
			current:   candidate,
			target:    target,
		})
	}
	if len(regrades) <= 0 {
		res = append(res, r.releaseActionPrintf("Service %s doesn't need a regrade to %s. Nothing to do.", serviceID, target))
		return res, nil
	}

	timer.next("finalize")

	// We have identified at least 1 regrade that needs to occur. Releasing
	// means cloning the repo, changing the resource file(s), committing and
	// pushing, and then making the release(s) to the platform.

	res = append(res, r.releaseActionClone())
	res = append(res, r.releaseActionUpdatePodController(serviceID, regrades))
	res = append(res, r.releaseActionCommitAndPush(fmt.Sprintf("Release %s to %s", target, serviceID)))
	res = append(res, r.releaseActionReleaseService(serviceID, string(target)))

	return res, nil
}

// Release whatever is in the cloned configuration, without changing anything
func (r *Releaser) releaseOneWithoutUpdate(serviceID flux.ServiceID) (res []flux.ReleaseAction, err error) {
	timer := r.newStageTimer("release_one_without_update")
	defer timer.finish()
	timer.next("finalize")

	actions := []flux.ReleaseAction{
		r.releaseActionPrintf("I'm going to release service %s using the config from the git repo, without updating it", serviceID),
		r.releaseActionClone(),
		r.releaseActionFindPodController(serviceID),
		r.releaseActionReleaseService(serviceID, "without update"),
	}
	return actions, nil
}

// Release whatever is in the cloned configuration, without changing anything
func (r *Releaser) releaseAllWithoutUpdate() (res []flux.ReleaseAction, err error) {
	timer := r.newStageTimer("release_all_without_update")
	defer timer.finish()
	timer.next("fetch_all_platform_services")

	serviceIDs, err := r.helper.AllServices()
	if err != nil {
		return nil, errors.Wrap(err, "fetching all platform services")
	}

	timer.next("finalize")

	actions := []flux.ReleaseAction{
		r.releaseActionPrintf("I'm going to release all services using the config from the git repo, without updating it."),
		r.releaseActionClone(),
	}

	for _, service := range serviceIDs {
		actions = append(actions,
			r.releaseActionFindPodController(service),
			r.releaseActionReleaseService(service, "without update (all services)"),
		)
	}

	return actions, nil
}

func (r *Releaser) execute(actions []flux.ReleaseAction) error {
	rc := flux.NewReleaseContext()
	defer rc.Clean()

	for i, action := range actions {
		r.helper.Log("description", action.Description)
		if action.Do == nil {
			continue
		}
		result, err := action.Do(rc)
		if err != nil {
			r.helper.Log("err", err)
			actions[i].Result = "Failed: " + err.Error()
			return err
		}
		actions[i].Result = result
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

func (r *Releaser) releaseActionPrintf(format string, args ...interface{}) flux.ReleaseAction {
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

func (r *Releaser) releaseActionClone() flux.ReleaseAction {
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
			return "OK", nil
		},
	}
}

func (r *Releaser) releaseActionFindPodController(service flux.ServiceID) flux.ReleaseAction {
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
			return "ok", nil
		},
	}
}

func (r *Releaser) releaseActionUpdatePodController(service flux.ServiceID, regrades []containerRegrade) flux.ReleaseAction {
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
			return "ok", nil
		},
	}
}

func (r *Releaser) releaseActionCommitAndPush(msg string) flux.ReleaseAction {
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

func (r *Releaser) releaseActionReleaseService(service flux.ServiceID, cause string) flux.ReleaseAction {
	return flux.ReleaseAction{
		Description: fmt.Sprintf("Release the service %s.", service),
		Do: func(rc *flux.ReleaseContext) (res string, err error) {
			defer func(begin time.Time) {
				r.metrics.ActionDuration.With(
					"action", "release_service",
					"success", fmt.Sprint(err == nil),
				).Observe(time.Since(begin).Seconds())
			}(time.Now())

			def, ok := rc.PodControllers[service]
			if !ok {
				return fmt.Sprintf("no definition for %s; ignoring", string(service)), nil
			}

			namespace, serviceName := service.Components()
			r.history.LogEvent(namespace, serviceName, "Starting release "+cause)

			err = r.helper.PlatformRelease(namespace, serviceName, def)
			if err == nil {
				r.history.LogEvent(namespace, serviceName, "Release "+cause+": done")
			} else {
				r.history.LogEvent(namespace, serviceName, "Release "+cause+": failed: "+err.Error())
			}

			return "", err
		},
	}
}
