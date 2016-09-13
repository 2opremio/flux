package flux

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/pkg/errors"

	"github.com/weaveworks/fluxy/history"
)

type server struct {
	helper      *Helper
	releaser    Releaser
	automator   Automator
	history     history.EventReader
	maxPlatform chan struct{} // semaphore for concurrent calls to the platform
	metrics     Metrics
}

type Automator interface {
	Automate(namespace, service string) error
	Deautomate(namespace, service string) error
	IsAutomated(namespace, service string) bool
}

type Releaser interface {
	Release(ServiceSpec, ImageSpec, ReleaseKind) ([]ReleaseAction, error)
}

type Metrics struct {
	ListServicesDuration metrics.Histogram
	ListImagesDuration   metrics.Histogram
	HistoryDuration      metrics.Histogram
}

func NewServer(
	helper *Helper,
	releaser Releaser,
	automator Automator,
	history history.EventReader,
	metrics Metrics,
) Service {
	return &server{
		helper:      helper,
		releaser:    releaser,
		automator:   automator,
		history:     history,
		maxPlatform: make(chan struct{}, 8),
		metrics:     metrics,
	}
}

// The server methods are deliberately awkward, cobbled together from existing
// platform and registry APIs. I want to avoid changing those components until I
// get something working. There's also a lot of code duplication here for the
// same reason: let's not add abstraction until it's merged, or nearly so, and
// it's clear where the abstraction should exist.

func (s *server) ListServices(namespace string) (res []ServiceStatus, err error) {
	defer func(begin time.Time) {
		s.metrics.ListServicesDuration.With(
			"namespace", namespace,
			"success", fmt.Sprint(err == nil),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	var serviceIDs []ServiceID
	if namespace == "" {
		serviceIDs, err = s.helper.AllServices()
	} else {
		serviceIDs, err = s.helper.NamespaceServices(namespace)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "fetching services for namespace %s on the platform", namespace)
	}

	var (
		statusc = make(chan ServiceStatus)
		errc    = make(chan error)
	)
	for _, serviceID := range serviceIDs {
		go func(serviceID ServiceID) {
			s.maxPlatform <- struct{}{}
			defer func() { <-s.maxPlatform }()

			c, err := s.containersFor(serviceID)
			if err != nil {
				errc <- errors.Wrapf(err, "fetching containers for %s", serviceID)
				return
			}

			namespace, service := serviceID.Components()
			platformSvc, err := s.helper.PlatformService(namespace, service)
			if err != nil {
				errc <- errors.Wrapf(err, "getting platform service %s", serviceID)
				return
			}

			statusc <- ServiceStatus{
				ID:         serviceID,
				Containers: c,
				Status:     platformSvc.Status,
				Automated:  s.automator.IsAutomated(namespace, service),
			}
		}(serviceID)
	}
	for i := 0; i < len(serviceIDs); i++ {
		select {
		case err := <-errc:
			s.helper.Log("err", err)
		case status := <-statusc:
			res = append(res, status)
		}
	}
	return res, nil
}

func (s *server) ListImages(spec ServiceSpec) (res []ImageStatus, err error) {
	defer func(begin time.Time) {
		s.metrics.ListImagesDuration.With(
			"service_spec", fmt.Sprint(spec),
			"success", fmt.Sprint(err == nil),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	serviceIDs, err := func() ([]ServiceID, error) {
		if spec == ServiceSpecAll {
			return s.helper.AllServices()
		}
		id, err := ParseServiceID(string(spec))
		return []ServiceID{id}, err
	}()
	if err != nil {
		return nil, errors.Wrapf(err, "fetching service ID(s)")
	}

	var (
		statusc = make(chan ImageStatus)
		errc    = make(chan error)
	)
	for _, serviceID := range serviceIDs {
		go func(serviceID ServiceID) {
			s.maxPlatform <- struct{}{}
			defer func() { <-s.maxPlatform }()

			c, err := s.containersFor(serviceID)
			if err != nil {
				errc <- errors.Wrapf(err, "fetching containers for %s", serviceID)
				return
			}

			statusc <- ImageStatus{
				ID:         serviceID,
				Containers: c,
			}
		}(serviceID)
	}
	for i := 0; i < len(serviceIDs); i++ {
		select {
		case err := <-errc:
			s.helper.Log("err", err)
		case status := <-statusc:
			res = append(res, status)
		}
	}

	return res, nil
}

func (s *server) History(spec ServiceSpec) (res []HistoryEntry, err error) {
	defer func(begin time.Time) {
		s.metrics.HistoryDuration.With(
			"service_spec", fmt.Sprint(spec),
			"success", fmt.Sprint(err == nil),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	var events []history.Event
	if spec == ServiceSpecAll {
		namespaces, err := s.helper.PlatformNamespaces()
		if err != nil {
			return nil, errors.Wrap(err, "fetching platform namespaces")
		}

		for _, namespace := range namespaces {
			ev, err := s.history.AllEvents(namespace)
			if err != nil {
				return nil, errors.Wrapf(err, "fetching all history events for namespace %s", namespace)
			}

			events = append(events, ev...)
		}
	} else {
		id, err := ParseServiceID(string(spec))
		if err != nil {
			return nil, errors.Wrapf(err, "parsing service ID from spec %s", spec)
		}

		namespace, service := id.Components()
		ev, err := s.history.EventsForService(namespace, service)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching history events for %s", id)
		}

		events = append(events, ev...)
	}

	res = make([]HistoryEntry, len(events))
	for i, event := range events {
		res[i] = HistoryEntry{
			Stamp: event.Stamp,
			Type:  "v0",
			Data:  fmt.Sprintf("%s: %s", event.Service, event.Msg),
		}
	}

	return res, nil
}

func (s *server) Automate(service ServiceID) error {
	ns, svc := service.Components()
	return s.automator.Automate(ns, svc)
}

func (s *server) Deautomate(service ServiceID) error {
	ns, svc := service.Components()
	return s.automator.Deautomate(ns, svc)
}

func (s *server) Release(service ServiceSpec, image ImageSpec, kind ReleaseKind) ([]ReleaseAction, error) {
	return s.releaser.Release(service, image, kind)
}

func (s *server) containersFor(id ServiceID) (res []Container, _ error) {
	namespace, service := id.Components()
	containers, err := s.helper.PlatformContainersFor(namespace, service)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching containers for %s", id)
	}

	var errs compositeError
	for _, container := range containers {
		imageID := ParseImageID(container.Image)

		// We may not be able to get image info from the repository,
		// but it's still worthwhile returning what we know.
		current := ImageDescription{ID: imageID}
		var available []ImageDescription

		imageRepo, err := s.helper.RegistryGetRepository(imageID.Repository())
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "fetching image repo for %s", imageID))
		} else {
			for _, image := range imageRepo.Images {
				description := ImageDescription{
					ID:        ParseImageID(image.String()),
					CreatedAt: image.CreatedAt,
				}
				available = append(available, description)
				if image.String() == container.Image {
					current = description
				}
			}
		}
		res = append(res, Container{
			Name:      container.Name,
			Current:   current,
			Available: available,
		})
	}

	if len(errs) > 0 {
		return res, errors.Wrap(errs, "one or more errors fetching image repos")
	}
	return res, nil
}

type compositeError []error

func (e compositeError) Error() string {
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = err.Error()
	}
	return strings.Join(msgs, "; ")
}
