package http

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	gh "github.com/google/go-github/github"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/middleware"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/api"
	"github.com/weaveworks/flux/http/httperror"
	"github.com/weaveworks/flux/http/websocket"
	"github.com/weaveworks/flux/integrations/github"
	"github.com/weaveworks/flux/jobs"
	"github.com/weaveworks/flux/platform/rpc"
)

func NewRouter() *mux.Router {
	r := mux.NewRouter()
	// Any versions not represented in the routes below are
	// deprecated. They are done separately so we can see them as
	// different methods in metrics and logging.
	for _, version := range []string{"v1", "v2"} {
		r.NewRoute().Name("Deprecated:" + version).PathPrefix("/" + version + "/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeError(w, r, http.StatusGone, ErrorDeprecated)
		})
	}

	r.NewRoute().Name("ListServices").Methods("GET").Path("/v3/services").Queries("namespace", "{namespace}") // optional namespace!
	r.NewRoute().Name("ListImages").Methods("GET").Path("/v3/images").Queries("service", "{service}")
	r.NewRoute().Name("PostRelease").Methods("POST").Path("/v4/release").Queries("service", "{service}", "image", "{image}", "kind", "{kind}")
	r.NewRoute().Name("GetRelease").Methods("GET").Path("/v4/release").Queries("id", "{id}")
	r.NewRoute().Name("Automate").Methods("POST").Path("/v3/automate").Queries("service", "{service}")
	r.NewRoute().Name("Deautomate").Methods("POST").Path("/v3/deautomate").Queries("service", "{service}")
	r.NewRoute().Name("Lock").Methods("POST").Path("/v3/lock").Queries("service", "{service}")
	r.NewRoute().Name("Unlock").Methods("POST").Path("/v3/unlock").Queries("service", "{service}")
	r.NewRoute().Name("History").Methods("GET").Path("/v3/history").Queries("service", "{service}")
	r.NewRoute().Name("Status").Methods("GET").Path("/v3/status")
	r.NewRoute().Name("GetConfig").Methods("GET").Path("/v4/config")
	r.NewRoute().Name("SetConfig").Methods("POST").Path("/v4/config")
	r.NewRoute().Name("GenerateDeployKeys").Methods("POST").Path("/v5/config/deploy-keys")
	r.NewRoute().Name("PostIntegrationsGithub").Methods("POST").Path("/v5/integrations/github").Queries("owner", "{owner}", "repository", "{repository}")
	r.NewRoute().Name("RegisterDaemon").Methods("GET").Path("/v4/daemon")
	r.NewRoute().Name("IsConnected").Methods("HEAD", "GET").Path("/v4/ping")
	r.NewRoute().Name("Watch").Methods("POST").Path("/v4/watch")
	r.NewRoute().Name("Unwatch").Methods("POST").Path("/v4/unwatch")
	r.NewRoute().Name("Hooks").Methods("POST").Path("/hooks/{externalInstanceID}") // Unversioned, as this gets put into remote services

	// We assume every request that doesn't match a route is a client
	// calling an old or hitherto unsupported API.
	r.NewRoute().Name("NotFound").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, MakeAPINotFound(r.URL.Path))
	})

	return r
}

func NewHandler(s api.FluxService, r *mux.Router, logger log.Logger) http.Handler {
	for method, handlerFunc := range map[string]func(api.FluxService) http.Handler{
		"ListServices":           handleListServices,
		"ListImages":             handleListImages,
		"PostRelease":            handlePostRelease,
		"GetRelease":             handleGetRelease,
		"Automate":               handleAutomate,
		"Deautomate":             handleDeautomate,
		"Lock":                   handleLock,
		"Unlock":                 handleUnlock,
		"History":                handleHistory,
		"Status":                 handleStatus,
		"GetConfig":              handleGetConfig,
		"SetConfig":              handleSetConfig,
		"GenerateDeployKeys":     handleGenerateKeys,
		"PostIntegrationsGithub": handlePostIntegrationsGithub,
		"RegisterDaemon":         handleRegister,
		"IsConnected":            handleIsConnected,
		"Watch":                  handleWatch,
		"Unwatch":                handleUnwatch,
		"Hooks":                  handleHooks,
	} {
		var handler http.Handler
		handler = handlerFunc(s)
		handler = logging(handler, log.NewContext(logger).With("method", method))

		r.Get(method).Handler(handler)
	}

	return middleware.Instrument{
		RouteMatcher: r,
		Duration:     requestDuration,
	}.Wrap(r)
}

func handleListServices(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		namespace := mux.Vars(r)["namespace"]
		res, err := s.ListServices(inst, namespace)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, res)
	})
}

func handleListImages(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		spec, err := flux.ParseServiceSpec(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service spec %q", service))
			return
		}

		d, err := s.ListImages(inst, spec)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, d)
	})
}

type postReleaseResponse struct {
	Status    string     `json:"status"`
	ReleaseID jobs.JobID `json:"release_id"`
}

func handlePostRelease(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			inst  = getInstanceID(r)
			vars  = mux.Vars(r)
			image = vars["image"]
			kind  = vars["kind"]
		)
		if err := r.ParseForm(); err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing form"))
			return
		}
		var serviceSpecs []flux.ServiceSpec
		for _, service := range r.Form["service"] {
			serviceSpec, err := flux.ParseServiceSpec(service)
			if err != nil {
				writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service spec %q", service))
				return
			}
			serviceSpecs = append(serviceSpecs, serviceSpec)
		}
		imageSpec, err := flux.ParseImageSpec(image)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing image spec %q", image))
			return
		}
		releaseKind, err := flux.ParseReleaseKind(kind)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing release kind %q", kind))
			return
		}

		var excludes []flux.ServiceID
		for _, ex := range r.URL.Query()["exclude"] {
			s, err := flux.ParseServiceID(ex)
			if err != nil {
				writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing excluded service %q", ex))
				return
			}
			excludes = append(excludes, s)
		}

		id, err := s.PostRelease(inst, jobs.ReleaseJobParams{
			ServiceSpecs: serviceSpecs,
			ImageSpec:    imageSpec,
			Kind:         releaseKind,
			Excludes:     excludes,
		})
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, postReleaseResponse{
			Status:    "Queued.",
			ReleaseID: id,
		})
	})
}

func handleGetRelease(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		id := mux.Vars(r)["id"]
		job, err := s.GetRelease(inst, jobs.JobID(id))
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, job)
	})
}

func handleAutomate(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		id, err := flux.ParseServiceID(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service ID %q", id))
			return
		}

		if err = s.Automate(inst, id); err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleDeautomate(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		id, err := flux.ParseServiceID(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service ID %q", id))
			return
		}

		if err = s.Deautomate(inst, id); err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleLock(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		id, err := flux.ParseServiceID(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service ID %q", id))
			return
		}

		if err = s.Lock(inst, id); err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleUnlock(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		id, err := flux.ParseServiceID(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service ID %q", id))
			return
		}

		if err = s.Unlock(inst, id); err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleHistory(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		service := mux.Vars(r)["service"]
		spec, err := flux.ParseServiceSpec(service)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.Wrapf(err, "parsing service spec %q", spec))
			return
		}

		h, err := s.History(inst, spec)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, h)
	})
}

func handleGetConfig(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		config, err := s.GetConfig(inst)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, config)
	})
}

func handleSetConfig(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)

		var config flux.UnsafeInstanceConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			writeError(w, r, http.StatusBadRequest, err)
			return
		}

		if err := s.SetConfig(inst, config); err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleGenerateKeys(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		err := s.GenerateDeployKey(inst)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handlePostIntegrationsGithub(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			inst  = getInstanceID(r)
			vars  = mux.Vars(r)
			owner = vars["owner"]
			repo  = vars["repository"]
			tok   = r.Header.Get("GithubToken")
		)

		if repo == "" || owner == "" || tok == "" {
			writeError(w, r, http.StatusUnprocessableEntity, errors.New("repo, owner or token is empty"))
			return
		}

		// Generate deploy key
		err := s.GenerateDeployKey(inst)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		// Obtain the generated key
		cfg, err := s.GetConfig(inst)
		if err != nil {
			errorResponse(w, r, err)
			return
		}
		publicKey := cfg.Git.HideKey().Key

		// Use the Github API to insert the key
		// Have to create a new instance here because there is no
		// clean way of injecting without significantly altering
		// the initialisation (at the top)
		gh := github.NewGithubClient(tok)
		err = gh.InsertDeployKey(owner, repo, publicKey)
		if err != nil {
			httpErr, isHttpErr := err.(*httperror.APIError)
			code := http.StatusInternalServerError
			if isHttpErr {
				code = httpErr.StatusCode
			}
			writeError(w, r, code, err)
			return
		}

		// Use the Github API to insert the webhook, if we have webhooks configured.
		endpoint, err := s.WebhookEndpoint(inst)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			return
		}
		if endpoint != "" {
			err = gh.InsertWebhook(owner, repo, endpoint)
			if err != nil {
				httpErr, isHttpErr := err.(*httperror.APIError)
				if isHttpErr {
					w.WriteHeader(httpErr.StatusCode)
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
				fmt.Fprintf(w, err.Error())
				return
			}
		}

		w.WriteHeader(http.StatusOK)
	})
}

func handleStatus(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		status, err := s.Status(inst)
		if err != nil {
			errorResponse(w, r, err)
			return
		}

		jsonResponse(w, r, status)
	})
}

func handleRegister(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)

		// This is not client-facing, so we don't do content
		// negotiation here.

		// Upgrade to a websocket
		ws, err := websocket.Upgrade(w, r, nil)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, err.Error())
			return
		}

		// Set up RPC. The service is a websocket _server_ but an RPC
		// _client_.
		rpcClient := rpc.NewClient(ws)

		// Make platform available to clients
		// This should block until the daemon disconnects
		// TODO: Handle the error here
		s.RegisterDaemon(inst, rpcClient)

		// Clean up
		// TODO: Handle the error here
		rpcClient.Close() // also closes the underlying socket
	})
}

func handleIsConnected(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)

		err := s.IsDaemonConnected(inst)
		if err == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		switch err.(type) {
		case flux.UserConfigProblem:
			// NB this has a specific contract for "cannot contact" -> // "404 not found"
			writeError(w, r, http.StatusNotFound, err)
		default:
			errorResponse(w, r, err)
		}
	})
}

type postWatchResponse struct {
	WebhookEndpoint string `json:"webhookEndpoint"`
}

func handleWatch(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		webhookEndpoint, err := s.Watch(inst)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			return
		}

		resp := postWatchResponse{WebhookEndpoint: webhookEndpoint}
		respBytes := bytes.Buffer{}
		if err = json.NewEncoder(&respBytes).Encode(resp); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes.Bytes())
		return
	})
}

func handleUnwatch(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := getInstanceID(r)
		err := s.Unwatch(inst)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	})
}

func handleHooks(s api.FluxService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Figure out where it came from
		// This is done by presence of headers.
		if r.Header.Get("X-GitHub-Delivery") == "" {
			// It's not from github... eh? They're the only ones we support right
			// now, so bail.
			w.WriteHeader(http.StatusNotFound)
			return
		}

		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			return
		}
		r.Body.Close()

		// Parse the event
		event, err := gh.ParseWebHook(gh.WebHookType(r), payload)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, err.Error())
			return
		}

		// For now we only do anything on push events from github
		switch event.(type) {
		case *gh.PushEvent:
			inst, err := internalInstanceIDFromExternal(mux.Vars(r)["externalInstanceID"])
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, err.Error())
				return
			}

			if err := s.RepoUpdate(inst); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, err.Error())
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		return
	})
}

// map external instance
func internalInstanceIDFromExternal(ext string) (flux.InstanceID, error) {
	return flux.DefaultInstanceID, fmt.Errorf("TODO: implement http.internalInstanceIDFromExternal")
}

// --- end handle

func mustGetPathTemplate(route *mux.Route) string {
	t, err := route.GetPathTemplate()
	if err != nil {
		panic(err)
	}
	return t
}

func makeURL(endpoint string, router *mux.Router, routeName string, urlParams ...string) (*url.URL, error) {
	if len(urlParams)%2 != 0 {
		panic("urlParams must be even!")
	}

	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing endpoint %s", endpoint)
	}

	routeURL, err := router.Get(routeName).URL()
	if err != nil {
		return nil, errors.Wrapf(err, "retrieving route path %s", routeName)
	}

	v := url.Values{}
	for i := 0; i < len(urlParams); i += 2 {
		v.Add(urlParams[i], urlParams[i+1])
	}

	endpointURL.Path = path.Join(endpointURL.Path, routeURL.Path)
	endpointURL.RawQuery = v.Encode()
	return endpointURL, nil
}

func getInstanceID(req *http.Request) flux.InstanceID {
	s := req.Header.Get(flux.InstanceIDHeaderKey)
	if s == "" {
		return flux.DefaultInstanceID
	}
	return flux.InstanceID(s)
}

func jsonResponse(w http.ResponseWriter, r *http.Request, result interface{}) {
	body, err := json.Marshal(result)
	if err != nil {
		errorResponse(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func errorResponse(w http.ResponseWriter, r *http.Request, apiError error) {
	var outErr *flux.BaseError
	var code int
	err := errors.Cause(apiError)
	switch err := err.(type) {
	case flux.Missing:
		code = http.StatusNotFound
		outErr = err.BaseError
	case flux.UserConfigProblem:
		code = http.StatusUnprocessableEntity
		outErr = err.BaseError
	case flux.ServerException:
		code = http.StatusInternalServerError
		outErr = err.BaseError
	default:
		code = http.StatusInternalServerError
		outErr = flux.CoverAllError(apiError)
	}

	writeError(w, r, code, outErr)
}

func writeError(w http.ResponseWriter, r *http.Request, code int, err error) {
	// An Accept header with "application/json" is sent by clients
	// understanding how to decode JSON errors. Older clients don't
	// send an Accept header, so we just give them the error text.
	if len(r.Header.Get("Accept")) > 0 {
		switch negotiateContentType(r, []string{"application/json", "text/plain"}) {
		case "application/json":
			body, encodeErr := json.Marshal(err)
			if encodeErr != nil {
				w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Error encoding error response: %s\n\nOriginal error: %s", encodeErr.Error(), err.Error())
				return
			}
			w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json; charset=utf-8")
			w.WriteHeader(code)
			w.Write(body)
			return
		case "text/plain":
			w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "text/plain; charset=utf-8")
			w.WriteHeader(code)
			switch err := err.(type) {
			case *flux.BaseError:
				fmt.Fprint(w, err.Help)
			default:
				fmt.Fprint(w, err.Error())
			}
			return
		}
	}
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "text/plain; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprint(w, err.Error())
}

func logging(next http.Handler, logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		begin := time.Now()
		cw := &codeWriter{w, http.StatusOK}
		tw := &teeWriter{cw, bytes.Buffer{}}

		next.ServeHTTP(tw, r)

		requestLogger := log.NewContext(logger).With(
			"url", mustUnescape(r.URL.String()),
			"took", time.Since(begin).String(),
			"status_code", cw.code,
		)
		if cw.code != http.StatusOK {
			requestLogger = requestLogger.With("error", strings.TrimSpace(tw.buf.String()))
		}
		requestLogger.Log()
	})
}

// codeWriter intercepts the HTTP status code. WriteHeader may not be called in
// case of success, so either prepopulate code with http.StatusOK, or check for
// zero on the read side.
type codeWriter struct {
	http.ResponseWriter
	code int
}

func (w *codeWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *codeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response does not implement http.Hijacker")
	}
	return hj.Hijack()
}

// teeWriter intercepts and stores the HTTP response.
type teeWriter struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (w *teeWriter) Write(p []byte) (int, error) {
	w.buf.Write(p) // best-effort
	return w.ResponseWriter.Write(p)
}

func (w *teeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response does not implement http.Hijacker")
	}
	return hj.Hijack()
}

func mustUnescape(s string) string {
	if unescaped, err := url.QueryUnescape(s); err == nil {
		return unescaped
	}
	return s
}
