package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/weaveworks/flux/api"
	transport "github.com/weaveworks/flux/http"
	"github.com/weaveworks/flux/http/client"
)

type rootOpts struct {
	URL   string
	Token string
	API   api.Client
}

type serviceOpts struct {
	*rootOpts
}

func newService(parent *rootOpts) *serviceOpts {
	return &serviceOpts{rootOpts: parent}
}

func newRoot() *rootOpts {
	return &rootOpts{}
}

var rootLongHelp = strings.TrimSpace(`
fluxctl helps you deploy your code.

Workflow:
  fluxctl list-services                                        # Which services are running?
  fluxctl list-images --service=default/foo                    # Which images are running/available?
  fluxctl release --service=default/foo --update-image=bar:v2  # Release new version.
`)

const envVariableURL = "FLUX_URL"
const envVariableToken = "FLUX_SERVICE_TOKEN"

func (opts *rootOpts) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "fluxctl",
		Long:              rootLongHelp,
		SilenceUsage:      true,
		SilenceErrors:     true,
		PersistentPreRunE: opts.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&opts.URL, "url", "u", "https://cloud.weave.works/api/flux",
		fmt.Sprintf("base URL of the flux service; you can also set the environment variable %s", envVariableURL))
	cmd.PersistentFlags().StringVarP(&opts.Token, "token", "t", "",
		fmt.Sprintf("Weave Cloud service token; you can also set the environment variable %s", envVariableToken))

	svcopts := newService(opts)

	cmd.AddCommand(
		newVersionCommand(),
		newServiceShow(svcopts).Command(),
		newServiceList(svcopts).Command(),
		newServiceRelease(svcopts).Command(),
		// FIXME change to syncStatus
		//		newServiceCheckRelease(svcopts).Command(),
		newServiceAutomate(svcopts).Command(),
		newServiceDeautomate(svcopts).Command(),
		newServiceLock(svcopts).Command(),
		newServiceUnlock(svcopts).Command(),
		newSave(opts).Command(),
	)

	return cmd
}

func (opts *rootOpts) PersistentPreRunE(cmd *cobra.Command, _ []string) error {
	opts.URL = getFromEnvIfNotSet(cmd.Flags(), "url", envVariableURL, opts.URL)
	if _, err := url.Parse(opts.URL); err != nil {
		return errors.Wrapf(err, "parsing URL")
	}
	opts.Token = getFromEnvIfNotSet(cmd.Flags(), "token", envVariableToken, opts.Token)
	// TODO: consider reducing this to just the API
	opts.API = client.New(http.DefaultClient, transport.NewAPIRouter(), opts.URL, api.Token(opts.Token))
	return nil
}

func getFromEnvIfNotSet(flags *pflag.FlagSet, flagName, envName, value string) string {
	if flags.Changed(flagName) {
		return value
	}
	if env := os.Getenv(envName); env != "" {
		return env
	}
	return value // not changed, so presumably the default
}
