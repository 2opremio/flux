package main

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/weaveworks/flux/api"
	"github.com/weaveworks/flux/git"
	"github.com/weaveworks/flux/history"
	"github.com/weaveworks/flux/job"
	"github.com/weaveworks/flux/update"
)

var ErrTimeout = errors.New("timeout")

// await polls for a job to complete, then for the resulting commit to
// be applied
func await(stdout, stderr io.Writer, client api.Client, jobID job.ID, apply, verbose bool) error {
	metadata, err := awaitJob(client, jobID)
	if err != nil && err.Error() != git.ErrNoChanges.Error() {
		return err
	}
	if metadata.Revision != "" {
		fmt.Fprintf(stderr, "Commit pushed: %s\n", metadata.ShortRevision())
	}
	if metadata.Result == nil {
		fmt.Fprintf(stderr, "Nothing to do\n")
		return nil
	}

	if apply && metadata.Revision != "" {
		if err := awaitSync(client, metadata.Revision); err != nil {
			return err
		}

		fmt.Fprintf(stderr, "Applied %s\n", metadata.Revision)
	}

	if metadata.Result != nil {
		update.PrintResults(stdout, metadata.Result, verbose)
	}
	return nil
}

// await polls for a job to have been completed, with exponential backoff.
func awaitJob(client api.Client, jobID job.ID) (history.CommitEventMetadata, error) {
	var result history.CommitEventMetadata
	err := backoff(100*time.Millisecond, 2, 50, 1*time.Minute, func() (bool, error) {
		j, err := client.JobStatus(jobID)
		if err != nil {
			return false, err
		}
		switch j.StatusString {
		case job.StatusFailed:
			return false, j
		case job.StatusSucceeded:
			if j.Err != "" {
				// How did we succeed but still get an error!?
				return false, j
			}
			result = j.Result
			return true, nil
		}
		return false, nil
	})
	return result, err
}

// await polls for a commit to have been applied, with exponential backoff.
func awaitSync(client api.Client, revision string) error {
	return backoff(1*time.Second, 2, 10, 1*time.Minute, func() (bool, error) {
		refs, err := client.SyncStatus(revision)
		return err == nil && len(refs) == 0, err
	})
}

// backoff polls for f() to have been completed, with exponential backoff.
func backoff(initialDelay, factor, maxFactor, timeout time.Duration, f func() (bool, error)) error {
	maxDelay := initialDelay * maxFactor
	finish := time.Now().UTC().Add(timeout)
	for delay := initialDelay; time.Now().UTC().Before(finish); delay = min(delay*factor, maxDelay) {
		ok, err := f()
		if ok || err != nil {
			return err
		}
		// If we will have time to try again, sleep
		if time.Now().UTC().Add(delay).Before(finish) {
			time.Sleep(delay)
		}
	}
	return ErrTimeout
}

func min(t1, t2 time.Duration) time.Duration {
	if t1 < t2 {
		return t1
	}
	return t2
}
