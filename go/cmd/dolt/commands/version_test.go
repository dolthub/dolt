// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-github/v57/github"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
)

func TestSlowLatestReleaseCheckDoesNotBlockLaterCalls(t *testing.T) {
	origTimeout := latestReleaseCheckTimeout
	origClient := newLatestReleaseGitHubClient
	latestReleaseCheckTimeout = 150 * time.Millisecond
	t.Cleanup(func() {
		latestReleaseCheckTimeout = origTimeout
		newLatestReleaseGitHubClient = origClient
	})

	var hits atomic.Int32
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
		<-unblock
	}))
	// Cleanups run last-in first-out: unblock the handler before closing the server.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(unblock) })

	newLatestReleaseGitHubClient = func() *github.Client {
		client := github.NewClient(nil)
		base, err := url.Parse(server.URL + "/")
		require.NoError(t, err)
		client.BaseURL = base
		return client
	}

	dEnv := createTestEnv()
	limit := latestReleaseCheckTimeout + 200*time.Millisecond

	var firstErr errhand.VerboseError
	mustFinishWithin(t, limit, func() {
		firstErr = checkAndPrintVersionOutOfDateWarning("1.2.3", dEnv)
	})
	require.Nil(t, firstErr)

	path := filepath.Join(testHomeDir, dbfactory.DoltDir, versionCheckFile)
	exists, _ := dEnv.FS.Exists(path)
	require.True(t, exists, "a failed check should be recorded so the next call does not wait again")

	var secondErr errhand.VerboseError
	mustFinishWithin(t, 100*time.Millisecond, func() {
		secondErr = checkAndPrintVersionOutOfDateWarning("1.2.3", dEnv)
	})
	require.Nil(t, secondErr)
	require.Equal(t, int32(1), hits.Load(), "a recorded failed check should not be retried while it is still fresh")
}

func mustFinishWithin(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("release check blocked longer than %s", limit)
	}
}
