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

package git

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readHelperPid waits for the holdpipe helper to record its pid in |path| and
// arranges for it to be killed when the test ends.
func readHelperPid(t *testing.T, path string) int {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for {
		b, err := os.ReadFile(path)
		if err == nil && len(b) > 0 {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil {
				t.Fatalf("bad pid file %q: %v", string(b), err)
			}
			t.Cleanup(func() {
				if p, err := os.FindProcess(pid); err == nil {
					_ = p.Kill()
				}
			})
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper never recorded its pid at %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A process that outlives git holds the pipe Run captures output on, and only
// WaitDelay stops Wait from blocking on it for as long as that process lives.
func TestRunner_WaitDelayBoundsAbandonedPipe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := helperCommandRunner(t)
	r.waitDelay = 2 * time.Second
	pidFile := filepath.Join(t.TempDir(), "pid")

	start := time.Now()
	_, err := r.Run(ctx, RunOptions{}, "orphanexit", pidFile)
	elapsed := time.Since(start)

	// Returning at all is the point: the orphan holds the pipe for a minute.
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("expected ErrWaitDelay while the orphan holds the pipe, got %v", err)
	}
	if elapsed < r.waitDelay {
		t.Fatalf("returned after %v, before the %v delay elapsed", elapsed, r.waitDelay)
	}
	readHelperPid(t, pidFile)
}

// A warning git prints to stderr while succeeding must not reach callers that
// parse stdout. https://github.com/dolthub/dolt/issues/12488
func TestRunner_RunReturnsOnlyStdoutOnSuccess(t *testing.T) {
	t.Parallel()

	out, err := helperCommandRunner(t).Run(context.Background(), RunOptions{}, "warnsize")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "321\n" {
		t.Fatalf("got %q, want only stdout", out)
	}
}

func TestRunner_RunFailureCarriesStdoutAndStderr(t *testing.T) {
	t.Parallel()

	out, err := helperCommandRunner(t).Run(context.Background(), RunOptions{}, "warnfail")
	var cerr *CmdError
	if !errors.As(err, &cerr) {
		t.Fatalf("expected a CmdError, got %v", err)
	}
	for _, want := range []string{"out\n", "fatal: boom\n"} {
		if !strings.Contains(string(out), want) || !strings.Contains(string(cerr.Output), want) {
			t.Fatalf("output %q / error output %q missing %q", out, cerr.Output, want)
		}
	}
}

// BlobSize against a git that warns on stderr and then prints the size.
func TestGitAPIImpl_BlobSize_IgnoresStderrWarning(t *testing.T) {
	t.Parallel()

	r := helperCommandRunner(t).WithExtraEnv(warnSizeEnv + "=1")
	n, err := NewGitAPIImpl(r).BlobSize(context.Background(), OID(strings.Repeat("a", 40)))
	if err != nil {
		t.Fatal(err)
	}
	if n != 321 {
		t.Fatalf("got size %d, want 321", n)
	}
}

// Fetch must not let git's automatic maintenance repack the repository: it detaches
// and can delete packs while the fetched objects are being read.
func TestGitAPIImpl_FetchRef_DoesNotAutoRepack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	localRepo, localRunner, _, remoteHead := newFetchableRepos(t, ctx)

	// Leave the repository due for a repack once the fetch lands: one pack already
	// present, the fetch kept as a second, and a limit of one. Maintenance runs in
	// the foreground so that its trace is complete when the fetch returns.
	if _, err := localRepo.SetRefToTree(ctx, "refs/heads/local", nil, "seed local"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"repack"},
		{"config", "gc.autoPackLimit", "1"},
		{"config", "fetch.unpackLimit", "1"},
		{"config", "gc.autoDetach", "false"},
		{"config", "maintenance.autoDetach", "false"},
	} {
		if _, err := localRunner.Run(ctx, RunOptions{}, args...); err != nil {
			t.Fatal(err)
		}
	}

	trace := filepath.Join(t.TempDir(), "trace")
	api := NewGitAPIImpl(localRunner.WithExtraEnv("GIT_TRACE2_EVENT=" + trace))
	if err := api.FetchRef(ctx, "origin", "refs/dolt/data", "refs/dolt/remotes/origin/data"); err != nil {
		t.Fatal(err)
	}
	requireTrackingRef(t, ctx, api, remoteHead)

	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"fetch"`) {
		t.Fatalf("trace does not record the fetch:\n%s", b)
	}
	// Older gits still launch "gc --auto" here, which gc.auto=0 makes a no-op, so
	// look for the repack rather than for the launch.
	if strings.Contains(string(b), `"repack"`) {
		t.Fatalf("fetch repacked the repository:\n%s", b)
	}
}
