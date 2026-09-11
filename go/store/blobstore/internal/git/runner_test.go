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
