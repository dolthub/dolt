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

//go:build !windows

package git

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Cancellation must reach the processes git spawned, not just git: a surviving
// transport holds the pipe Wait reads.
func TestRunner_CancelKillsProcessGroup(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := helperCommandRunner(t)
	pidFile := filepath.Join(t.TempDir(), "pid")

	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, RunOptions{}, "orphanwait", pidFile)
		done <- err
	}()

	transport := readHelperPid(t, pidFile)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected a cancellation error, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run never returned after cancellation")
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := syscall.Kill(transport, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d survived cancellation: the kill did not reach the process group", transport)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
