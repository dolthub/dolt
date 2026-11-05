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

package nbs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	filelocks "github.com/dolthub/file-locks"
	"github.com/stretchr/testify/require"
)

// The locks belong to the open file rather than the process, so another Lock in this
// process stands in for another process.
func otherProcess(t *testing.T, dir string) *filelocks.Lock {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	lock, err := filelocks.New(filepath.Join(dir, spoolDirLockName))
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	return lock
}

func newTestSpoolDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spools")
	t.Cleanup(func() {
		spoolDirHoldsMu.Lock()
		h, ok := spoolDirHolds[dir]
		delete(spoolDirHolds, dir)
		spoolDirHoldsMu.Unlock()
		if ok && h.lock != nil {
			_ = h.lock.Close()
		}
	})
	return dir
}

func writeOrphan(t *testing.T, dir, name string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("orphan"), 0o644))
	return path
}

func TestHoldSpoolDir_RemovesOrphansWhenUnused(t *testing.T) {
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	orphan := writeOrphan(t, dir, "nbs-spool-orphan")

	require.NoError(t, holdSpoolDir(ctx, dir))
	_, err := os.Stat(orphan)
	require.True(t, os.IsNotExist(err), "the orphan must be removed")
	_, err = os.Stat(filepath.Join(dir, spoolDirLockName))
	require.NoError(t, err, "the lock file must stay")

	// We hold it shared: others can share it, and nobody can clean it.
	other := otherProcess(t, dir)
	require.Equal(t, filelocks.ErrLocked, other.TryLock())
	require.NoError(t, other.TryRLock())
}

func TestHoldSpoolDir_LeavesFilesOfLiveProcess(t *testing.T) {
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	live := writeOrphan(t, dir, "nbs-spool-live")
	other := otherProcess(t, dir)
	require.NoError(t, other.RLock())

	require.NoError(t, holdSpoolDir(ctx, dir))
	_, err := os.Stat(live)
	require.NoError(t, err, "another live process may be using this spool")
}

func TestHoldSpoolDir_CleansOnlyOnFirstUse(t *testing.T) {
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	require.NoError(t, holdSpoolDir(ctx, dir))

	ours := writeOrphan(t, dir, "nbs-spool-ours")
	require.NoError(t, holdSpoolDir(ctx, dir))
	_, err := os.Stat(ours)
	require.NoError(t, err, "a process must not clean up its own spools")
}

func TestHoldSpoolDir_WaitsForCleanup(t *testing.T) {
	dir := newTestSpoolDir(t)
	cleaner := otherProcess(t, dir)
	require.NoError(t, cleaner.Lock())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, holdSpoolDir(ctx, dir), context.DeadlineExceeded)

	done := make(chan error, 1)
	go func() { done <- holdSpoolDir(context.Background(), dir) }()
	select {
	case err := <-done:
		require.Failf(t, "must wait for the cleanup to finish", "returned %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, cleaner.Unlock())
	require.NoError(t, <-done)
	require.Equal(t, filelocks.ErrLocked, cleaner.TryLock())
}

func TestHoldSpoolDir_RecoversFromRemovedDir(t *testing.T) {
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	require.NoError(t, holdSpoolDir(ctx, dir))
	require.NoError(t, os.RemoveAll(dir))

	require.NoError(t, holdSpoolDir(ctx, dir))
	require.Equal(t, filelocks.ErrLocked, otherProcess(t, dir).TryLock(), "the new lock file must be held")
}

func TestHoldSpoolDir_ReturnsCleanupErrors(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory this user cannot remove files from")
	}
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	writeOrphan(t, dir, "nbs-spool-orphan")
	lock := otherProcess(t, dir)
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	require.ErrorIs(t, holdSpoolDir(ctx, dir), os.ErrPermission)
	require.NoError(t, lock.TryLock(), "a failed cleanup must not keep the lock")
}

func TestUnlockSpoolDir_PanicsOnError(t *testing.T) {
	dir := newTestSpoolDir(t)
	lock := otherProcess(t, dir)
	require.NoError(t, lock.RLock())
	unlockSpoolDir(lock, dir)

	// Unlocking a lock which is not held is an error from Unlock.
	require.Panics(t, func() { unlockSpoolDir(lock, dir) })
}

func TestCleanSpoolDir(t *testing.T) {
	t.Run("missing directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "spools")
		require.NoError(t, CleanSpoolDir(dir))
		_, err := os.Stat(dir)
		require.True(t, os.IsNotExist(err), "cleaning must not make the directory")
	})
	t.Run("unused directory", func(t *testing.T) {
		dir := newTestSpoolDir(t)
		orphan := writeOrphan(t, dir, "nbs-spool-orphan")
		require.NoError(t, CleanSpoolDir(dir))
		_, err := os.Stat(orphan)
		require.True(t, os.IsNotExist(err), "the orphan must be removed")
		require.NoError(t, otherProcess(t, dir).TryLock(), "cleaning must not keep the lock")
	})
	t.Run("directory in use", func(t *testing.T) {
		dir := newTestSpoolDir(t)
		live := writeOrphan(t, dir, "nbs-spool-live")
		require.NoError(t, otherProcess(t, dir).RLock())
		require.NoError(t, CleanSpoolDir(dir), "a live process is not an error")
		_, err := os.Stat(live)
		require.NoError(t, err, "another live process may be using this spool")
	})
	t.Run("this process is using it", func(t *testing.T) {
		dir := newTestSpoolDir(t)
		require.NoError(t, holdSpoolDir(context.Background(), dir))
		ours := writeOrphan(t, dir, "nbs-spool-ours")
		require.NoError(t, CleanSpoolDir(dir))
		_, err := os.Stat(ours)
		require.NoError(t, err)
	})
	t.Run("cleanup fails", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("needs a directory this user cannot remove files from")
		}
		dir := newTestSpoolDir(t)
		writeOrphan(t, dir, "nbs-spool-orphan")
		lock := otherProcess(t, dir)
		require.NoError(t, os.Chmod(dir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		require.ErrorIs(t, CleanSpoolDir(dir), os.ErrPermission)
		require.NoError(t, lock.TryLock(), "a failed cleanup must not keep the lock")
	})
}

func TestReleaseSpoolDir(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, ReleaseSpoolDir(filepath.Join(t.TempDir(), "never-held")))

	dir := newTestSpoolDir(t)
	require.NoError(t, holdSpoolDir(ctx, dir))
	other := otherProcess(t, dir)

	spool := writeOrphan(t, dir, "nbs-spool-open")
	require.NoError(t, ReleaseSpoolDir(dir))
	require.Equal(t, filelocks.ErrLocked, other.TryLock(), "a spool may still be in use, so the lock stays")

	require.NoError(t, os.Remove(spool))
	require.NoError(t, ReleaseSpoolDir(dir))
	require.NoError(t, other.TryLock(), "an empty directory is released")
	require.NoError(t, other.Unlock())

	// The next spool takes the lock again.
	require.NoError(t, holdSpoolDir(ctx, dir))
	require.Equal(t, filelocks.ErrLocked, other.TryLock())
}

func TestCreateSpoolFile_HoldsSpoolDir(t *testing.T) {
	ctx := context.Background()
	dir := newTestSpoolDir(t)
	orphan := writeOrphan(t, dir, "nbs-spool-orphan")

	f, _, err := createSpoolFile(ctx, dir, bytes.NewReader([]byte("table")))
	require.NoError(t, err)
	_, err = os.Stat(orphan)
	require.True(t, os.IsNotExist(err))
	require.Equal(t, filelocks.ErrLocked, otherProcess(t, dir).TryLock())

	require.NoError(t, removeSpoolFile(f))
	_, err = os.Stat(f.Name())
	require.True(t, os.IsNotExist(err))
}
