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
	"os"
	"path/filepath"
	"sync"
	"testing"

	filelocks "github.com/dolthub/file-locks"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// The locks are flock(2) and LockFileEx locks, which belong to the open file rather than
// the process. So a second claim in this process sees the first exactly as another
// process would, and closing a claim's lock is what the kernel does when its process dies.

// claimForTest claims a spool directory under |tempDir| as if for another process, and
// writes one spool file into it. The claim is released at the end of the test.
func claimForTest(t *testing.T, tempDir string) (ownedSpoolDir, string) {
	t.Helper()
	d, err := claimSpoolDir(tempDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.lock.Close() })
	f, err := os.CreateTemp(d.path, "nbs-spool-")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return d, f.Name()
}

func requireExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func requireNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "%s must be removed, stat err: %v", path, err)
}

func TestSpoolDir_RemovesOrphanFromDeadOwner(t *testing.T) {
	tempDir := t.TempDir()

	dead, deadSpool := claimForTest(t, tempDir)
	// The owner dies: its lock is released without removing anything.
	require.NoError(t, dead.lock.Close())

	// An owner that died between making its directory and making its LOCK file.
	noLock, err := os.MkdirTemp(tempDir, spoolDirPrefix)
	require.NoError(t, err)

	// Things a sweep must not touch.
	legacy := filepath.Join(tempDir, "nbs-spool-123")
	require.NoError(t, os.WriteFile(legacy, []byte("x"), 0o644))
	other := filepath.Join(tempDir, "other-dir")
	require.NoError(t, os.Mkdir(other, 0o755))

	next, nextSpool := claimForTest(t, tempDir)

	requireNotExists(t, deadSpool)
	requireNotExists(t, dead.path)
	requireNotExists(t, noLock)
	requireExists(t, legacy)
	requireExists(t, other)
	requireExists(t, nextSpool)
	require.NotEqual(t, dead.path, next.path)
}

func TestSpoolDir_KeepsLiveOwnersSpools(t *testing.T) {
	tempDir := t.TempDir()

	live, liveSpool := claimForTest(t, tempDir)
	f, err := os.Open(liveSpool)
	require.NoError(t, err)
	defer f.Close()

	// Another process starts and sweeps the shared temp dir.
	other, otherSpool := claimForTest(t, tempDir)
	require.NotEqual(t, live.path, other.path)
	requireExists(t, liveSpool)
	requireExists(t, otherSpool)

	// Once the first owner dies, the next sweep removes its directory and nothing else.
	require.NoError(t, live.lock.Close())
	_, thirdSpool := claimForTest(t, tempDir)
	requireNotExists(t, live.path)
	requireExists(t, otherSpool)
	requireExists(t, thirdSpool)
}

func TestSpoolDir_ConcurrentClaimsKeepEachOthersSpools(t *testing.T) {
	tempDir := t.TempDir()

	const owners = 16
	claims := make([]ownedSpoolDir, owners)
	spools := make([]string, owners)
	var wg sync.WaitGroup
	for i := 0; i < owners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := claimSpoolDir(tempDir)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			claims[i] = d
			f, err := os.CreateTemp(d.path, "nbs-spool-")
			if err != nil {
				t.Errorf("spool: %v", err)
				return
			}
			spools[i] = f.Name()
			_ = f.Close()
		}()
	}
	wg.Wait()
	t.Cleanup(func() {
		for _, d := range claims {
			if d.lock != nil {
				_ = d.lock.Close()
			}
		}
	})
	if t.Failed() {
		return
	}

	// Every claim swept the temp dir while others were being made, and none of the
	// directories or spools of a live owner was removed.
	for i := range claims {
		requireExists(t, spools[i])
	}
}

func TestSpoolDir_SpoolsIntoOwnedDir(t *testing.T) {
	tempDir := t.TempDir()
	old := tempfiles.MovableTempFileProvider
	tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(tempDir)
	t.Cleanup(func() {
		tempfiles.MovableTempFileProvider = old
		spoolDirsMu.Lock()
		if d, ok := spoolDirs[tempDir]; ok && d.lock != nil {
			_ = d.lock.Close()
		}
		delete(spoolDirs, tempDir)
		spoolDirsMu.Unlock()
	})

	ra, err := spoolTableReaderAt(bytes.NewReader([]byte("table bytes")))
	require.NoError(t, err)
	dir := filepath.Dir(ra.f.Name())
	require.Equal(t, spoolDir(tempDir), dir)
	require.NotEqual(t, tempDir, dir)

	// The directory is locked by this process, so a sweep from another process keeps it.
	lock, err := filelocks.New(filepath.Join(dir, spoolDirLockName))
	require.NoError(t, err)
	require.ErrorIs(t, lock.TryLock(), filelocks.ErrLocked)
	require.NoError(t, lock.Close())

	require.NoError(t, ra.Close())
	requireNotExists(t, ra.f.Name())
}
