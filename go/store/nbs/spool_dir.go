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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	filelocks "github.com/dolthub/file-locks"
)

// Spool files hold whole table files and are removed when their reader closes. A process
// that dies first (OOM kill, SIGKILL, power loss) leaves its spools behind, and
// TempFileProvider.Clean only knows the files of the current process.
//
// So each process spools into its own directory under the temp dir and holds an advisory
// lock on that directory's LOCK file for as long as it runs. The kernel drops the lock
// when the process exits, however it exits. Before a process makes its spool directory,
// it removes every spool directory whose lock it can take: no live process owns those.
//
// Several processes can share one temp dir. They all take the spoolDirsLockName lock in
// the temp dir to make or sweep spool directories, so a sweep never sees a directory
// that its owner has made but not locked yet.
const (
	spoolDirPrefix       = "nbs-spools-"
	spoolDirLockName     = "LOCK"
	spoolDirsLockName    = "nbs-spools.lock"
	spoolDirsLockTimeout = 10 * time.Second
)

// ownedSpoolDir is a spool directory this process made and holds the lock of.
type ownedSpoolDir struct {
	path string
	// lock is held until the process exits. It is never closed, and the map below keeps
	// it reachable so that a finalizer cannot close it either.
	lock *filelocks.Lock
}

var (
	spoolDirsMu sync.Mutex
	spoolDirs   = map[string]ownedSpoolDir{}
)

// spoolDir returns this process's spool directory under |tempDir|, making it on first use.
// If it cannot be made, for example because another user owns the lock file in a shared
// temp dir, spools go directly in |tempDir| and are not swept.
func spoolDir(tempDir string) string {
	spoolDirsMu.Lock()
	defer spoolDirsMu.Unlock()
	if d, ok := spoolDirs[tempDir]; ok {
		return d.path
	}
	d, err := claimSpoolDir(tempDir)
	if err != nil {
		d = ownedSpoolDir{path: tempDir}
	}
	spoolDirs[tempDir] = d
	return d.path
}

// claimSpoolDir removes orphaned spool directories under |tempDir|, then makes a new one
// and locks it.
func claimSpoolDir(tempDir string) (ownedSpoolDir, error) {
	dirsLock, err := filelocks.New(filepath.Join(tempDir, spoolDirsLockName))
	if err != nil {
		return ownedSpoolDir{}, err
	}
	defer dirsLock.Close()
	if err := dirsLock.LockWithTimeout(spoolDirsLockTimeout); err != nil {
		return ownedSpoolDir{}, err
	}

	removeOrphanedSpoolDirs(tempDir)

	path, err := os.MkdirTemp(tempDir, spoolDirPrefix)
	if err != nil {
		return ownedSpoolDir{}, err
	}
	lock, err := filelocks.New(filepath.Join(path, spoolDirLockName))
	if err == nil {
		if err = lock.TryLock(); err != nil {
			_ = lock.Close()
		}
	}
	if err != nil {
		_ = os.RemoveAll(path)
		return ownedSpoolDir{}, err
	}
	return ownedSpoolDir{path: path, lock: lock}, nil
}

// removeOrphanedSpoolDirs removes the spool directories under |tempDir| that no live
// process holds the lock of. The caller holds the spoolDirsLockName lock. Errors are
// ignored: a directory that cannot be removed now is tried again by the next process.
func removeOrphanedSpoolDirs(tempDir string) {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), spoolDirPrefix) {
			continue
		}
		path := filepath.Join(tempDir, e.Name())
		lock, err := filelocks.New(filepath.Join(path, spoolDirLockName))
		if err != nil {
			continue
		}
		err = lock.TryLock()
		// Close before removing, which Windows needs. Nobody can claim |path| in between:
		// a process only locks a spool directory it has just made, under the lock we hold.
		_ = lock.Close()
		if err == nil {
			_ = os.RemoveAll(path)
		}
	}
}
