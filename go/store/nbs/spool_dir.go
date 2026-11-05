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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	filelocks "github.com/dolthub/file-locks"
)

// Spool files are removed when their reader closes, or at a clean exit. A process that
// dies first (OOM kill, SIGKILL, power loss) leaves its spools behind.
//
// So every process that spools into a directory holds a shared lock on the directory's
// LOCK file for as long as it uses the directory. The kernel drops the lock when the
// process exits, however it exits. Before taking its shared lock, a process tries once
// to take the lock exclusively. If it gets it, no live process is using the directory,
// so everything in it is an orphan and it removes them. If another process holds the
// lock, it leaves the directory alone, without waiting. Orphans then wait for the next
// process that finds the directory unused.
//
// Reads do not touch the lock. They use files which are already open, in a directory
// that cannot be cleaned while this process holds its shared lock.
//
// LOCK is never removed by a cleanup: processes could otherwise end up holding locks
// on different files.
const spoolDirLockName = "LOCK"

// unlockSpoolDir releases |lock|, which this process holds on the spool directory |dir|.
// A lock which cannot be released would block other processes for as long as this one
// lives, so a failure panics.
func unlockSpoolDir(lock *filelocks.Lock, dir string) {
	if err := lock.Unlock(); err != nil {
		panic(fmt.Sprintf("failed to unlock spool directory %s: %v", dir, err))
	}
}

// spoolDirHold is this process's shared lock on a spool directory.
type spoolDirHold struct {
	mu   sync.Mutex
	lock *filelocks.Lock
}

var (
	spoolDirHoldsMu sync.Mutex
	spoolDirHolds   = map[string]*spoolDirHold{}
)

// holdSpoolDir makes sure this process holds its shared lock on |dir|, making the
// directory and cleaning it up the first time. It waits for another process's cleanup to
// finish, until |ctx| is done.
func holdSpoolDir(ctx context.Context, dir string) error {
	spoolDirHoldsMu.Lock()
	h, ok := spoolDirHolds[dir]
	if !ok {
		h = &spoolDirHold{}
		spoolDirHolds[dir] = h
	}
	spoolDirHoldsMu.Unlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	lockPath := filepath.Join(dir, spoolDirLockName)
	if h.lock != nil {
		_, err := os.Stat(lockPath)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
		// The directory was removed under us, so our lock excludes nobody now.
		unlockSpoolDir(h.lock, dir)
		err = h.lock.Close()
		h.lock = nil
		if err != nil {
			return err
		}
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	lock, err := filelocks.New(lockPath)
	if err != nil {
		return err
	}
	// Cleaning up is only worth one attempt: if another process holds the lock, skip the
	// cleanup and move on without waiting.
	err = lock.TryLock()
	if err == nil {
		err = removeOrphanedSpools(dir)
		unlockSpoolDir(lock, dir)
	} else if errors.Is(err, filelocks.ErrLocked) {
		err = nil
	}
	if err == nil {
		err = lock.RLockWithContext(ctx)
	}
	if err != nil {
		return errors.Join(err, lock.Close())
	}
	h.lock = lock
	return nil
}

// removeOrphanedSpools removes everything in |dir| except its lock file. The caller holds
// the lock exclusively.
func removeOrphanedSpools(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if e.Name() != spoolDirLockName {
			errs = append(errs, os.RemoveAll(filepath.Join(dir, e.Name())))
		}
	}
	return errors.Join(errs...)
}

// ReleaseSpoolDir gives up this process's shared lock on |dir|, for callers which have
// closed the stores spooling into it. If spools are still in the directory, they may
// still be in use, so the lock is kept until the process exits.
func ReleaseSpoolDir(dir string) error {
	spoolDirHoldsMu.Lock()
	h, ok := spoolDirHolds[dir]
	spoolDirHoldsMu.Unlock()
	if !ok {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lock == nil {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, e := range entries {
		if e.Name() != spoolDirLockName {
			return nil
		}
	}
	unlockSpoolDir(h.lock, dir)
	err = h.lock.Close()
	h.lock = nil
	return err
}
