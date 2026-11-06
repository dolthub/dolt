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

package tempfiles

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	filelocks "github.com/dolthub/file-locks"

	"github.com/dolthub/dolt/go/libraries/utils/file"
)

// A LockedDir is a directory of temp files whose owner holds an exclusive lock on the lock
// file next to it, so that CleanDir can tell whether the owner is alive:
//
//	parent/locked-123.lock
//	parent/locked-123/
//
// The kernel drops the lock when the process exits, however it exits, so temp files left by
// a process which died can be found and removed.
//
// A lock file is the last thing removed, while its lock is still held. Opening a lock file
// makes it if it is not there, so one inside the directory could reappear partway through the
// removal and stop the directory from being removed.
type LockedDir struct {
	dir  string
	lock *filelocks.Lock
}

const (
	lockedDirPattern    = "locked-*.lock"
	lockedDirLockSuffix = ".lock"

	// CleanDir can take a lock between the creation of its file and its owner taking it. That
	// takes a cleanup running at just that moment, and an attempt costs almost nothing.
	lockedDirAttempts = 100
)

// NewLockedDir makes a new LockedDir in |parent|, making |parent| if needed.
func NewLockedDir(parent string) (*LockedDir, error) {
	var err error
	for i := 0; i < lockedDirAttempts; i++ {
		var d *LockedDir
		d, err = tryNewLockedDir(parent)
		if err == nil {
			return d, nil
		}
		if !errors.Is(err, filelocks.ErrLocked) && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return nil, err
}

// tryNewLockedDir makes one attempt at NewLockedDir. CleanDir can get to the new lock file
// first, which shows up as its lock being held.
func tryNewLockedDir(parent string) (*LockedDir, error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(parent, lockedDirPattern)
	if err != nil {
		return nil, err
	}
	lockPath := f.Name()
	if err := f.Close(); err != nil {
		return nil, errors.Join(err, os.Remove(lockPath))
	}
	lock, err := filelocks.New(lockPath)
	if err != nil {
		return nil, err
	}
	if err := lock.TryLock(); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	dir := strings.TrimSuffix(lockPath, lockedDirLockSuffix)
	if err := os.Mkdir(dir, 0o755); err != nil {
		err = errors.Join(err, os.Remove(lockPath))
		unlockDir(lock, dir)
		return nil, errors.Join(err, lock.Close())
	}
	return &LockedDir{dir: dir, lock: lock}, nil
}

// NewFile makes a new temp file in the directory. It is made through MovableTempFileProvider
// so that Clean removes it if it is never removed.
func (d *LockedDir) NewFile(pattern string) (*os.File, error) {
	return MovableTempFileProvider.NewFile(d.dir, pattern)
}

// RemoveFile closes and removes |f|, which NewFile made.
func (d *LockedDir) RemoveFile(f *os.File) error {
	return errors.Join(f.Close(), file.Remove(f.Name()))
}

// CleanDir removes the LockedDirs in |parent| left by dead processes. It does not wait for a
// lock, and it does nothing if |parent| does not exist.
func CleanDir(parent string) error {
	entries, err := os.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), lockedDirLockSuffix) {
			errs = append(errs, cleanLockedDir(filepath.Join(parent, e.Name())))
		}
	}
	return errors.Join(errs...)
}

// cleanLockedDir removes the directory locked by |lockPath| and then the lock file, if it can
// take the lock right away.
func cleanLockedDir(lockPath string) error {
	lock, err := filelocks.New(lockPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	err = lock.TryLock()
	if errors.Is(err, filelocks.ErrLocked) {
		return lock.Close()
	}
	if err != nil {
		return errors.Join(err, lock.Close())
	}
	dir := strings.TrimSuffix(lockPath, lockedDirLockSuffix)
	err = removeLockedDir(dir)
	unlockDir(lock, dir)
	return errors.Join(err, lock.Close())
}

// removeLockedDir removes |dir| and then its lock file. The caller holds the lock.
func removeLockedDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.Remove(dir + lockedDirLockSuffix)
}

// unlockDir releases |lock|, which this process holds on |dir|. A lock which cannot be released
// would keep the directory from being cleaned for as long as this process lives, so a failure
// panics.
func unlockDir(lock *filelocks.Lock, dir string) {
	if err := lock.Unlock(); err != nil {
		panic(fmt.Sprintf("failed to unlock %s: %v", dir, err))
	}
}
