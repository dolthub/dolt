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
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

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
//
// Close gives the directory up once every file made in it has been removed with RemoveFile,
// so an owner can close it while files are still in use.
type LockedDir struct {
	dir  string
	lock *filelocks.Lock

	mu sync.Mutex
	// files is the number of files made by NewFile and not yet removed by RemoveFile.
	files  int
	closed bool
}

// ErrLockedDirClosed is returned by NewFile after Close.
var ErrLockedDirClosed = errors.New("locked directory is closed")

const (
	lockedDirPrefix     = "locked-"
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
	lockPath, err := newLockFile(parent)
	if err != nil {
		return nil, err
	}
	lock, err := tryLockFileWhileDeletePending(lockPath)
	if err != nil {
		return nil, err
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
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrLockedDirClosed
	}
	f, err := MovableTempFileProvider.NewFile(d.dir, pattern)
	if err != nil {
		return nil, err
	}
	d.files++
	return f, nil
}

// RemoveFile closes and removes |f|, which NewFile made.
func (d *LockedDir) RemoveFile(f *os.File) error {
	err := errors.Join(f.Close(), file.Remove(f.Name()))
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files--
	if d.closed && d.files == 0 {
		err = errors.Join(err, d.release())
	}
	return err
}

// Close removes the directory and gives up the lock, now if no files remain and otherwise when
// the last one is removed. Closing twice is harmless.
func (d *LockedDir) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if d.files == 0 {
		return d.release()
	}
	return nil
}

// release removes the directory and gives up the lock. The caller holds d.mu.
func (d *LockedDir) release() error {
	err := removeLockedDir(d.dir)
	unlockDir(d.lock, d.dir)
	return errors.Join(err, d.lock.Close())
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
	lock, err := tryLockFileWhileDeletePending(lockPath)
	if err == filelocks.ErrLocked {
		// Its owner is alive. A failure to close the lock would have been joined to this, so it
		// is not ignored here.
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		// Another cleanup removed it, along with the directory holding it.
		return nil
	}
	if err != nil {
		return err
	}
	dir := strings.TrimSuffix(lockPath, lockedDirLockSuffix)
	err = removeLockedDir(dir)
	unlockDir(lock, dir)
	return errors.Join(err, lock.Close())
}

// newLockFile makes a new, empty lock file in |parent| with a random name, as os.CreateTemp
// would, and returns its path.
func newLockFile(parent string) (string, error) {
	for i := 0; i < 10000; i++ {
		path := filepath.Join(parent, lockedDirPrefix+strconv.FormatUint(uint64(rand.Uint32()), 10)+lockedDirLockSuffix)
		err := createLockFile(path)
		if err == nil {
			return path, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", &os.PathError{Op: "createtemp", Path: filepath.Join(parent, lockedDirPrefix+"*"+lockedDirLockSuffix), Err: os.ErrExist}
}

// tryLockFile opens the lock file at |lockPath|, making it if it is not there, and tries once
// to take its lock.
func tryLockFile(lockPath string) (*filelocks.Lock, error) {
	lock, err := filelocks.New(lockPath)
	if err != nil {
		return nil, err
	}
	if err := lock.TryLock(); err != nil {
		if cerr := lock.Close(); cerr != nil {
			return nil, errors.Join(err, cerr)
		}
		return nil, err
	}
	return lock, nil
}

// deletePendingWait is how long an error which may mean a file is being deleted is retried.
// A file stays in that state only until whoever is deleting it closes their handle.
const deletePendingWait = 100 * time.Millisecond

// tryLockFileWhileDeletePending is tryLockFile, retried for a moment while the lock file may
// be in the middle of being deleted.
//
// A lock file is removed while its lock is held, and on Windows a file being deleted cannot be
// opened until the handle deleting it closes. Windows reports that as "access denied", the same
// error as a real permission problem, so it is retried for a moment: a deletion finishes, and the
// lock file is made again, while a real permission problem is still there and is returned.
func tryLockFileWhileDeletePending(lockPath string) (*filelocks.Lock, error) {
	return retryWhile(deletePendingWait, mayBeDeletePending, func() (*filelocks.Lock, error) {
		return tryLockFile(lockPath)
	})
}

// mayBeDeletePending reports whether |err| could mean that the file being opened is being
// deleted.
func mayBeDeletePending(err error) bool {
	return runtime.GOOS == "windows" && errors.Is(err, os.ErrPermission)
}

// retryWhile calls |try| until it succeeds, it fails with an error for which |retry| is false,
// or |wait| has passed. It returns the result of the last call.
func retryWhile(wait time.Duration, retry func(error) bool, try func() (*filelocks.Lock, error)) (*filelocks.Lock, error) {
	deadline := time.Now().Add(wait)
	for {
		lock, err := try()
		if err == nil || !retry(err) || !time.Now().Before(deadline) {
			return lock, err
		}
		time.Sleep(time.Millisecond)
	}
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
