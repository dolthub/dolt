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

package dbfactory

import (
	"errors"
	"fmt"
	"os"

	"github.com/dolthub/fslock"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// SafeToIgnoreMarkerFile is the name of the in-progress database
// marker file.
//
// Directory scans skip directories containing this file to avoid
// serving incomplete or abandoned databases.
const SafeToIgnoreMarkerFile = ".dolt_safe_to_ignore"

// safeToIgnoreMarkerPerm is the marker's file mode, world-readable so
// any process scanning the data directory can detect it.
const safeToIgnoreMarkerPerm os.FileMode = 0o644

// FsCreateTx coordinates database creation across processes using an
// OS-level file lock on SafeToIgnoreMarkerFile.
//
// The lock is acquired via [filesys.CreateFilesysLock], using
// [flock(2)] on POSIX systems and [LockFileEx] on Windows. The
// operating system kernel releases the lock automatically when all
// file descriptors close on process termination ([_exit(2)]).
//
// [flock(2)]: https://man7.org/linux/man-pages/man2/flock.2.html
// [_exit(2)]: https://man7.org/linux/man-pages/man2/_exit.2.html
// [LockFileEx]: https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
type FsCreateTx struct {
	fs      filesys.Filesys
	path    string
	lock    filesys.FilesysLock
	cleanup func() error
	done    bool
}

// ErrLocked indicates that another process holds the directory lock.
var ErrLocked = errors.New("database directory lock is already held")

// ErrExists indicates that a completed database already exists.
var ErrExists = errors.New("database directory already exists")

// BeginCreate starts creation of a database in |path| under |fs| by
// writing an in-progress marker and acquiring an OS file lock.
//
// If an abandoned incomplete database from a terminated process is
// present, BeginCreate automatically reclaims it before acquiring the
// lock.
//
// If another active process holds the lock, BeginCreate returns
// ErrLocked. If a completed database already exists on disk,
// BeginCreate returns ErrExists. Uncommitted state is removed on
// FsCreateTx.Rollback, or finalized on FsCreateTx.Commit.
func BeginCreate(fs filesys.Filesys, path string) (*FsCreateTx, error) {
	exists, isDir := fs.Exists(path)
	if exists && !isDir {
		return nil, fmt.Errorf("%s is not a directory", path)
	}

	subFs, err := fs.WithWorkingDir(path)
	if err != nil {
		return nil, err
	}

	cleanup := func() error { return removeAll(fs, path) }

	if exists {
		cleanup = func() error { return removeDoltDir(fs, path) }
		if marked, _ := subFs.Exists(SafeToIgnoreMarkerFile); marked {
			lck, err := tryLockMarker(subFs)
			if err != nil {
				return nil, err
			}
			_ = lck.Unlock()
			if err := cleanup(); err != nil {
				return nil, err
			}
		} else if doltExists, _ := subFs.Exists(DoltDir); doltExists {
			return nil, ErrExists
		}
	}

	if err := fs.MkDirs(path); err != nil {
		return nil, err
	}
	if err := subFs.WriteFile(SafeToIgnoreMarkerFile, nil, safeToIgnoreMarkerPerm); err != nil {
		return nil, err
	}
	lck, err := tryLockMarker(subFs)
	if err != nil {
		return nil, err
	}
	return &FsCreateTx{fs: fs, path: path, lock: lck, cleanup: cleanup}, nil
}

// Rollback releases the OS file lock and cleans up uncommitted database
// state from disk.
//
// If Commit was already called, Rollback is a no-op.
func (tx *FsCreateTx) Rollback() error {
	if tx == nil || tx.done {
		return nil
	}
	tx.done = true
	unlockErr := tx.unlock()
	var cleanErr error
	if tx.cleanup != nil {
		cleanErr = tx.cleanup()
	}
	return errors.Join(unlockErr, cleanErr)
}

// Commit releases the OS file lock and makes the database permanent by
// removing the in-progress marker from disk.
//
// If Commit was already called, subsequent calls are a no-op.
func (tx *FsCreateTx) Commit() error {
	if tx == nil || tx.done {
		return nil
	}
	tx.done = true
	unlockErr := tx.unlock()
	subFs, err := tx.fs.WithWorkingDir(tx.path)
	if err != nil {
		return errors.Join(unlockErr, err)
	}
	return errors.Join(unlockErr, removeMarker(subFs))
}

// unlock releases the transaction's OS file lock if held.
func (tx *FsCreateTx) unlock() error {
	if tx.lock != nil {
		err := tx.lock.Unlock()
		tx.lock = nil
		return err
	}
	return nil
}

// removeAll deletes the directory at |path| and clears in-memory
// caches.
func removeAll(fs filesys.Filesys, path string) error {
	cacheErr, gitErr := clearCaches(fs, path)
	return errors.Join(cacheErr, gitErr, fs.Delete(path, true))
}

// removeDoltDir deletes DoltDir and the marker file under |path| while
// preserving the parent directory.
func removeDoltDir(fs filesys.Filesys, path string) error {
	cacheErr, gitErr := clearCaches(fs, path)
	subFs, err := fs.WithWorkingDir(path)
	if err != nil {
		return errors.Join(cacheErr, gitErr, err)
	}
	var doltErr error
	if exists, _ := subFs.Exists(DoltDir); exists {
		doltErr = subFs.Delete(DoltDir, true)
	}
	return errors.Join(cacheErr, gitErr, doltErr, removeMarker(subFs))
}

// clearCaches removes singleton cache entries and closes git remotes
// under |path|.
func clearCaches(fs filesys.Filesys, path string) (error, error) {
	if absPath, err := fs.Abs(path); err == nil {
		return DeleteFromSingletonCache(SingletonCacheKeyForDatabaseDir(absPath), false),
			CloseGitRemotesUnderRoot(absPath)
	}
	return nil, nil
}

// tryLockMarker attempts to lock the in-progress marker file.
//
// It returns ErrLocked if another process holds the file lock.
func tryLockMarker(subFs filesys.Filesys) (filesys.FilesysLock, error) {
	absPath, err := subFs.Abs(SafeToIgnoreMarkerFile)
	if err != nil {
		return nil, err
	}
	lck := filesys.CreateFilesysLock(subFs, absPath)
	locked, err := lck.TryLock()
	if errors.Is(err, fslock.ErrLocked) || (!locked && err == nil) {
		return nil, ErrLocked
	}
	if err != nil {
		return nil, err
	}
	return lck, nil
}

// removeMarker durably removes the in-progress marker from |fs|.
//
// An already-absent marker is ignored.
func removeMarker(fs filesys.Filesys) error {
	err := fs.DeleteFileDurably(SafeToIgnoreMarkerFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
