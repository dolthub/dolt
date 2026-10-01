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
	"path/filepath"
	"syscall"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ErrExists indicates that a completed database already exists.
var ErrExists = errors.New("database directory already exists")

// SafeToIgnoreMarkerFile marks an incomplete database directory.
//
// Interrupted creations in older Dolt versions left this file
// behind. IsTempDir checks for it to skip or reclaim them.
const SafeToIgnoreMarkerFile = ".dolt_safe_to_ignore"

// FSCreateTx coordinates database creation by isolating writes
// in a temporary directory before moving to the final path.
//
// Uncommitted state is written to a temporary directory created on
// the same filesystem volume. The database is made permanent via
// FSCreateTx.Commit, or discarded via FSCreateTx.Rollback.
type FSCreateTx struct {
	fs             filesys.Filesys
	subFs          filesys.Filesys
	destPath       string
	tempPath       string
	destPathExists bool
	done           bool
	uuid           uuid.UUID
}

// BeginCreate starts database creation at |destPath| under |fs| by
// provisioning an isolated temporary directory under |u|.
//
// If |u| is [uuid.Nil], a fallback UUIDv7 is generated via
// [uuid.NewV7]. Uncommitted state remains isolated in the temporary
// directory until FSCreateTx.Commit moves it to |destPath|. If
// |destPath| already contains a complete database, BeginCreate
// returns ErrExists.
func BeginCreate(
	fs filesys.Filesys,
	destPath string,
	u uuid.UUID,
) (*FSCreateTx, error) {
	exists, isDir := fs.Exists(destPath)
	if exists && !isDir {
		return nil, fmt.Errorf("%s is not a directory", destPath)
	}

	if exists {
		subFs, err := fs.WithWorkingDir(destPath)
		if err != nil {
			return nil, err
		}
		if doltExists, _ := subFs.Exists(DoltDir); doltExists {
			return nil, ErrExists
		}
	}

	tempPath, u, err := CreateTempDir(fs, destPath, exists, u)
	if err != nil {
		return nil, err
	}

	subFs, err := fs.WithWorkingDir(tempPath)
	if err != nil {
		_ = fs.Delete(tempPath, true)
		return nil, err
	}

	return &FSCreateTx{
		fs:             fs,
		subFs:          subFs,
		destPath:       destPath,
		tempPath:       tempPath,
		destPathExists: exists,
		uuid:           u,
	}, nil
}

// NewFSCreateTxForRecovery constructs an FSCreateTx from an
// existing temporary directory for crash recovery roll-forward or
// rollback.
func NewFSCreateTxForRecovery(fs filesys.Filesys, tempPath, destPath string, destPathExists bool, u uuid.UUID) *FSCreateTx {
	return &FSCreateTx{
		fs:             fs,
		destPath:       destPath,
		tempPath:       tempPath,
		destPathExists: destPathExists,
		uuid:           u,
	}
}

// FS returns the [filesys.Filesys] rooted at the temporary dir.
//
// Callers must perform all initial database writes using this
// filesystem before calling FSCreateTx.Commit.
func (tx *FSCreateTx) FS() filesys.Filesys {
	return tx.subFs
}

// TempPath returns the path to the isolated temporary directory.
func (tx *FSCreateTx) TempPath() string {
	return tx.tempPath
}

// UUID returns the unique [uuid.UUID] generated for this
// transaction's temporary directory.
func (tx *FSCreateTx) UUID() uuid.UUID {
	return tx.uuid
}

// DestPathExists reports whether the destination path existed
// when database creation began.
func (tx *FSCreateTx) DestPathExists() bool {
	return tx.destPathExists
}

// Rollback discards uncommitted database state by deleting the
// temporary directory.
func (tx *FSCreateTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true

	cacheErr, gitErr := clearCaches(tx.fs, tx.tempPath)
	delErr := tx.fs.Delete(tx.tempPath, true)
	return errors.Join(cacheErr, gitErr, delErr)
}

// Commit moves the temporary directory to the destination path.
//
// On POSIX systems, [filesys.Filesys.MoveDir] executes atomic
// [rename(2)].
//
// On Windows, directory renames lack formal POSIX atomicity
// guarantees. On NTFS, [os.Rename] invokes [MoveFileExW] on the
// same volume. Directories are [B+ tree] index structures. Moving a
// directory on the same volume updates its parent directory index
// entry without copying files, making its contents visible all at
// once. Transient sharing violations are retried automatically by
// [filesys.Filesys.MoveDir]. If the destination path already exists,
// the move fails with ErrExists.
//
// Storage formats without atomic directory renames, such as FAT or
// network shares, rely on write isolation to prevent exposing
// partial database state, but cannot guarantee atomic commit
// visibility.
//
// [rename(2)]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/rename.html
// [MoveFileExW]: https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw
// [B+ tree]: https://learn.microsoft.com/en-us/sysinternals/resources/archive/v01n05
func (tx *FSCreateTx) Commit() error {
	if tx.done {
		return nil
	}

	cacheErr, gitErr := clearCaches(tx.fs, tx.tempPath)

	var moveErr error
	if tx.destPathExists {
		src := filepath.Join(tx.tempPath, DoltDir)
		dest := filepath.Join(tx.destPath, DoltDir)
		_ = tx.fs.Delete(filepath.Join(dest, "tmp"), false)
		_ = tx.fs.Delete(dest, false)
		moveErr = tx.fs.MoveDir(src, dest)
		if moveErr == nil {
			_ = tx.fs.Delete(tx.tempPath, true)
			_ = tx.fs.Delete(filepath.Join(tx.destPath, SafeToIgnoreMarkerFile), false)
		}
	} else {
		moveErr = tx.fs.MoveDir(tx.tempPath, tx.destPath)
	}

	if moveErr != nil {
		if errors.Is(moveErr, os.ErrExist) || errors.Is(moveErr, syscall.ENOTEMPTY) || errors.Is(moveErr, syscall.EEXIST) {
			moveErr = errors.Join(ErrExists, moveErr)
		}
		return errors.Join(cacheErr, gitErr, moveErr)
	}

	tx.done = true
	return errors.Join(cacheErr, gitErr)
}

// clearCaches removes singleton cache entries and closes remotes
// under |path|.
func clearCaches(fs filesys.Filesys, path string) (error, error) {
	if absPath, err := fs.Abs(path); err == nil {
		k := SingletonCacheKeyForDatabaseDir(absPath)
		cErr := DeleteFromSingletonCache(k, false)
		gErr := CloseGitRemotesUnderRoot(absPath)
		return cErr, gErr
	}
	return nil, nil
}
