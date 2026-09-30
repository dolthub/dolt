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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// TempDirPrefix is the filename prefix for temporary database
// directories during creation and clone.
const TempDirPrefix = ".tmp-dolt-"

const (
	// uuidStringLength is the 36-character length of a standard UUID string representation.
	uuidStringLength = 36
	// uuidSuffixLength is the hyphen delimiter plus the 36-character UUID string.
	uuidSuffixLength = uuidStringLength + 1
	// expectedUUIDVersion is UUID version 7 per RFC 9562 §6.2.
	expectedUUIDVersion = 7
)

// CreateTempDir creates an isolated temporary directory under |fs|
// for isolating database writes destined for |destPath|.
//
// When |insideDest| is true, the temporary directory is created
// inside |destPath| to avoid leaking into the parent directory.
// Otherwise, it is created as a sibling to ensure both paths share
// the same filesystem volume for atomic directory renames via
// [rename(2)].
//
// The temporary directory is named with the target database name,
// the creator process ID ([os.Getpid]), and the full 128-bit |u|
// (time-ordered UUIDv7 as defined in [RFC 9562 §6.2]). If |u| is
// [uuid.Nil], a fallback UUIDv7 is generated via [uuid.NewV7].
//
// [rename(2)]: https://man7.org/linux/man-pages/man2/rename.2.html
// [RFC 9562 §6.2]: https://www.rfc-editor.org/rfc/rfc9562.html#section-6.2
func CreateTempDir(fs filesys.Filesys, destPath string, insideDest bool, u uuid.UUID) (string, uuid.UUID, error) {
	if u == uuid.Nil {
		var err error
		u, err = uuid.NewV7()
		if err != nil {
			return "", uuid.Nil, err
		}
	}
	pid := os.Getpid()

	if insideDest {
		name := fmt.Sprintf("%s%d-%s", TempDirPrefix, pid, u)
		dir := filepath.Join(destPath, name)
		return dir, u, fs.MkDirs(dir)
	}

	name := fmt.Sprintf("%s%s-%d-%s", TempDirPrefix, filepath.Base(destPath), pid, u)
	dir := filepath.Join(filepath.Dir(destPath), name)
	return dir, u, fs.MkDirs(dir)
}

// IsTempDir reports whether |path| under |fs| matches
// TempDirPrefix or is a legacy incomplete database carrying
// SafeToIgnoreMarkerFile.
func IsTempDir(fs filesys.Filesys, path string) bool {
	if strings.HasPrefix(filepath.Base(path), TempDirPrefix) {
		return true
	}
	// Detect legacy incomplete databases left by older Dolt releases
	// that used an in-progress marker instead of temporary dirs.
	marked, _ := fs.Exists(filepath.Join(path, SafeToIgnoreMarkerFile))
	return marked
}

// CleanupTempDirs inspects |parentDir| under |fs| and removes
// temporary database directories where |filter| returns true.
//
// For each directory matching IsTempDir, CleanupTempDirs calls
// |filter| with the directory path. If |filter| is nil or returns
// true, the directory is removed.
//
// If |ctx| is cancelled, CleanupTempDirs stops scanning and returns
// [ctx.Err]. Deletion errors are collected and returned using
// [errors.Join].
func CleanupTempDirs(ctx context.Context, fs filesys.Filesys, parentDir string, filter func(path string) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	iterErr := fs.Iter(parentDir, false, func(path string, _ int64, isDir bool) bool {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			return true
		}
		if !isDir || !IsTempDir(fs, path) || (filter != nil && !filter(path)) {
			return false
		}
		if err := fs.Delete(path, true); err != nil {
			errs = append(errs, err)
		}
		return false
	})
	return errors.Join(append(errs, iterErr)...)
}

// IsTempDirStale reports whether temporary database directory |path|
// is stale by testing creator process liveness and operating system
// PID reuse.
func IsTempDirStale(path string) bool {
	pid, createdAt, ok := parseTempDirMeta(filepath.Base(path))
	if !ok {
		return false
	}
	procStart, err := processStartTime(pid)
	if err != nil {
		if errors.Is(err, ErrProcUnavailable) {
			// Process exists but start time unavailable (Darwin/BSD);
			// assume active process to prevent purging live writes.
			return false
		}
		// Process does not exist (ESRCH); creator has terminated.
		return true
	}
	// If the process holding this PID was launched after the directory
	// was created, the PID was recycled by the operating system.
	return procStart.After(createdAt.Add(1 * time.Second))
}

// ParseTempDirMeta extracts the target database name, creator PID,
// UUID, and creation timestamp from temporary directory name
// |dirName|.
//
// If |dirName| is not a valid temporary database directory matching
// TempDirPrefix, ParseTempDirMeta returns ok as false.
func ParseTempDirMeta(dirName string) (dbName string, pid int, u uuid.UUID, createdAt time.Time, ok bool) {
	base := filepath.Base(dirName)
	if !strings.HasPrefix(base, TempDirPrefix) ||
		len(base) < len(TempDirPrefix)+uuidSuffixLength ||
		base[len(base)-uuidSuffixLength] != '-' {
		return "", 0, uuid.Nil, time.Time{}, false
	}
	parsedUUID, err := uuid.Parse(base[len(base)-uuidStringLength:])
	if err != nil || parsedUUID.Version() != expectedUUIDVersion {
		return "", 0, uuid.Nil, time.Time{}, false
	}
	prefix := base[:len(base)-uuidSuffixLength]
	lastDash := strings.LastIndex(prefix, "-")
	if lastDash == -1 || lastDash < len(TempDirPrefix)-1 {
		return "", 0, uuid.Nil, time.Time{}, false
	}
	parsedPID, err := strconv.Atoi(prefix[lastDash+1:])
	if err != nil {
		return "", 0, uuid.Nil, time.Time{}, false
	}
	if lastDash > len(TempDirPrefix)-1 {
		dbName = prefix[len(TempDirPrefix):lastDash]
	}
	sec, nsec := parsedUUID.Time().UnixTime()
	return dbName, parsedPID, parsedUUID, time.Unix(sec, nsec), true
}

// parseTempDirMeta extracts the creator PID and creation timestamp
// from temporary directory name |dirName|.
func parseTempDirMeta(dirName string) (int, time.Time, bool) {
	_, pid, _, createdAt, ok := ParseTempDirMeta(dirName)
	return pid, createdAt, ok
}
