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

package git

import (
	"errors"
	"fmt"
)

// ErrUnimplemented is returned by stubbed write-path APIs. It is intentionally
// exported so higher layers (e.g. GitBlobstore) can wrap or match it.
var ErrUnimplemented = errors.New("unimplemented")

// RefNotFoundError indicates that a ref (e.g. refs/dolt/data) could not be resolved.
type RefNotFoundError struct {
	Ref string
}

func (e *RefNotFoundError) Error() string {
	return fmt.Sprintf("git ref not found: %s", e.Ref)
}

// PathNotFoundError indicates that a tree path could not be resolved within a commit.
type PathNotFoundError struct {
	Commit string
	Path   string
}

func (e *PathNotFoundError) Error() string {
	return fmt.Sprintf("git path not found: %s:%s", e.Commit, e.Path)
}

// NotBlobError indicates that a resolved path did not refer to a blob object.
type NotBlobError struct {
	Commit string
	Path   string
	Type   string
}

func (e *NotBlobError) Error() string {
	if e.Type == "" {
		return fmt.Sprintf("git path is not a blob: %s:%s", e.Commit, e.Path)
	}
	return fmt.Sprintf("git path is not a blob (%s): %s:%s", e.Type, e.Commit, e.Path)
}

// MergeConflictError indicates that a merge could not be completed without resolving conflicts.
// For GitBlobstore usage, this typically means the same blobstore key was changed differently
// on both sides.
type MergeConflictError struct {
	Paths []string
}

func (e *MergeConflictError) Error() string {
	if len(e.Paths) == 0 {
		return "git merge conflict"
	}
	// Avoid overly large errors; callers can log more details if needed.
	const max = 10
	paths := e.Paths
	more := ""
	if len(paths) > max {
		paths = paths[:max]
		more = fmt.Sprintf(" (and %d more)", len(e.Paths)-max)
	}
	return fmt.Sprintf("git merge conflict on %d paths%s: %v", len(e.Paths), more, paths)
}

func IsPathNotFound(err error) bool {
	var e *PathNotFoundError
	return errors.As(err, &e)
}
