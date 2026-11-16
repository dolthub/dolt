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

//go:build darwin

package file

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// RenameNoReplace is like Rename, except that it fails with an error
// satisfying errors.Is(err, fs.ErrExist) instead of replacing an existing
// |newpath|.
func RenameNoReplace(oldpath, newpath string) error {
	err := unix.RenamexNp(oldpath, newpath, unix.RENAME_EXCL)
	if errors.Is(err, unix.ENOTSUP) {
		// The filesystem does not support RENAME_EXCL.
		return linkNoReplace(oldpath, newpath)
	}
	if err != nil {
		return &os.LinkError{Op: "renamex_np", Old: oldpath, New: newpath, Err: err}
	}
	return nil
}
