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

//go:build windows

package file

import (
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// RenameNoReplace is like Rename, except that it fails with an error
// satisfying errors.Is(err, fs.ErrExist) instead of replacing an existing
// |newpath|.
func RenameNoReplace(oldpath, newpath string) error {
	err := moveFileNoReplace(oldpath, newpath)
	if isAccessError(err) {
		// Windows has background processes which open file and
		// can falsely trip this error, nevertheless we persist with a retry
		for waitTime := time.Duration(1); isAccessError(err) && waitTime <= 10000; waitTime *= 10 {
			time.Sleep(waitTime * time.Millisecond)
			err = moveFileNoReplace(oldpath, newpath)
		}
	}
	return err
}

func moveFileNoReplace(oldpath, newpath string) error {
	from, err := windows.UTF16PtrFromString(oldpath)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	to, err := windows.UTF16PtrFromString(newpath)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	// Without MOVEFILE_REPLACE_EXISTING, MoveFileEx fails with
	// ERROR_ALREADY_EXISTS if |newpath| exists.
	if err := windows.MoveFileEx(from, to, 0); err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	return nil
}
