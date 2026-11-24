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

package tempfiles

import (
	"os"

	"golang.org/x/sys/windows"
)

// createLockFile makes the empty file |path|, failing if it already exists.
//
// It does not use os.OpenFile, which opens files on Windows without FILE_SHARE_DELETE. Until
// that handle was closed nobody could delete the file, so a cleanup which took the new lock
// file's lock before its owner did failed to remove it.
func createLockFile(path string) error {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := windows.CreateFile(
		p,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return &os.PathError{Op: "open", Path: path, Err: err}
	}
	if err := windows.CloseHandle(h); err != nil {
		return &os.PathError{Op: "close", Path: path, Err: err}
	}
	return nil
}
