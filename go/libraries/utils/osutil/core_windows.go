// Copyright 2019 Liquidata, Inc.
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

// +build windows

package osutil

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

const (
	IsWindows     = true
	PathDelimiter = string(byte(filepath.Separator))
)

var (
	SystemVolume   = filepath.VolumeName(os.Getenv("SYSTEMROOT"))
	FileSystemRoot = SystemVolume + PathDelimiter
)

// PathToNative will convert a Unix path into the Windows-native variant (only if running on a Windows machine)
func PathToNative(p string) string {
	if len(p) == 0 {
		return p
	}
	p = filepath.FromSlash(p)
	if !StartsWithWindowsVolume(p) {
		if p[0] == PathDelimiter[0] {
			p = SystemVolume + p
		} else {
			p = FileSystemRoot + p
		}
	}
	return p
}

// IsWindowsSharingViolation returns if the error is a Windows sharing violation
func IsWindowsSharingViolation(err error) bool {
	if pathErr, ok := err.(*os.PathError); ok && pathErr != nil && pathErr.Err == windows.ERROR_SHARING_VIOLATION {
		return true
	}
	return false
}
