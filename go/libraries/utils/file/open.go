// Copyright 2021 Dolthub, Inc.
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

// +build !windows

package file

import (
	"os"
)

// The file issues are only observed on Windows, therefore on other platforms these function no differently than direct
// calls.

// Rename functions exactly like os.Rename, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Remove functions exactly like os.Remove, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func Remove(name string) error {
	return os.Remove(name)
}

// RemoveAll functions exactly like os.RemoveAll, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func RemoveAll(path string) error {
	return os.RemoveAll(path)
}
