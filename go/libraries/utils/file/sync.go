// Copyright 2023 Dolthub, Inc.
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

package file

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
)

func SyncDirectoryHandle(dir string) (err error) {
	if runtime.GOOS == "windows" {
		// directory sync not supported on Windows
		return
	}
	var d *os.File
	if d, err = os.Open(dir); err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriteFileAtomically will attempt to write the contents of |rd| to a file named
// |name|, atomically. It uses POSIX filesystem semantics to attempt to
// accomplish this. It writes the entire contents of |rd| to a temporary file,
// fsync's the file, renames the file to its desired name, and then fsyncs the
// directory handle for the directory where these operations took place.
//
// Note: On non-Unix platforms, os.Rename is not an atomic operation.
func WriteFileAtomically(name string, rd io.Reader, mode os.FileMode) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	dir := filepath.Dir(name)
	f, err := os.CreateTemp(dir, filepath.Base(name)+"-*")
	if err != nil {
		return err
	}
	_, err = io.Copy(f, rd)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}

	err = f.Sync()
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}

	err = f.Close()
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	err = os.Chmod(f.Name(), mode)
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	err = os.Rename(f.Name(), name)
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	return SyncDirectoryHandle(filepath.Dir(f.Name()))
}
