// Copyright 2019 Dolthub, Inc.
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

package filesys

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/file"
)

// LocalFS is the machines local filesystem
var LocalFS = &localFS{}

type localFS struct {
	cwd string
}

// LocalFilesysWithWorkingDir returns a new Filesys implementation backed by the local filesystem with the supplied
// working directory.  Path relative operations occur relative to this directory.
func LocalFilesysWithWorkingDir(cwd string) (Filesys, error) {
	absCWD, err := filepath.Abs(cwd)
	if err != nil {
		return nil, err
	}

	// We're going to turn this into a URL, so we need to make sure that windows separators are converted to /
	absCWD = filepath.ToSlash(absCWD)

	stat, err := os.Stat(absCWD)

	if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("'%s' is not a valid directory", absCWD)
	}

	return &localFS{absCWD}, nil
}

// Exists will tell you if a file or directory with a given path already exists, and if it does is it a directory
func (fs *localFS) Exists(path string) (exists bool, isDir bool) {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return false, false
	}

	stat, err := os.Stat(path)

	if err != nil {
		return false, false
	}

	return true, stat.IsDir()
}

// WithWorkingDir returns a copy of this file system with a new working dir as given.
func (fs localFS) WithWorkingDir(path string) (Filesys, error) {
	abs, err := fs.Abs(path)
	if err != nil {
		return nil, err
	}

	fs.cwd = abs
	return &fs, nil
}

var errStopMarker = errors.New("stop")

// Iter iterates over the files and subdirectories within a given directory (Optionally recursively).
func (fs *localFS) Iter(path string, recursive bool, cb FSIterCB) error {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return err
	}

	if !recursive {
		dirEntries, err := os.ReadDir(path)

		if err != nil {
			return err
		}

		for _, entry := range dirEntries {
			fi, err := entry.Info()
			if err != nil {
				return err
			}

			stop := cb(filepath.Join(path, fi.Name()), fi.Size(), fi.IsDir())

			if stop {
				return nil
			}
		}

		return nil
	}

	return fs.iter(path, cb)
}

func (fs *localFS) iter(dir string, cb FSIterCB) error {
	var err error
	dir, err = fs.Abs(dir)

	if err != nil {
		return err
	}

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if dir != path {
			stop := cb(path, info.Size(), info.IsDir())

			if stop {
				return errStopMarker
			}
		}
		return nil
	})

	if err == errStopMarker {
		return nil
	}

	return err
}

// OpenForRead opens a file for reading
func (fs *localFS) OpenForRead(fp string) (io.ReadCloser, error) {
	var err error
	fp, err = fs.Abs(fp)

	if err != nil {
		return nil, err
	}

	if exists, isDir := fs.Exists(fp); !exists {
		return nil, os.ErrNotExist
	} else if isDir {
		return nil, ErrIsDir
	}

	return os.Open(fp)
}

// ReadFile reads the entire contents of a file
func (fs *localFS) ReadFile(fp string) ([]byte, error) {
	var err error
	fp, err = fs.Abs(fp)

	if err != nil {
		return nil, err
	}

	return os.ReadFile(fp)
}

// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
// it will be overwritten.
func (fs *localFS) OpenForWrite(fp string, perm os.FileMode) (io.WriteCloser, error) {
	var err error
	fp, err = fs.Abs(fp)

	if err != nil {
		return nil, err
	}

	return os.OpenFile(fp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
}

// OpenForWriteAppend opens a file for writing. The file will be created if it does not exist, and it will
// append only to that new file. If file exists, it will append to existing file.
func (fs *localFS) OpenForWriteAppend(fp string, perm os.FileMode) (io.WriteCloser, error) {
	var err error
	fp, err = fs.Abs(fp)

	if err != nil {
		return nil, err
	}

	return os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm)
}

// WriteFile writes the entire data buffer to a given file.  The file will be created if it does not exist,
// and if it does exist it will be overwritten.
func (fs *localFS) WriteFile(fp string, data []byte) error {
	var err error
	fp, err = fs.Abs(fp)

	if err != nil {
		return err
	}

	return os.WriteFile(fp, data, os.ModePerm)
}

// MkDirs creates a folder and all the parent folders that are necessary to create it.
func (fs *localFS) MkDirs(path string) error {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return err
	}

	_, err = os.Stat(path)

	if err != nil {
		return os.MkdirAll(path, os.ModePerm)
	}

	return nil
}

// DeleteFile will delete a file at the given path
func (fs *localFS) DeleteFile(path string) error {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return err
	}

	if exists, isDir := fs.Exists(path); exists && !isDir {
		if isDir {
			return ErrIsDir
		}

		return file.Remove(path)
	}

	return os.ErrNotExist
}

// Delete will delete an empty directory, or a file.  If trying delete a directory that is not empty you can set force to
// true in order to delete the dir and all of it's contents
func (fs *localFS) Delete(path string, force bool) error {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return err
	}

	if !force {
		return file.Remove(path)
	} else {
		return file.RemoveAll(path)
	}
}

// MoveFile will move a file from the srcPath in the filesystem to the destPath
func (fs *localFS) MoveFile(srcPath, destPath string) error {
	var err error
	srcPath, err = fs.Abs(srcPath)

	if err != nil {
		return err
	}

	destPath, err = fs.Abs(destPath)

	if err != nil {
		return err
	}

	return file.Rename(srcPath, destPath)
}

// converts a path to an absolute path.  If it's already an absolute path the input path will be returned unaltered
func (fs *localFS) Abs(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}

	if fs.cwd == "" {
		return filepath.Abs(path)
	} else {
		return filepath.Join(fs.cwd, path), nil
	}
}

// LastModified gets the last modified timestamp for a file or directory at a given path
func (fs *localFS) LastModified(path string) (t time.Time, exists bool) {
	var err error
	path, err = fs.Abs(path)

	if err != nil {
		return time.Time{}, false
	}

	stat, err := os.Stat(path)

	if err != nil {
		return time.Time{}, false
	}

	return stat.ModTime(), true
}
