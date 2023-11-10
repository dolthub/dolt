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
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"
)

var ErrIsDir = errors.New("operation not valid on a directory")
var ErrIsFile = errors.New("operation not valid on a file")
var ErrDirNotExist = errors.New("directory does not exist")

// ReadableFS is an interface providing read access to objs in a filesystem
type ReadableFS interface {

	// OpenForRead opens a file for reading
	OpenForRead(fp string) (io.ReadCloser, error)

	// ReadFile reads the entire contents of a file
	ReadFile(fp string) ([]byte, error)

	// Exists will tell you if a file or directory with a given path already exists, and if it does is it a directory
	Exists(path string) (exists bool, isDir bool)

	// converts a path to an absolute path.  If it's already an absolute path the input path will be returned unaltered
	Abs(path string) (string, error)

	// LastModified gets the last modified timestamp for a file or directory at a given path
	LastModified(path string) (t time.Time, exists bool)
}

// WritableFS is an interface providing write access to objs in a filesystem
type WritableFS interface {
	// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
	// it will be overwritten.
	OpenForWrite(fp string, perm os.FileMode) (io.WriteCloser, error)

	// OpenForWriteAppend opens a file for writing. The file will be created if it does not exist, and it will
	// append only to that new file. If file exists, it will append to existing file.
	OpenForWriteAppend(fp string, perm os.FileMode) (io.WriteCloser, error)

	// WriteFileWithPerms writes the entire data buffer to a given file.  The file will be created if it does not exist,
	// and if it does exist it will be overwritten. WriteFile attempts to write the file atomically and durably.
	WriteFile(fp string, data []byte, perm os.FileMode) error

	// MkDirs creates a folder and all the parent folders that are necessary to create it.
	MkDirs(path string) error

	// DeleteFile will delete a file at the given path
	DeleteFile(path string) error

	// Delete will delete an empty directory, or a file.  If trying delete a directory that is not empty you can set force to
	// true in order to delete the dir and all of it's contents
	Delete(path string, force bool) error

	// MoveFile will move a file from the srcPath in the filesystem to the destPath
	MoveFile(srcPath, destPath string) error

	// MoveDir will move a directory from the srcPath in the filesystem to the destPath. For example,
	// MoveDir("foo", "bar/baz") will move the "foo" directory to "bar/baz", meaning the contents of "foo" are now
	// directly under the "baz" directory. All subdirectories on |destPath| must already exist before MoveDir is called.
	MoveDir(srcPath, destPath string) error

	// TempDir returns the path of a new temporary directory.
	TempDir() string
}

// FSIterCB specifies the signature of the function that will be called for every item found while iterating.
type FSIterCB func(path string, size int64, isDir bool) (stop bool)

// WalkableFS is an interface for walking the files and subdirectories of a directory
type WalkableFS interface {

	// Iter iterates over the files and subdirectories within a given directory (Optionally recursively).  There
	// are no guarantees about the ordering of results.  Two calls to iterate the same directory may yield
	// differently ordered results.
	Iter(directory string, recursive bool, cb FSIterCB) error
}

// ReadWriteFS is an interface whose implementors will provide read, and write implementations but may not allow
// for files to be listed.
type ReadWriteFS interface {
	ReadableFS
	WritableFS
}

// Filesys is an interface whose implementors will provide read, write, and list mechanisms
type Filesys interface {
	ReadableFS
	WritableFS
	WalkableFS

	// WithWorkingDir returns a copy of this Filesys with the given working directory
	WithWorkingDir(path string) (Filesys, error)
}

func UnmarshalJSONFile(fs ReadableFS, path string, dest interface{}) error {
	data, err := fs.ReadFile(path)

	if err != nil {
		return err
	}

	return json.Unmarshal(data, dest)
}

func CopyFile(srcPath, destPath string, srcFS, destFS Filesys) (err error) {
	rd, err := srcFS.OpenForRead(srcPath)
	if err != nil {
		return err
	}
	defer func() {
		cerr := rd.Close()
		if err == nil {
			err = cerr
		}
	}()

	wr, err := destFS.OpenForWrite(destPath, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() {
		cerr := wr.Close()
		if err == nil {
			err = cerr
		}
	}()

	_, err = io.Copy(wr, rd)
	return
}
