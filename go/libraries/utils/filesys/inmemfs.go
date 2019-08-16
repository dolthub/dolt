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

package filesys

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
)

type memObj interface {
	isDir() bool
	parent() *memDir
}

type memFile struct {
	absPath   string
	data      []byte
	parentDir *memDir
}

func (mf *memFile) isDir() bool {
	return false
}

func (mf *memFile) parent() *memDir {
	return mf.parentDir
}

type memDir struct {
	absPath   string
	objs      map[string]memObj
	parentDir *memDir
}

func newEmptyDir(path string, parent *memDir) *memDir {
	return &memDir{path, make(map[string]memObj), parent}
}

func (md *memDir) isDir() bool {
	return true
}

func (md *memDir) parent() *memDir {
	return md.parentDir
}

// InMemFS is an in memory filesystem implementation that is primarily intended for testing
type InMemFS struct {
	cwd  string
	objs map[string]memObj
}

// EmptyInMemFS creates an empty InMemFS instance
func EmptyInMemFS(workingDir string) *InMemFS {
	return NewInMemFS([]string{}, map[string][]byte{}, workingDir)
}

// NewInMemFS creates an InMemFS with directories and folders provided.
func NewInMemFS(dirs []string, files map[string][]byte, cwd string) *InMemFS {
	if cwd == "" {
		cwd = osutil.FileSystemRoot
	}
	cwd = osutil.PathToNative(cwd)

	if !filepath.IsAbs(cwd) {
		panic("cwd for InMemFilesys must be absolute path.")
	}

	fs := &InMemFS{cwd, map[string]memObj{osutil.FileSystemRoot: newEmptyDir(osutil.FileSystemRoot, nil)}}

	if dirs != nil {
		for _, dir := range dirs {
			absDir := fs.getAbsPath(dir)
			fs.mkDirs(absDir)
		}
	}

	if files != nil {
		for path, val := range files {
			path = fs.getAbsPath(path)

			dir := filepath.Dir(path)
			targetDir, err := fs.mkDirs(dir)

			if err != nil {
				panic("Initializing InMemFS with invalid data.")
			}

			newFile := &memFile{path, val, targetDir}

			targetDir.objs[path] = newFile
			fs.objs[path] = newFile
		}
	}

	return fs
}

func (fs *InMemFS) getAbsPath(path string) string {
	path = fs.pathToNative(path)
	if strings.HasPrefix(path, osutil.FileSystemRoot) {
		return filepath.Clean(path)
	}

	return filepath.Join(fs.cwd, path)
}

// Exists will tell you if a file or directory with a given path already exists, and if it does is it a directory
func (fs *InMemFS) Exists(path string) (exists bool, isDir bool) {
	path = fs.getAbsPath(path)

	if obj, ok := fs.objs[path]; ok {
		return true, obj.isDir()
	}

	return false, false
}

// Iter iterates over the files and subdirectories within a given directory (Optionally recursively).  There
// are no guarantees about the ordering of results.  Two calls to iterate the same directory may yield
// differently ordered results.
func (fs *InMemFS) Iter(path string, recursive bool, cb FSIterCB) error {
	_, err := fs.iter(fs.getAbsPath(path), recursive, cb)

	return err
}

func (fs *InMemFS) iter(path string, recursive bool, cb FSIterCB) (bool, error) {
	path = filepath.Clean(path)
	obj, ok := fs.objs[path]

	if !ok {
		return true, os.ErrNotExist
	} else if !obj.isDir() {
		return true, ErrIsDir
	}

	dir := obj.(*memDir)

	for k, v := range dir.objs {
		var size int
		if !v.isDir() {
			size = len(v.(*memFile).data)
		}

		stop := cb(k, int64(size), v.isDir())

		if stop {
			return true, nil
		}

		if v.isDir() && recursive {
			stop, err := fs.iter(k, recursive, cb)

			if stop || err != nil {
				return stop, err
			}
		}
	}

	return false, nil
}

// OpenForRead opens a file for reading
func (fs *InMemFS) OpenForRead(fp string) (io.ReadCloser, error) {
	fp = fs.getAbsPath(fp)

	if exists, isDir := fs.Exists(fp); !exists {
		return nil, os.ErrNotExist
	} else if isDir {
		return nil, ErrIsDir
	}

	fileObj := fs.objs[fp].(*memFile)
	buf := bytes.NewBuffer(fileObj.data)

	return ioutil.NopCloser(buf), nil
}

// ReadFile reads the entire contents of a file
func (fs *InMemFS) ReadFile(fp string) ([]byte, error) {
	fp = fs.getAbsPath(fp)
	r, err := fs.OpenForRead(fp)

	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(r)
}

type inMemFSWriteCloser struct {
	path      string
	parentDir *memDir
	fs        *InMemFS
	buf       *bytes.Buffer
}

func (fsw *inMemFSWriteCloser) Write(p []byte) (int, error) {
	return fsw.buf.Write(p)
}

func (fsw *inMemFSWriteCloser) Close() error {
	data := fsw.buf.Bytes()
	newFile := &memFile{fsw.path, data, fsw.parentDir}
	fsw.parentDir.objs[fsw.path] = newFile
	fsw.fs.objs[fsw.path] = newFile

	return nil
}

// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
// it will be overwritten.
func (fs *InMemFS) OpenForWrite(fp string) (io.WriteCloser, error) {
	fp = fs.getAbsPath(fp)

	if exists, isDir := fs.Exists(fp); exists && isDir {
		return nil, ErrIsDir
	}

	dir := filepath.Dir(fp)
	parentDir, err := fs.mkDirs(dir)

	if err != nil {
		return nil, err
	}

	return &inMemFSWriteCloser{fp, parentDir, fs, bytes.NewBuffer(make([]byte, 0, 512))}, nil
}

// WriteFile writes the entire data buffer to a given file.  The file will be created if it does not exist,
// and if it does exist it will be overwritten.
func (fs *InMemFS) WriteFile(fp string, data []byte) error {
	w, err := fs.OpenForWrite(fp)

	if err != nil {
		return err
	}

	err = iohelp.WriteAll(w, data)

	if err != nil {
		return err
	}

	return w.Close()
}

// MkDirs creates a folder and all the parent folders that are necessary to create it.
func (fs *InMemFS) MkDirs(path string) error {
	path = fs.getAbsPath(path)
	_, err := fs.mkDirs(path)
	return err
}

func (fs *InMemFS) mkDirs(path string) (*memDir, error) {
	path = fs.getAbsPath(path)
	elements := strings.Split(path, osutil.PathDelimiter)

	currPath := osutil.FileSystemRoot
	parentObj, ok := fs.objs[currPath]

	if !ok {
		panic("Filesystem does not have a root directory.")
	}

	parentDir := parentObj.(*memDir)
	for i, element := range elements {
		// When iterating Windows-style paths, the first slash is after the volume, e.g. C:/
		// We check if the first element (like "C:") plus the delimiter is the same as the system root
		// If so, we skip it as we add the system root when creating the InMemFS
		if i == 0 && osutil.IsWindows && element+osutil.PathDelimiter == osutil.FileSystemRoot {
			continue
		}
		currPath = filepath.Join(currPath, element)

		if obj, ok := fs.objs[currPath]; !ok {
			newDir := newEmptyDir(currPath, parentDir)
			parentDir.objs[currPath] = newDir
			fs.objs[currPath] = newDir
			parentDir = newDir
		} else if !obj.isDir() {
			return nil, errors.New("Could not create directory with same path as existing file: " + currPath)
		} else {
			parentDir = obj.(*memDir)
		}
	}

	return parentDir, nil
}

// DeleteFile will delete a file at the given path
func (fs *InMemFS) DeleteFile(path string) error {
	path = fs.getAbsPath(path)

	if obj, ok := fs.objs[path]; ok {
		if obj.isDir() {
			return ErrIsDir
		}

		delete(fs.objs, path)

		parentDir := obj.parent()
		if parentDir != nil {
			delete(parentDir.objs, path)
		}
	} else {
		return os.ErrNotExist
	}

	return nil
}

// Delete will delete an empty directory, or a file.  If trying delete a directory that is not empty you can set force to
// true in order to delete the dir and all of it's contents
func (fs *InMemFS) Delete(path string, force bool) error {
	path = fs.getAbsPath(path)

	if exists, isDir := fs.Exists(path); !exists {
		return os.ErrNotExist
	} else if !isDir {
		return fs.DeleteFile(path)
	}

	isEmpty := true
	toDelete := map[string]bool{path: true}
	err := fs.Iter(path, true, func(path string, size int64, isDir bool) (stop bool) {
		isEmpty = false
		if !force {
			return true
		}

		toDelete[path] = isDir

		return false
	})

	if err != nil {
		return err
	}

	if !force && !isEmpty {
		return errors.New(path + " is a directory which is not empty. Delete the contents first, or set force to true")
	}

	for currPath := range toDelete {
		currObj := fs.objs[currPath]
		delete(fs.objs, currPath)

		parentDir := currObj.parent()
		if parentDir != nil {
			delete(parentDir.objs, currPath)
		}
	}

	return nil
}

func (fs *InMemFS) MoveFile(srcPath, destPath string) error {
	srcPath = fs.getAbsPath(srcPath)
	destPath = fs.getAbsPath(destPath)

	if exists, destIsDir := fs.Exists(destPath); exists && destIsDir {
		return ErrIsDir
	}

	if obj, ok := fs.objs[srcPath]; ok {
		if obj.isDir() {
			return ErrIsDir
		}

		destDir := filepath.Dir(destPath)
		destParentDir, err := fs.mkDirs(destDir)

		if err != nil {
			return err
		}

		destObj := &memFile{destPath, obj.(*memFile).data, destParentDir}

		fs.objs[destPath] = destObj
		delete(fs.objs, srcPath)

		parentDir := obj.parent()
		if parentDir != nil {
			delete(parentDir.objs, srcPath)
		}

		destParentDir.objs[destPath] = destObj

		return nil
	}

	return os.ErrNotExist
}

// converts a path to an absolute path.  If it's already an absolute path the input path will be returned unaltered
func (fs *InMemFS) Abs(path string) (string, error) {
	path = fs.pathToNative(path)
	if filepath.IsAbs(path) {
		return path, nil
	}

	return filepath.Join(fs.cwd, path), nil
}

func (fs *InMemFS) pathToNative(path string) string {
	if len(path) >= 1 {
		if path[0] == '.' {
			if len(path) == 1 {
				return fs.cwd
			}
			if len(path) >= 2 && (path[1] == '/' || path[1] == '\\') {
				return filepath.Join(fs.cwd, path[2:])
			}
			return filepath.Join(fs.cwd, path)
		} else if !osutil.StartsWithWindowsVolume(path) && path[0] != '/' && path[0] != '\\' {
			return filepath.Join(fs.cwd, path)
		}
	}
	return osutil.PathToNative(path)
}
