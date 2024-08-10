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
	"bytes"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/lockutil"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
)

// InMemNowFunc is a func() time.Time that can be used to supply the current time.  The default value gets the current
// time from the system clock, but it can be set to something else in order to support reproducible tests.
var InMemNowFunc = time.Now

type memObj interface {
	isDir() bool
	parent() *memDir
	modTime() time.Time
}

type memFile struct {
	absPath   string
	data      []byte
	parentDir *memDir
	time      time.Time
}

func (mf *memFile) isDir() bool {
	return false
}

func (mf *memFile) parent() *memDir {
	return mf.parentDir
}

func (mf *memFile) modTime() time.Time {
	return mf.time
}

type memDir struct {
	absPath   string
	objs      map[string]memObj
	parentDir *memDir
	time      time.Time
}

func newEmptyDir(path string, parent *memDir) *memDir {
	return &memDir{path, make(map[string]memObj), parent, InMemNowFunc()}
}

func (md *memDir) isDir() bool {
	return true
}

func (md *memDir) parent() *memDir {
	return md.parentDir
}

func (md *memDir) modTime() time.Time {
	return md.time
}

// InMemFS is an in memory filesystem implementation that is primarily intended for testing
type InMemFS struct {
	rwLock *sync.RWMutex
	cwd    string
	objs   map[string]memObj
}

var _ Filesys = (*InMemFS)(nil)

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

	fs := &InMemFS{&sync.RWMutex{}, cwd, map[string]memObj{osutil.FileSystemRoot: newEmptyDir(osutil.FileSystemRoot, nil)}}

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

			now := InMemNowFunc()
			newFile := &memFile{path, val, targetDir, now}

			targetDir.time = now
			targetDir.objs[path] = newFile
			fs.objs[path] = newFile
		}
	}

	return fs
}

// WithWorkingDir returns a copy of this file system with the current working dir set to the path given
func (fs InMemFS) WithWorkingDir(path string) (Filesys, error) {
	abs, err := fs.Abs(path)
	if err != nil {
		return nil, err
	}

	fs.cwd = abs
	return &fs, nil
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
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()

	return fs.exists(path)
}

func (fs *InMemFS) exists(path string) (exists bool, isDir bool) {
	path = fs.getAbsPath(path)

	if obj, ok := fs.objs[path]; ok {
		return true, obj.isDir()
	}

	return false, false
}

type iterEntry struct {
	path  string
	size  int64
	isDir bool
}

func (fs *InMemFS) getIterEntries(path string, recursive bool) ([]iterEntry, error) {
	var entries []iterEntry
	_, err := fs.iter(fs.getAbsPath(path), recursive, func(path string, size int64, isDir bool) (stop bool) {
		entries = append(entries, iterEntry{path, size, isDir})
		return false
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Iter iterates over the files and subdirectories within a given directory (Optionally recursively).  There
// are no guarantees about the ordering of results. It is also possible that concurrent delete operations could render
// a file path invalid when the callback is made.
func (fs *InMemFS) Iter(path string, recursive bool, cb FSIterCB) error {
	entries, err := func() ([]iterEntry, error) {
		fs.rwLock.RLock()
		defer fs.rwLock.RUnlock()

		return fs.getIterEntries(path, recursive)
	}()

	if err != nil {
		return err
	}

	for _, entry := range entries {
		cb(entry.path, entry.size, entry.isDir)
	}

	return nil
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
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()

	fp = fs.getAbsPath(fp)

	if exists, isDir := fs.exists(fp); !exists {
		return nil, os.ErrNotExist
	} else if isDir {
		return nil, ErrIsDir
	}

	fileObj := fs.objs[fp].(*memFile)
	buf := bytes.NewReader(fileObj.data)

	return io.NopCloser(buf), nil
}

// ReadFile reads the entire contents of a file
func (fs *InMemFS) ReadFile(fp string) ([]byte, error) {
	fp = fs.getAbsPath(fp)
	r, err := fs.OpenForRead(fp)

	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}

type inMemFSWriteCloser struct {
	path      string
	parentDir *memDir
	fs        *InMemFS
	buf       *bytes.Buffer
	rwLock    *sync.RWMutex
}

func (fsw *inMemFSWriteCloser) Write(p []byte) (int, error) {
	return fsw.buf.Write(p)
}

func (fsw *inMemFSWriteCloser) Close() error {
	fsw.rwLock.Lock()
	defer fsw.rwLock.Unlock()

	now := InMemNowFunc()
	data := fsw.buf.Bytes()
	newFile := &memFile{fsw.path, data, fsw.parentDir, now}
	fsw.parentDir.time = now
	fsw.parentDir.objs[fsw.path] = newFile
	fsw.fs.objs[fsw.path] = newFile

	return nil
}

// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
// it will be overwritten.
func (fs *InMemFS) OpenForWrite(fp string, perm os.FileMode) (io.WriteCloser, error) {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	fp = fs.getAbsPath(fp)

	if exists, isDir := fs.exists(fp); exists && isDir {
		return nil, ErrIsDir
	}

	dir := filepath.Dir(fp)
	parentDir, err := fs.mkDirs(dir)

	if err != nil {
		return nil, err
	}

	return &inMemFSWriteCloser{fp, parentDir, fs, bytes.NewBuffer(make([]byte, 0, 512)), fs.rwLock}, nil
}

// OpenForWriteAppend opens a file for writing.  The file will be created if it does not exist, and if it does exist
// it will append to existing file.
func (fs *InMemFS) OpenForWriteAppend(fp string, perm os.FileMode) (io.WriteCloser, error) {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	fp = fs.getAbsPath(fp)

	if exists, isDir := fs.exists(fp); exists && isDir {
		return nil, ErrIsDir
	}

	dir := filepath.Dir(fp)
	parentDir, err := fs.mkDirs(dir)

	if err != nil {
		return nil, err
	}

	return &inMemFSWriteCloser{fp, parentDir, fs, bytes.NewBuffer(make([]byte, 0, 512)), fs.rwLock}, nil
}

// WriteFile writes the entire data buffer to a given file.  The file will be created if it does not exist,
// and if it does exist it will be overwritten.
func (fs *InMemFS) WriteFile(fp string, data []byte, perm os.FileMode) error {
	w, err := fs.OpenForWrite(fp, perm)

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
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

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
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	return fs.deleteFile(path)
}

func (fs *InMemFS) deleteFile(path string) error {
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
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	path = fs.getAbsPath(path)

	if exists, isDir := fs.exists(path); !exists {
		return os.ErrNotExist
	} else if !isDir {
		return fs.deleteFile(path)
	}

	toDelete := map[string]bool{path: true}
	entries, err := fs.getIterEntries(path, true)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		toDelete[entry.path] = entry.isDir
	}

	isEmpty := len(toDelete) == 1

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

func (fs *InMemFS) MoveDir(srcPath, destPath string) error {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	srcPath = fs.getAbsPath(srcPath)
	destPath = fs.getAbsPath(destPath)

	destPathParent, _ := filepath.Split(destPath)
	if exists, destIsDir := fs.exists(destPathParent); !exists || !destIsDir {
		return ErrDirNotExist
	}

	obj, ok := fs.objs[srcPath]
	if !ok {
		return os.ErrNotExist
	}

	if !obj.isDir() {
		return ErrIsFile
	}

	return fs.moveDirHelper(obj.(*memDir), destPath)
}

func (fs *InMemFS) moveDirHelper(dir *memDir, destPath string) error {
	// All calls to moveDirHelper MUST happen with the filesystem's read-write mutex locked
	if err := lockutil.AssertRWMutexIsLocked(fs.rwLock); err != nil {
		return fmt.Errorf("moveDirHelper called without first acquiring filesystem read-write lock")
	}

	if _, exists := fs.objs[destPath]; exists {
		return fmt.Errorf("destination path exists: %s", destPath)
	}

	if _, exists := fs.objs[filepath.Dir(destPath)]; !exists {
		return fmt.Errorf("destination parent dir does NOT exist: %s", filepath.Dir(destPath))
	}

	// Create the base directory in the new location before we process the files in dir
	parentDir := filepath.Dir(destPath)
	destParentDir := fs.objs[parentDir].(*memDir)
	destObj := &memDir{
		absPath:   destPath,
		objs:      make(map[string]memObj),
		parentDir: destParentDir,
		time:      InMemNowFunc(),
	}
	fs.objs[destPath] = destObj
	destParentDir.objs[destPath] = destObj
	destParentDir.time = InMemNowFunc()

	for _, v := range dir.objs {
		switch obj := v.(type) {
		case *memDir:
			base := filepath.Base(obj.absPath)
			newPath := filepath.Join(destPath, base)
			if err := fs.moveDirHelper(obj, newPath); err != nil {
				return err
			}
		case *memFile:
			base := filepath.Base(obj.absPath)
			newDestPath := filepath.Join(destPath, base)
			if err := fs.moveFileHelper(obj, newDestPath); err != nil {
				return err
			}
			delete(dir.objs, obj.absPath)
			delete(fs.objs, obj.absPath)
		default:
			return fmt.Errorf("unexpectededed type of memory object: %T", v)
		}
	}

	delete(dir.parentDir.objs, dir.absPath)
	delete(fs.objs, dir.absPath)
	return nil
}

// MoveFile will move a file from the srcPath in the filesystem to the destPath
func (fs *InMemFS) MoveFile(srcPath, destPath string) error {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	srcPath = fs.getAbsPath(srcPath)
	destPath = fs.getAbsPath(destPath)

	if exists, destIsDir := fs.exists(destPath); exists && destIsDir {
		return ErrIsDir
	}

	if obj, ok := fs.objs[srcPath]; ok {
		if obj.isDir() {
			return ErrIsDir
		}

		return fs.moveFileHelper(obj.(*memFile), destPath)
	}

	return os.ErrNotExist
}

func (fs *InMemFS) moveFileHelper(obj *memFile, destPath string) error {
	// All calls to moveFileHelper MUST happen with the filesystem's read-write mutex locked
	if err := lockutil.AssertRWMutexIsLocked(fs.rwLock); err != nil {
		return fmt.Errorf("moveFileHelper called without first acquiring filesystem read-write lock")
	}

	destDir := filepath.Dir(destPath)
	destParentDir, err := fs.mkDirs(destDir)
	if err != nil {
		return err
	}

	now := InMemNowFunc()
	destObj := &memFile{destPath, obj.data, destParentDir, now}

	fs.objs[destPath] = destObj

	delete(fs.objs, obj.absPath)

	parentDir := obj.parent()
	if parentDir != nil {
		parentDir.time = now
		delete(parentDir.objs, obj.absPath)
	}

	destParentDir.objs[destPath] = destObj
	destParentDir.time = now

	return nil
}

func (fs *InMemFS) CopyFile(srcPath, destPath string) error {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	srcPath = fs.getAbsPath(srcPath)
	destPath = fs.getAbsPath(destPath)

	if exists, destIsDir := fs.exists(destPath); exists && destIsDir {
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

		destData := make([]byte, len(obj.(*memFile).data))
		copy(destData, obj.(*memFile).data)

		now := InMemNowFunc()
		destObj := &memFile{destPath, destData, destParentDir, now}

		fs.objs[destPath] = destObj
		destParentDir.objs[destPath] = destObj
		destParentDir.time = now

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

// LastModified gets the last modified timestamp for a file or directory at a given path
func (fs *InMemFS) LastModified(path string) (t time.Time, exists bool) {
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()

	path = fs.getAbsPath(path)

	if obj, ok := fs.objs[path]; ok {
		return obj.modTime(), true
	}

	return time.Time{}, false
}

func (fs *InMemFS) TempDir() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	s := base32.HexEncoding.EncodeToString(buf)
	return "/var/folders/gc/" + s + "/T/"
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
