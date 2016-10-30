// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zipfs

/*

This provides a practical example of mounting Go-fuse path filesystems
on top of each other.

It is a file system that configures a Zip filesystem at /zipmount when
symlinking path/to/zipfile to /config/zipmount

*/

import (
	"log"
	"path/filepath"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

const (
	CONFIG_PREFIX = "config/"
)

////////////////////////////////////////////////////////////////

// MultiZipFs is a path filesystem that mounts zipfiles.
type MultiZipFs struct {
	lock          sync.RWMutex
	zips          map[string]nodefs.Node
	dirZipFileMap map[string]string

	// zip files that we are in the process of unmounting.
	zombie map[string]bool

	nodeFs *pathfs.PathNodeFs
	pathfs.FileSystem
}

func NewMultiZipFs() *MultiZipFs {
	m := &MultiZipFs{
		zips:          make(map[string]nodefs.Node),
		zombie:        make(map[string]bool),
		dirZipFileMap: make(map[string]string),
		FileSystem:    pathfs.NewDefaultFileSystem(),
	}
	return m
}

func (fs *MultiZipFs) String() string {
	return "MultiZipFs"
}

func (fs *MultiZipFs) OnMount(nodeFs *pathfs.PathNodeFs) {
	fs.nodeFs = nodeFs
}

func (fs *MultiZipFs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	fs.lock.RLock()
	defer fs.lock.RUnlock()

	stream = make([]fuse.DirEntry, 0, len(fs.zips)+2)
	if name == "" {
		var d fuse.DirEntry
		d.Name = "config"
		d.Mode = fuse.S_IFDIR | 0700
		stream = append(stream, fuse.DirEntry(d))
	}

	if name == "config" {
		for k := range fs.zips {
			var d fuse.DirEntry
			d.Name = k
			d.Mode = fuse.S_IFLNK
			stream = append(stream, fuse.DirEntry(d))
		}
	}

	return stream, fuse.OK
}

func (fs *MultiZipFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	a := &fuse.Attr{}
	a.Owner = *fuse.CurrentOwner()
	if name == "" {
		// Should not write in top dir.
		a.Mode = fuse.S_IFDIR | 0500
		return a, fuse.OK
	}

	if name == "config" {
		a.Mode = fuse.S_IFDIR | 0700
		return a, fuse.OK
	}

	dir, base := filepath.Split(name)
	if dir != "" && dir != CONFIG_PREFIX {
		return nil, fuse.ENOENT
	}
	submode := uint32(fuse.S_IFDIR | 0700)
	if dir == CONFIG_PREFIX {
		submode = fuse.S_IFLNK | 0600
	}

	fs.lock.RLock()
	defer fs.lock.RUnlock()

	a.Mode = submode
	_, hasDir := fs.zips[base]
	if hasDir {
		return a, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *MultiZipFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	dir, basename := filepath.Split(name)
	if dir == CONFIG_PREFIX {
		fs.lock.Lock()
		defer fs.lock.Unlock()
		if fs.zombie[basename] {
			return fuse.ENOENT
		}
		root, ok := fs.zips[basename]
		if !ok {
			return fuse.ENOENT
		}

		name := fs.dirZipFileMap[basename]
		fs.zombie[basename] = true
		delete(fs.zips, basename)
		delete(fs.dirZipFileMap, basename)

		// Drop the lock to ensure that notify doesn't cause a deadlock.
		fs.lock.Unlock()
		code = fs.nodeFs.UnmountNode(root.Inode())
		fs.lock.Lock()
		delete(fs.zombie, basename)
		if !code.Ok() {
			// Failed: reinstate
			fs.zips[basename] = root
			fs.dirZipFileMap[basename] = name
		}
		return code
	}
	return fuse.EPERM
}

func (fs *MultiZipFs) Readlink(path string, context *fuse.Context) (val string, code fuse.Status) {
	dir, base := filepath.Split(path)
	if dir != CONFIG_PREFIX {
		return "", fuse.ENOENT
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()
	if fs.zombie[base] {
		return "", fuse.ENOENT
	}
	zipfile, ok := fs.dirZipFileMap[base]
	if !ok {
		return "", fuse.ENOENT
	}
	return zipfile, fuse.OK

}
func (fs *MultiZipFs) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	dir, base := filepath.Split(linkName)
	if dir != CONFIG_PREFIX {
		return fuse.EPERM
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()
	if fs.zombie[base] {
		return fuse.EBUSY
	}

	_, ok := fs.dirZipFileMap[base]
	if ok {
		return fuse.EBUSY
	}

	root, err := NewArchiveFileSystem(value)
	if err != nil {
		log.Println("NewZipArchiveFileSystem failed.", err)
		return fuse.EINVAL
	}

	code = fs.nodeFs.Mount(base, root, nil)
	if !code.Ok() {
		return code
	}

	fs.dirZipFileMap[base] = value
	fs.zips[base] = root
	return fuse.OK
}
