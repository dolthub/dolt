// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchmark

import (
	"path/filepath"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type StatFS struct {
	pathfs.FileSystem
	entries map[string]*fuse.Attr
	dirs    map[string][]fuse.DirEntry
}

func (fs *StatFS) Add(name string, a *fuse.Attr) {
	name = strings.TrimRight(name, "/")
	_, ok := fs.entries[name]
	if ok {
		return
	}

	fs.entries[name] = a
	if name == "/" || name == "" {
		return
	}

	dir, base := filepath.Split(name)
	dir = strings.TrimRight(dir, "/")
	fs.dirs[dir] = append(fs.dirs[dir], fuse.DirEntry{Name: base, Mode: a.Mode})
	fs.Add(dir, &fuse.Attr{Mode: fuse.S_IFDIR | 0755})
}

func (fs *StatFS) AddFile(name string) {
	fs.Add(name, &fuse.Attr{Mode: fuse.S_IFREG | 0644})
}

func (fs *StatFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if d := fs.dirs[name]; d != nil {
		return &fuse.Attr{Mode: 0755 | fuse.S_IFDIR}, fuse.OK
	}
	e := fs.entries[name]
	if e == nil {
		return nil, fuse.ENOENT
	}

	return e, fuse.OK
}

func (fs *StatFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	entries := fs.dirs[name]
	if entries == nil {
		return nil, fuse.ENOENT
	}
	return entries, fuse.OK
}

func NewStatFS() *StatFS {
	return &StatFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		entries:    make(map[string]*fuse.Attr),
		dirs:       make(map[string][]fuse.DirEntry),
	}
}
