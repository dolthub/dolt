package benchmark

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

var delay = 0 * time.Microsecond

type StatFs struct {
	pathfs.FileSystem
	entries map[string]*fuse.Attr
	dirs    map[string][]fuse.DirEntry
	delay   time.Duration
}

func (me *StatFs) Add(name string, a *fuse.Attr) {
	name = strings.TrimRight(name, "/")
	_, ok := me.entries[name]
	if ok {
		return
	}

	me.entries[name] = a
	if name == "/" || name == "" {
		return
	}

	dir, base := filepath.Split(name)
	dir = strings.TrimRight(dir, "/")
	me.dirs[dir] = append(me.dirs[dir], fuse.DirEntry{Name: base, Mode: a.Mode})
	me.Add(dir, &fuse.Attr{Mode: fuse.S_IFDIR | 0755})
}

func (me *StatFs) AddFile(name string) {
	me.Add(name, &fuse.Attr{Mode: fuse.S_IFREG | 0644})
}

func (me *StatFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	e := me.entries[name]
	if e == nil {
		return nil, fuse.ENOENT
	}

	if me.delay > 0 {
		time.Sleep(me.delay)
	}
	return e, fuse.OK
}

func (me *StatFs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	entries := me.dirs[name]
	if entries == nil {
		return nil, fuse.ENOENT
	}
	return entries, fuse.OK
}

func NewStatFs() *StatFs {
	return &StatFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		entries:    make(map[string]*fuse.Attr),
		dirs:       make(map[string][]fuse.DirEntry),
	}
}
