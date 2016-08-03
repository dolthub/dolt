package unionfs

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

const _XATTRSEP = "@XATTR@"

type attrResponse struct {
	*fuse.Attr
	fuse.Status
}

type xattrResponse struct {
	data []byte
	fuse.Status
}

type dirResponse struct {
	entries []fuse.DirEntry
	fuse.Status
}

type linkResponse struct {
	linkContent string
	fuse.Status
}

// Caches filesystem metadata.
type cachingFileSystem struct {
	pathfs.FileSystem

	attributes *TimedCache
	dirs       *TimedCache
	links      *TimedCache
	xattr      *TimedCache
}

func readDir(fs pathfs.FileSystem, name string) *dirResponse {
	origStream, code := fs.OpenDir(name, nil)

	r := &dirResponse{nil, code}
	if !code.Ok() {
		return r
	}
	r.entries = origStream
	return r
}

func getAttr(fs pathfs.FileSystem, name string) *attrResponse {
	a, code := fs.GetAttr(name, nil)
	return &attrResponse{
		Attr:   a,
		Status: code,
	}
}

func getXAttr(fs pathfs.FileSystem, nameAttr string) *xattrResponse {
	ns := strings.SplitN(nameAttr, _XATTRSEP, 2)
	a, code := fs.GetXAttr(ns[0], ns[1], nil)
	return &xattrResponse{
		data:   a,
		Status: code,
	}
}

func readLink(fs pathfs.FileSystem, name string) *linkResponse {
	a, code := fs.Readlink(name, nil)
	return &linkResponse{
		linkContent: a,
		Status:      code,
	}
}

func NewCachingFileSystem(fs pathfs.FileSystem, ttl time.Duration) pathfs.FileSystem {
	c := new(cachingFileSystem)
	c.FileSystem = fs
	c.attributes = NewTimedCache(func(n string) (interface{}, bool) {
		a := getAttr(fs, n)
		return a, a.Ok()
	}, ttl)
	c.dirs = NewTimedCache(func(n string) (interface{}, bool) {
		d := readDir(fs, n)
		return d, d.Ok()
	}, ttl)
	c.links = NewTimedCache(func(n string) (interface{}, bool) {
		l := readLink(fs, n)
		return l, l.Ok()
	}, ttl)
	c.xattr = NewTimedCache(func(n string) (interface{}, bool) {
		l := getXAttr(fs, n)
		return l, l.Ok()
	}, ttl)
	return c
}

func (fs *cachingFileSystem) DropCache() {
	for _, c := range []*TimedCache{fs.attributes, fs.dirs, fs.links, fs.xattr} {
		c.DropAll(nil)
	}
}

func (fs *cachingFileSystem) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == _DROP_CACHE {
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0777,
		}, fuse.OK
	}

	r := fs.attributes.Get(name).(*attrResponse)
	return r.Attr, r.Status
}

func (fs *cachingFileSystem) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	key := name + _XATTRSEP + attr
	r := fs.xattr.Get(key).(*xattrResponse)
	return r.data, r.Status
}

func (fs *cachingFileSystem) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	r := fs.links.Get(name).(*linkResponse)
	return r.linkContent, r.Status
}

func (fs *cachingFileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	r := fs.dirs.Get(name).(*dirResponse)
	return r.entries, r.Status
}

func (fs *cachingFileSystem) String() string {
	return fmt.Sprintf("cachingFileSystem(%v)", fs.FileSystem)
}

func (fs *cachingFileSystem) Open(name string, flags uint32, context *fuse.Context) (f nodefs.File, status fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 && name == _DROP_CACHE {
		log.Println("Dropping cache for", fs)
		fs.DropCache()
	}
	return fs.FileSystem.Open(name, flags, context)
}
