// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type NotifyFs struct {
	pathfs.FileSystem
	size  uint64
	exist bool

	sizeChan  chan uint64
	existChan chan bool
}

func newNotifyFs() *NotifyFs {
	return &NotifyFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		sizeChan:   make(chan uint64, 1),
		existChan:  make(chan bool, 1),
	}
}

func (fs *NotifyFs) Exists() bool {
	select {
	case s := <-fs.existChan:
		fs.exist = s
	default:
	}
	return fs.exist
}

func (fs *NotifyFs) Size() uint64 {
	select {
	case s := <-fs.sizeChan:
		fs.size = s
	default:
	}
	return fs.size
}

func (fs *NotifyFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	}
	if name == "file" || (name == "dir/file" && fs.Exists()) {
		return &fuse.Attr{Mode: fuse.S_IFREG | 0644, Size: fs.Size()}, fuse.OK
	}
	if name == "dir" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *NotifyFs) Open(name string, f uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	return nodefs.NewDataFile([]byte{42}), fuse.OK
}

type NotifyTest struct {
	fs        *NotifyFs
	pathfs    *pathfs.PathNodeFs
	connector *nodefs.FileSystemConnector
	dir       string
	state     *fuse.Server
}

func NewNotifyTest(t *testing.T) *NotifyTest {
	me := &NotifyTest{}
	me.fs = newNotifyFs()
	me.dir = testutil.TempDir()
	entryTtl := 100 * time.Millisecond
	opts := &nodefs.Options{
		EntryTimeout:    entryTtl,
		AttrTimeout:     entryTtl,
		NegativeTimeout: entryTtl,
		Debug:           testutil.VerboseTest(),
	}

	me.pathfs = pathfs.NewPathNodeFs(me.fs, nil)
	var err error
	me.state, me.connector, err = nodefs.MountRoot(me.dir, me.pathfs.Root(), opts)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go me.state.Serve()
	if err := me.state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	return me
}

func (t *NotifyTest) Clean() {
	err := t.state.Unmount()
	if err == nil {
		os.RemoveAll(t.dir)
	}
}

func TestInodeNotify(t *testing.T) {
	test := NewNotifyTest(t)
	defer test.Clean()

	fs := test.fs
	dir := test.dir

	fs.sizeChan <- 42

	fi, err := os.Lstat(dir + "/file")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if fi.Mode()&os.ModeType != 0 || fi.Size() != 42 {
		t.Error(fi)
	}

	fs.sizeChan <- 666

	fi, err = os.Lstat(dir + "/file")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if fi.Mode()&os.ModeType != 0 || fi.Size() == 666 {
		t.Error(fi)
	}

	code := test.pathfs.FileNotify("file", -1, 0)
	if !code.Ok() {
		t.Error(code)
	}

	fi, err = os.Lstat(dir + "/file")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if fi.Mode()&os.ModeType != 0 || fi.Size() != 666 {
		t.Error(fi)
	}
}

func TestEntryNotify(t *testing.T) {
	test := NewNotifyTest(t)
	defer test.Clean()

	dir := test.dir
	test.fs.sizeChan <- 42
	test.fs.existChan <- false

	fn := dir + "/dir/file"
	fi, _ := os.Lstat(fn)
	if fi != nil {
		t.Errorf("File should not exist, %#v", fi)
	}

	test.fs.existChan <- true
	fi, _ = os.Lstat(fn)
	if fi != nil {
		t.Errorf("negative entry should have been cached: %#v", fi)
	}

	code := test.pathfs.EntryNotify("dir", "file")
	if !code.Ok() {
		t.Errorf("EntryNotify returns error: %v", code)
	}

	fi, err := os.Lstat(fn)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}
