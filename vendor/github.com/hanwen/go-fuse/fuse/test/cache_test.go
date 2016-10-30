// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type cacheFs struct {
	pathfs.FileSystem
}

func (fs *cacheFs) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	f, c := fs.FileSystem.Open(name, flags, context)
	if !c.Ok() {
		return f, c
	}
	return &nodefs.WithFlags{
		File:      f,
		FuseFlags: fuse.FOPEN_KEEP_CACHE,
	}, c

}

func setupCacheTest(t *testing.T) (string, *pathfs.PathNodeFs, func()) {
	dir := testutil.TempDir()
	os.Mkdir(dir+"/mnt", 0755)
	os.Mkdir(dir+"/orig", 0755)

	fs := &cacheFs{
		pathfs.NewLoopbackFileSystem(dir + "/orig"),
	}
	pfs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{Debug: testutil.VerboseTest()})

	opts := nodefs.NewOptions()
	opts.Debug = testutil.VerboseTest()
	state, _, err := nodefs.MountRoot(dir+"/mnt", pfs.Root(), opts)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	return dir, pfs, func() {
		err := state.Unmount()
		if err == nil {
			os.RemoveAll(dir)
		}
	}
}

func TestFopenKeepCache(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("FOPEN_KEEP_CACHE is broken on Darwin.")
	}

	wd, pathfs, clean := setupCacheTest(t)
	defer clean()

	before := "before"
	after := "after"
	if err := ioutil.WriteFile(wd+"/orig/file.txt", []byte(before), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	c, err := ioutil.ReadFile(wd + "/mnt/file.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if string(c) != before {
		t.Fatalf("ReadFile: got %q, want %q", c, before)
	}

	if err := ioutil.WriteFile(wd+"/orig/file.txt", []byte(after), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	c, err = ioutil.ReadFile(wd + "/mnt/file.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if string(c) != before {
		t.Fatalf("ReadFile: got %q, want cached %q", c, before)
	}

	if minor := pathfs.Connector().Server().KernelSettings().Minor; minor < 12 {
		t.Skip("protocol v%d has no notify support.", minor)
	}

	code := pathfs.EntryNotify("", "file.txt")
	if !code.Ok() {
		t.Errorf("EntryNotify: %v", code)
	}

	c, err = ioutil.ReadFile(wd + "/mnt/file.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if string(c) != after {
		t.Fatalf("ReadFile: got %q after notify, want %q", c, after)
	}
}

type nonseekFs struct {
	pathfs.FileSystem
	Length int
}

func (fs *nonseekFs) GetAttr(name string, context *fuse.Context) (fi *fuse.Attr, status fuse.Status) {
	if name == "file" {
		return &fuse.Attr{Mode: fuse.S_IFREG | 0644}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *nonseekFs) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	if name != "file" {
		return nil, fuse.ENOENT
	}

	data := bytes.Repeat([]byte{42}, fs.Length)
	f := nodefs.NewDataFile(data)
	return &nodefs.WithFlags{
		File:      f,
		FuseFlags: fuse.FOPEN_NONSEEKABLE,
	}, fuse.OK
}

func TestNonseekable(t *testing.T) {
	fs := &nonseekFs{FileSystem: pathfs.NewDefaultFileSystem()}
	fs.Length = 200 * 1024

	dir := testutil.TempDir()
	defer os.RemoveAll(dir)
	nfs := pathfs.NewPathNodeFs(fs, nil)
	opts := nodefs.NewOptions()
	opts.Debug = testutil.VerboseTest()
	state, _, err := nodefs.MountRoot(dir, nfs.Root(), opts)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	defer state.Unmount()

	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	f, err := os.Open(dir + "/file")
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	defer f.Close()

	b := make([]byte, 200)
	n, err := f.ReadAt(b, 20)
	if err == nil || n > 0 {
		t.Errorf("file was opened nonseekable, but seek successful")
	}
}

func TestGetAttrRace(t *testing.T) {
	dir := testutil.TempDir()
	defer os.RemoveAll(dir)
	os.Mkdir(dir+"/mnt", 0755)
	os.Mkdir(dir+"/orig", 0755)

	fs := pathfs.NewLoopbackFileSystem(dir + "/orig")
	pfs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{Debug: testutil.VerboseTest()})
	state, _, err := nodefs.MountRoot(dir+"/mnt", pfs.Root(),
		&nodefs.Options{Debug: testutil.VerboseTest()})
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	defer state.Unmount()

	var wg sync.WaitGroup

	n := 100
	wg.Add(n)
	var statErr error
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			fn := dir + "/mnt/file"
			err := ioutil.WriteFile(fn, []byte{42}, 0644)
			if err != nil {
				statErr = err
				return
			}
			_, err = os.Lstat(fn)
			if err != nil {
				statErr = err
			}
		}()
	}
	wg.Wait()
	if statErr != nil {
		t.Error(statErr)
	}
}
