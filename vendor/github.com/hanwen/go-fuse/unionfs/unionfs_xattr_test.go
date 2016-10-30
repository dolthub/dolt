// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type TestFS struct {
	pathfs.FileSystem
	xattrRead int
}

func (fs *TestFS) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	switch path {
	case "":
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	case "file":
		return &fuse.Attr{Mode: fuse.S_IFREG | 0755}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *TestFS) GetXAttr(path string, name string, context *fuse.Context) ([]byte, fuse.Status) {
	if path == "file" && name == "user.attr" {
		fs.xattrRead++
		return []byte{42}, fuse.OK
	}
	return nil, fuse.ENOATTR
}

func TestXAttrCaching(t *testing.T) {
	wd := testutil.TempDir()
	defer os.RemoveAll(wd)
	os.Mkdir(wd+"/mnt", 0700)
	err := os.Mkdir(wd+"/rw", 0700)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	rwFS := pathfs.NewLoopbackFileSystem(wd + "/rw")
	roFS := &TestFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
	}

	ufs, err := NewUnionFs([]pathfs.FileSystem{rwFS,
		NewCachingFileSystem(roFS, entryTtl)}, testOpts)
	if err != nil {
		t.Fatalf("NewUnionFs: %v", err)
	}

	opts := &nodefs.Options{
		EntryTimeout:    entryTtl / 2,
		AttrTimeout:     entryTtl / 2,
		NegativeTimeout: entryTtl / 2,
		Debug:           testutil.VerboseTest(),
	}

	pathfs := pathfs.NewPathNodeFs(ufs,
		&pathfs.PathNodeFsOptions{ClientInodes: true,
			Debug: testutil.VerboseTest()})

	server, _, err := nodefs.MountRoot(wd+"/mnt", pathfs.Root(), opts)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	defer server.Unmount()
	go server.Serve()
	server.WaitMount()

	if fi, err := os.Lstat(wd + "/mnt"); err != nil || !fi.IsDir() {
		t.Fatalf("root not readable: %v, %v", err, fi)
	}

	buf := make([]byte, 1024)
	n, err := Getxattr(wd+"/mnt/file", "user.attr", buf)
	if err != nil {
		t.Fatalf("Getxattr: %v", err)
	}
	want := "\x2a"
	got := string(buf[:n])
	if got != want {
		t.Fatalf("Got %q want %q", got, err)
	}

	time.Sleep(entryTtl / 3)

	n, err = Getxattr(wd+"/mnt/file", "user.attr", buf)
	if err != nil {
		t.Fatalf("Getxattr: %v", err)
	}
	got = string(buf[:n])
	if got != want {
		t.Fatalf("Got %q want %q", got, err)
	}

	time.Sleep(entryTtl / 3)

	// Make sure that an interceding Getxattr() to a filesystem that doesn't implement GetXAttr() doesn't affect future calls.
	Getxattr(wd, "whatever", buf)

	n, err = Getxattr(wd+"/mnt/file", "user.attr", buf)
	if err != nil {
		t.Fatalf("Getxattr: %v", err)
	}
	got = string(buf[:n])
	if got != want {
		t.Fatalf("Got %q want %q", got, err)
	}

	if roFS.xattrRead != 1 {
		t.Errorf("got xattrRead=%d, want 1", roFS.xattrRead)
	}
}
