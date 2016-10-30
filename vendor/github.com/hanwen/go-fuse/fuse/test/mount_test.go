// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

func TestMountOnExisting(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	err := os.Mkdir(ts.mnt+"/mnt", 0777)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	nfs := nodefs.NewDefaultNode()
	code := ts.connector.Mount(ts.rootNode(), "mnt", nfs, nil)
	if code != fuse.EBUSY {
		t.Fatal("expect EBUSY:", code)
	}

	err = os.Remove(ts.mnt + "/mnt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	code = ts.connector.Mount(ts.rootNode(), "mnt", nfs, nil)
	if !code.Ok() {
		t.Fatal("expect OK:", code)
	}

	code = ts.pathFs.Unmount("mnt")
	if !code.Ok() {
		t.Errorf("Unmount failed: %v", code)
	}
}

func TestMountRename(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	fs := pathfs.NewPathNodeFs(pathfs.NewLoopbackFileSystem(ts.orig), nil)
	code := ts.connector.Mount(ts.rootNode(), "mnt", fs.Root(), nil)
	if !code.Ok() {
		t.Fatal("mount should succeed")
	}
	err := os.Rename(ts.mnt+"/mnt", ts.mnt+"/foobar")
	if fuse.ToStatus(err) != fuse.EBUSY {
		t.Fatal("rename mount point should fail with EBUSY:", err)
	}
	ts.pathFs.Unmount("mnt")
}

func TestMountReaddir(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	fs := pathfs.NewPathNodeFs(pathfs.NewLoopbackFileSystem(ts.orig), nil)
	code := ts.connector.Mount(ts.rootNode(), "mnt", fs.Root(), nil)
	if !code.Ok() {
		t.Fatal("mount should succeed")
	}

	entries, err := ioutil.ReadDir(ts.mnt)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "mnt" {
		t.Error("wrong readdir result", entries)
	}
	ts.pathFs.Unmount("mnt")
}

func TestRecursiveMount(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	err := ioutil.WriteFile(ts.orig+"/hello.txt", []byte("blabla"), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	fs := pathfs.NewPathNodeFs(pathfs.NewLoopbackFileSystem(ts.orig), nil)
	code := ts.connector.Mount(ts.rootNode(), "mnt", fs.Root(), nil)
	if !code.Ok() {
		t.Fatal("mount should succeed")
	}

	submnt := ts.mnt + "/mnt"
	_, err = os.Lstat(submnt)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	_, err = os.Lstat(filepath.Join(submnt, "hello.txt"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	f, err := os.Open(filepath.Join(submnt, "hello.txt"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	code = ts.pathFs.Unmount("mnt")
	if code != fuse.EBUSY {
		t.Error("expect EBUSY")
	}

	if err := f.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	// We can't avoid a sleep here: the file handle release is not
	// synchronized.
	t.Log("Waiting for kernel to flush file-close to fuse...")
	time.Sleep(testTtl)

	code = ts.pathFs.Unmount("mnt")
	if code != fuse.OK {
		t.Error("umount failed.", code)
	}
}

func TestDeletedUnmount(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	submnt := filepath.Join(ts.mnt, "mnt")
	pfs2 := pathfs.NewPathNodeFs(pathfs.NewLoopbackFileSystem(ts.orig), nil)
	code := ts.connector.Mount(ts.rootNode(), "mnt", pfs2.Root(), nil)
	if !code.Ok() {
		t.Fatal("Mount error", code)
	}
	f, err := os.Create(filepath.Join(submnt, "hello.txt"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	err = os.Remove(filepath.Join(submnt, "hello.txt"))
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = f.Write([]byte("bla"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	code = ts.pathFs.Unmount("mnt")
	if code != fuse.EBUSY {
		t.Error("expect EBUSY for unmount with open files", code)
	}

	f.Close()
	time.Sleep((3 * testTtl) / 2)
	code = ts.pathFs.Unmount("mnt")
	if !code.Ok() {
		t.Error("should succeed", code)
	}
}

func TestDefaultNodeMount(t *testing.T) {
	dir := testutil.TempDir()
	defer os.RemoveAll(dir)
	root := nodefs.NewDefaultNode()
	s, conn, err := nodefs.MountRoot(dir, root, nil)
	if err != nil {
		t.Fatalf("MountRoot: %v", err)
	}
	go s.Serve()
	if err := s.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	defer s.Unmount()

	if err := conn.Mount(root.Inode(), "sub", nodefs.NewDefaultNode(), nil); !err.Ok() {
		t.Fatalf("Mount: %v", err)
	}

	if entries, err := ioutil.ReadDir(dir); err != nil {
		t.Fatalf("ReadDir: %v", err)
	} else if len(entries) != 1 {
		t.Fatalf("got %d entries", len(entries))
	} else if entries[0].Name() != "sub" {
		t.Fatalf("got %q, want %q", entries[0].Name(), "sub")
	}
}

func TestLiveness(t *testing.T) {
	dir := testutil.TempDir()
	defer os.RemoveAll(dir)
	root := nodefs.NewDefaultNode()
	s, _, err := nodefs.MountRoot(dir, root, nil)
	if err != nil {
		t.Fatalf("MountRoot: %v", err)
	}
	go s.Serve()
	if err := s.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	defer s.Unmount()

	if _, err := ioutil.ReadDir(dir); err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	// We previously encountered a sitation where a finalizer would close our fd out from under us. Try to force both finalizers to run and object destruction to complete.
	runtime.GC()
	runtime.GC()

	if _, err := ioutil.ReadDir(dir); err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
}
