// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zipfs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

func testZipFile() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("need runtime.Caller()'s file name to discover testdata")
	}
	dir, _ := filepath.Split(file)
	return filepath.Join(dir, "test.zip")
}

func setupZipfs(t *testing.T) (mountPoint string, cleanup func()) {
	root, err := NewArchiveFileSystem(testZipFile())
	if err != nil {
		t.Fatalf("NewArchiveFileSystem failed: %v", err)
	}

	mountPoint = testutil.TempDir()
	state, _, err := nodefs.MountRoot(mountPoint, root, &nodefs.Options{
		Debug: testutil.VerboseTest(),
	})

	go state.Serve()
	state.WaitMount()

	return mountPoint, func() {
		state.Unmount()
		os.RemoveAll(mountPoint)
	}
}

func TestZipFs(t *testing.T) {
	mountPoint, clean := setupZipfs(t)
	defer clean()
	entries, err := ioutil.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 2 {
		t.Error("wrong length", entries)
	}
	fi, err := os.Stat(mountPoint + "/subdir")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if !fi.IsDir() {
		t.Error("directory type", fi)
	}

	fi, err = os.Stat(mountPoint + "/file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if fi.IsDir() {
		t.Error("file type", fi)
	}

	f, err := os.Open(mountPoint + "/file.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	b := make([]byte, 1024)
	n, err := f.Read(b)

	b = b[:n]
	if string(b) != "hello\n" {
		t.Error("content fail", b[:n])
	}
	f.Close()
}

func TestLinkCount(t *testing.T) {
	mp, clean := setupZipfs(t)
	defer clean()

	fi, err := os.Stat(mp + "/file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if fuse.ToStatT(fi).Nlink != 1 {
		t.Fatal("wrong link count", fuse.ToStatT(fi).Nlink)
	}
}
