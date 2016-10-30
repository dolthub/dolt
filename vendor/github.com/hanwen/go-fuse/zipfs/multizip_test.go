// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zipfs

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

const testTtl = 100 * time.Millisecond

func setupMzfs(t *testing.T) (mountPoint string, state *fuse.Server, cleanup func()) {
	fs := NewMultiZipFs()
	mountPoint = testutil.TempDir()
	nfs := pathfs.NewPathNodeFs(fs, nil)
	state, _, err := nodefs.MountRoot(mountPoint, nfs.Root(), &nodefs.Options{
		EntryTimeout:    testTtl,
		AttrTimeout:     testTtl,
		NegativeTimeout: 0.0,
		Debug:           testutil.VerboseTest(),
	})
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go state.Serve()
	state.WaitMount()

	return mountPoint, state, func() {
		state.Unmount()
		os.RemoveAll(mountPoint)
	}
}

func TestMultiZipReadonly(t *testing.T) {
	mountPoint, _, cleanup := setupMzfs(t)
	defer cleanup()

	_, err := os.Create(mountPoint + "/random")
	if err == nil {
		t.Error("Must fail writing in root.")
	}

	_, err = os.OpenFile(mountPoint+"/config/zipmount", os.O_WRONLY, 0)
	if err == nil {
		t.Error("Must fail without O_CREATE")
	}
}

func TestMultiZipFs(t *testing.T) {
	mountPoint, server, cleanup := setupMzfs(t)
	defer cleanup()

	zipFile := testZipFile()

	entries, err := ioutil.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Name()) != "config" {
		t.Errorf("wrong names return. %v", entries)
	}

	err = os.Symlink(zipFile, mountPoint+"/config/zipmount")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	fi, err := os.Lstat(mountPoint + "/zipmount")
	if !fi.IsDir() {
		t.Errorf("Expect directory at /zipmount")
	}

	entries, err = ioutil.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 2 {
		t.Error("Expect 2 entries", entries)
	}

	val, err := os.Readlink(mountPoint + "/config/zipmount")
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}
	if val != zipFile {
		t.Errorf("expected %v got %v", zipFile, val)
	}

	fi, err = os.Lstat(mountPoint + "/zipmount")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("expect directory for /zipmount, got %v", fi)
	}

	// Check that zipfs itself works.
	fi, err = os.Stat(mountPoint + "/zipmount/subdir")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if !fi.IsDir() {
		t.Error("directory type", fi)
	}

	// Removing the config dir unmount
	err = os.Remove(mountPoint + "/config/zipmount")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// If FUSE supports invalid inode notifications we expect this node to be gone. Otherwise we'll just make sure that it's not reachable.
	if server.KernelSettings().SupportsNotify(fuse.NOTIFY_INVAL_INODE) {
		fi, err = os.Stat(mountPoint + "/zipmount")
		if err == nil {
			t.Errorf("stat should fail after unmount, got %#v", fi)
		}
	} else {
		entries, err = ioutil.ReadDir(mountPoint)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "zipmount" {
				t.Error("Should not have entry: ", e)
			}
		}
	}

}
