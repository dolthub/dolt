// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

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

const entryTtl = 100 * time.Millisecond

var testAOpts = AutoUnionFsOptions{
	UnionFsOptions: testOpts,
	Options: nodefs.Options{
		EntryTimeout:    entryTtl,
		AttrTimeout:     entryTtl,
		NegativeTimeout: 0,
		Debug:           testutil.VerboseTest(),
	},
	HideReadonly: true,
	Version:      "version",
}

func init() {
	testAOpts.Options.Debug = testutil.VerboseTest()
}

func WriteFile(t *testing.T, name string, contents string) {
	err := ioutil.WriteFile(name, []byte(contents), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
}

func setup(t *testing.T) (workdir string, server *fuse.Server, cleanup func()) {
	wd := testutil.TempDir()
	err := os.Mkdir(wd+"/mnt", 0700)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	err = os.Mkdir(wd+"/store", 0700)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	os.Mkdir(wd+"/ro", 0700)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	WriteFile(t, wd+"/ro/file1", "file1")
	WriteFile(t, wd+"/ro/file2", "file2")

	fs := NewAutoUnionFs(wd+"/store", testAOpts)

	nfs := pathfs.NewPathNodeFs(fs, nil)
	state, _, err := nodefs.MountRoot(wd+"/mnt", nfs.Root(), &testAOpts.Options)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go state.Serve()
	state.WaitMount()

	return wd, state, func() {
		state.Unmount()
		os.RemoveAll(wd)
	}
}

func TestDebug(t *testing.T) {
	wd, _, clean := setup(t)
	defer clean()

	c, err := ioutil.ReadFile(wd + "/mnt/status/debug")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(c) == 0 {
		t.Fatal("No debug found.")
	}
}

func TestVersion(t *testing.T) {
	wd, _, clean := setup(t)
	defer clean()

	c, err := ioutil.ReadFile(wd + "/mnt/status/gounionfs_version")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(c) == 0 {
		t.Fatal("No version found.")
	}
}

func TestAutoFsSymlink(t *testing.T) {
	wd, server, clean := setup(t)
	defer clean()

	err := os.Mkdir(wd+"/store/backing1", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	err = os.Symlink(wd+"/ro", wd+"/store/backing1/READONLY")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	err = os.Symlink(wd+"/store/backing1", wd+"/mnt/config/manual1")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	fi, err := os.Lstat(wd + "/mnt/manual1/file1")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	entries, err := ioutil.ReadDir(wd + "/mnt")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 3 {
		t.Error("readdir mismatch", entries)
	}

	err = os.Remove(wd + "/mnt/config/manual1")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	scan := wd + "/mnt/config/" + _SCAN_CONFIG
	err = ioutil.WriteFile(scan, []byte("something"), 0644)
	if err != nil {
		t.Error("error writing:", err)
	}

	// If FUSE supports invalid inode notifications we expect this node to be gone. Otherwise we'll just make sure that it's not reachable.
	if server.KernelSettings().SupportsNotify(fuse.NOTIFY_INVAL_INODE) {
		fi, _ = os.Lstat(wd + "/mnt/manual1")
		if fi != nil {
			t.Error("Should not have file:", fi)
		}
	} else {
		entries, err = ioutil.ReadDir(wd + "/mnt")
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "manual1" {
				t.Error("Should not have entry: ", e)
			}
		}
	}

	_, err = os.Lstat(wd + "/mnt/backing1/file1")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}

func TestDetectSymlinkedDirectories(t *testing.T) {
	wd, _, clean := setup(t)
	defer clean()

	err := os.Mkdir(wd+"/backing1", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	err = os.Symlink(wd+"/ro", wd+"/backing1/READONLY")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	err = os.Symlink(wd+"/backing1", wd+"/store/backing1")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	scan := wd + "/mnt/config/" + _SCAN_CONFIG
	err = ioutil.WriteFile(scan, []byte("something"), 0644)
	if err != nil {
		t.Error("error writing:", err)
	}

	_, err = os.Lstat(wd + "/mnt/backing1")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}

func TestExplicitScan(t *testing.T) {
	wd, _, clean := setup(t)
	defer clean()

	err := os.Mkdir(wd+"/store/backing1", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	os.Symlink(wd+"/ro", wd+"/store/backing1/READONLY")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	fi, _ := os.Lstat(wd + "/mnt/backing1")
	if fi != nil {
		t.Error("Should not have file:", fi)
	}

	scan := wd + "/mnt/config/" + _SCAN_CONFIG
	_, err = os.Lstat(scan)
	if err != nil {
		t.Error(".scan_config missing:", err)
	}

	err = ioutil.WriteFile(scan, []byte("something"), 0644)
	if err != nil {
		t.Error("error writing:", err)
	}

	_, err = os.Lstat(wd + "/mnt/backing1")
	if err != nil {
		t.Error("Should have workspace backing1:", err)
	}
}

func TestCreationChecks(t *testing.T) {
	wd, _, clean := setup(t)
	defer clean()

	err := os.Mkdir(wd+"/store/foo", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	os.Symlink(wd+"/ro", wd+"/store/foo/READONLY")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	err = os.Mkdir(wd+"/store/ws2", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	os.Symlink(wd+"/ro", wd+"/store/ws2/READONLY")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	err = os.Symlink(wd+"/store/foo", wd+"/mnt/config/bar")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	err = os.Symlink(wd+"/store/foo", wd+"/mnt/config/foo")
	code := fuse.ToStatus(err)
	if code != fuse.EBUSY {
		t.Error("Should return EBUSY", err)
	}

	err = os.Symlink(wd+"/store/ws2", wd+"/mnt/config/config")
	code = fuse.ToStatus(err)
	if code != fuse.EINVAL {
		t.Error("Should return EINVAL", err)
	}
}
