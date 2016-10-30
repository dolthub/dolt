// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pathfs

import (
	"os"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type ownerFs struct {
	FileSystem
}

const _RANDOM_OWNER = 31415265

func (fs *ownerFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	a := &fuse.Attr{
		Mode: fuse.S_IFREG | 0644,
	}
	a.Uid = _RANDOM_OWNER
	a.Gid = _RANDOM_OWNER
	return a, fuse.OK
}

func setupOwnerTest(t *testing.T, opts *nodefs.Options) (workdir string, cleanup func()) {
	wd := testutil.TempDir()

	fs := &ownerFs{NewDefaultFileSystem()}
	nfs := NewPathNodeFs(fs, nil)
	state, _, err := nodefs.MountRoot(wd, nfs.Root(), opts)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	return wd, func() {
		state.Unmount()
		os.RemoveAll(wd)
	}
}

func TestOwnerDefault(t *testing.T) {
	wd, cleanup := setupOwnerTest(t, nodefs.NewOptions())
	defer cleanup()

	var stat syscall.Stat_t
	err := syscall.Lstat(wd+"/foo", &stat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if int(stat.Uid) != os.Getuid() || int(stat.Gid) != os.Getgid() {
		t.Fatal("Should use current uid for mount")
	}
}

func TestOwnerRoot(t *testing.T) {
	wd, cleanup := setupOwnerTest(t, &nodefs.Options{})
	defer cleanup()

	var st syscall.Stat_t
	err := syscall.Lstat(wd+"/foo", &st)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if st.Uid != _RANDOM_OWNER || st.Gid != _RANDOM_OWNER {
		t.Fatal("Should use FS owner uid")
	}
}

func TestOwnerOverride(t *testing.T) {
	wd, cleanup := setupOwnerTest(t, &nodefs.Options{Owner: &fuse.Owner{Uid: 42, Gid: 43}})
	defer cleanup()

	var stat syscall.Stat_t
	err := syscall.Lstat(wd+"/foo", &stat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if stat.Uid != 42 || stat.Gid != 43 {
		t.Fatal("Should use current uid for mount")
	}
}
