// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type flipNode struct {
	nodefs.Node
	ok chan int
}

func (f *flipNode) GetAttr(out *fuse.Attr, file nodefs.File, c *fuse.Context) fuse.Status {
	select {
	case <-f.ok:
		// use a status that is easily recognizable.
		return fuse.Status(syscall.EXDEV)
	default:
	}
	return f.Node.GetAttr(out, file, c)
}

func TestDeleteNotify(t *testing.T) {
	dir := testutil.TempDir()
	defer os.RemoveAll(dir)
	root := nodefs.NewMemNodeFSRoot(dir + "/backing")
	conn := nodefs.NewFileSystemConnector(root,
		&nodefs.Options{PortableInodes: true})
	mnt := dir + "/mnt"
	err := os.Mkdir(mnt, 0755)
	if err != nil {
		t.Fatal(err)
	}

	state, err := fuse.NewServer(conn.RawFS(), mnt, &fuse.MountOptions{
		Debug: testutil.VerboseTest(),
	})
	if err != nil {
		t.Fatal(err)
	}
	go state.Serve()
	defer state.Unmount()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	_, code := root.Mkdir("testdir", 0755, nil)
	if !code.Ok() {
		t.Fatal(code)
	}

	ch := root.Inode().RmChild("testdir")
	ch.Node().SetInode(nil)
	flip := flipNode{
		Node: ch.Node(),
		ok:   make(chan int),
	}
	root.Inode().NewChild("testdir", true, &flip)

	err = ioutil.WriteFile(mnt+"/testdir/testfile", []byte{42}, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Do the test here, so we surely have state.KernelSettings()
	if state.KernelSettings().Minor < 18 {
		t.Log("Kernel does not support deletion notify; aborting test.")
		return
	}
	buf := bytes.Buffer{}
	cmd := exec.Command("/usr/bin/tail", "-f", "testfile")
	cmd.Dir = mnt + "/testdir"
	cmd.Stdin = &buf
	cmd.Stdout = &bytes.Buffer{}
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cmd.Process.Kill()
		time.Sleep(100 * time.Millisecond)
	}()

	// Wait until tail opened the file.
	time.Sleep(100 * time.Millisecond)
	err = os.Remove(mnt + "/testdir/testfile")
	if err != nil {
		t.Fatal(err)
	}

	// Simulate deletion+mkdir coming from the network
	close(flip.ok)
	oldCh := root.Inode().RmChild("testdir")
	_, code = root.Inode().Node().Mkdir("testdir", 0755, nil)
	if !code.Ok() {
		t.Fatal("mkdir status", code)
	}
	conn.DeleteNotify(root.Inode(), oldCh, "testdir")

	_, err = os.Lstat(mnt + "/testdir")
	if err != nil {
		t.Fatalf("lstat after del + mkdir failed: %v", err)
	}
}
