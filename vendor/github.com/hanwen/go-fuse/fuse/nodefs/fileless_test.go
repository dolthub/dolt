// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nodefs

import (
	"io/ioutil"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
)

type nodeReadNode struct {
	Node
	dir    bool
	noOpen bool
	data   []byte
}

func newNodeReadNode(noOpen, dir bool, d []byte) *nodeReadNode {
	return &nodeReadNode{
		Node:   NewDefaultNode(),
		noOpen: noOpen,
		dir:    dir,
		data:   d,
	}
}

func (n *nodeReadNode) Open(flags uint32, context *fuse.Context) (file File, code fuse.Status) {
	if n.noOpen {
		return nil, fuse.ENOSYS
	}
	return nil, fuse.OK
}

func (n *nodeReadNode) Read(file File, dest []byte, off int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	e := off + int64(len(dest))
	if int(e) > len(n.data) {
		e = int64(len(n.data))
	}
	return fuse.ReadResultData(n.data[off:int(e)]), fuse.OK
}

func (n *nodeReadNode) GetAttr(out *fuse.Attr, file File, context *fuse.Context) (code fuse.Status) {
	if n.dir {
		out.Mode = fuse.S_IFDIR | 0755
	} else {
		out.Mode = fuse.S_IFREG | 0644
	}
	out.Size = uint64(len(n.data))
	return fuse.OK
}

func (n *nodeReadNode) Lookup(out *fuse.Attr, name string, context *fuse.Context) (*Inode, fuse.Status) {
	ch := n.Inode().NewChild(name, false, newNodeReadNode(n.noOpen, false, []byte(name)))
	return ch, ch.Node().GetAttr(out, nil, context)
}

func TestNoOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "nodefs")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}

	root := newNodeReadNode(true, true, nil)
	root.noOpen = true

	s, _, err := MountRoot(dir, root, &Options{Debug: VerboseTest()})
	if err != nil {
		t.Fatalf("MountRoot: %v", err)
	}
	defer s.Unmount()
	go s.Serve()
	if err := s.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	if s.KernelSettings().Minor < 23 {
		t.Skip("Kernel does not support open-less read/writes. Skipping test.")
	}

	content, err := ioutil.ReadFile(dir + "/file")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := "file"
	if string(content) != want {
		t.Fatalf("got %q, want %q", content, want)
	}

	content, err = ioutil.ReadFile(dir + "/file2")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want = "file2"
	if string(content) != want {
		t.Fatalf("got %q, want %q", content, want)
	}
}

func TestNodeRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "nodefs")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}

	root := newNodeReadNode(false, true, nil)
	opts := NewOptions()
	opts.Debug = VerboseTest()
	s, _, err := MountRoot(dir, root, opts)
	if err != nil {
		t.Fatalf("MountRoot: %v", err)
	}
	defer s.Unmount()
	go s.Serve()
	if err := s.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	content, err := ioutil.ReadFile(dir + "/file")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := "file"
	if string(content) != want {
		t.Fatalf("got %q, want %q", content, want)
	}
}
