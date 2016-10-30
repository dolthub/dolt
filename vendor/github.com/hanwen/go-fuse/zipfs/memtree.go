// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zipfs

import (
	"fmt"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

type MemFile interface {
	Stat(out *fuse.Attr)
	Data() []byte
}

type memNode struct {
	nodefs.Node
	file MemFile
	fs   *MemTreeFs
}

// memTreeFs creates a tree of internal Inodes.  Since the tree is
// loaded in memory completely at startup, it does not need inode
// discovery through Lookup() at serve time.
type MemTreeFs struct {
	root  *memNode
	files map[string]MemFile
	Name  string
}

func NewMemTreeFs(files map[string]MemFile) *MemTreeFs {
	fs := &MemTreeFs{
		root:  &memNode{Node: nodefs.NewDefaultNode()},
		files: files,
	}
	fs.root.fs = fs
	return fs
}

func (fs *MemTreeFs) String() string {
	return fs.Name
}

func (fs *MemTreeFs) Root() nodefs.Node {
	return fs.root
}

func (fs *MemTreeFs) onMount() {
	for k, v := range fs.files {
		fs.addFile(k, v)
	}
	fs.files = nil
}

func (n *memNode) OnMount(c *nodefs.FileSystemConnector) {
	n.fs.onMount()
}

func (n *memNode) Print(indent int) {
	s := ""
	for i := 0; i < indent; i++ {
		s = s + " "
	}

	children := n.Inode().Children()
	for k, v := range children {
		if v.IsDir() {
			fmt.Println(s + k + ":")
			mn, ok := v.Node().(*memNode)
			if ok {
				mn.Print(indent + 2)
			}
		} else {
			fmt.Println(s + k)
		}
	}
}

func (n *memNode) OpenDir(context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	children := n.Inode().Children()
	stream = make([]fuse.DirEntry, 0, len(children))
	for k, v := range children {
		mode := fuse.S_IFREG | 0666
		if v.IsDir() {
			mode = fuse.S_IFDIR | 0777
		}
		stream = append(stream, fuse.DirEntry{
			Name: k,
			Mode: uint32(mode),
		})
	}
	return stream, fuse.OK
}

func (n *memNode) Open(flags uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}

	return nodefs.NewDataFile(n.file.Data()), fuse.OK
}

func (n *memNode) Deletable() bool {
	return false
}

func (n *memNode) GetAttr(out *fuse.Attr, file nodefs.File, context *fuse.Context) fuse.Status {
	if n.Inode().IsDir() {
		out.Mode = fuse.S_IFDIR | 0777
		return fuse.OK
	}
	n.file.Stat(out)
	return fuse.OK
}

func (n *MemTreeFs) addFile(name string, f MemFile) {
	comps := strings.Split(name, "/")

	node := n.root.Inode()
	for i, c := range comps {
		child := node.GetChild(c)
		if child == nil {
			fsnode := &memNode{
				Node: nodefs.NewDefaultNode(),
				fs:   n,
			}
			if i == len(comps)-1 {
				fsnode.file = f
			}

			child = node.NewChild(c, fsnode.file == nil, fsnode)
		}
		node = child
	}
}
