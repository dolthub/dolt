// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

// NomsFS
//
// This is an implementation of a FUSE filesystem on top of Noms. The hierarchy is arranged with the following basic types:
//
// Filesystem {
//	root Inode
// }
//
// Inode {
//	attr Attr {
//		ctime Number
//		gid Number
//		mtime Number
//		mode Number
//		uid Number
//		xattr Map<String, Blob>
//	}
//	contents File | Symlink | Directory
// }
//
// File {
//	data Ref<Blob>
// }
//
// Symlink {
//	targetPath String
// }
//
// Directory {
//	entries Map<String, Inode>
// }
//
// While we don't currently support hard links, this could be achieved by storing a list of parents rather than a single parent *and* processing all metadata every time we mount a dataset to identify commonalities. Hard links are stupid anyways.
// XXX TODO: If the head gets out of sync it actually shouldn't be a problem to resync and try a transaction again (though we may need to redo some of the error checking that FUSE has done for us). The place where it may be problematic is around writes to open files where we rely on the in-core parent map. If we get out of sync we will need to invalidate that map in part or whole. We can try to be smart about it, but in certain circumstances (e.g. if an open file has moved) then there's not much we can do other than return fuse.EBADF (or something). We can add a valid bit to the nNode structure so that open files will be able to tell.
// XXX TODO: The map of nodes really only needs entries that pertain to open files and their paths. That structure should likely be refcounted; when a nNode goes to 0 it can be removed from the map. This would also help with re-syncing since fixing up the smaller map would be faster.
//

type nomsFile struct {
	nodefs.File

	fs   *nomsFS
	node *nNode
}

type nomsFS struct {
	pathfs.FileSystem

	mdLock *sync.Mutex // protect filesystem metadata

	db   datas.Database
	ds   datas.Dataset
	head types.Struct

	// This map lets us find the name of a file and its parent given an inode. This lets us splice changes back into the hierarchy upon modification.
	nodes map[hash.Hash]*nNode
}

// This represents a node in the filesystem hierarchy. The key will match the hash for the inode unless there is cached, yet-to-be-flushed data.
type nNode struct {
	nLock  *sync.Mutex
	parent *nNode
	name   string
	key    hash.Hash
	inode  types.Struct
}

type mount func(fs pathfs.FileSystem)

var fsType, inodeType, attrType, directoryType, fileType, symlinkType *types.Type

func init() {
	inodeType = types.MakeStructType("Inode", []string{"attr", "contents"}, []*types.Type{
		types.MakeStructType("Attr", []string{"ctime", "gid", "mode", "mtime", "uid", "xattr"}, []*types.Type{types.NumberType, types.NumberType, types.NumberType, types.NumberType, types.NumberType, types.MakeMapType(types.StringType, types.BlobType)}),
		types.MakeUnionType(types.MakeStructType("Directory", []string{"entries"}, []*types.Type{
			types.MakeMapType(types.StringType, types.MakeCycleType(1))}),
			types.MakeStructType("File", []string{"data"}, []*types.Type{types.MakeRefType(types.BlobType)}),
			types.MakeStructType("Symlink", []string{"targetPath"}, []*types.Type{types.StringType}),
		),
	})

	// Root around for some useful types.
	attrType = inodeType.Desc.(types.StructDesc).Field("attr")
	for _, elemType := range inodeType.Desc.(types.StructDesc).Field("contents").Desc.(types.CompoundDesc).ElemTypes {
		switch elemType.Desc.(types.StructDesc).Name {
		case "Directory":
			directoryType = elemType
		case "File":
			fileType = elemType
		case "Symlink":
			symlinkType = elemType
		}
	}

	fsType = types.MakeStructType("Filesystem", []string{"root"}, []*types.Type{inodeType})
}

func start(dataset string, mount mount) {
	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(dataset)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}

	hv, ok := ds.MaybeHeadValue()
	if ok {
		if !types.IsSubtype(fsType, hv.Type()) {
			fmt.Fprintf(os.Stderr, "Invalid dataset head: expected type '%s' but found type '%s'\n", fsType.Desc.(types.StructDesc).Name, hv.Type().Desc.(types.StructDesc).Name)
			return
		}
	} else {
		rootAttr := makeAttr(0777) // create the root directory with maximally permissive permissions
		rootDir := types.NewStructWithType(directoryType, types.ValueSlice{types.NewMap()})
		rootInode := types.NewStructWithType(inodeType, types.ValueSlice{rootAttr, rootDir})
		hv = types.NewStructWithType(fsType, types.ValueSlice{rootInode})
	}

	mount(&nomsFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		db:         db,
		ds:         ds,
		head:       hv.(types.Struct),
		mdLock:     &sync.Mutex{},
		nodes:      make(map[hash.Hash]*nNode),
	})
}

var debug bool

func main() {
	flag.BoolVar(&debug, "d", false, "debug")
	flag.Parse()
	if len(flag.Args()) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s dataset mount_point\n", path.Base(os.Args[0]))
		return
	}

	start(flag.Arg(0), func(fs pathfs.FileSystem) {
		nfs := pathfs.NewPathNodeFs(fs, nil)

		server, _, err := nodefs.MountRoot(flag.Arg(1), nfs.Root(), &nodefs.Options{Debug: debug})
		if err != nil {
			fmt.Println("Mount failed; attempting unmount")
			syscall.Unmount(flag.Arg(1), 0)
			server, _, err = nodefs.MountRoot(flag.Arg(1), nfs.Root(), &nodefs.Options{Debug: debug})
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Mount failed: %s\n", err)
			return
		}

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT)
		go func() {
			<-sig
			fmt.Println("unmounting...")
			server.Unmount()
			// Ignore any subsequent ^C
			signal.Reset(syscall.SIGINT)
		}()

		fmt.Println("running...")
		server.Serve()
		fmt.Println("done.")
	})
}

func (fs *nomsFS) StatFs(path string) *fuse.StatfsOut {
	// We'll pretend this is a 4PB device that could hold a billion files, a truthful hyperbole.
	return &fuse.StatfsOut{
		Bsize:  4096,
		Blocks: 1 << 40,
		Bfree:  1 << 40,
		Bavail: 1 << 40,
		Files:  1 << 30,
		Ffree:  1 << 30,
	}
}

func (fs *nomsFS) OpenDir(path string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return nil, code
	}

	inode := np.inode

	if nodeType(inode) != "Directory" {
		return nil, fuse.ENOTDIR
	}

	entries := inode.Get("contents").(types.Struct).Get("entries").(types.Map)

	c := make([]fuse.DirEntry, 0, entries.Len())

	entries.IterAll(func(k, v types.Value) {
		c = append(c, fuse.DirEntry{
			Name: string(k.(types.String)),
		})
	})

	return c, fuse.OK
}

func (fs *nomsFS) Open(path string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return nil, code
	}

	nfile := nomsFile{
		File: nodefs.NewDefaultFile(),

		fs:   fs,
		node: np,
	}

	return nfile, fuse.OK
}

func (fs *nomsFS) Truncate(path string, size uint64, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return code
	}

	np.nLock.Lock()
	defer np.nLock.Unlock()

	inode := np.inode
	attr := inode.Get("attr").(types.Struct)
	file := inode.Get("contents").(types.Struct)
	ref := file.Get("data").(types.Ref)
	blob := ref.TargetValue(fs.db).(types.Blob)

	blob = blob.Splice(size, blob.Len()-size, nil)
	ref = fs.db.WriteValue(blob)
	file = file.Set("data", ref)

	inode = inode.Set("contents", file).Set("attr", updateMtime(attr))
	fs.updateNode(np, inode)
	fs.splice(np)
	fs.commit()

	return fuse.OK
}

func (fs *nomsFS) Create(path string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.createCommon(path, mode, func() types.Value {
		blob := types.NewEmptyBlob()
		return types.NewStructWithType(fileType, types.ValueSlice{fs.ds.Database().WriteValue(blob)})
	})
	if code != fuse.OK {
		return nil, code
	}

	nfile := nomsFile{
		File: nodefs.NewDefaultFile(),

		fs:   fs,
		node: np,
	}
	return nfile, fuse.OK
}

func (fs *nomsFS) Mkdir(path string, mode uint32, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	_, code := fs.createCommon(path, mode, func() types.Value {
		return types.NewStructWithType(directoryType, types.ValueSlice{types.NewMap()})
	})

	return code
}

func (fs *nomsFS) Symlink(targetPath string, path string, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	_, code := fs.createCommon(path, 0755, func() types.Value {
		return types.NewStructWithType(symlinkType, types.ValueSlice{types.String(targetPath)})
	})

	return code
}

func (fs *nomsFS) createCommon(path string, mode uint32, createContents func() types.Value) (*nNode, fuse.Status) {
	components := strings.Split(path, "/")

	fname := components[len(components)-1]
	components = components[:len(components)-1]

	// Grab the spot in the hierarchy where the new node will go.
	parent, code := fs.getPathComponents(components)
	if code != fuse.OK {
		return nil, code
	}

	if nodeType(parent.inode) != "Directory" {
		return nil, fuse.ENOTDIR
	}

	// Create the new node.
	inode := types.NewStructWithType(inodeType, types.ValueSlice{makeAttr(mode), createContents()})

	np := fs.getNode(inode, fname, parent)

	// Insert the new node into the hierarchy.
	fs.splice(np)
	fs.commit()

	return np, fuse.OK
}

func (fs *nomsFS) Readlink(path string, context *fuse.Context) (string, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return "", code
	}

	inode := np.inode
	d.Chk.Equal(nodeType(inode), "Symlink")
	link := inode.Get("contents")

	return string(link.(types.Struct).Get("targetPath").(types.String)), fuse.OK
}

func (fs *nomsFS) Unlink(path string, context *fuse.Context) fuse.Status {

	// Since we don't support hard links we don't need to worry about checking the link count.

	return fs.removeCommon(path, func(inode types.Value) {
		d.Chk.NotEqual(nodeType(inode), "Directory")
	})
}

func (fs *nomsFS) Rmdir(path string, context *fuse.Context) (code fuse.Status) {
	return fs.removeCommon(path, func(inode types.Value) {
		d.Chk.Equal(nodeType(inode), "Directory")
	})
}

func (fs *nomsFS) removeCommon(path string, typeCheck func(inode types.Value)) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return code
	}

	typeCheck(np.inode)

	parent := np.parent

	dir := parent.inode.Get("contents").(types.Struct)
	entries := dir.Get("entries").(types.Map)

	entries = entries.Remove(types.String(np.name))
	dir = dir.Set("entries", entries)

	fs.deleteNode(np)

	fs.updateNode(parent, parent.inode.Set("contents", dir))
	fs.splice(parent)
	fs.commit()

	return fuse.OK
}

func (nfile nomsFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	nfile.node.nLock.Lock()
	defer nfile.node.nLock.Unlock()

	file := nfile.node.inode.Get("contents")

	d.Chk.Equal(nodeType(nfile.node.inode), "File")

	ref := file.(types.Struct).Get("data").(types.Ref)
	blob := ref.TargetValue(nfile.fs.db).(types.Blob)

	br := blob.Reader()

	_, err := br.Seek(off, 0)
	if err != nil {
		return nil, fuse.EIO
	}
	n, err := br.Read(dest)
	if err != nil {
		return fuse.ReadResultData(dest[:n]), fuse.EIO
	}

	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (nfile nomsFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	nfile.node.nLock.Lock()
	defer nfile.node.nLock.Unlock()

	inode := nfile.node.inode
	d.Chk.Equal(nodeType(inode), "File")

	attr := inode.Get("attr").(types.Struct)
	file := inode.Get("contents").(types.Struct)
	ref := file.Get("data").(types.Ref)
	blob := ref.TargetValue(nfile.fs.db).(types.Blob)

	ll := uint64(blob.Len())
	oo := uint64(off)
	d.PanicIfFalse(ll >= oo)
	del := uint64(len(data))
	if ll-oo < del {
		del = ll - oo
	}

	blob = blob.Splice(uint64(off), del, data)
	ref = nfile.fs.db.WriteValue(blob)
	file = file.Set("data", ref)

	nfile.fs.bufferNode(nfile.node, inode.Set("contents", file).Set("attr", updateMtime(attr)))

	return uint32(len(data)), fuse.OK
}

func (nfile nomsFile) Flush() fuse.Status {
	nfile.fs.mdLock.Lock()
	nfile.node.nLock.Lock()
	defer nfile.fs.mdLock.Unlock()
	defer nfile.node.nLock.Unlock()

	np := nfile.fs.nodes[nfile.node.key]
	if np == nfile.node {
		nfile.fs.commitNode(nfile.node)
		nfile.fs.splice(nfile.node)
		nfile.fs.commit()
	}

	return fuse.OK
}

func makeAttr(mode uint32) types.Struct {
	now := time.Now()
	ctime := types.Number(float64(now.Unix()) + float64(now.Nanosecond())/1000000000)
	mtime := ctime

	user := fuse.CurrentOwner()
	gid := types.Number(float64(user.Gid))
	uid := types.Number(float64(user.Uid))

	return types.NewStructWithType(attrType, types.ValueSlice{ctime, gid, types.Number(mode), mtime, uid, types.NewMap()})
}

func updateMtime(attr types.Struct) types.Struct {
	now := time.Now()
	mtime := types.Number(float64(now.Unix()) + float64(now.Nanosecond())/1000000000)

	return attr.Set("mtime", mtime)
}

func nodeType(inode types.Value) string {
	return inode.(types.Struct).Get("contents").Type().Desc.(types.StructDesc).Name
}

func (fs *nomsFS) getNode(inode types.Struct, name string, parent *nNode) *nNode {
	// The parent has to be a directory.
	if parent != nil {
		d.Chk.Equal("Directory", nodeType(parent.inode))
	}

	np, ok := fs.nodes[inode.Hash()]
	if ok {
		d.Chk.Equal(np.parent, parent)
		d.Chk.Equal(np.name, name)
	} else {
		np = &nNode{
			nLock:  &sync.Mutex{},
			parent: parent,
			name:   name,
			key:    inode.Hash(),
			inode:  inode,
		}
		fs.nodes[np.key] = np
	}
	return np
}

func (fs *nomsFS) updateNode(np *nNode, inode types.Struct) {
	delete(fs.nodes, np.key)
	np.inode = inode
	np.key = inode.Hash()
	fs.nodes[np.key] = np
}

func (fs *nomsFS) bufferNode(np *nNode, inode types.Struct) {
	np.inode = inode
}

func (fs *nomsFS) commitNode(np *nNode) {
	fs.updateNode(np, np.inode)
}

func (fs *nomsFS) deleteNode(np *nNode) {
	delete(fs.nodes, np.inode.Hash())
}

// Rewrite the hierarchy starting frpm np and walking back to the root.
func (fs *nomsFS) splice(np *nNode) {
	for np.parent != nil {
		dir := np.parent.inode.Get("contents").(types.Struct)
		entries := dir.Get("entries").(types.Map)

		entries = entries.Set(types.String(np.name), np.inode)
		dir = dir.Set("entries", entries)

		fs.updateNode(np.parent, np.parent.inode.Set("contents", dir))

		np = np.parent
	}

	fs.head = fs.head.Set("root", np.inode)
}

func (fs *nomsFS) commit() {
	ds, ee := fs.db.CommitValue(fs.ds, fs.head)
	if ee != nil {
		panic("Unexpected changes to dataset (is it mounted in multiple locations?")
	}
	fs.ds = ds
}

func (fs *nomsFS) getPath(path string) (*nNode, fuse.Status) {
	if path == "" {
		return fs.getPathComponents([]string{})
	}
	return fs.getPathComponents(strings.Split(path, "/"))
}

func (fs *nomsFS) getPathComponents(components []string) (*nNode, fuse.Status) {
	inode := fs.head.Get("root").(types.Struct)
	np := fs.getNode(inode, "", nil)

	for _, component := range components {
		d.Chk.NotEqual(component, "")

		contents := inode.Get("contents")
		if contents.Type().Desc.(types.StructDesc).Name != "Directory" {
			return nil, fuse.ENOTDIR
		}

		v, ok := contents.(types.Struct).Get("entries").(types.Map).
			MaybeGet(types.String(component))
		if !ok {
			return nil, fuse.ENOENT
		}
		inode = v.(types.Struct)
		np = fs.getNode(inode, component, np)
	}

	return np, fuse.OK
}

func (fs *nomsFS) Rename(oldPath string, newPath string, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	// We find the node, new parent, and node representing the shared point in the hierarchy in order to then minimize repeated work when splicing the hierarchy back together below.
	np, nparent, nshared, fname, code := fs.getPaths(oldPath, newPath)
	if code != fuse.OK {
		return code
	}

	// Remove the node from the old spot in the hierarchy.
	oparent := np.parent

	dir := oparent.inode.Get("contents").(types.Struct)
	entries := dir.Get("entries").(types.Map)

	entries = entries.Remove(types.String(np.name))
	dir = dir.Set("entries", entries)

	fs.updateNode(oparent, oparent.inode.Set("contents", dir))

	// Insert it into the new spot in the hierarchy
	np.parent = nparent
	np.name = fname
	fs.splices(oparent, np, nshared)

	fs.commit()

	return fuse.OK
}

func (fs *nomsFS) getPaths(oldPath string, newPath string) (oldNode *nNode, newParent *nNode, sharedNode *nNode, newName string, code fuse.Status) {
	ocomp := strings.Split(oldPath, "/")
	ncomp := strings.Split(newPath, "/")
	newName = ncomp[len(ncomp)-1]
	ncomp = ncomp[:len(ncomp)-1]

	inode := fs.head.Get("root").(types.Struct)
	sharedNode = fs.getNode(inode, "", nil)

	var i int
	var component string
	for i, component = range ocomp {
		if i >= len(ncomp) || component != ncomp[i] {
			break
		}

		contents := inode.Get("contents")
		if contents.Type().Desc.(types.StructDesc).Name != "Directory" {
			return nil, nil, nil, "", fuse.ENOTDIR
		}

		v, ok := contents.(types.Struct).Get("entries").(types.Map).
			MaybeGet(types.String(component))
		if !ok {
			return nil, nil, nil, "", fuse.ENOENT
		}

		inode = v.(types.Struct)
		sharedNode = fs.getNode(inode, component, sharedNode)
	}

	pinode := inode
	oldNode = sharedNode
	for _, component := range ocomp[i:] {
		contents := inode.Get("contents")
		if contents.Type().Desc.(types.StructDesc).Name != "Directory" {
			return nil, nil, nil, "", fuse.ENOTDIR
		}

		v, ok := contents.(types.Struct).Get("entries").(types.Map).
			MaybeGet(types.String(component))
		if !ok {
			return nil, nil, nil, "", fuse.ENOENT
		}

		inode = v.(types.Struct)
		oldNode = fs.getNode(inode, component, oldNode)
	}

	inode = pinode
	newParent = sharedNode
	for _, component := range ncomp[i:] {
		contents := inode.Get("contents")
		if contents.Type().Desc.(types.StructDesc).Name != "Directory" {
			return nil, nil, nil, "", fuse.ENOTDIR
		}

		v, ok := contents.(types.Struct).Get("entries").(types.Map).
			MaybeGet(types.String(component))
		if !ok {
			return nil, nil, nil, "", fuse.ENOENT
		}

		inode = v.(types.Struct)
		newParent = fs.getNode(inode, component, newParent)
	}

	code = fuse.OK
	return
}

func (fs *nomsFS) splices(np1, np2, npShared *nNode) {
	// Splice each until we get to the shared parent directory.
	for _, np := range []*nNode{np1, np2} {
		for np != npShared {
			dir := np.parent.inode.Get("contents").(types.Struct)
			entries := dir.Get("entries").(types.Map)

			entries = entries.Set(types.String(np.name), np.inode)
			dir = dir.Set("entries", entries)

			fs.updateNode(np.parent, np.parent.inode.Set("contents", dir))

			np = np.parent
		}
	}

	// Splice the shared parent.
	fs.splice(npShared)
}

func (fs *nomsFS) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return nil, code
	}

	inode := np.inode
	attr := inode.Get("attr").(types.Struct)
	contents := inode.Get("contents").(types.Struct)

	mode := uint32(float64(attr.Get("mode").(types.Number)))
	ctime := float64(attr.Get("ctime").(types.Number))
	gid := float64(attr.Get("gid").(types.Number))
	mtime := float64(attr.Get("mtime").(types.Number))
	uid := float64(attr.Get("uid").(types.Number))

	at := &fuse.Attr{
		Mode:      mode,
		Mtime:     uint64(mtime),
		Mtimensec: uint32(math.Floor(mtime) * 1000000000),
		Ctime:     uint64(ctime),
		Ctimensec: uint32(math.Floor(ctime) * 1000000000),
	}

	at.Owner.Gid = uint32(gid)
	at.Owner.Uid = uint32(uid)

	switch contents.Type().Desc.(types.StructDesc).Name {
	case "File":
		blob := contents.Get("data").(types.Ref).TargetValue(fs.db).(types.Blob)
		at.Mode |= fuse.S_IFREG
		at.Size = blob.Len()
	case "Directory":
		at.Mode |= fuse.S_IFDIR
		at.Size = contents.Get("entries").(types.Map).Len()
	case "Symlink":
		at.Mode |= fuse.S_IFLNK
	}

	return at, fuse.OK
}

func (fs *nomsFS) Chown(path string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	return fs.setAttr(path, func(attr types.Struct) types.Struct {
		return attr.Set("uid", types.Number(uid)).Set("gid", types.Number(gid))
	})
}

func (fs *nomsFS) Utimens(path string, atime *time.Time, mtime *time.Time, context *fuse.Context) fuse.Status {
	if mtime == nil {
		return fuse.OK
	}
	return fs.setAttr(path, func(attr types.Struct) types.Struct {
		return attr.Set("mtime", types.Number(float64(mtime.Unix())+float64(mtime.Nanosecond())/1000000000))
	})
}

func (fs *nomsFS) Chmod(path string, mode uint32, context *fuse.Context) fuse.Status {
	return fs.setAttr(path, func(attr types.Struct) types.Struct {
		return attr.Set("mode", types.Number(mode))
	})
}

func (fs *nomsFS) setAttr(path string, updateAttr func(attr types.Struct) types.Struct) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return code
	}

	inode := np.inode
	attr := inode.Get("attr").(types.Struct)
	attr = updateAttr(attr)
	inode = inode.Set("attr", attr)

	fs.updateNode(np, inode)
	fs.splice(np)
	fs.commit()

	return fuse.OK
}

func (fs *nomsFS) GetXAttr(path string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return nil, code
	}

	xattr := np.inode.Get("attr").(types.Struct).Get("xattr").(types.Map)

	v, found := xattr.MaybeGet(types.String(attribute))
	if !found {
		if runtime.GOOS == "darwin" {
			return nil, fuse.Status(93) // syscall.ENOATTR
		}
		return nil, fuse.ENODATA
	}

	blob := v.(types.Blob)

	data := make([]byte, blob.Len())
	blob.Reader().Read(data)

	return data, fuse.OK
}

func (fs *nomsFS) ListXAttr(path string, context *fuse.Context) ([]string, fuse.Status) {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return nil, code
	}

	xattr := np.inode.Get("attr").(types.Struct).Get("xattr").(types.Map)

	keys := make([]string, 0, xattr.Len())
	xattr.IterAll(func(key, value types.Value) {
		keys = append(keys, string(key.(types.String)))
	})

	return keys, fuse.OK
}

func (fs *nomsFS) RemoveXAttr(path string, key string, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return code
	}

	inode := np.inode
	attr := np.inode.Get("attr").(types.Struct)
	xattr := attr.Get("xattr").(types.Map)

	xattr = xattr.Remove(types.String(key))
	attr = attr.Set("xattr", xattr)
	inode = inode.Set("attr", attr)

	fs.updateNode(np, inode)
	fs.splice(np)
	fs.commit()

	return fuse.OK
}

func (fs *nomsFS) SetXAttr(path string, key string, data []byte, flags int, context *fuse.Context) fuse.Status {
	fs.mdLock.Lock()
	defer fs.mdLock.Unlock()
	np, code := fs.getPath(path)
	if code != fuse.OK {
		return code
	}

	inode := np.inode
	attr := np.inode.Get("attr").(types.Struct)
	xattr := attr.Get("xattr").(types.Map)
	blob := types.NewBlob(bytes.NewReader(data))

	xattr = xattr.Set(types.String(key), blob)
	attr = attr.Set("xattr", xattr)
	inode = inode.Set("attr", attr)

	fs.updateNode(np, inode)
	fs.splice(np)
	fs.commit()

	return fuse.OK
}
