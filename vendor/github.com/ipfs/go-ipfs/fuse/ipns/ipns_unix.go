// +build !nofuse

// package fuse/ipns implements a fuse filesystem that interfaces
// with ipns, the naming system for ipfs.
package ipns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	core "github.com/ipfs/go-ipfs/core"
	dag "github.com/ipfs/go-ipfs/merkledag"
	mfs "github.com/ipfs/go-ipfs/mfs"
	namesys "github.com/ipfs/go-ipfs/namesys"
	path "github.com/ipfs/go-ipfs/path"
	ft "github.com/ipfs/go-ipfs/unixfs"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	fuse "gx/ipfs/QmaFNtBAXX4nVMQWbUqNysXyhevUj1k4B1y5uS45LC7Vw9/fuse"
	fs "gx/ipfs/QmaFNtBAXX4nVMQWbUqNysXyhevUj1k4B1y5uS45LC7Vw9/fuse/fs"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

func init() {
	if os.Getenv("IPFS_FUSE_DEBUG") != "" {
		fuse.Debug = func(msg interface{}) {
			fmt.Println(msg)
		}
	}
}

var log = logging.Logger("fuse/ipns")

// FileSystem is the readwrite IPNS Fuse Filesystem.
type FileSystem struct {
	Ipfs     *core.IpfsNode
	RootNode *Root
}

// NewFileSystem constructs new fs using given core.IpfsNode instance.
func NewFileSystem(ipfs *core.IpfsNode, sk ci.PrivKey, ipfspath, ipnspath string) (*FileSystem, error) {

	kmap := map[string]ci.PrivKey{
		"local": sk,
	}
	root, err := CreateRoot(ipfs, kmap, ipfspath, ipnspath)
	if err != nil {
		return nil, err
	}

	return &FileSystem{Ipfs: ipfs, RootNode: root}, nil
}

// Root constructs the Root of the filesystem, a Root object.
func (f *FileSystem) Root() (fs.Node, error) {
	log.Debug("filesystem, get root")
	return f.RootNode, nil
}

func (f *FileSystem) Destroy() {
	err := f.RootNode.Close()
	if err != nil {
		log.Errorf("Error Shutting Down Filesystem: %s\n", err)
	}
}

// Root is the root object of the filesystem tree.
type Root struct {
	Ipfs *core.IpfsNode
	Keys map[string]ci.PrivKey

	// Used for symlinking into ipfs
	IpfsRoot  string
	IpnsRoot  string
	LocalDirs map[string]fs.Node
	Roots     map[string]*keyRoot

	LocalLinks map[string]*Link
}

func ipnsPubFunc(ipfs *core.IpfsNode, k ci.PrivKey) mfs.PubFunc {
	return func(ctx context.Context, c *cid.Cid) error {
		return ipfs.Namesys.Publish(ctx, k, path.FromCid(c))
	}
}

func loadRoot(ctx context.Context, rt *keyRoot, ipfs *core.IpfsNode, name string) (fs.Node, error) {
	p, err := path.ParsePath("/ipns/" + name)
	if err != nil {
		log.Errorf("mkpath %s: %s", name, err)
		return nil, err
	}

	node, err := core.Resolve(ctx, ipfs.Namesys, ipfs.Resolver, p)
	switch err {
	case nil:
	case namesys.ErrResolveFailed:
		node = ft.EmptyDirNode()
	default:
		log.Errorf("looking up %s: %s", p, err)
		return nil, err
	}

	pbnode, ok := node.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	root, err := mfs.NewRoot(ctx, ipfs.DAG, pbnode, ipnsPubFunc(ipfs, rt.k))
	if err != nil {
		return nil, err
	}

	rt.root = root

	switch val := root.GetValue().(type) {
	case *mfs.Directory:
		return &Directory{dir: val}, nil
	case *mfs.File:
		return &FileNode{fi: val}, nil
	default:
		return nil, errors.New("unrecognized type")
	}
}

type keyRoot struct {
	k     ci.PrivKey
	alias string
	root  *mfs.Root
}

func CreateRoot(ipfs *core.IpfsNode, keys map[string]ci.PrivKey, ipfspath, ipnspath string) (*Root, error) {
	ldirs := make(map[string]fs.Node)
	roots := make(map[string]*keyRoot)
	links := make(map[string]*Link)
	for alias, k := range keys {
		pid, err := peer.IDFromPrivateKey(k)
		if err != nil {
			return nil, err
		}
		name := pid.Pretty()

		kr := &keyRoot{k: k, alias: alias}
		fsn, err := loadRoot(ipfs.Context(), kr, ipfs, name)
		if err != nil {
			return nil, err
		}

		roots[name] = kr
		ldirs[name] = fsn

		// set up alias symlink
		links[alias] = &Link{
			Target: name,
		}
	}

	return &Root{
		Ipfs:       ipfs,
		IpfsRoot:   ipfspath,
		IpnsRoot:   ipnspath,
		Keys:       keys,
		LocalDirs:  ldirs,
		LocalLinks: links,
		Roots:      roots,
	}, nil
}

// Attr returns file attributes.
func (*Root) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Debug("Root Attr")
	a.Mode = os.ModeDir | 0111 // -rw+x
	return nil
}

// Lookup performs a lookup under this node.
func (s *Root) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "mach_kernel", ".hidden", "._.":
		// Just quiet some log noise on OS X.
		return nil, fuse.ENOENT
	}

	if lnk, ok := s.LocalLinks[name]; ok {
		return lnk, nil
	}

	nd, ok := s.LocalDirs[name]
	if ok {
		switch nd := nd.(type) {
		case *Directory:
			return nd, nil
		case *FileNode:
			return nd, nil
		default:
			return nil, fuse.EIO
		}
	}

	// other links go through ipns resolution and are symlinked into the ipfs mountpoint
	resolved, err := s.Ipfs.Namesys.Resolve(s.Ipfs.Context(), name)
	if err != nil {
		log.Warningf("ipns: namesys resolve error: %s", err)
		return nil, fuse.ENOENT
	}

	segments := resolved.Segments()
	if segments[0] == "ipfs" {
		p := path.Join(resolved.Segments()[1:])
		return &Link{s.IpfsRoot + "/" + p}, nil
	}

	log.Error("Invalid path.Path: ", resolved)
	return nil, errors.New("invalid path from ipns record")
}

func (r *Root) Close() error {
	for _, mr := range r.Roots {
		err := mr.root.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Forget is called when the filesystem is unmounted. probably.
// see comments here: http://godoc.org/bazil.org/fuse/fs#FSDestroyer
func (r *Root) Forget() {
	err := r.Close()
	if err != nil {
		log.Error(err)
	}
}

// ReadDirAll reads a particular directory. Will show locally available keys
// as well as a symlink to the peerID key
func (r *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debug("Root ReadDirAll")

	var listing []fuse.Dirent
	for alias, k := range r.Keys {
		pid, err := peer.IDFromPrivateKey(k)
		if err != nil {
			continue
		}
		ent := fuse.Dirent{
			Name: pid.Pretty(),
			Type: fuse.DT_Dir,
		}
		link := fuse.Dirent{
			Name: alias,
			Type: fuse.DT_Link,
		}
		listing = append(listing, ent, link)
	}
	return listing, nil
}

// Directory is wrapper over an mfs directory to satisfy the fuse fs interface
type Directory struct {
	dir *mfs.Directory
}

type FileNode struct {
	fi *mfs.File
}

// File is wrapper over an mfs file to satisfy the fuse fs interface
type File struct {
	fi mfs.FileDescriptor
}

// Attr returns the attributes of a given node.
func (d *Directory) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Debug("Directory Attr")
	a.Mode = os.ModeDir | 0555
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	return nil
}

// Attr returns the attributes of a given node.
func (fi *FileNode) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Debug("File Attr")
	size, err := fi.fi.Size()
	if err != nil {
		// In this case, the dag node in question may not be unixfs
		return fmt.Errorf("fuse/ipns: failed to get file.Size(): %s", err)
	}
	a.Mode = os.FileMode(0666)
	a.Size = uint64(size)
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	return nil
}

// Lookup performs a lookup under this node.
func (s *Directory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	child, err := s.dir.Child(name)
	if err != nil {
		// todo: make this error more versatile.
		return nil, fuse.ENOENT
	}

	switch child := child.(type) {
	case *mfs.Directory:
		return &Directory{dir: child}, nil
	case *mfs.File:
		return &FileNode{fi: child}, nil
	default:
		// NB: if this happens, we do not want to continue, unpredictable behaviour
		// may occur.
		panic("invalid type found under directory. programmer error.")
	}
}

// ReadDirAll reads the link structure as directory entries
func (dir *Directory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var entries []fuse.Dirent
	listing, err := dir.dir.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, entry := range listing {
		dirent := fuse.Dirent{Name: entry.Name}

		switch mfs.NodeType(entry.Type) {
		case mfs.TDir:
			dirent.Type = fuse.DT_Dir
		case mfs.TFile:
			dirent.Type = fuse.DT_File
		}

		entries = append(entries, dirent)
	}

	if len(entries) > 0 {
		return entries, nil
	}
	return nil, fuse.ENOENT
}

func (fi *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	_, err := fi.fi.Seek(req.Offset, io.SeekStart)
	if err != nil {
		return err
	}

	fisize, err := fi.fi.Size()
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	readsize := min(req.Size, int(fisize-req.Offset))
	n, err := fi.fi.CtxReadFull(ctx, resp.Data[:readsize])
	resp.Data = resp.Data[:n]
	return err
}

func (fi *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// TODO: at some point, ensure that WriteAt here respects the context
	wrote, err := fi.fi.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	resp.Size = wrote
	return nil
}

func (fi *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	errs := make(chan error, 1)
	go func() {
		errs <- fi.fi.Flush()
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fi *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		cursize, err := fi.fi.Size()
		if err != nil {
			return err
		}
		if cursize != int64(req.Size) {
			err := fi.fi.Truncate(int64(req.Size))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Fsync flushes the content in the file to disk, but does not
// update the dag tree internally
func (fi *FileNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	errs := make(chan error, 1)
	go func() {
		errs <- fi.fi.Sync()
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fi *File) Forget() {
	err := fi.fi.Sync()
	if err != nil {
		log.Debug("forget file error: ", err)
	}
}

func (dir *Directory) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	child, err := dir.dir.Mkdir(req.Name)
	if err != nil {
		return nil, err
	}

	return &Directory{dir: child}, nil
}

func (fi *FileNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var mfsflag int
	switch {
	case req.Flags.IsReadOnly():
		mfsflag = mfs.OpenReadOnly
	case req.Flags.IsWriteOnly():
		mfsflag = mfs.OpenWriteOnly
	case req.Flags.IsReadWrite():
		mfsflag = mfs.OpenReadWrite
	default:
		return nil, errors.New("unsupported flag type")
	}

	fd, err := fi.fi.Open(mfsflag, true)
	if err != nil {
		return nil, err
	}

	if req.Flags&fuse.OpenTruncate != 0 {
		if req.Flags.IsReadOnly() {
			log.Error("tried to open a readonly file with truncate")
			return nil, fuse.ENOTSUP
		}
		log.Info("Need to truncate file!")
		err := fd.Truncate(0)
		if err != nil {
			return nil, err
		}
	} else if req.Flags&fuse.OpenAppend != 0 {
		log.Info("Need to append to file!")
		if req.Flags.IsReadOnly() {
			log.Error("tried to open a readonly file with append")
			return nil, fuse.ENOTSUP
		}

		_, err := fd.Seek(0, io.SeekEnd)
		if err != nil {
			log.Error("seek reset failed: ", err)
			return nil, err
		}
	}

	return &File{fi: fd}, nil
}

func (fi *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fi.fi.Close()
}

func (dir *Directory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	// New 'empty' file
	nd := dag.NodeWithData(ft.FilePBData(nil, 0))
	err := dir.dir.AddChild(req.Name, nd)
	if err != nil {
		return nil, nil, err
	}

	child, err := dir.dir.Child(req.Name)
	if err != nil {
		return nil, nil, err
	}

	fi, ok := child.(*mfs.File)
	if !ok {
		return nil, nil, errors.New("child creation failed")
	}

	nodechild := &FileNode{fi: fi}

	var openflag int
	switch {
	case req.Flags.IsReadOnly():
		openflag = mfs.OpenReadOnly
	case req.Flags.IsWriteOnly():
		openflag = mfs.OpenWriteOnly
	case req.Flags.IsReadWrite():
		openflag = mfs.OpenReadWrite
	default:
		return nil, nil, errors.New("unsupported open mode")
	}

	fd, err := fi.Open(openflag, true)
	if err != nil {
		return nil, nil, err
	}

	return nodechild, &File{fi: fd}, nil
}

func (dir *Directory) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	err := dir.dir.Unlink(req.Name)
	if err != nil {
		return fuse.ENOENT
	}
	return nil
}

// Rename implements NodeRenamer
func (dir *Directory) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	cur, err := dir.dir.Child(req.OldName)
	if err != nil {
		return err
	}

	err = dir.dir.Unlink(req.OldName)
	if err != nil {
		return err
	}

	switch newDir := newDir.(type) {
	case *Directory:
		nd, err := cur.GetNode()
		if err != nil {
			return err
		}

		err = newDir.dir.AddChild(req.NewName, nd)
		if err != nil {
			return err
		}
	case *FileNode:
		log.Error("Cannot move node into a file!")
		return fuse.EPERM
	default:
		log.Error("Unknown node type for rename target dir!")
		return errors.New("Unknown fs node type!")
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// to check that out Node implements all the interfaces we want
type ipnsRoot interface {
	fs.Node
	fs.HandleReadDirAller
	fs.NodeStringLookuper
}

var _ ipnsRoot = (*Root)(nil)

type ipnsDirectory interface {
	fs.HandleReadDirAller
	fs.Node
	fs.NodeCreater
	fs.NodeMkdirer
	fs.NodeRemover
	fs.NodeRenamer
	fs.NodeStringLookuper
}

var _ ipnsDirectory = (*Directory)(nil)

type ipnsFile interface {
	fs.HandleFlusher
	fs.HandleReader
	fs.HandleWriter
	fs.HandleReleaser
}

type ipnsFileNode interface {
	fs.Node
	fs.NodeFsyncer
	fs.NodeOpener
}

var _ ipnsFileNode = (*FileNode)(nil)
var _ ipnsFile = (*File)(nil)
