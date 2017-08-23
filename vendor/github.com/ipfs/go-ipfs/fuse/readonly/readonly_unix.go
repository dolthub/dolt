// +build linux darwin freebsd netbsd openbsd
// +build !nofuse

package readonly

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"

	core "github.com/ipfs/go-ipfs/core"
	mdag "github.com/ipfs/go-ipfs/merkledag"
	path "github.com/ipfs/go-ipfs/path"
	uio "github.com/ipfs/go-ipfs/unixfs/io"
	ftpb "github.com/ipfs/go-ipfs/unixfs/pb"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	lgbl "gx/ipfs/QmT4PgCNdv73hnFAqzHqwW44q7M9PWpykSswHDxndquZbc/go-libp2p-loggables"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	fuse "gx/ipfs/QmaFNtBAXX4nVMQWbUqNysXyhevUj1k4B1y5uS45LC7Vw9/fuse"
	fs "gx/ipfs/QmaFNtBAXX4nVMQWbUqNysXyhevUj1k4B1y5uS45LC7Vw9/fuse/fs"
)

var log = logging.Logger("fuse/ipfs")

// FileSystem is the readonly IPFS Fuse Filesystem.
type FileSystem struct {
	Ipfs *core.IpfsNode
}

// NewFileSystem constructs new fs using given core.IpfsNode instance.
func NewFileSystem(ipfs *core.IpfsNode) *FileSystem {
	return &FileSystem{Ipfs: ipfs}
}

// Root constructs the Root of the filesystem, a Root object.
func (f FileSystem) Root() (fs.Node, error) {
	return &Root{Ipfs: f.Ipfs}, nil
}

// Root is the root object of the filesystem tree.
type Root struct {
	Ipfs *core.IpfsNode
}

// Attr returns file attributes.
func (*Root) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0111 // -rw+x
	return nil
}

// Lookup performs a lookup under this node.
func (s *Root) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Debugf("Root Lookup: '%s'", name)
	switch name {
	case "mach_kernel", ".hidden", "._.":
		// Just quiet some log noise on OS X.
		return nil, fuse.ENOENT
	}

	nd, err := s.Ipfs.Resolver.ResolvePath(ctx, path.Path(name))
	if err != nil {
		// todo: make this error more versatile.
		return nil, fuse.ENOENT
	}

	pbnd, ok := nd.(*mdag.ProtoNode)
	if !ok {
		log.Error("fuse node was not a protobuf node")
		return nil, fuse.ENOTSUP
	}

	return &Node{Ipfs: s.Ipfs, Nd: pbnd}, nil
}

// ReadDirAll reads a particular directory. Disallowed for root.
func (*Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debug("read Root")
	return nil, fuse.EPERM
}

// Node is the core object representing a filesystem tree node.
type Node struct {
	Ipfs   *core.IpfsNode
	Nd     *mdag.ProtoNode
	cached *ftpb.Data
}

func (s *Node) loadData() error {
	s.cached = new(ftpb.Data)
	return proto.Unmarshal(s.Nd.Data(), s.cached)
}

// Attr returns the attributes of a given node.
func (s *Node) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Debug("Node attr")
	if s.cached == nil {
		if err := s.loadData(); err != nil {
			return fmt.Errorf("readonly: loadData() failed: %s", err)
		}
	}
	switch s.cached.GetType() {
	case ftpb.Data_Directory:
		a.Mode = os.ModeDir | 0555
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	case ftpb.Data_File:
		size := s.cached.GetFilesize()
		a.Mode = 0444
		a.Size = uint64(size)
		a.Blocks = uint64(len(s.Nd.Links()))
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	case ftpb.Data_Raw:
		a.Mode = 0444
		a.Size = uint64(len(s.cached.GetData()))
		a.Blocks = uint64(len(s.Nd.Links()))
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	case ftpb.Data_Symlink:
		a.Mode = 0777 | os.ModeSymlink
		a.Size = uint64(len(s.cached.GetData()))
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	default:
		return fmt.Errorf("Invalid data type - %s", s.cached.GetType())
	}
	return nil
}

// Lookup performs a lookup under this node.
func (s *Node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Debugf("Lookup '%s'", name)
	nodes, err := s.Ipfs.Resolver.ResolveLinks(ctx, s.Nd, []string{name})
	if err != nil {
		// todo: make this error more versatile.
		return nil, fuse.ENOENT
	}

	pbnd, ok := nodes[len(nodes)-1].(*mdag.ProtoNode)
	if !ok {
		log.Error("fuse lookup got non-protobuf node")
		return nil, fuse.ENOTSUP
	}

	return &Node{Ipfs: s.Ipfs, Nd: pbnd}, nil
}

// ReadDirAll reads the link structure as directory entries
func (s *Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debug("Node ReadDir")
	entries := make([]fuse.Dirent, len(s.Nd.Links()))
	for i, link := range s.Nd.Links() {
		n := link.Name
		if len(n) == 0 {
			n = link.Cid.String()
		}
		entries[i] = fuse.Dirent{Name: n, Type: fuse.DT_File}
	}

	if len(entries) > 0 {
		return entries, nil
	}
	return nil, fuse.ENOENT
}

func (s *Node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	if s.cached.GetType() != ftpb.Data_Symlink {
		return "", fuse.Errno(syscall.EINVAL)
	}
	return string(s.cached.GetData()), nil
}

func (s *Node) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	c := s.Nd.Cid()

	// setup our logging event
	lm := make(lgbl.DeferredMap)
	lm["fs"] = "ipfs"
	lm["key"] = func() interface{} { return c.String() }
	lm["req_offset"] = req.Offset
	lm["req_size"] = req.Size
	defer log.EventBegin(ctx, "fuseRead", lm).Done()

	r, err := uio.NewDagReader(ctx, s.Nd, s.Ipfs.DAG)
	if err != nil {
		return err
	}
	o, err := r.Seek(req.Offset, io.SeekStart)
	lm["res_offset"] = o
	if err != nil {
		return err
	}

	buf := resp.Data[:min(req.Size, int(int64(r.Size())-req.Offset))]
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = resp.Data[:n]
	lm["res_size"] = n
	return nil // may be non-nil / not succeeded
}

// to check that out Node implements all the interfaces we want
type roRoot interface {
	fs.Node
	fs.HandleReadDirAller
	fs.NodeStringLookuper
}

var _ roRoot = (*Root)(nil)

type roNode interface {
	fs.HandleReadDirAller
	fs.HandleReader
	fs.Node
	fs.NodeStringLookuper
	fs.NodeReadlinker
}

var _ roNode = (*Node)(nil)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
