package mfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	dag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"
	ufspb "github.com/ipfs/go-ipfs/unixfs/pb"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

var ErrNotYetImplemented = errors.New("not yet implemented")
var ErrInvalidChild = errors.New("invalid child node")
var ErrDirExists = errors.New("directory already has entry by that name")

type Directory struct {
	dserv  dag.DAGService
	parent childCloser

	childDirs map[string]*Directory
	files     map[string]*File

	lock sync.Mutex
	ctx  context.Context

	dirbuilder *uio.Directory

	modTime time.Time

	name string
}

func NewDirectory(ctx context.Context, name string, node node.Node, parent childCloser, dserv dag.DAGService) (*Directory, error) {
	db, err := uio.NewDirectoryFromNode(dserv, node)
	if err != nil {
		return nil, err
	}

	return &Directory{
		dserv:      dserv,
		ctx:        ctx,
		name:       name,
		dirbuilder: db,
		parent:     parent,
		childDirs:  make(map[string]*Directory),
		files:      make(map[string]*File),
		modTime:    time.Now(),
	}, nil
}

// GetPrefix gets the CID prefix of the root node
func (d *Directory) GetPrefix() *cid.Prefix {
	return d.dirbuilder.GetPrefix()
}

// SetPrefix sets the CID prefix
func (d *Directory) SetPrefix(prefix *cid.Prefix) {
	d.dirbuilder.SetPrefix(prefix)
}

// closeChild updates the child by the given name to the dag node 'nd'
// and changes its own dag node
func (d *Directory) closeChild(name string, nd node.Node, sync bool) error {
	mynd, err := d.closeChildUpdate(name, nd, sync)
	if err != nil {
		return err
	}

	if sync {
		return d.parent.closeChild(d.name, mynd, true)
	}
	return nil
}

// closeChildUpdate is the portion of closeChild that needs to be locked around
func (d *Directory) closeChildUpdate(name string, nd node.Node, sync bool) (*dag.ProtoNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.updateChild(name, nd)
	if err != nil {
		return nil, err
	}

	if sync {
		return d.flushCurrentNode()
	}
	return nil, nil
}

func (d *Directory) flushCurrentNode() (*dag.ProtoNode, error) {
	nd, err := d.dirbuilder.GetNode()
	if err != nil {
		return nil, err
	}

	_, err = d.dserv.Add(nd)
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	return pbnd.Copy().(*dag.ProtoNode), nil
}

func (d *Directory) updateChild(name string, nd node.Node) error {
	err := d.dirbuilder.AddChild(d.ctx, name, nd)
	if err != nil {
		return err
	}

	d.modTime = time.Now()

	return nil
}

func (d *Directory) Type() NodeType {
	return TDir
}

// childNode returns a FSNode under this directory by the given name if it exists.
// it does *not* check the cached dirs and files
func (d *Directory) childNode(name string) (FSNode, error) {
	nd, err := d.childFromDag(name)
	if err != nil {
		return nil, err
	}

	return d.cacheNode(name, nd)
}

// cacheNode caches a node into d.childDirs or d.files and returns the FSNode.
func (d *Directory) cacheNode(name string, nd node.Node) (FSNode, error) {
	switch nd := nd.(type) {
	case *dag.ProtoNode:
		i, err := ft.FromBytes(nd.Data())
		if err != nil {
			return nil, err
		}

		switch i.GetType() {
		case ufspb.Data_Directory, ufspb.Data_HAMTShard:
			ndir, err := NewDirectory(d.ctx, name, nd, d, d.dserv)
			if err != nil {
				return nil, err
			}

			d.childDirs[name] = ndir
			return ndir, nil
		case ufspb.Data_File, ufspb.Data_Raw, ufspb.Data_Symlink:
			nfi, err := NewFile(name, nd, d, d.dserv)
			if err != nil {
				return nil, err
			}
			d.files[name] = nfi
			return nfi, nil
		case ufspb.Data_Metadata:
			return nil, ErrNotYetImplemented
		default:
			return nil, ErrInvalidChild
		}
	case *dag.RawNode:
		nfi, err := NewFile(name, nd, d, d.dserv)
		if err != nil {
			return nil, err
		}
		d.files[name] = nfi
		return nfi, nil
	default:
		return nil, fmt.Errorf("unrecognized node type in cache node")
	}
}

// Child returns the child of this directory by the given name
func (d *Directory) Child(name string) (FSNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.childUnsync(name)
}

func (d *Directory) Uncache(name string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.files, name)
	delete(d.childDirs, name)
}

// childFromDag searches through this directories dag node for a child link
// with the given name
func (d *Directory) childFromDag(name string) (node.Node, error) {
	return d.dirbuilder.Find(d.ctx, name)
}

// childUnsync returns the child under this directory by the given name
// without locking, useful for operations which already hold a lock
func (d *Directory) childUnsync(name string) (FSNode, error) {
	cdir, ok := d.childDirs[name]
	if ok {
		return cdir, nil
	}

	cfile, ok := d.files[name]
	if ok {
		return cfile, nil
	}

	return d.childNode(name)
}

type NodeListing struct {
	Name string
	Type int
	Size int64
	Hash string
}

func (d *Directory) ListNames(ctx context.Context) ([]string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var out []string
	err := d.dirbuilder.ForEachLink(ctx, func(l *node.Link) error {
		out = append(out, l.Name)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(out)

	return out, nil
}

func (d *Directory) List(ctx context.Context) ([]NodeListing, error) {
	var out []NodeListing
	err := d.ForEachEntry(ctx, func(nl NodeListing) error {
		out = append(out, nl)
		return nil
	})
	return out, err
}

func (d *Directory) ForEachEntry(ctx context.Context, f func(NodeListing) error) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.dirbuilder.ForEachLink(ctx, func(l *node.Link) error {
		c, err := d.childUnsync(l.Name)
		if err != nil {
			return err
		}

		nd, err := c.GetNode()
		if err != nil {
			return err
		}

		child := NodeListing{
			Name: l.Name,
			Type: int(c.Type()),
			Hash: nd.Cid().String(),
		}

		if c, ok := c.(*File); ok {
			size, err := c.Size()
			if err != nil {
				return err
			}
			child.Size = size
		}

		return f(child)
	})
}

func (d *Directory) Mkdir(name string) (*Directory, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	fsn, err := d.childUnsync(name)
	if err == nil {
		switch fsn := fsn.(type) {
		case *Directory:
			return fsn, os.ErrExist
		case *File:
			return nil, os.ErrExist
		default:
			return nil, fmt.Errorf("unrecognized type: %#v", fsn)
		}
	}

	ndir := ft.EmptyDirNode()
	ndir.SetPrefix(d.GetPrefix())

	_, err = d.dserv.Add(ndir)
	if err != nil {
		return nil, err
	}

	err = d.dirbuilder.AddChild(d.ctx, name, ndir)
	if err != nil {
		return nil, err
	}

	dirobj, err := NewDirectory(d.ctx, name, ndir, d, d.dserv)
	if err != nil {
		return nil, err
	}

	d.childDirs[name] = dirobj
	return dirobj, nil
}

func (d *Directory) Unlink(name string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.childDirs, name)
	delete(d.files, name)

	return d.dirbuilder.RemoveChild(d.ctx, name)
}

func (d *Directory) Flush() error {
	nd, err := d.GetNode()
	if err != nil {
		return err
	}

	return d.parent.closeChild(d.name, nd, true)
}

// AddChild adds the node 'nd' under this directory giving it the name 'name'
func (d *Directory) AddChild(name string, nd node.Node) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, err := d.childUnsync(name)
	if err == nil {
		return ErrDirExists
	}

	_, err = d.dserv.Add(nd)
	if err != nil {
		return err
	}

	err = d.dirbuilder.AddChild(d.ctx, name, nd)
	if err != nil {
		return err
	}

	d.modTime = time.Now()
	return nil
}

func (d *Directory) sync() error {
	for name, dir := range d.childDirs {
		nd, err := dir.GetNode()
		if err != nil {
			return err
		}

		err = d.updateChild(name, nd)
		if err != nil {
			return err
		}
	}

	for name, file := range d.files {
		nd, err := file.GetNode()
		if err != nil {
			return err
		}

		err = d.updateChild(name, nd)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Directory) Path() string {
	cur := d
	var out string
	for cur != nil {
		out = path.Join(cur.name, out)
		cur = cur.parent.(*Directory)
	}
	return out
}

func (d *Directory) GetNode() (node.Node, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.sync()
	if err != nil {
		return nil, err
	}

	nd, err := d.dirbuilder.GetNode()
	if err != nil {
		return nil, err
	}

	_, err = d.dserv.Add(nd)
	if err != nil {
		return nil, err
	}

	return nd.Copy(), err
}
