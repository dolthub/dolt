package io

import (
	"context"
	"fmt"
	"os"

	mdag "github.com/ipfs/go-ipfs/merkledag"
	format "github.com/ipfs/go-ipfs/unixfs"
	hamt "github.com/ipfs/go-ipfs/unixfs/hamt"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

// ShardSplitThreshold specifies how large of an unsharded directory
// the Directory code will generate. Adding entries over this value will
// result in the node being restructured into a sharded object.
var ShardSplitThreshold = 1000

// UseHAMTSharding is a global flag that signifies whether or not to use the
// HAMT sharding scheme for directory creation
var UseHAMTSharding = false

// DefaultShardWidth is the default value used for hamt sharding width.
var DefaultShardWidth = 256

type Directory struct {
	dserv   mdag.DAGService
	dirnode *mdag.ProtoNode

	shard *hamt.HamtShard
}

// NewDirectory returns a Directory. It needs a DAGService to add the Children
func NewDirectory(dserv mdag.DAGService) *Directory {
	db := new(Directory)
	db.dserv = dserv
	if UseHAMTSharding {
		s, err := hamt.NewHamtShard(dserv, DefaultShardWidth)
		if err != nil {
			panic(err) // will only panic if DefaultShardWidth is a bad value
		}
		db.shard = s
	} else {
		db.dirnode = format.EmptyDirNode()
	}
	return db
}

// ErrNotADir implies that the given node was not a unixfs directory
var ErrNotADir = fmt.Errorf("merkledag node was not a directory or shard")

func NewDirectoryFromNode(dserv mdag.DAGService, nd node.Node) (*Directory, error) {
	pbnd, ok := nd.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotADir
	}

	pbd, err := format.FromBytes(pbnd.Data())
	if err != nil {
		return nil, err
	}

	switch pbd.GetType() {
	case format.TDirectory:
		return &Directory{
			dserv:   dserv,
			dirnode: pbnd.Copy().(*mdag.ProtoNode),
		}, nil
	case format.THAMTShard:
		shard, err := hamt.NewHamtFromDag(dserv, nd)
		if err != nil {
			return nil, err
		}

		return &Directory{
			dserv: dserv,
			shard: shard,
		}, nil
	default:
		return nil, ErrNotADir
	}
}

// SetPrefix sets the prefix of the root node
func (d *Directory) SetPrefix(prefix *cid.Prefix) {
	if d.dirnode != nil {
		d.dirnode.SetPrefix(prefix)
	}
	if d.shard != nil {
		d.shard.SetPrefix(prefix)
	}
}

// AddChild adds a (name, key)-pair to the root node.
func (d *Directory) AddChild(ctx context.Context, name string, nd node.Node) error {
	if d.shard == nil {
		if !UseHAMTSharding {
			_ = d.dirnode.RemoveNodeLink(name)
			return d.dirnode.AddNodeLinkClean(name, nd)
		}

		err := d.switchToSharding(ctx)
		if err != nil {
			return err
		}
	}

	return d.shard.Set(ctx, name, nd)
}

func (d *Directory) switchToSharding(ctx context.Context) error {
	s, err := hamt.NewHamtShard(d.dserv, DefaultShardWidth)
	if err != nil {
		return err
	}
	s.SetPrefix(&d.dirnode.Prefix)

	d.shard = s
	for _, lnk := range d.dirnode.Links() {
		cnd, err := d.dserv.Get(ctx, lnk.Cid)
		if err != nil {
			return err
		}

		err = d.shard.Set(ctx, lnk.Name, cnd)
		if err != nil {
			return err
		}
	}

	d.dirnode = nil
	return nil
}

func (d *Directory) ForEachLink(ctx context.Context, f func(*node.Link) error) error {
	if d.shard == nil {
		for _, l := range d.dirnode.Links() {
			if err := f(l); err != nil {
				return err
			}
		}
		return nil
	}

	return d.shard.ForEachLink(ctx, f)
}

func (d *Directory) Links(ctx context.Context) ([]*node.Link, error) {
	if d.shard == nil {
		return d.dirnode.Links(), nil
	}

	return d.shard.EnumLinks(ctx)
}

func (d *Directory) Find(ctx context.Context, name string) (node.Node, error) {
	if d.shard == nil {
		lnk, err := d.dirnode.GetNodeLink(name)
		switch err {
		case mdag.ErrLinkNotFound:
			return nil, os.ErrNotExist
		default:
			return nil, err
		case nil:
		}

		return d.dserv.Get(ctx, lnk.Cid)
	}

	lnk, err := d.shard.Find(ctx, name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, d.dserv)
}

func (d *Directory) RemoveChild(ctx context.Context, name string) error {
	if d.shard == nil {
		return d.dirnode.RemoveNodeLink(name)
	}

	return d.shard.Remove(ctx, name)
}

// GetNode returns the root of this Directory
func (d *Directory) GetNode() (node.Node, error) {
	if d.shard == nil {
		return d.dirnode, nil
	}

	return d.shard.Node()
}

// GetPrefix returns the CID Prefix used
func (d *Directory) GetPrefix() *cid.Prefix {
	if d.shard == nil {
		return &d.dirnode.Prefix
	}

	return d.shard.Prefix()
}
