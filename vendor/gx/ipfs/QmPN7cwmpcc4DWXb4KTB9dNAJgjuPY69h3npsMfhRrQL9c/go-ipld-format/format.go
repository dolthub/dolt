package format

import (
	"context"
	"fmt"

	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
)

type Resolver interface {
	// Resolve resolves a path through this node, stopping at any link boundary
	// and returning the object found as well as the remaining path to traverse
	Resolve(path []string) (interface{}, []string, error)

	// Tree lists all paths within the object under 'path', and up to the given depth.
	// To list the entire object (similar to `find .`) pass "" and -1
	Tree(path string, depth int) []string
}

type Node interface {
	blocks.Block
	Resolver

	// ResolveLink is a helper function that calls resolve and asserts the
	// output is a link
	ResolveLink(path []string) (*Link, []string, error)

	// Copy returns a deep copy of this node
	Copy() Node

	// Links is a helper function that returns all links within this object
	Links() []*Link

	// TODO: not sure if stat deserves to stay
	Stat() (*NodeStat, error)

	// Size returns the size in bytes of the serialized object
	Size() (uint64, error)
}

type NodeGetter interface {
	Get(context.Context, *cid.Cid) (Node, error)
}

// Link represents an IPFS Merkle DAG Link between Nodes.
type Link struct {
	// utf string name. should be unique per object
	Name string // utf8

	// cumulative size of target object
	Size uint64

	// multihash of the target object
	Cid *cid.Cid
}

// NodeStat is a statistics object for a Node. Mostly sizes.
type NodeStat struct {
	Hash           string
	NumLinks       int // number of links in link table
	BlockSize      int // size of the raw, encoded data
	LinksSize      int // size of the links segment
	DataSize       int // size of the data segment
	CumulativeSize int // cumulative size of object and its references
}

func (ns NodeStat) String() string {
	f := "NodeStat{NumLinks: %d, BlockSize: %d, LinksSize: %d, DataSize: %d, CumulativeSize: %d}"
	return fmt.Sprintf(f, ns.NumLinks, ns.BlockSize, ns.LinksSize, ns.DataSize, ns.CumulativeSize)
}

// MakeLink creates a link to the given node
func MakeLink(n Node) (*Link, error) {
	s, err := n.Size()
	if err != nil {
		return nil, err
	}

	return &Link{
		Size: s,
		Cid:  n.Cid(),
	}, nil
}

// GetNode returns the MDAG Node that this link points to
func (l *Link) GetNode(ctx context.Context, serv NodeGetter) (Node, error) {
	return serv.Get(ctx, l.Cid)
}
