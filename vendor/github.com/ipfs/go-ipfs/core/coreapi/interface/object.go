package iface

import (
	"context"
	"io"

	options "github.com/ipfs/go-ipfs/core/coreapi/interface/options"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
)

// ObjectStat provides information about dag nodes
type ObjectStat struct {
	// Cid is the CID of the node
	Cid cid.Cid

	// NumLinks is number of links the node contains
	NumLinks int

	// BlockSize is size of the raw serialized node
	BlockSize int

	// LinksSize is size of the links block section
	LinksSize int

	// DataSize is the size of data block section
	DataSize int

	// CumulativeSize is size of the tree (BlockSize + link sizes)
	CumulativeSize int
}

// ChangeType denotes type of change in ObjectChange
type ChangeType int

const (
	// DiffAdd is set when a link was added to the graph
	DiffAdd ChangeType = iota

	// DiffRemove is set when a link was removed from the graph
	DiffRemove

	// DiffMod is set when a link was changed in the graph
	DiffMod
)

// ObjectChange represents a change ia a graph
type ObjectChange struct {
	// Type of the change, either:
	// * DiffAdd - Added a link
	// * DiffRemove - Removed a link
	// * DiffMod - Modified a link
	Type ChangeType

	// Path to the changed link
	Path string

	// Before holds the link path before the change. Note that when a link is
	// added, this will be nil.
	Before ResolvedPath

	// After holds the link path after the change. Note that when a link is
	// removed, this will be nil.
	After ResolvedPath
}

// ObjectAPI specifies the interface to MerkleDAG and contains useful utilities
// for manipulating MerkleDAG data structures.
type ObjectAPI interface {
	// New creates new, empty (by default) dag-node.
	New(context.Context, ...options.ObjectNewOption) (ipld.Node, error)

	// Put imports the data into merkledag
	Put(context.Context, io.Reader, ...options.ObjectPutOption) (ResolvedPath, error)

	// Get returns the node for the path
	Get(context.Context, Path) (ipld.Node, error)

	// Data returns reader for data of the node
	Data(context.Context, Path) (io.Reader, error)

	// Links returns lint or links the node contains
	Links(context.Context, Path) ([]*ipld.Link, error)

	// Stat returns information about the node
	Stat(context.Context, Path) (*ObjectStat, error)

	// AddLink adds a link under the specified path. child path can point to a
	// subdirectory within the patent which must be present (can be overridden
	// with WithCreate option).
	AddLink(ctx context.Context, base Path, name string, child Path, opts ...options.ObjectAddLinkOption) (ResolvedPath, error)

	// RmLink removes a link from the node
	RmLink(ctx context.Context, base Path, link string) (ResolvedPath, error)

	// AppendData appends data to the node
	AppendData(context.Context, Path, io.Reader) (ResolvedPath, error)

	// SetData sets the data contained in the node
	SetData(context.Context, Path, io.Reader) (ResolvedPath, error)

	// Diff returns a set of changes needed to transform the first object into the
	// second.
	Diff(context.Context, Path, Path) ([]ObjectChange, error)
}
