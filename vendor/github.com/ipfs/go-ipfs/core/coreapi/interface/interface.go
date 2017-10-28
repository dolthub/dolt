package iface

import (
	"context"
	"errors"
	"io"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	ipld "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type Path interface {
	String() string
	Cid() *cid.Cid
	Root() *cid.Cid
	Resolved() bool
}

// TODO: should we really copy these?
//       if we didn't, godoc would generate nice links straight to go-ipld-format
type Node ipld.Node
type Link ipld.Link

type Reader interface {
	io.ReadSeeker
	io.Closer
}

type CoreAPI interface {
	Unixfs() UnixfsAPI
	ResolvePath(context.Context, Path) (Path, error)
	ResolveNode(context.Context, Path) (Node, error)
}

type UnixfsAPI interface {
	Add(context.Context, io.Reader) (Path, error)
	Cat(context.Context, Path) (Reader, error)
	Ls(context.Context, Path) ([]*Link, error)
}

// type ObjectAPI interface {
// 	New() (cid.Cid, Object)
// 	Get(string) (Object, error)
// 	Links(string) ([]*Link, error)
// 	Data(string) (Reader, error)
// 	Stat(string) (ObjectStat, error)
// 	Put(Object) (cid.Cid, error)
// 	SetData(string, Reader) (cid.Cid, error)
// 	AppendData(string, Data) (cid.Cid, error)
// 	AddLink(string, string, string) (cid.Cid, error)
// 	RmLink(string, string) (cid.Cid, error)
// }

// type ObjectStat struct {
// 	Cid            cid.Cid
// 	NumLinks       int
// 	BlockSize      int
// 	LinksSize      int
// 	DataSize       int
// 	CumulativeSize int
// }

var ErrIsDir = errors.New("object is a directory")
var ErrOffline = errors.New("can't resolve, ipfs node is offline")
