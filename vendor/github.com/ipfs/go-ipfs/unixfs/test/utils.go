package testu

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/ipfs/go-ipfs/importer/chunk"
	h "github.com/ipfs/go-ipfs/importer/helpers"
	trickle "github.com/ipfs/go-ipfs/importer/trickle"
	mdag "github.com/ipfs/go-ipfs/merkledag"
	mdagmock "github.com/ipfs/go-ipfs/merkledag/test"
	ft "github.com/ipfs/go-ipfs/unixfs"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

func SizeSplitterGen(size int64) chunk.SplitterGen {
	return func(r io.Reader) chunk.Splitter {
		return chunk.NewSizeSplitter(r, size)
	}
}

func GetDAGServ() mdag.DAGService {
	return mdagmock.Mock()
}

// NodeOpts is used by GetNode, GetEmptyNode and GetRandomNode
type NodeOpts struct {
	Prefix cid.Prefix
	// ForceRawLeaves if true will force the use of raw leaves
	ForceRawLeaves bool
	// RawLeavesUsed is true if raw leaves or either implicitly or explicitly enabled
	RawLeavesUsed bool
}

var UseProtoBufLeaves = NodeOpts{Prefix: mdag.V0CidPrefix()}
var UseRawLeaves = NodeOpts{Prefix: mdag.V0CidPrefix(), ForceRawLeaves: true, RawLeavesUsed: true}
var UseCidV1 = NodeOpts{Prefix: mdag.V1CidPrefix(), RawLeavesUsed: true}
var UseBlake2b256 NodeOpts

func init() {
	UseBlake2b256 = UseCidV1
	UseBlake2b256.Prefix.MhType = mh.Names["blake2b-256"]
	UseBlake2b256.Prefix.MhLength = -1
}

func GetNode(t testing.TB, dserv mdag.DAGService, data []byte, opts NodeOpts) node.Node {
	in := bytes.NewReader(data)

	dbp := h.DagBuilderParams{
		Dagserv:   dserv,
		Maxlinks:  h.DefaultLinksPerBlock,
		Prefix:    &opts.Prefix,
		RawLeaves: opts.RawLeavesUsed,
	}

	node, err := trickle.TrickleLayout(dbp.New(SizeSplitterGen(500)(in)))
	if err != nil {
		t.Fatal(err)
	}

	return node
}

func GetEmptyNode(t testing.TB, dserv mdag.DAGService, opts NodeOpts) node.Node {
	return GetNode(t, dserv, []byte{}, opts)
}

func GetRandomNode(t testing.TB, dserv mdag.DAGService, size int64, opts NodeOpts) ([]byte, node.Node) {
	in := io.LimitReader(u.NewTimeSeededRand(), size)
	buf, err := ioutil.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}

	node := GetNode(t, dserv, buf, opts)
	return buf, node
}

func ArrComp(a, b []byte) error {
	if len(a) != len(b) {
		return fmt.Errorf("Arrays differ in length. %d != %d", len(a), len(b))
	}
	for i, v := range a {
		if v != b[i] {
			return fmt.Errorf("Arrays differ at index: %d", i)
		}
	}
	return nil
}

func PrintDag(nd *mdag.ProtoNode, ds mdag.DAGService, indent int) {
	pbd, err := ft.FromBytes(nd.Data())
	if err != nil {
		panic(err)
	}

	for i := 0; i < indent; i++ {
		fmt.Print(" ")
	}
	fmt.Printf("{size = %d, type = %s, children = %d", pbd.GetFilesize(), pbd.GetType().String(), len(pbd.GetBlocksizes()))
	if len(nd.Links()) > 0 {
		fmt.Println()
	}
	for _, lnk := range nd.Links() {
		child, err := lnk.GetNode(context.Background(), ds)
		if err != nil {
			panic(err)
		}
		PrintDag(child.(*mdag.ProtoNode), ds, indent+1)
	}
	if len(nd.Links()) > 0 {
		for i := 0; i < indent; i++ {
			fmt.Print(" ")
		}
	}
	fmt.Println("}")
}
