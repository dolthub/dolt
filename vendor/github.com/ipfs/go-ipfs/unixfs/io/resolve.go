package io

import (
	"context"

	dag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	hamt "github.com/ipfs/go-ipfs/unixfs/hamt"

	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

// ResolveUnixfsOnce resolves a single hop of a path through a graph in a
// unixfs context. This includes handling traversing sharded directories.
func ResolveUnixfsOnce(ctx context.Context, ds dag.DAGService, nd node.Node, names []string) (*node.Link, []string, error) {
	switch nd := nd.(type) {
	case *dag.ProtoNode:
		upb, err := ft.FromBytes(nd.Data())
		if err != nil {
			// Not a unixfs node, use standard object traversal code
			lnk, err := nd.GetNodeLink(names[0])
			if err != nil {
				return nil, nil, err
			}

			return lnk, names[1:], nil
		}

		switch upb.GetType() {
		case ft.THAMTShard:
			s, err := hamt.NewHamtFromDag(ds, nd)
			if err != nil {
				return nil, nil, err
			}

			out, err := s.Find(ctx, names[0])
			if err != nil {
				return nil, nil, err
			}

			return out, names[1:], nil
		default:
			lnk, err := nd.GetNodeLink(names[0])
			if err != nil {
				return nil, nil, err
			}

			return lnk, names[1:], nil
		}
	default:
		lnk, rest, err := nd.ResolveLink(names)
		if err != nil {
			return nil, nil, err
		}
		return lnk, rest, nil
	}
}
