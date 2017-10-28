package dagutils

import (
	"context"
	"errors"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	bserv "github.com/ipfs/go-ipfs/blockservice"
	offline "github.com/ipfs/go-ipfs/exchange/offline"
	dag "github.com/ipfs/go-ipfs/merkledag"
	path "github.com/ipfs/go-ipfs/path"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	syncds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
)

type Editor struct {
	root *dag.ProtoNode

	// tmp is a temporary in memory (for now) dagstore for all of the
	// intermediary nodes to be stored in
	tmp dag.DAGService

	// src is the dagstore with *all* of the data on it, it is used to pull
	// nodes from for modification (nil is a valid value)
	src dag.DAGService
}

func NewMemoryDagService() dag.DAGService {
	// build mem-datastore for editor's intermediary nodes
	bs := bstore.NewBlockstore(syncds.MutexWrap(ds.NewMapDatastore()))
	bsrv := bserv.New(bs, offline.Exchange(bs))
	return dag.NewDAGService(bsrv)
}

// root is the node to be modified, source is the dagstore to pull nodes from (optional)
func NewDagEditor(root *dag.ProtoNode, source dag.DAGService) *Editor {
	return &Editor{
		root: root,
		tmp:  NewMemoryDagService(),
		src:  source,
	}
}

func (e *Editor) GetNode() *dag.ProtoNode {
	return e.root.Copy().(*dag.ProtoNode)
}

func (e *Editor) GetDagService() dag.DAGService {
	return e.tmp
}

func addLink(ctx context.Context, ds dag.DAGService, root *dag.ProtoNode, childname string, childnd node.Node) (*dag.ProtoNode, error) {
	if childname == "" {
		return nil, errors.New("cannot create link with no name!")
	}

	// ensure that the node we are adding is in the dagservice
	_, err := ds.Add(childnd)
	if err != nil {
		return nil, err
	}

	_ = ds.Remove(root)

	// ensure no link with that name already exists
	_ = root.RemoveNodeLink(childname) // ignore error, only option is ErrNotFound

	if err := root.AddNodeLinkClean(childname, childnd); err != nil {
		return nil, err
	}

	if _, err := ds.Add(root); err != nil {
		return nil, err
	}
	return root, nil
}

func (e *Editor) InsertNodeAtPath(ctx context.Context, pth string, toinsert node.Node, create func() *dag.ProtoNode) error {
	splpath := path.SplitList(pth)
	nd, err := e.insertNodeAtPath(ctx, e.root, splpath, toinsert, create)
	if err != nil {
		return err
	}
	e.root = nd
	return nil
}

func (e *Editor) insertNodeAtPath(ctx context.Context, root *dag.ProtoNode, path []string, toinsert node.Node, create func() *dag.ProtoNode) (*dag.ProtoNode, error) {
	if len(path) == 1 {
		return addLink(ctx, e.tmp, root, path[0], toinsert)
	}

	nd, err := root.GetLinkedProtoNode(ctx, e.tmp, path[0])
	if err != nil {
		// if 'create' is true, we create directories on the way down as needed
		if err == dag.ErrLinkNotFound && create != nil {
			nd = create()
			err = nil // no longer an error case
		} else if err == dag.ErrNotFound {
			// try finding it in our source dagstore
			nd, err = root.GetLinkedProtoNode(ctx, e.src, path[0])
		}

		// if we receive an ErrNotFound, then our second 'GetLinkedNode' call
		// also fails, we want to error out
		if err != nil {
			return nil, err
		}
	}

	ndprime, err := e.insertNodeAtPath(ctx, nd, path[1:], toinsert, create)
	if err != nil {
		return nil, err
	}

	_ = e.tmp.Remove(root)

	_ = root.RemoveNodeLink(path[0])
	err = root.AddNodeLinkClean(path[0], ndprime)
	if err != nil {
		return nil, err
	}

	_, err = e.tmp.Add(root)
	if err != nil {
		return nil, err
	}

	return root, nil
}

func (e *Editor) RmLink(ctx context.Context, pth string) error {
	splpath := path.SplitList(pth)
	nd, err := e.rmLink(ctx, e.root, splpath)
	if err != nil {
		return err
	}
	e.root = nd
	return nil
}

func (e *Editor) rmLink(ctx context.Context, root *dag.ProtoNode, path []string) (*dag.ProtoNode, error) {
	if len(path) == 1 {
		// base case, remove node in question
		err := root.RemoveNodeLink(path[0])
		if err != nil {
			return nil, err
		}

		_, err = e.tmp.Add(root)
		if err != nil {
			return nil, err
		}

		return root, nil
	}

	// search for node in both tmp dagstore and source dagstore
	nd, err := root.GetLinkedProtoNode(ctx, e.tmp, path[0])
	if err == dag.ErrNotFound {
		nd, err = root.GetLinkedProtoNode(ctx, e.src, path[0])
	}

	if err != nil {
		return nil, err
	}

	nnode, err := e.rmLink(ctx, nd, path[1:])
	if err != nil {
		return nil, err
	}

	_ = e.tmp.Remove(root)

	_ = root.RemoveNodeLink(path[0])
	err = root.AddNodeLinkClean(path[0], nnode)
	if err != nil {
		return nil, err
	}

	_, err = e.tmp.Add(root)
	if err != nil {
		return nil, err
	}

	return root, nil
}

func (e *Editor) Finalize(ds dag.DAGService) (*dag.ProtoNode, error) {
	nd := e.GetNode()
	err := copyDag(nd, e.tmp, ds)
	return nd, err
}

func copyDag(nd *dag.ProtoNode, from, to dag.DAGService) error {
	_, err := to.Add(nd)
	if err != nil {
		return err
	}

	for _, lnk := range nd.Links() {
		child, err := lnk.GetNode(context.Background(), from)
		if err != nil {
			if err == dag.ErrNotFound {
				// not found means we didnt modify it, and it should
				// already be in the target datastore
				continue
			}
			return err
		}

		childpb, ok := child.(*dag.ProtoNode)
		if !ok {
			return dag.ErrNotProtobuf
		}

		err = copyDag(childpb, from, to)
		if err != nil {
			return err
		}
	}
	return nil
}
