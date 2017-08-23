package trickle

import (
	"context"
	"errors"
	"fmt"

	h "github.com/ipfs/go-ipfs/importer/helpers"
	dag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"

	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

// layerRepeat specifies how many times to append a child tree of a
// given depth. Higher values increase the width of a given node, which
// improves seek speeds.
const layerRepeat = 4

func TrickleLayout(db *h.DagBuilderHelper) (node.Node, error) {
	root := db.NewUnixfsNode()
	if err := db.FillNodeLayer(root); err != nil {
		return nil, err
	}
	for level := 1; !db.Done(); level++ {
		for i := 0; i < layerRepeat && !db.Done(); i++ {
			next := db.NewUnixfsNode()
			if err := fillTrickleRec(db, next, level); err != nil {
				return nil, err
			}
			if err := root.AddChild(next, db); err != nil {
				return nil, err
			}
		}
	}

	out, err := db.Add(root)
	if err != nil {
		return nil, err
	}

	if err := db.Close(); err != nil {
		return nil, err
	}

	return out, nil
}

func fillTrickleRec(db *h.DagBuilderHelper, node *h.UnixfsNode, depth int) error {
	// Always do this, even in the base case
	if err := db.FillNodeLayer(node); err != nil {
		return err
	}

	for i := 1; i < depth && !db.Done(); i++ {
		for j := 0; j < layerRepeat && !db.Done(); j++ {
			next := db.NewUnixfsNode()
			if err := fillTrickleRec(db, next, i); err != nil {
				return err
			}

			if err := node.AddChild(next, db); err != nil {
				return err
			}
		}
	}
	return nil
}

// TrickleAppend appends the data in `db` to the dag, using the Trickledag format
func TrickleAppend(ctx context.Context, basen node.Node, db *h.DagBuilderHelper) (out node.Node, err_out error) {
	base, ok := basen.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	defer func() {
		if err_out == nil {
			if err := db.Close(); err != nil {
				err_out = err
			}
		}
	}()

	// Convert to unixfs node for working with easily
	ufsn, err := h.NewUnixfsNodeFromDag(base)
	if err != nil {
		return nil, err
	}

	// Get depth of this 'tree'
	n, layerProgress := trickleDepthInfo(ufsn, db.Maxlinks())
	if n == 0 {
		// If direct blocks not filled...
		if err := db.FillNodeLayer(ufsn); err != nil {
			return nil, err
		}

		if db.Done() {
			return ufsn.GetDagNode()
		}

		// If continuing, our depth has increased by one
		n++
	}

	// Last child in this node may not be a full tree, lets file it up
	if err := appendFillLastChild(ctx, ufsn, n-1, layerProgress, db); err != nil {
		return nil, err
	}

	// after appendFillLastChild, our depth is now increased by one
	if !db.Done() {
		n++
	}

	// Now, continue filling out tree like normal
	for i := n; !db.Done(); i++ {
		for j := 0; j < layerRepeat && !db.Done(); j++ {
			next := db.NewUnixfsNode()
			err := fillTrickleRec(db, next, i)
			if err != nil {
				return nil, err
			}

			err = ufsn.AddChild(next, db)
			if err != nil {
				return nil, err
			}
		}
	}

	return ufsn.GetDagNode()
}

// appendFillLastChild will take in an incomplete trickledag node (uncomplete meaning, not full) and
// fill it out to the specified depth with blocks from the given DagBuilderHelper
func appendFillLastChild(ctx context.Context, ufsn *h.UnixfsNode, depth int, layerFill int, db *h.DagBuilderHelper) error {
	if ufsn.NumChildren() <= db.Maxlinks() {
		return nil
	}
	// Recursive step, grab last child
	last := ufsn.NumChildren() - 1
	lastChild, err := ufsn.GetChild(ctx, last, db.GetDagServ())
	if err != nil {
		return err
	}

	// Fill out last child (may not be full tree)
	nchild, err := trickleAppendRec(ctx, lastChild, db, depth-1)
	if err != nil {
		return err
	}

	// Update changed child in parent node
	ufsn.RemoveChild(last, db)
	err = ufsn.AddChild(nchild, db)
	if err != nil {
		return err
	}

	// Partially filled depth layer
	if layerFill != 0 {
		for ; layerFill < layerRepeat && !db.Done(); layerFill++ {
			next := db.NewUnixfsNode()
			err := fillTrickleRec(db, next, depth)
			if err != nil {
				return err
			}

			err = ufsn.AddChild(next, db)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// recursive call for TrickleAppend
func trickleAppendRec(ctx context.Context, ufsn *h.UnixfsNode, db *h.DagBuilderHelper, depth int) (*h.UnixfsNode, error) {
	if depth == 0 || db.Done() {
		return ufsn, nil
	}

	// Get depth of this 'tree'
	n, layerProgress := trickleDepthInfo(ufsn, db.Maxlinks())
	if n == 0 {
		// If direct blocks not filled...
		if err := db.FillNodeLayer(ufsn); err != nil {
			return nil, err
		}
		n++
	}

	// If at correct depth, no need to continue
	if n == depth {
		return ufsn, nil
	}

	if err := appendFillLastChild(ctx, ufsn, n, layerProgress, db); err != nil {
		return nil, err
	}

	// after appendFillLastChild, our depth is now increased by one
	if !db.Done() {
		n++
	}

	// Now, continue filling out tree like normal
	for i := n; i < depth && !db.Done(); i++ {
		for j := 0; j < layerRepeat && !db.Done(); j++ {
			next := db.NewUnixfsNode()
			if err := fillTrickleRec(db, next, i); err != nil {
				return nil, err
			}

			if err := ufsn.AddChild(next, db); err != nil {
				return nil, err
			}
		}
	}

	return ufsn, nil
}

func trickleDepthInfo(node *h.UnixfsNode, maxlinks int) (int, int) {
	n := node.NumChildren()
	if n < maxlinks {
		return 0, 0
	}

	return ((n - maxlinks) / layerRepeat) + 1, (n - maxlinks) % layerRepeat
}

// VerifyTrickleDagStructure checks that the given dag matches exactly the trickle dag datastructure
// layout
func VerifyTrickleDagStructure(nd node.Node, ds dag.DAGService, direct int, layerRepeat int) error {
	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return dag.ErrNotProtobuf
	}

	return verifyTDagRec(pbnd, -1, direct, layerRepeat, ds)
}

// Recursive call for verifying the structure of a trickledag
func verifyTDagRec(nd *dag.ProtoNode, depth, direct, layerRepeat int, ds dag.DAGService) error {
	if depth == 0 {
		// zero depth dag is raw data block
		if len(nd.Links()) > 0 {
			return errors.New("expected direct block")
		}

		pbn, err := ft.FromBytes(nd.Data())
		if err != nil {
			return err
		}

		if pbn.GetType() != ft.TRaw {
			return errors.New("Expected raw block")
		}
		return nil
	}

	// Verify this is a branch node
	pbn, err := ft.FromBytes(nd.Data())
	if err != nil {
		return err
	}

	if pbn.GetType() != ft.TFile {
		return fmt.Errorf("expected file as branch node, got: %s", pbn.GetType())
	}

	if len(pbn.Data) > 0 {
		return errors.New("branch node should not have data")
	}

	for i := 0; i < len(nd.Links()); i++ {
		childi, err := nd.Links()[i].GetNode(context.TODO(), ds)
		if err != nil {
			return err
		}

		childpb, ok := childi.(*dag.ProtoNode)
		if !ok {
			return fmt.Errorf("cannot operate on non-protobuf nodes")
		}

		if i < direct {
			// Direct blocks
			err := verifyTDagRec(childpb, 0, direct, layerRepeat, ds)
			if err != nil {
				return err
			}
		} else {
			// Recursive trickle dags
			rdepth := ((i - direct) / layerRepeat) + 1
			if rdepth >= depth && depth > 0 {
				return errors.New("Child dag was too deep!")
			}
			err := verifyTDagRec(childpb, rdepth, direct, layerRepeat, ds)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
