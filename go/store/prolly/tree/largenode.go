package tree

import (
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
)

// LargeNode wraps a Node that uses 32 bit offsets in its encoding. (Currently only vector index nodes do this.)
type LargeNode struct {
	SmallNode    Node
	keys, values message.ItemAccess32
}

func LargeNodeFromNode(nd Node) (node LargeNode, err error) {
	node = LargeNode{SmallNode: nd}
	node.keys, node.values, _, _, err = message.GetVectorIndexKeysAndValues32(nd.msg)
	return node, err
}

func (nd LargeNode) Count() int {
	return nd.SmallNode.Count()
}

func (nd LargeNode) TreeCount() (int, error) {
	return nd.SmallNode.TreeCount()
}

// GetKey returns the |ith| key of this node
func (nd LargeNode) GetKey(i int) Item {
	return nd.keys.GetItem(i, nd.SmallNode.msg)
}

// GetValue returns the |ith| value of this node.
func (nd LargeNode) GetValue(i int) Item {
	return nd.values.GetItem(i, nd.SmallNode.msg)
}

func (nd LargeNode) LoadSubtrees() (LargeNode, error) {
	smallNodeWithSubtrees, err := nd.SmallNode.LoadSubtrees()
	if err != nil {
		return LargeNode{}, err
	}
	return LargeNode{
		SmallNode: smallNodeWithSubtrees,
		keys:      nd.keys,
		values:    nd.values,
	}, nil
}

func (nd LargeNode) HashOf() hash.Hash {
	return nd.SmallNode.HashOf()
}

func (nd LargeNode) Level() int {
	return nd.SmallNode.Level()
}

func (nd LargeNode) WouldFitInSmallItemAccess() bool {
	return nd.keys.WouldFitInSmallItemAccess() && nd.values.WouldFitInSmallItemAccess()
}
