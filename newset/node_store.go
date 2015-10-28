package newset

import "github.com/attic-labs/noms/ref"

// nodeStore is an in-memory mapping from ref to node.
type nodeStore struct {
	d map[ref.Ref]node
}

func newNodeStore() nodeStore {
	return nodeStore{make(map[ref.Ref]node)}
}
