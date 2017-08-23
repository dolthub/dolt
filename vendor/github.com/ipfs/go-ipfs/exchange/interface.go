// package exchange defines the IPFS exchange interface
package exchange

import (
	"context"
	"io"

	blocks "gx/ipfs/QmVA4mafxbfH5aEvNz8fyoxC6J1xhAtw88B4GerPznSZBg/go-block-format"

	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
)

// Any type that implements exchange.Interface may be used as an IPFS block
// exchange protocol.
type Interface interface { // type Exchanger interface
	Fetcher

	// TODO Should callers be concerned with whether the block was made
	// available on the network?
	HasBlock(blocks.Block) error

	IsOnline() bool

	io.Closer
}

// Fetcher is an object that can be used to retrieve blocks
type Fetcher interface {
	// GetBlock returns the block associated with a given key.
	GetBlock(context.Context, *cid.Cid) (blocks.Block, error)
	GetBlocks(context.Context, []*cid.Cid) (<-chan blocks.Block, error)
}
