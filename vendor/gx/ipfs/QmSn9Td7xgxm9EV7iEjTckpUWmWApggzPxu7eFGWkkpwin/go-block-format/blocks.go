// Package blocks contains the lowest level of IPLD data structures.
// A block is raw data accompanied by a CID. The CID contains the multihash
// corresponding to the block.
package blocks

import (
	"errors"
	"fmt"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

// ErrWrongHash is returned when the Cid of a block is not the expected
// according to the contents. It is currently used only when debugging.
var ErrWrongHash = errors.New("data did not match given hash")

// Block provides abstraction for blocks implementations.
type Block interface {
	RawData() []byte
	Cid() *cid.Cid
	String() string
	Loggable() map[string]interface{}
}

// A BasicBlock is a singular block of data in ipfs. It implements the Block
// interface.
type BasicBlock struct {
	cid  *cid.Cid
	data []byte
}

// NewBlock creates a Block object from opaque data. It will hash the data.
func NewBlock(data []byte) *BasicBlock {
	// TODO: fix assumptions
	return &BasicBlock{data: data, cid: cid.NewCidV0(u.Hash(data))}
}

// NewBlockWithCid creates a new block when the hash of the data
// is already known, this is used to save time in situations where
// we are able to be confident that the data is correct.
func NewBlockWithCid(data []byte, c *cid.Cid) (*BasicBlock, error) {
	if u.Debug {
		chkc, err := c.Prefix().Sum(data)
		if err != nil {
			return nil, err
		}

		if !chkc.Equals(c) {
			return nil, ErrWrongHash
		}
	}
	return &BasicBlock{data: data, cid: c}, nil
}

// Multihash returns the hash contained in the block CID.
func (b *BasicBlock) Multihash() mh.Multihash {
	return b.cid.Hash()
}

// RawData returns the block raw contents as a byte slice.
func (b *BasicBlock) RawData() []byte {
	return b.data
}

// Cid returns the content identifier of the block.
func (b *BasicBlock) Cid() *cid.Cid {
	return b.cid
}

// String provides a human-readable representation of the block CID.
func (b *BasicBlock) String() string {
	return fmt.Sprintf("[Block %s]", b.Cid())
}

// Loggable returns a go-log loggable item.
func (b *BasicBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"block": b.Cid().String(),
	}
}
