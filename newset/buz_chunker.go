package newset

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/kch42/buzhash"
	"github.com/attic-labs/noms/ref"
)

const (
	buzPattern = uint32(1<<6 - 1) // Average size of 64 elements
)

// buzChunker chunks refs using the buzhash algorithm:
//
// https://en.wikipedia.org/wiki/Rolling_hash#Cyclic_polynomial
//
// Chunk boundaries are triggered when the buzhash state ends with the binary pattern 11111, so the average chunk size will be 64 elements (there is a 1/64 chance of seeing 11111).
// buzChunker resets the buzhash state on every chunk boundary.
type buzChunker struct {
	h *buzhash.BuzHash
}

func newBuzChunker() Chunker {
	return &buzChunker{newBuzHash()}
}

func (c *buzChunker) Add(r ref.Ref) bool {
	c.h.Write(r.DigestSlice())
	isBoundary := c.h.Sum32()&buzPattern == buzPattern
	if isBoundary {
		c.h = newBuzHash()
	}
	return isBoundary
}

func newBuzHash() *buzhash.BuzHash {
	return buzhash.NewBuzHash(uint32(8 * ref.NewHash().BlockSize()))
}
