package newset

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/kch42/buzhash"
	"github.com/attic-labs/noms/ref"
)

const (
	buzPattern = uint32(1<<6 - 1) // Average size of 64 elements
)

type buzChunker struct {
	h *buzhash.BuzHash
}

func newBuzChunker() *buzChunker {
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

func (c *buzChunker) New() Chunker {
	return newBuzChunker()
}

func newBuzHash() *buzhash.BuzHash {
	return buzhash.NewBuzHash(uint32(8 * ref.NewHash().BlockSize()))
}
