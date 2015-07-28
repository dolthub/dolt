package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlobLeafLen(t *testing.T) {
	assert := assert.New(t)
	b := newBlobLeaf([]byte{})
	assert.Equal(uint64(0), b.Len())
	b = newBlobLeaf([]byte{0x01})
	assert.Equal(uint64(1), b.Len())
}

func TestBlobLeafEquals(t *testing.T) {
	assert := assert.New(t)
	b1 := newBlobLeaf([]byte{0x01})
	b11 := b1
	b12 := newBlobLeaf([]byte{0x01})
	b2 := newBlobLeaf([]byte{0x02})
	b3 := newBlobLeaf([]byte{0x02, 0x03})
	AssertSymEq(assert, b1, b11)
	AssertSymEq(assert, b1, b12)
	AssertSymNe(assert, b1, b2)
	AssertSymNe(assert, b2, b3)
	AssertSymNe(assert, b1, Int32(1))
}

func TestBlobLeafChunks(t *testing.T) {
	assert := assert.New(t)
	b := newBlobLeaf([]byte{})
	assert.Equal(0, len(b.Chunks()))
	b = newBlobLeaf([]byte{0x01})
	assert.Equal(0, len(b.Chunks()))
}
