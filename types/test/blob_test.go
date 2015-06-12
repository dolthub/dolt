package test

import (
	"testing"

	_ "github.com/attic-labs/noms/enc"
	. "github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func AssertSymEq(assert *assert.Assertions, a, b Value) {
	assert.True(a.Equals(b))
	assert.True(b.Equals(a))
}

func AssertSymNe(assert *assert.Assertions, a, b Value) {
	assert.False(a.Equals(b))
	assert.False(b.Equals(a))
}

func TestBlobLen(t *testing.T) {
	assert := assert.New(t)
	b := NewBlob([]byte{})
	assert.Equal(uint64(0), b.Len())
	b = NewBlob([]byte{0x01})
	assert.Equal(uint64(1), b.Len())
}

func TestBlobEquals(t *testing.T) {
	assert := assert.New(t)
	b1 := NewBlob([]byte{0x01})
	b11 := b1
	b12 := NewBlob([]byte{0x01})
	b2 := NewBlob([]byte{0x02})
	b3 := NewBlob([]byte{0x02, 0x03})
	AssertSymEq(assert, b1, b11)
	AssertSymEq(assert, b1, b12)
	AssertSymNe(assert, b1, b2)
	AssertSymNe(assert, b2, b3)
	AssertSymNe(assert, b1, Int32(1))
}
