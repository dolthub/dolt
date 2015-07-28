package types

import (
	"bytes"
	"testing"

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
	b, err := NewBlob(&bytes.Buffer{})
	assert.NoError(err)
	assert.Equal(uint64(0), b.Len())
	b, err = NewBlob(bytes.NewBuffer([]byte{0x01}))
	assert.NoError(err)
	assert.Equal(uint64(1), b.Len())
}

func TestBlobEquals(t *testing.T) {
	assert := assert.New(t)
	b1, _ := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b11 := b1
	b12, _ := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b2, _ := NewBlob(bytes.NewBuffer([]byte{0x02}))
	b3, _ := NewBlob(bytes.NewBuffer([]byte{0x02, 0x03}))
	AssertSymEq(assert, b1, b11)
	AssertSymEq(assert, b1, b12)
	AssertSymNe(assert, b1, b2)
	AssertSymNe(assert, b2, b3)
	AssertSymNe(assert, b1, Int32(1))
}
