package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestBlobLeafEncode(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	buf := bytes.NewBuffer([]byte{})
	b1, err := NewBlob(buf)
	assert.NoError(err)
	bl1, ok := b1.(blobLeaf)
	assert.True(ok)
	r1, err := blobLeafEncode(bl1, cs)
	// echo -n 'b ' | sha1sum
	assert.Equal("sha1-e1bc846440ec2fb557a5a271e785cd4c648883fa", r1.String())

	buf = bytes.NewBufferString("Hello, World!")
	b2, err := NewBlob(buf)
	assert.NoError(err)
	bl2, ok := b2.(blobLeaf)
	assert.True(ok)
	r2, err := blobLeafEncode(bl2, cs)
	// echo -n 'b Hello, World!' | sha1sum
	assert.Equal("sha1-135fe1453330547994b2ce8a1b238adfbd7df87e", r2.String())
}

func TestBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	reader := bytes.NewBufferString("b ")
	v1, err := blobLeafDecode(reader, nil)
	assert.NoError(err)
	bl1 := newBlobLeaf([]byte{})
	assert.True(bl1.Equals(v1))

	reader = bytes.NewBufferString("b Hello World!")
	v2, err := blobLeafDecode(reader, nil)
	assert.NoError(err)
	bl2 := newBlobLeaf([]byte("Hello World!"))
	assert.True(bl2.Equals(v2))
}
