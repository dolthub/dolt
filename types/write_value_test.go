package types

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestWriteValue(t *testing.T) {
	assert := assert.New(t)

	var s *chunks.MemoryStore

	testEncode := func(expected string, v Value) ref.Ref {
		s = &chunks.MemoryStore{}
		r := WriteValue(v, s)

		// Assuming that MemoryStore works correctly, we don't need to check the actual serialization, only the hash. Neat.
		assert.EqualValues(sha1.Sum([]byte(expected)), r.Digest(), "Incorrect ref serializing %+v. Got: %#x", v, r.Digest())
		return r
	}

	// Encoding details for each codec is tested elsewhere.
	// Here we just want to make sure codecs are selected correctly.
	b, err := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01, 0x02}))
	assert.NoError(err)
	testEncode(string([]byte{'b', ' ', 0x00, 0x01, 0x02}), b)
	testEncode(string("j \"foo\"\n"), NewString("foo"))

}

func TestWriteBlobLeaf(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	buf := bytes.NewBuffer([]byte{})
	b1, err := NewBlob(buf)
	assert.NoError(err)
	bl1, ok := b1.(blobLeaf)
	assert.True(ok)
	r1 := WriteValue(bl1, cs)
	// echo -n 'b ' | sha1sum
	assert.Equal("sha1-e1bc846440ec2fb557a5a271e785cd4c648883fa", r1.String())

	buf = bytes.NewBufferString("Hello, World!")
	b2, err := NewBlob(buf)
	assert.NoError(err)
	bl2, ok := b2.(blobLeaf)
	assert.True(ok)
	r2 := WriteValue(bl2, cs)
	// echo -n 'b Hello, World!' | sha1sum
	assert.Equal("sha1-135fe1453330547994b2ce8a1b238adfbd7df87e", r2.String())
}
