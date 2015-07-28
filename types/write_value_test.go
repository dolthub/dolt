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
		r, err := WriteValue(v, s)
		assert.NoError(err)

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
