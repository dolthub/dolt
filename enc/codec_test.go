package enc

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	assert := assert.New(t)
	s := chunks.NopStore{}

	testEncode := func(expected string, v interface{}) {
		w := s.Put()
		err := Encode(v, w)
		assert.NoError(err)

		// Assuming that MemoryStore works correctly, we don't need to check the actual serialization, only the hash. Neat.
		r, err := w.Ref()
		assert.NoError(err)
		assert.EqualValues(sha1.Sum([]byte(expected)), r.Digest(), "Incorrect ref serializing %+v. Got: %#x", v, r.Digest())
		return
	}

	// Encoding details for each codec are tested elsewhere.
	// Here we just want to make sure codecs are selected correctly.
	b := bytes.NewBuffer([]byte{0x00, 0x01, 0x02})
	testEncode(string([]byte{'b', ' ', 0x00, 0x01, 0x02}), b)
	testEncode(string("j \"foo\"\n"), "foo")
}

func TestInvalidDecode(t *testing.T) {
	assert := assert.New(t)

	v, err := Decode(bytes.NewBuffer([]byte{}))
	assert.Nil(v)
	assert.NotNil(err)

	v, err = Decode(bytes.NewBuffer([]byte{0xff}))
	assert.Nil(v)
	assert.NotNil(err)
}

func TestSelectJSONDecoder(t *testing.T) {
	assert := assert.New(t)

	v, err := Decode(bytes.NewBufferString(`j "foo"`))
	assert.NoError(err)
	assert.EqualValues("foo", v)
}

func TestSelectBlobDecoder(t *testing.T) {
	assert := assert.New(t)

	v, err := Decode(bytes.NewBuffer([]byte{'b', ' ', 0x2B}))
	assert.NoError(err)
	assert.EqualValues([]byte{0x2B}, v)
}
