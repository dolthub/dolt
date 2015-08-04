package enc

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestBlobLeafEncode(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NopStore{}

	buf := bytes.NewBuffer([]byte{})
	w := cs.Put()
	assert.NoError(blobLeafEncode(buf, w))
	r, err := w.Ref()
	assert.NoError(err)
	// echo -n 'b ' | sha1sum
	assert.Equal("sha1-e1bc846440ec2fb557a5a271e785cd4c648883fa", r.String())

	buf = bytes.NewBufferString("Hello, World!")
	w = cs.Put()
	assert.NoError(blobLeafEncode(buf, w))
	r, err = w.Ref()
	assert.NoError(err)
	// echo -n 'b Hello, World!' | sha1sum
	assert.Equal("sha1-135fe1453330547994b2ce8a1b238adfbd7df87e", r.String())
}

func TestBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	reader := bytes.NewBufferString("b ")
	v1, err := blobLeafDecode(reader)
	assert.NoError(err)
	assert.EqualValues([]byte{}, v1)

	reader = bytes.NewBufferString("b Hello World!")
	v2, err := blobLeafDecode(reader)
	assert.NoError(err)
	assert.EqualValues([]byte("Hello World!"), v2)
}
