package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadValueBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	blobLeafDecode := func(r io.Reader) Value {
		i := decode(r)
		return NewBlob(i.(io.Reader))
	}

	reader := bytes.NewBufferString("b ")
	v1 := blobLeafDecode(reader)
	bl1 := newBlobLeaf([]byte{})
	assert.True(bl1.Equals(v1))

	reader = bytes.NewBufferString("b Hello World!")
	v2 := blobLeafDecode(reader)
	bl2 := newBlobLeaf([]byte("Hello World!"))
	assert.True(bl2.Equals(v2))
}
