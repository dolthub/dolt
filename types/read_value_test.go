package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

func TestTolerateUngettableRefs(t *testing.T) {
	assert := assert.New(t)
	v := ReadValue(ref.Ref{}, chunks.NewTestStore())
	assert.Nil(v)
}

func TestBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	blobLeafDecode := func(r io.Reader) Value {
		i := enc.Decode(r)
		return fromEncodeable(i, nil).Deref(nil)
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
