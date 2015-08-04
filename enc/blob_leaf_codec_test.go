package enc

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlobLeafEncode(t *testing.T) {
	assert := assert.New(t)

	src := &bytes.Buffer{}
	dst := &bytes.Buffer{}
	assert.NoError(blobLeafEncode(dst, src))
	assert.EqualValues(blobTag, dst.Bytes())

	src = bytes.NewBufferString("Hello, World!")
	dst = &bytes.Buffer{}
	assert.NoError(blobLeafEncode(dst, src))
	assert.EqualValues(append(blobTag, []byte("Hello, World!")...), dst.Bytes())
}

func TestBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	out := &bytes.Buffer{}
	inputReader := bytes.NewBuffer(blobTag)
	decoded, err := blobLeafDecode(inputReader)
	assert.NoError(err)
	_, err = io.Copy(out, decoded)
	assert.NoError(err)
	assert.EqualValues([]byte(nil), out.Bytes())

	out.Truncate(0)
	inputReader = bytes.NewBufferString("b Hello World!")
	decoded, err = blobLeafDecode(inputReader)
	assert.NoError(err)
	_, err = io.Copy(out, decoded)
	assert.NoError(err)
	assert.EqualValues([]byte("Hello World!"), out.Bytes())
}
