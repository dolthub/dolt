package types

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlobLeafEncode(t *testing.T) {
	assert := assert.New(t)

	src := &bytes.Buffer{}
	dst := &bytes.Buffer{}
	blobLeafEncode(dst, src)
	assert.EqualValues(blobTag, dst.Bytes())

	content := []byte("Hello, World!")
	src = bytes.NewBuffer(content)
	dst = &bytes.Buffer{}
	blobLeafEncode(dst, src)
	assert.EqualValues(append(blobTag, content...), dst.Bytes())
}

func TestBlobLeafDecode(t *testing.T) {
	assert := assert.New(t)

	out := &bytes.Buffer{}
	inputReader := bytes.NewReader(blobTag)
	decoded := blobLeafDecode(inputReader)
	data, err := ioutil.ReadAll(decoded)
	assert.NoError(err)
	assert.EqualValues([]byte{}, data)

	out.Truncate(0)
	content := []byte("Hello, World!")
	inputReader = bytes.NewReader(append(blobTag, content...))
	decoded = blobLeafDecode(inputReader)
	_, err = io.Copy(out, decoded)
	assert.NoError(err)
	assert.EqualValues(content, out.Bytes())
}
