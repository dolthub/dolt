package enc

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	assert := assert.New(t)

	// Encoding details for each codec are tested elsewhere.
	// Here we just want to make sure codecs are selected correctly.
	dst := &bytes.Buffer{}
	assert.NoError(Encode(dst, bytes.NewReader([]byte{0x00, 0x01, 0x02})))
	assert.Equal([]byte{'b', ' ', 0x00, 0x01, 0x02}, dst.Bytes())

	dst.Reset()
	assert.NoError(Encode(dst, "foo"))
	assert.Equal("j \"foo\"\n", string(dst.Bytes()))
}

func TestInvalidDecode(t *testing.T) {
	assert := assert.New(t)

	v, err := Decode(bytes.NewReader([]byte{}))
	assert.Nil(v)
	assert.NotNil(err)

	v, err = Decode(bytes.NewReader([]byte{0xff}))
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

	decoded, err := Decode(bytes.NewReader([]byte{'b', ' ', 0x2B}))
	assert.NoError(err)
	out := &bytes.Buffer{}
	_, err = io.Copy(out, decoded.(io.Reader))
	assert.NoError(err)
	assert.EqualValues([]byte{0x2B}, out.Bytes())
}
