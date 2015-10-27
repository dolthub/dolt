package enc

import (
	"bytes"
	"io"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/d"
)

func TestEncode(t *testing.T) {
	assert := assert.New(t)

	// Encoding details for each codec are tested elsewhere.
	// Here we just want to make sure codecs are selected correctly.
	dst := &bytes.Buffer{}
	Encode(dst, bytes.NewReader([]byte{0x00, 0x01, 0x02}))
	assert.Equal([]byte{'b', ' ', 0x00, 0x01, 0x02}, dst.Bytes())

	dst.Reset()
	Encode(dst, []interface{}{42})
	assert.Equal("t [42]\n", string(dst.Bytes()))
}

func TestInvalidDecode(t *testing.T) {
	assert := assert.New(t)

	d.IsUsageError(assert, func() {
		Decode(bytes.NewReader([]byte{}))
	})

	d.IsUsageError(assert, func() {
		Decode(bytes.NewReader([]byte{0xff}))
	})
}

func TestSelectBlobDecoder(t *testing.T) {
	assert := assert.New(t)

	decoded := Decode(bytes.NewReader([]byte{'b', ' ', 0x2B}))
	out := &bytes.Buffer{}
	_, err := io.Copy(out, decoded.(io.Reader))
	assert.NoError(err)
	assert.EqualValues([]byte{0x2B}, out.Bytes())
}

func TestSelectTypedDecoder(t *testing.T) {
	v := Decode(bytes.NewBufferString(`t [42]`))
	assert.Equal(t, []interface{}{float64(42)}, v)
}
