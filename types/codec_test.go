// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/testify/assert"
)

func TestEncode(t *testing.T) {
	assert := assert.New(t)

	// Encoding details for each codec are tested elsewhere.
	// Here we just want to make sure codecs are selected correctly.
	dst := &bytes.Buffer{}
	encode(dst, bytes.NewReader([]byte{0x00, 0x01, 0x02}))
	assert.Equal([]byte{'b', ' ', 0x00, 0x01, 0x02}, dst.Bytes())

	dst.Reset()
	encode(dst, []interface{}{42})
	assert.Equal("t [42]", string(dst.Bytes()))
}

func TestInvalidDecode(t *testing.T) {
	assert := assert.New(t)

	d.IsUsageError(assert, func() {
		decode(bytes.NewReader([]byte{}))
	})

	d.IsUsageError(assert, func() {
		decode(bytes.NewReader([]byte{0xff}))
	})
}

func TestSelectBlobDecoder(t *testing.T) {
	assert := assert.New(t)

	decoded := decode(bytes.NewReader([]byte{'b', ' ', 0x2B}))
	out := &bytes.Buffer{}
	_, err := io.Copy(out, decoded.(io.Reader))
	assert.NoError(err)
	assert.EqualValues([]byte{0x2B}, out.Bytes())
}

func TestSelectTypedDecoder(t *testing.T) {
	v := decode(bytes.NewBufferString(`t [42]`))
	assert.Equal(t, []interface{}{float64(42)}, v)
}
