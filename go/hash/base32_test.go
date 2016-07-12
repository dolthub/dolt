// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestBase32Encode(t *testing.T) {
	assert := assert.New(t)

	d := make([]byte, 20, 20)
	assert.Equal("00000000000000000000000000000000", encode(d))
	d[19] = 1
	assert.Equal("00000000000000000000000000000001", encode(d))
	d[19] = 10
	assert.Equal("0000000000000000000000000000000a", encode(d))
	d[19] = 20
	assert.Equal("0000000000000000000000000000000k", encode(d))
	d[19] = 31
	assert.Equal("0000000000000000000000000000000v", encode(d))
	d[19] = 32
	assert.Equal("00000000000000000000000000000010", encode(d))
	d[19] = 63
	assert.Equal("0000000000000000000000000000001v", encode(d))
	d[19] = 64
	assert.Equal("00000000000000000000000000000020", encode(d))

	// Largest!
	for i := 0; i < 20; i++ {
		d[i] = 0xff
	}
	assert.Equal("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv", encode(d))
}

func TestBase32Decode(t *testing.T) {
	assert := assert.New(t)

	d := make([]byte, 20, 20)
	assert.Equal(d, decode("00000000000000000000000000000000"))

	d[19] = 1
	assert.Equal(d, decode("00000000000000000000000000000001"))
	d[19] = 10
	assert.Equal(d, decode("0000000000000000000000000000000a"))
	d[19] = 20
	assert.Equal(d, decode("0000000000000000000000000000000k"))
	d[19] = 31
	assert.Equal(d, decode("0000000000000000000000000000000v"))
	d[19] = 32
	assert.Equal(d, decode("00000000000000000000000000000010"))
	d[19] = 63
	assert.Equal(d, decode("0000000000000000000000000000001v"))
	d[19] = 64
	assert.Equal(d, decode("00000000000000000000000000000020"))

	// Largest!
	for i := 0; i < 20; i++ {
		d[i] = 0xff
	}
	assert.Equal(d, decode("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"))
}
