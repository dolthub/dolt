// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestWriteBigEndianIntegers(t *testing.T) {
	assert := assert.New(t)

	w := binaryNomsWriter{make([]byte, initialBufferSize, initialBufferSize), 0}
	w.writeUint32(uint32(1))
	w.writeUint64(uint64(1))

	var u32 uint32
	var u64 uint64
	r := bytes.NewBuffer(w.buff)
	err := binary.Read(r, binary.BigEndian, &u32)
	assert.NoError(err)
	err = binary.Read(r, binary.BigEndian, &u64)
	assert.NoError(err)

	assert.True(u32 == uint32(1))
	assert.True(u64 == uint64(1))
}

func TestReadBigEndianIntegers(t *testing.T) {
	assert := assert.New(t)

	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, uint32(1))
	assert.NoError(err)
	err = binary.Write(buf, binary.BigEndian, uint64(1))
	assert.NoError(err)

	r := binaryNomsReader{buf.Bytes(), 0}
	assert.True(r.readUint32() == uint32(1))
	assert.True(r.readUint64() == uint64(1))
}
