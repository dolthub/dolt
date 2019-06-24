// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/stretchr/testify/assert"
)

func TestValidatingBatchingSinkDecode(t *testing.T) {
	v := Float(42)
	c := EncodeValue(v)
	storage := &chunks.TestStorage{}
	vdc := NewValidatingDecoder(storage.NewView())

	dc := vdc.Decode(&c)
	assert.True(t, v.Equals(*dc.Value))
}

func assertPanicsOnInvalidChunk(t *testing.T, data []interface{}) {
	storage := &chunks.TestStorage{}
	vs := NewValueStore(storage.NewView())
	dataAsByteSlice := toBinaryNomsReaderData(data)
	dec := newValueDecoder(dataAsByteSlice, vs)
	v := dec.readValue()

	c := EncodeValue(v)
	vdc := NewValidatingDecoder(storage.NewView())

	assert.Panics(t, func() {
		vdc.Decode(&c)
	})
}

func TestValidatingBatchingSinkDecodeInvalidUnion(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(UnionKind), uint64(2) /* len */, uint8(FloatKind), uint8(BoolKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldOrder(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint64(2), /* len */
		"b", "a",
		uint8(FloatKind), uint8(FloatKind),
		false, false,
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S ", uint64(0), /* len */
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint64(1), /* len */
		"b ", uint8(FloatKind), false,
	}
	assertPanicsOnInvalidChunk(t, data)
}
