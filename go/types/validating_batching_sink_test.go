// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
)

func TestValidatingBatchingSinkPrepare(t *testing.T) {
	cs := chunks.NewTestStore()
	hints := Hints{}
	chnx := []chunks.Chunk{
		EncodeValue(Number(42), nil),
		EncodeValue(Number(-7), nil),
		EncodeValue(String("oy"), nil),
		EncodeValue(Bool(true), nil),
		EncodeValue(NewBlob(), nil),
	}
	for _, c := range chnx {
		cs.Put(c)
		hints[c.Hash()] = struct{}{}
	}

	vbs := NewValidatingBatchingSink(cs)
	vbs.Prepare(hints)
	for h := range hints {
		vbs.vs.isPresent(h)
	}
}

func TestValidatingBatchingSinkDecode(t *testing.T) {
	v := Number(42)
	c := EncodeValue(v, nil)
	vbs := NewValidatingBatchingSink(chunks.NewTestStore())

	dc := vbs.DecodeUnqueued(&c)
	assert.True(t, v.Equals(*dc.Value))
}

func TestValidatingBatchingSinkDecodeAlreadyEnqueued(t *testing.T) {
	v := Number(42)
	c := EncodeValue(v, nil)
	vbs := NewValidatingBatchingSink(chunks.NewTestStore())

	assert.NoError(t, vbs.Enqueue(c, v))
	dc := vbs.DecodeUnqueued(&c)
	assert.Nil(t, dc.Chunk)
	assert.Nil(t, dc.Value)
}

func assertPanicsOnInvalidChunk(t *testing.T, data []interface{}) {
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)
	r := &nomsTestReader{data, 0}
	dec := newValueDecoder(r, vs, staticTypeCache)
	v := dec.readValue()

	c := EncodeValue(v, nil)
	vbs := NewValidatingBatchingSink(cs)

	assert.Panics(t, func() {
		vbs.DecodeUnqueued(&c)
	})
}

func TestValidatingBatchingSinkDecodeInvalidUnion(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(UnionKind), uint32(2) /* len */, uint8(BoolKind), uint8(NumberKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldOrder(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint32(2) /* len */, "b", uint8(NumberKind), "a", uint8(NumberKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S ", uint32(0), /* len */
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint32(1) /* len */, "b ", uint8(NumberKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkEnqueueAndFlush(t *testing.T) {
	v := Number(42)
	c := EncodeValue(v, nil)
	cs := chunks.NewTestStore()
	vbs := NewValidatingBatchingSink(cs)

	assert.NoError(t, vbs.Enqueue(c, v))
	assert.NoError(t, vbs.Flush())
	assert.Equal(t, 1, cs.Writes)
}

func TestValidatingBatchingSinkEnqueueImplicitFlush(t *testing.T) {
	cs := chunks.NewTestStore()
	vbs := NewValidatingBatchingSink(cs)

	for i := 0; i <= batchSize; i++ {
		v := Number(i)
		assert.NoError(t, vbs.Enqueue(EncodeValue(v, nil), v))
	}
	assert.Equal(t, batchSize, cs.Writes)
	assert.NoError(t, vbs.Flush())
	assert.Equal(t, 1, cs.Writes-batchSize)
}
