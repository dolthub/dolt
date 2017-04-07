// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
)

func TestValidatingBatchingSinkDecode(t *testing.T) {
	v := Number(42)
	c := EncodeValue(v, nil)
	vbs := NewValidatingBatchingSink(chunks.NewTestStore())

	dc := vbs.DecodeUnqueued(&c)
	assert.True(t, v.Equals(*dc.Value))
}

func assertPanicsOnInvalidChunk(t *testing.T, data []interface{}) {
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)
	r := &nomsTestReader{data, 0}
	dec := newValueDecoder(r, vs)
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
		uint8(UnionKind), uint32(2) /* len */, uint8(NumberKind), uint8(BoolKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldOrder(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint32(2), /* len */
		"b", "a",
		uint8(NumberKind), uint8(NumberKind),
		false, false,
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
		uint8(StructKind), "S", uint32(1), /* len */
		"b ", uint8(NumberKind), false,
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkEnqueueAndFlush(t *testing.T) {
	v := Number(42)
	c := EncodeValue(v, nil)
	cs := chunks.NewTestStore()
	vbs := NewValidatingBatchingSink(cs)

	vbs.Put(c, v)
	vbs.Flush()
	assert.Equal(t, 1, cs.Writes)
}

func TestValidatingBatchingSinkPanicIfDangling(t *testing.T) {
	b := Bool(true)
	r := NewRef(b)

	t.Run("Panic", func(t *testing.T) {
		t.Run("PreFlush", func(t *testing.T) {
			t.Parallel()
			vbs := NewValidatingBatchingSink(chunks.NewTestStore())
			vbs.Put(EncodeValue(r, nil), r)
			assert.Panics(t, vbs.PanicIfDangling)
		})
		t.Run("PostFlush", func(t *testing.T) {
			t.Parallel()
			vbs := NewValidatingBatchingSink(chunks.NewTestStore())
			vbs.Put(EncodeValue(r, nil), r)
			vbs.Flush()
			assert.Panics(t, vbs.PanicIfDangling)
		})
	})

	t.Run("Success", func(t *testing.T) {
		t.Run("BatchInOrder", func(t *testing.T) {
			t.Parallel()
			vbs := NewValidatingBatchingSink(chunks.NewTestStore())
			vbs.Put(EncodeValue(b, nil), b)
			vbs.Put(EncodeValue(r, nil), r)
			assert.NotPanics(t, vbs.PanicIfDangling)
		})
		t.Run("BatchOutOfOrder", func(t *testing.T) {
			t.Parallel()
			vbs := NewValidatingBatchingSink(chunks.NewTestStore())
			vbs.Put(EncodeValue(r, nil), r)
			vbs.Put(EncodeValue(b, nil), b)
			assert.NotPanics(t, vbs.PanicIfDangling)
		})
		t.Run("ExistingChunk", func(t *testing.T) {
			t.Parallel()
			cs := chunks.NewTestStore()
			cs.Put(EncodeValue(b, nil))

			vbs := NewValidatingBatchingSink(cs)
			vbs.Put(EncodeValue(r, nil), r)
			assert.NotPanics(t, vbs.PanicIfDangling)
		})
	})
}
