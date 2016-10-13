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
