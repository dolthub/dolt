// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
)

var (
	testVals = []Value{
		Bool(true),
		Number(1),
		String("hi"),
		NewBlob(bytes.NewReader([]byte("hi"))),
		// compoundBlob
		NewSet(String("hi")),
		NewList(String("hi")),
		NewMap(String("hi"), String("hi")),
	}
)

func isEncodedOutOfLine(v Value) int {
	switch v.(type) {
	case Ref:
		return 1
	}
	return 0
}

func TestIncrementalLoadList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)

	expected := NewList(testVals...)
	ref := vs.WriteValue(expected).TargetHash()

	actualVar := vs.ReadValue(ref)
	actual := actualVar.(List)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	// There will be one read per chunk.
	chunkReads := make([]int, expected.Len())
	for i := uint64(0); i < expected.Len(); i++ {
		v := actual.Get(i)
		assert.True(expected.Get(i).Equals(v))

		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)

		// Do it again to make sure multiple derefs don't do multiple loads.
		_ = actual.Get(i)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)
	}
}

func SkipTestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)

	expected := NewSet(testVals...)
	ref := vs.WriteValue(expected).TargetHash()

	actualVar := vs.ReadValue(ref)
	actual := actualVar.(Set)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	actual.Iter(func(v Value) (stop bool) {
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads)
		return
	})
}

func SkipTestIncrementalLoadMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)

	expected := NewMap(testVals...)
	ref := vs.WriteValue(expected).TargetHash()

	actualVar := vs.ReadValue(ref)
	actual := actualVar.(Map)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	actual.Iter(func(k, v Value) (stop bool) {
		expectedCount += isEncodedOutOfLine(k)
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads)
		return
	})
}

func SkipTestIncrementalAddRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)

	expectedItem := Number(42)
	ref := vs.WriteValue(expectedItem)

	expected := NewList(ref)
	ref = vs.WriteValue(expected)
	actualVar := vs.ReadValue(ref.TargetHash())

	assert.Equal(1, cs.Reads)
	assert.True(expected.Equals(actualVar))

	actual := actualVar.(List)
	actualItem := actual.Get(0)
	assert.Equal(2, cs.Reads)
	assert.True(expectedItem.Equals(actualItem))

	// do it again to make sure caching works.
	actualItem = actual.Get(0)
	assert.Equal(2, cs.Reads)
	assert.True(expectedItem.Equals(actualItem))
}
