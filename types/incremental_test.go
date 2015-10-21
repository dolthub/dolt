package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

var (
	testVals = []Value{
		Bool(true),
		Int8(1),
		Int16(1),
		Int32(1),
		Int64(1),
		UInt8(1),
		UInt16(1),
		UInt32(1),
		UInt64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
		newBlobLeaf([]byte("hi")),
		// compoundBlob
		NewSet(NewString("hi")),
		NewList(NewString("hi")),
		NewMap(NewString("hi"), NewString("hi")),
	}
)

func isEncodedOutOfLine(v Value) int {
	switch v.(type) {
	case blobLeaf, compoundBlob, Set, List, Map:
		return 1
	}
	return 0
}

func SkipTestIncrementalLoadList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()

	expected := NewList(testVals...)
	ref := WriteValue(expected, cs)

	actualVar := ReadValue(ref, cs)
	actual := actualVar.(List)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	// There will be one read per chunk.
	chunkReads := make([]int, expected.Len())
	if cl, ok := actual.(compoundList); ok {
		reads := 0
		start := uint64(0)
		for _, end := range cl.offsets {
			reads++
			for i := start; i < end; i++ {
				chunkReads[i] = reads
			}
			start = end
		}
	}
	for i := uint64(0); i < expected.Len(); i++ {
		v := actual.Get(i)
		assert.True(expected.Get(i).Equals(v))

		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)

		// Do it again to make sure multiple derefs don't do multiple loads.
		v = actual.Get(i)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)
	}
}

func SkipTestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()

	expected := NewSet(testVals...)
	ref := WriteValue(expected, cs)

	actualVar := ReadValue(ref, cs)
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

	expected := NewMap(testVals...)
	ref := WriteValue(expected, cs)

	actualVar := ReadValue(ref, cs)
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

	expectedItem := UInt32(42)
	ref := WriteValue(expectedItem, cs)

	expected := NewList(Ref{ref})
	ref = WriteValue(expected, cs)
	actualVar := ReadValue(ref, cs)

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
