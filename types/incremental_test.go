package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

var (
	testVals = []Value{
		Bool(true),
		Int16(1),
		Int32(1),
		Int64(1),
		UInt16(1),
		UInt32(1),
		UInt64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
		NewBlob([]byte("hi")),
		NewSet(NewString("hi")),
		NewList(NewString("hi")),
		NewMap(NewString("hi"), NewString("hi")),
	}
)

func isEncodedOutOfLine(v Value) int {
	switch v.(type) {
	case Blob, Set, List, Map:
		return 1
	}
	return 0
}

func TestIncrementalLoadList(t *testing.T) {
	assert := assert.New(t)
	cs := &testStore{ChunkStore: &chunks.MemoryStore{}}

	expected := NewList(testVals...)
	ref, err := WriteValue(expected, cs)
	assert.NoError(err)

	actualVar, err := ReadValue(ref, cs)
	assert.NoError(err)
	actual := actualVar.(List)

	expectedCount := cs.count
	assert.Equal(1, expectedCount)
	for i := uint64(0); i < expected.Len(); i++ {
		v := actual.Get(i)
		assert.True(expected.Get(i).Equals(v))

		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.count)

		// Do it again to make sure multiple derefs don't do multiple loads.
		v = actual.Get(i)
		assert.Equal(expectedCount, cs.count)
	}
}

func TestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	cs := &testStore{ChunkStore: &chunks.MemoryStore{}}

	expected := NewSet(testVals...)
	ref, err := WriteValue(expected, cs)
	assert.NoError(err)

	actualVar, err := ReadValue(ref, cs)
	assert.NoError(err)
	actual := actualVar.(Set)

	expectedCount := cs.count
	assert.Equal(1, expectedCount)
	actual.Iter(func(v Value) (stop bool) {
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.count)
		return
	})
}
