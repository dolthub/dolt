package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

var (
	testVals = []Value{
		Bool(true),
		Number(1),
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
	ref := vs.WriteValue(expected).TargetRef()

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
		v = actual.Get(i)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)
	}
}

func SkipTestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := newLocalValueStore(cs)

	expected := NewSet(testVals...)
	ref := vs.WriteValue(expected).TargetRef()

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
	ref := vs.WriteValue(expected).TargetRef()

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
	ref := vs.WriteValue(expectedItem).TargetRef()

	expected := NewList(NewRef(ref))
	ref = vs.WriteValue(expected).TargetRef()
	actualVar := vs.ReadValue(ref)

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
