package test

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestIncrementalLoad(t *testing.T) {
	assert := assert.New(t)
	cs := &testStore{ChunkStore: &chunks.MemoryStore{}}

	expected := NewList(
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
		NewMap(NewString("hi"), NewString("hi")))

	ref, err := WriteValue(expected, cs)
	assert.NoError(err)

	actualVar, err := ReadValue(ref, cs)
	assert.NoError(err)
	actual := actualVar.(List)

	// NOTE: No equals test here because Ref() ends up expanding entire tree, which would kill the point of the test.

	prev := cs.count
	assert.Equal(1, prev)
	for i := uint64(0); i < expected.Len(); i++ {
		v := actual.Get(i)
		assert.True(expected.Get(i).Equals(v))

		next := prev
		switch v.(type) {
		case Blob, Set, List, Map:
			next += 1
		}

		assert.Equal(next, cs.count)

		// Do it again to make sure multiple derefs don't change anything.
		v = actual.Get(i)
		assert.Equal(next, cs.count)

		prev = next
	}
}
