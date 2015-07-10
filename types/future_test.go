package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestResolvedFuture(t *testing.T) {
	assert := assert.New(t)
	v := Int32(42)
	f := futureFromValue(v)
	v2, err := f.Deref(nil)
	assert.NoError(err)
	assert.True(v.Equals(v2))
}

func TestUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)

	cs := &testStore{ChunkStore: &chunks.MemoryStore{}}
	v := NewString("hello")
	r, _ := WriteValue(v, cs)

	f := futureFromRef(r)
	v2, err := f.Deref(cs)
	assert.Equal(1, cs.count)
	assert.NoError(err)
	assert.True(v.Equals(v2))

	v3, err := f.Deref(cs)
	assert.Equal(1, cs.count)
	assert.NoError(err)
	assert.True(v2.Equals(v3))
}
