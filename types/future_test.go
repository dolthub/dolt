package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
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

func TestEqualsFastPath(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	v := Int32(1)
	r, err := WriteValue(v, cs)
	assert.NoError(err)

	fv := futureFromValue(v)
	fr := futureFromRef(r)

	count := 0
	getRefOverride = func(val Value) ref.Ref {
		count += 1
		return getRefNoOverride(val)
	}
	defer func() { getRefOverride = nil }()

	assert.True(futuresEqual(fv, fr))
	assert.True(futuresEqual(fr, fv))
	assert.Equal(2, count)

	_, err = fr.Deref(cs)
	assert.NoError(err)

	count = 0
	assert.True(futuresEqual(fv, fv))
	assert.True(futuresEqual(fv, fr))
	assert.True(futuresEqual(fr, fv))

	assert.Equal(0, count)
}
