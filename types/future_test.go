package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestResolvedFuture(t *testing.T) {
	assert := assert.New(t)
	v := Int32(42)
	f := futureFromValue(v)
	v2 := f.Deref(nil)
	assert.True(v.Equals(v2))
}

func TestUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)

	cs := &chunks.TestStore{}
	v := NewString("hello")
	r := WriteValue(v, cs)

	f := futureFromRef(r)
	v2 := f.Deref(cs)
	assert.Equal(1, cs.Reads)
	assert.True(v.Equals(v2))

	v3 := f.Deref(cs)
	assert.Equal(1, cs.Reads)
	assert.True(v2.Equals(v3))
}

func TestEqualsFastPath(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	v := Int32(1)
	r := WriteValue(v, cs)

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

	fr.Deref(cs)

	count = 0
	assert.True(futuresEqual(fv, fv))
	assert.True(futuresEqual(fv, fr))
	assert.True(futuresEqual(fr, fv))

	assert.Equal(0, count)
}
