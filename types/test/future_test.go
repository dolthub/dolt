package test

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestResolvedFuture(t *testing.T) {
	assert := assert.New(t)

	v := types.Int32(42)
	f := types.FutureFromValue(v)
	v2, err := f.Deref()
	assert.NoError(err)
	assert.True(v.Equals(v2))
	assert.True(f.Equals(v))
	// TODO: ? Should Future<T> and T be equal?
	// Right now they aren't because the primitives compare by primitive equality, not by computing refs. They'd have to special case the future case, or else be slower when comparing to another primitive.
	// assert.True(v.Equals(f))
}

func TestUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)

	cs := &chunks.MemoryStore{}
	v := types.NewString("hello")
	r, _ := enc.WriteValue(v, cs)

	numResolves := 0
	res := func(r ref.Ref) (types.Value, error) {
		numResolves += 1
		return enc.ReadValue(r, cs)
	}

	f := types.FutureFromRef(r, res)
	v2, err := f.Deref()
	assert.Equal(1, numResolves)
	assert.NoError(err)
	assert.True(v.Equals(v2))
	assert.True(f.Equals(v))
	assert.True(v.Equals(f))

	v3, err := f.Deref()
	assert.Equal(1, numResolves)
	assert.NoError(err)
	assert.True(v2.Equals(v3))
}
