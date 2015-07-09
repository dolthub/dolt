package test

import (
	"io"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

type testSource struct {
	chunks.ChunkStore
	count int
}

func (s *testSource) Get(ref ref.Ref) (io.ReadCloser, error) {
	s.count += 1
	return s.ChunkStore.Get(ref)
}

func TestResolvedFuture(t *testing.T) {
	assert := assert.New(t)
	v := types.Int32(42)
	f := types.FutureFromValue(v)
	v2, err := f.Deref(nil)
	assert.NoError(err)
	assert.True(v.Equals(v2))
}

func TestUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)

	cs := &testSource{ChunkStore: &chunks.MemoryStore{}}
	v := types.NewString("hello")
	r, _ := types.WriteValue(v, cs)

	f := types.FutureFromRef(r)
	v2, err := f.Deref(cs)
	assert.Equal(1, cs.count)
	assert.NoError(err)
	assert.True(v.Equals(v2))

	v3, err := f.Deref(cs)
	assert.Equal(1, cs.count)
	assert.NoError(err)
	assert.True(v2.Equals(v3))
}
