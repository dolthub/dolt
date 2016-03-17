package datas

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestHasCachingChunkStore(t *testing.T) {
	assert := assert.New(t)
	ts := chunks.NewTestStore()
	ccs := newHasCachingChunkStore(ts)
	input := "abc"

	c := chunks.NewChunk([]byte(input))
	c1 := ccs.Get(c.Ref())
	assert.True(c1.IsEmpty())

	assert.False(ccs.Has(c.Ref()))
	assert.Equal(ts.Hases, 0)

	ccs.Put(c)
	assert.True(ccs.Has(c.Ref()))
	assert.Equal(ts.Hases, 0)

	c1 = ccs.Get(c.Ref())
	assert.False(c1.IsEmpty())

	assert.True(ccs.Has(c.Ref()))
	assert.Equal(ts.Hases, 0)

	c = chunks.NewChunk([]byte("stuff"))
	assert.False(ccs.Has(c.Ref()))
	assert.Equal(ts.Hases, 1)
}
