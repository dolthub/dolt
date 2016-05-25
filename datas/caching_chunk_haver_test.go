package datas

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/testify/assert"
)

func TestCachingChunkHaver(t *testing.T) {
	assert := assert.New(t)
	ts := chunks.NewTestStore()
	ccs := newCachingChunkHaver(ts)
	input := "abc"

	c := chunks.NewChunk([]byte(input))
	assert.False(ccs.Has(c.Hash()))
	assert.Equal(ts.Hases, 1)
	assert.False(ccs.Has(c.Hash()))
	assert.Equal(ts.Hases, 1)

	ts.Put(c)
	ccs = newCachingChunkHaver(ts)
	assert.True(ccs.Has(c.Hash()))
	assert.Equal(ts.Hases, 2)
	assert.True(ccs.Has(c.Hash()))
	assert.Equal(ts.Hases, 2)
}
