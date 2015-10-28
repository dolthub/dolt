package newset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestFixedSizeChunker(t *testing.T) {
	assert := assert.New(t)
	r := ref.Ref{}

	chunker := newFixedSizeChunker(1)
	assert.True(chunker.Add(r))
	assert.True(chunker.Add(r))

	chunker = newFixedSizeChunker(3)
	assert.False(chunker.Add(r))
	assert.False(chunker.Add(r))
	assert.True(chunker.Add(r))
	assert.False(chunker.Add(r))
	assert.False(chunker.Add(r))
	assert.True(chunker.Add(r))
}
