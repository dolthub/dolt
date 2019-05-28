package remotestorage

import (
	"errors"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
)

type chunkCache interface {
	Put(c []chunks.Chunk)
	Get(h hash.HashSet) map[hash.Hash]chunks.Chunk
	Has(h hash.HashSet) (absent hash.HashSet)
	PutChunk(chunk *chunks.Chunk) bool
	GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunk
	//IterAll(func(c chunks.Chunk) (stop bool))
	//Clear()
}

var ErrCachedChunkNotFound = errors.New("cached chunk not found")
