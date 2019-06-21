package remotestorage

import (
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// chunkCache is an interface used for caching chunks
type chunkCache interface {
	// Put puts a slice of chunks into the cache.
	Put(c []chunks.Chunk)

	// Get gets a map of hash to chunk for a set of hashes.  In the event that a chunk is not in the cache, chunks.Empty.
	// is put in it's place
	Get(h hash.HashSet) map[hash.Hash]chunks.Chunk

	// Has takes a set of hashes and returns the set of hashes that the cache currently does not have in it.
	Has(h hash.HashSet) (absent hash.HashSet)

	// PutChunk puts a single chunk in the cache.  true returns in the event that the chunk was cached successfully
	// and false is returned if that chunk is already is the cache.
	PutChunk(chunk *chunks.Chunk) bool

	// GetAndClearChunksToFlush gets a map of hash to chunk which includes all the chunks that were put in the cache
	// between the last time GetAndClearChunksToFlush was called and now.
	GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunk
}
