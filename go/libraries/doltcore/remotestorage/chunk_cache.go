// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotestorage

import (
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// ChunkCache is an interface used for caching chunks
type ChunkCache interface {
	// Put puts a slice of chunks into the cache.
	Put(c []nbs.ToChunker) bool

	// Get gets a map of hash to chunk for a set of hashes.  In the event that a chunk is not in the cache, chunks.Empty.
	// is put in it's place
	Get(h hash.HashSet) map[hash.Hash]nbs.ToChunker

	// Has takes a set of hashes and returns the set of hashes that the cache currently does not have in it.
	Has(h hash.HashSet) (absent hash.HashSet)

	// PutChunk puts a single chunk in the cache.  true returns in the event that the chunk was cached successfully
	// and false is returned if that chunk is already is the cache.
	PutChunk(chunk nbs.ToChunker) bool

	// GetAndClearChunksToFlush gets a map of hash to chunk which includes all the chunks that were put in the cache
	// between the last time GetAndClearChunksToFlush was called and now.
	GetAndClearChunksToFlush() map[hash.Hash]nbs.ToChunker
}
