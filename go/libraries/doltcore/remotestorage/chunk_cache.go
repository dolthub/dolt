// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

// chunkCache is an interface used for caching chunks
type chunkCache interface {
	// Put puts a slice of chunks into the cache.
	Put(c []chunks.Chunkable)

	// Get gets a map of hash to chunk for a set of hashes.  In the event that a chunk is not in the cache, chunks.Empty.
	// is put in it's place
	Get(h hash.HashSet) map[hash.Hash]chunks.Chunkable

	// Has takes a set of hashes and returns the set of hashes that the cache currently does not have in it.
	Has(h hash.HashSet) (absent hash.HashSet)

	// PutChunk puts a single chunk in the cache.  true returns in the event that the chunk was cached successfully
	// and false is returned if that chunk is already is the cache.
	PutChunk(chunk chunks.Chunkable) bool

	// GetAndClearChunksToFlush gets a map of hash to chunk which includes all the chunks that were put in the cache
	// between the last time GetAndClearChunksToFlush was called and now.
	GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunkable
}
