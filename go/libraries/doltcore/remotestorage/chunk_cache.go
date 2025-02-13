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

// ChunkCache is an interface used for caching chunks and has presence that
// has already been fetched from remotestorage. Care should be taken when
// using ChunkCache if it is possible for the remote to GC, since in that
// case the cache could contain stale data.
type ChunkCache interface {
	// Insert some observed / fetched chunks into the cached. These
	// chunks may or may not be returned in the future.
	InsertChunks(cs []nbs.CompressedChunk)
	// Get previously cached chunks, if they are still available.
	GetCachedChunks(h hash.HashSet) map[hash.Hash]nbs.CompressedChunk

	// Insert all hashes in |h| as existing in the remote.
	InsertHas(h hash.HashSet)
	// Returns the absent set from |h|, filtering it by records
	// which are known to be present in the remote based on
	// previous |InsertHas| calls.
	GetCachedHas(h hash.HashSet) (absent hash.HashSet)
}
