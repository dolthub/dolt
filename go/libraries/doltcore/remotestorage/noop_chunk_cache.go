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

// noopChunkCache is a ChunkCache implementation that stores nothing
// ever.  Using a noopChunkCache with a remotestore.DoltChunkStore
// will cause the DoltChunkStore to behave incorrectly when _writing_
// dolt repositories; this should only be used for read-only use
// cases.
var noopChunkCache = &noopChunkCacheImpl{}

type noopChunkCacheImpl struct {
}

func (*noopChunkCacheImpl) Put(chnks []nbs.ToChunker) bool {
	return false
}

func (*noopChunkCacheImpl) Get(hashes hash.HashSet) map[hash.Hash]nbs.ToChunker {
	return make(map[hash.Hash]nbs.ToChunker)
}

func (*noopChunkCacheImpl) Has(hashes hash.HashSet) (absent hash.HashSet) {
	return hashes
}

func (*noopChunkCacheImpl) PutChunk(ch nbs.ToChunker) bool {
	return false
}

func (*noopChunkCacheImpl) GetAndClearChunksToFlush() map[hash.Hash]nbs.ToChunker {
	panic("noopChunkCache does not support GetAndClearChunksToFlush().")
}
