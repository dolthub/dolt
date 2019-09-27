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

// noopChunkCache is a ChunkCache implementation that stores nothing
// ever.  Using a noopChunkCache with a remotestore.DoltChunkStore
// will cause the DoltChunkStore to behave incorrectly when _writing_
// dolt repositories; this should only be used for read-only use
// cases.
var noopChunkCache = &noopChunkCacheImpl{}

type noopChunkCacheImpl struct {
}

func (*noopChunkCacheImpl) Put(chnks []chunks.Chunkable) {
}

func (*noopChunkCacheImpl) Get(hashes hash.HashSet) map[hash.Hash]chunks.Chunkable {
	return make(map[hash.Hash]chunks.Chunkable)
}

func (*noopChunkCacheImpl) Has(hashes hash.HashSet) (absent hash.HashSet) {
	return hashes
}

func (*noopChunkCacheImpl) PutChunk(ch chunks.Chunkable) bool {
	return true
}

func (*noopChunkCacheImpl) GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunkable {
	panic("noopChunkCache does not support GetAndClearChunksToFlush().")
}
