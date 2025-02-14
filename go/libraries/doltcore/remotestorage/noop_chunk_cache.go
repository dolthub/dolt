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
// ever. This causes all fetches to go to the remote server.
var noopChunkCache = &noopChunkCacheImpl{}

type noopChunkCacheImpl struct {
}

func (*noopChunkCacheImpl) InsertChunks(cs []nbs.ToChunker) {
}

func (*noopChunkCacheImpl) GetCachedChunks(h hash.HashSet) map[hash.Hash]nbs.ToChunker {
	return nil
}

func (*noopChunkCacheImpl) InsertHas(h hash.HashSet) {
}

func (*noopChunkCacheImpl) GetCachedHas(h hash.HashSet) (absent hash.HashSet) {
	return h
}
