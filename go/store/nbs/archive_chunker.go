// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"github.com/dolthub/gozstd"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type ArchiveToChunker struct {
	h          hash.Hash
	dictionary *gozstd.DDict
	chunkData  []byte
}

var _ ToChunker = (*ArchiveToChunker)(nil)

func NewArchiveToChunker(h hash.Hash, dict *gozstd.DDict, chunkData []byte) ToChunker {
	return ArchiveToChunker{h: h, dictionary: dict, chunkData: chunkData}
}

func (a ArchiveToChunker) Hash() hash.Hash {
	return a.h
}

func (a ArchiveToChunker) ToChunk() (chunks.Chunk, error) {
	dict := a.dictionary
	data := a.chunkData
	rawChunk, err := gozstd.DecompressDict(nil, data, dict)
	if err != nil {
		return chunks.EmptyChunk, err
	}

	newChunk := chunks.NewChunk(rawChunk)

	// TODO: remove this once we have confidence in archives.
	if newChunk.Hash() != a.h {
		panic("Hash Mismatch!!")
	}

	return newChunk, err
}

func (a ArchiveToChunker) IsEmpty() bool {
	return len(a.chunkData) == 0
}

func (a ArchiveToChunker) IsGhost() bool {
	// archives are never ghosts. They are only instantiated when the chunk is found.
	return false
}
