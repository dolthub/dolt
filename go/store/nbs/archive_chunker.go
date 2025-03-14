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

// DecompBundle is a bundle of a dictionary and its raw bytes. This is necesary because we sometimes need to copy
// the raw dictionary from one archive to another. The C interface around zStd objects doesn't give us a way to
// get the raw dictionary bytes, so we'll use this struct as the primary interface to pass around dictionaries.
type DecompBundle struct {
	dictionary    *gozstd.DDict
	rawDictionary *[]byte
}

// NewDecompBundle creates a new DecompBundle from a zStd compressed dictionary. The input should be the same
// bytes we store on disk and transport over the wire. The uncompressed form is preserved in the result.
func NewDecompBundle(compressedDict []byte) (*DecompBundle, error) {
	// Standard zStd decompression. No dictionary for dictionaries.
	rawDict, err := gozstd.Decompress(nil, compressedDict)
	if err != nil {
		return nil, err
	}

	dict, err := gozstd.NewDDict(rawDict)
	if err != nil {
		return nil, err
	}

	return &DecompBundle{dictionary: dict, rawDictionary: &rawDict}, nil
}

type ArchiveToChunker struct {
	h    hash.Hash
	dict *DecompBundle
	// The chunk data in it's compressed form, using the dict
	chunkData []byte
}

var _ ToChunker = (*ArchiveToChunker)(nil)

func NewArchiveToChunker(h hash.Hash, dict *DecompBundle, chunkData []byte) ToChunker {
	return ArchiveToChunker{
		h:         h,
		dict:      dict,
		chunkData: chunkData}
}

func (a ArchiveToChunker) Hash() hash.Hash {
	return a.h
}

func (a ArchiveToChunker) ToChunk() (chunks.Chunk, error) {
	dict := a.dict.dictionary
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
