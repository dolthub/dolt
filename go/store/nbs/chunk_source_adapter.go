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

package nbs

import (
	"context"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type chunkSourceAdapter struct {
	tableReader
	h hash.Hash
}

func (csa chunkSourceAdapter) hash() hash.Hash {
	return csa.h
}

func (csa chunkSourceAdapter) suffix() string {
	return ""
}

func newReaderFromIndexData(ctx context.Context, q MemoryQuotaProvider, idxData []byte, name hash.Hash, tra tableReaderAt, blockSize uint64) (cs chunkSource, err error) {
	index, err := parseTableIndexByCopy(ctx, idxData, q)
	if err != nil {
		return nil, err
	}

	tr, err := newTableReader(index, tra, blockSize)
	if err != nil {
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

func (csa chunkSourceAdapter) close() error {
	return csa.tableReader.close()
}

func (csa chunkSourceAdapter) clone() (chunkSource, error) {
	tr, err := csa.tableReader.clone()
	if err != nil {
		return &chunkSourceAdapter{}, err
	}
	return &chunkSourceAdapter{tr, csa.h}, nil
}

func (csa chunkSourceAdapter) IterateAllChunksFast(ctx context.Context, cb func(hash.Hash, chunks.Chunk) error, stats *Stats) error {
	// Use the same efficient iteration as fileTableReader since they both wrap tableReader
	count := csa.tableReader.idx.chunkCount()
	
	for i := uint32(0); i < count; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		
		var h hash.Hash
		_, err := csa.tableReader.idx.indexEntry(i, &h)
		if err != nil {
			return err
		}
		
		data, _, err := csa.tableReader.get(ctx, h, nil, stats)
		if err != nil {
			return err
		}
		
		chunk := chunks.NewChunkWithHash(h, data)
		err = cb(h, chunk)
		if err != nil {
			return err
		}
	}
	
	return nil
}
