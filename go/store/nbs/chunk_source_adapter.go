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

	"github.com/dolthub/dolt/go/store/hash"
)

type chunkSourceAdapter struct {
	tableReader
	h hash.Hash
}

func (csa chunkSourceAdapter) hash() hash.Hash {
	return csa.h
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

func (csa chunkSourceAdapter) getAllChunkHashes(_ context.Context, _ chan hash.Hash) {
	//TODO implement me
	panic("implement me")
}
