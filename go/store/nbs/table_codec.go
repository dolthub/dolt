// Copyright 2026 Dolthub, Inc.
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

// A recordCodec reads the records of a table file that a codec wrote. A table without one stores them as they are.
type recordCodec interface {
	// chunkFromRecord returns chunk |h| from its |record| at |offset|.
	chunkFromRecord(ctx context.Context, tr tableReader, h hash.Hash, offset uint64, record []byte) (CompressedChunk, error)
	// chunkerFromRead returns the |i|th chunk of |rb|, read into |buff|, as it is stored, for writers that copy it.
	chunkerFromRead(ctx context.Context, tr tableReader, rb readBatch, buff []byte, i int) (ToChunker, error)
	// locateRecords is called with the ranges of the records a remote client is sent.
	locateRecords(ctx context.Context, tr tableReader, ranges map[hash.Hash]Range) error
}

// openTableCodec sets the codec of table |tr|, which was opened with |opts|. It closes |tr| if it fails.
var openTableCodec = func(tr tableReader, opts openOpts) (tableReader, error) {
	return tr, nil
}

// tableIndexExtra returns the size of what the table with index |idx| holds beyond its plain index and footer.
var tableIndexExtra = func(idx tableIndex) uint64 {
	return 0
}

// conjoinedTableTail returns what the table conjoining |sources| holds between its index and its footer, or nil.
var conjoinedTableTail = func(sources []sourceWithSize) ([]byte, error) {
	return nil, nil
}

// finishConjoinedTable is called with the |plan| of a conjoined table, once its index is merged, and its |tail|.
var finishConjoinedTable = func(plan *compactionPlan, tail []byte) {}

func (tr tableReader) chunkFromRecord(ctx context.Context, h hash.Hash, offset uint64, record []byte) (CompressedChunk, error) {
	if tr.codec == nil {
		return NewCompressedChunk(h, record)
	}
	return tr.codec.chunkFromRecord(ctx, tr, h, offset, record)
}

func (tr tableReader) extractChunkFromRead(ctx context.Context, rb readBatch, buff []byte, i int) (ToChunker, error) {
	if tr.codec == nil {
		return rb.ExtractChunkFromRead(buff, i)
	}
	return tr.codec.chunkerFromRead(ctx, tr, rb, buff, i)
}

func (tr tableReader) locateRecords(ctx context.Context, ranges map[hash.Hash]Range) error {
	if tr.codec == nil {
		return nil
	}
	return tr.codec.locateRecords(ctx, tr, ranges)
}
