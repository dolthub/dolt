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
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
)

type blobstorePersister struct {
	bs        blobstore.Blobstore
	blockSize uint64
	q         MemoryQuotaProvider
}

var _ tablePersister = &blobstorePersister{}

// Persist makes the contents of mt durable. Chunks already present in
// |haver| may be dropped in the process.
func (bsp *blobstorePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	name, data, chunkCount, err := mt.write(haver, stats)

	if err != nil {
		return emptyChunkSource{}, nil
	}

	if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	_, err = blobstore.PutBytes(ctx, bsp.bs, name.String(), data)

	if err != nil {
		return emptyChunkSource{}, err
	}

	bsTRA := &bsTableReaderAt{name.String(), bsp.bs}
	return newReaderFromIndexData(ctx, bsp.q, data, name, bsTRA, bsp.blockSize)
}

// ConjoinAll (Not currently implemented) conjoins all chunks in |sources| into a single,
// new chunkSource.
func (bsp *blobstorePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	plan, err := planConcatenateConjoin(sources, stats)
	if err != nil {
		return nil, err
	}

	conjoinees := make([]string, 0, len(sources)+1)
	for _, src := range sources {
		conjoinees = append(conjoinees, src.hash().String())
	}

	idxKey := uuid.New().String()
	if _, err = blobstore.PutBytes(ctx, bsp.bs, idxKey, plan.mergedIndex); err != nil {
		return nil, err
	}
	conjoinees = append(conjoinees, idxKey) // mergedIndex goes last

	name := nameFromSuffixes(plan.suffixes())
	if _, err = bsp.bs.Concatenate(ctx, name.String(), conjoinees); err != nil {
		return nil, err
	}
	return newBSChunkSource(ctx, bsp.bs, name, plan.chunkCount, bsp.q, stats)
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *blobstorePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newBSChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, stats)
}

func (bsp *blobstorePersister) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return bsp.bs.Exists(ctx, name.String())
}

func (bsp *blobstorePersister) PruneTableFiles(ctx context.Context, contents manifestContents, t time.Time) error {
	return chunks.ErrUnsupportedOperation
}

func (bsp *blobstorePersister) Close() error {
	return nil
}

type bsTableReaderAt struct {
	key string
	bs  blobstore.Blobstore
}

// ReadAtWithStats is the bsTableReaderAt implementation of the tableReaderAt interface
func (bsTRA *bsTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (int, error) {
	br := blobstore.NewBlobRange(off, int64(len(p)))
	rc, _, err := bsTRA.bs.Get(ctx, bsTRA.key, br)

	if err != nil {
		return 0, err
	}
	defer rc.Close()

	totalRead := 0
	for totalRead < len(p) {
		n, err := rc.Read(p[totalRead:])

		if err != nil && err != io.EOF {
			return 0, err
		}

		totalRead += n

		if err == io.EOF {
			break
		}
	}

	return totalRead, nil
}

func newBSChunkSource(ctx context.Context, bs blobstore.Blobstore, name addr, chunkCount uint32, q MemoryQuotaProvider, stats *Stats) (cs chunkSource, err error) {
	index, err := loadTableIndex(ctx, stats, chunkCount, q, func(p []byte) error {
		rc, _, err := bs.Get(ctx, name.String(), blobstore.NewBlobRange(-int64(len(p)), 0))
		if err != nil {
			return err
		}
		defer rc.Close()

		_, err = io.ReadFull(rc, p)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	tr, err := newTableReader(index, &bsTableReaderAt{name.String(), bs}, s3BlockSize)
	if err != nil {
		_ = index.Close()
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

// planConcatenateConjoin computes a conjoin plan for tablePersisters that conjoin
// by concatenating existing chunk sources (leaving behind old chunk indexes, footers).
func planConcatenateConjoin(sources chunkSources, stats *Stats) (compactionPlan, error) {
	var sized []sourceWithSize
	for _, src := range sources {
		index, err := src.index()
		if err != nil {
			return compactionPlan{}, err
		}
		sized = append(sized, sourceWithSize{src, index.tableFileSize()})
	}
	return planConjoin(sized, stats)
}
