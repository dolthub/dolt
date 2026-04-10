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
	"bytes"
	"context"
	"io"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// singleBlobBSPersister writes table files as single blobs (no .records/.tail split)
// while still supporting ConjoinAll. This avoids intermediate blobs in the git tree
// that the standard blobstorePersister creates.
type singleBlobBSPersister struct {
	bs        blobstore.Blobstore
	q         MemoryQuotaProvider
	blockSize uint64
}

var _ tablePersister = &singleBlobBSPersister{}
var _ tableFilePersister = &singleBlobBSPersister{}

func (bsp *singleBlobBSPersister) Persist(ctx context.Context, behavior dherrors.FatalBehavior, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	address, data, _, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	} else if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	} else if chunkCount == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}
	name := address.String()

	_, err = bsp.bs.Put(ctx, name, int64(len(data)), bytes.NewBuffer(data))
	if err != nil {
		return nil, gcBehavior_Continue, err
	}

	rdr := &bsTableReaderAt{key: name, bs: bsp.bs}
	src, err := newReaderFromIndexData(ctx, bsp.q, data, address, rdr, bsp.blockSize)
	if err != nil {
		return nil, gcBehavior_Continue, err
	}
	return src, gcBehavior_Continue, nil
}

func (bsp *singleBlobBSPersister) ConjoinAll(ctx context.Context, behavior dherrors.FatalBehavior, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	plan, err := planRangeCopyConjoin(ctx, sources, bsp.q, stats)
	if err != nil {
		return nil, nil, err
	}
	defer plan.closer()

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, nil, nil
	}

	name := plan.name.String() + plan.suffix

	// Read chunk records from each source via range reads and stream them
	// together with the merged index into a single blob. No intermediate
	// .records or .tail blobs are created.
	readers := make([]io.Reader, 0, len(plan.sources.sws)+1)
	closers := make([]io.Closer, 0, len(plan.sources.sws))

	for _, sws := range plan.sources.sws {
		srcName := sws.source.hash().String() + sws.source.suffix()
		dataLen := int64(sws.dataLen)
		rng := blobstore.NewBlobRange(0, dataLen)
		rdr, _, _, err := bsp.bs.Get(ctx, srcName, rng)
		if err != nil {
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, err
		}
		closers = append(closers, rdr)
		readers = append(readers, io.LimitReader(rdr, dataLen))
	}

	readers = append(readers, bytes.NewReader(plan.mergedIndex))
	totalSize := int64(plan.totalCompressedData) + int64(len(plan.mergedIndex))

	_, err = bsp.bs.Put(ctx, name, totalSize, io.MultiReader(readers...))
	for _, c := range closers {
		c.Close()
	}
	if err != nil {
		return nil, nil, err
	}

	var cs chunkSource
	if plan.suffix == ArchiveFileSuffix {
		cs, err = newBSArchiveChunkSource(ctx, bsp.bs, plan.name, bsp.q, stats)
	} else {
		cs, err = newBSTableChunkSource(ctx, bsp.bs, plan.name, plan.chunkCount, bsp.q, stats)
	}

	return cs, func() {}, err
}

func (bsp *singleBlobBSPersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	cs, err := newBSTableChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, stats)
	if err == nil {
		return cs, nil
	}

	if blobstore.IsNotFoundError(err) {
		return newBSArchiveChunkSource(ctx, bsp.bs, name, bsp.q, stats)
	}

	return nil, err
}

func (bsp *singleBlobBSPersister) Exists(ctx context.Context, name string, _ uint32, _ *Stats) (bool, io.Closer, error) {
	exists, err := bsp.bs.Exists(ctx, name)
	if err != nil {
		return false, nil, err
	}
	if exists {
		return true, noopPendingHandle{}, nil
	}
	return false, nil, nil
}

func (bsp *singleBlobBSPersister) PruneTableFiles(ctx context.Context) error {
	return nil
}

func (bsp *singleBlobBSPersister) Close() error {
	if c, ok := bsp.bs.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (bsp *singleBlobBSPersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func (bsp *singleBlobBSPersister) Path() string {
	return ""
}

func (bsp *singleBlobBSPersister) CopyTableFile(ctx context.Context, r io.Reader, name string, fileSz uint64, _ uint64) (io.Closer, error) {
	_, err := bsp.bs.Put(ctx, name, int64(fileSz), r)
	return noopPendingHandle{}, err
}
