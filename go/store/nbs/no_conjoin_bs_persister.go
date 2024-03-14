// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"io"
	"time"

	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type noConjoinBlobstorePersister struct {
	bs        blobstore.Blobstore
	blockSize uint64
	q         MemoryQuotaProvider
}

var _ tablePersister = &noConjoinBlobstorePersister{}
var _ tableFilePersister = &noConjoinBlobstorePersister{}

// Persist makes the contents of mt durable. Chunks already present in
// |haver| may be dropped in the process.
func (bsp *noConjoinBlobstorePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	address, data, chunkCount, err := mt.write(haver, stats)
	if err != nil {
		return emptyChunkSource{}, err
	} else if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}
	name := address.String()

	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() (err error) {
		fmt.Fprintf(color.Output, "Persist: bs.Put: name: %s\n", name)
		_, err = bsp.bs.Put(ectx, name, int64(len(data)), bytes.NewBuffer(data))
		return
	})
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	rdr := &bsTableReaderAt{name, bsp.bs}
	return newReaderFromIndexData(ctx, bsp.q, data, address, rdr, bsp.blockSize)
}

// ConjoinAll implements tablePersister.
func (bsp *noConjoinBlobstorePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	return emptyChunkSource{}, func() {}, fmt.Errorf("no conjoin blobstore persister does not implement ConjoinAll")
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *noConjoinBlobstorePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newBSChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, stats)
}

func (bsp *noConjoinBlobstorePersister) Exists(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (bool, error) {
	return bsp.bs.Exists(ctx, name.String())
}

func (bsp *noConjoinBlobstorePersister) PruneTableFiles(ctx context.Context, keeper func() []hash.Hash, t time.Time) error {
	return nil
}

func (bsp *noConjoinBlobstorePersister) Close() error {
	return nil
}

func (bsp *noConjoinBlobstorePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func (bsp *noConjoinBlobstorePersister) Path() string {
	return ""
}

func (bsp *noConjoinBlobstorePersister) CopyTableFile(ctx context.Context, r io.Reader, name string, fileSz uint64, chunkCount uint32) error {
	// sanity check file size
	if fileSz < indexSize(chunkCount)+footerSize {
		return fmt.Errorf("table file size %d too small for chunk count %d", fileSz, chunkCount)
	}

	_, err := bsp.bs.Put(ctx, name, int64(fileSz), r)
	return err
}
