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
	"errors"
	"io"
	"time"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
)

type blobstorePersister struct {
	bs         blobstore.Blobstore
	blockSize  uint64
	indexCache *indexCache
}

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
	return newReaderFromIndexData(bsp.indexCache, data, name, bsTRA, bsp.blockSize)
}

// ConjoinAll (Not currently implemented) conjoins all chunks in |sources| into a single,
// new chunkSource.
func (bsp *blobstorePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	return emptyChunkSource{}, nil
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *blobstorePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newBSChunkSource(ctx, bsp.bs, name, chunkCount, bsp.blockSize, bsp.indexCache, stats)
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

func newBSChunkSource(ctx context.Context, bs blobstore.Blobstore, name addr, chunkCount uint32, blockSize uint64, indexCache *indexCache, stats *Stats) (cs chunkSource, err error) {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer func() {
			unlockErr := indexCache.unlockEntry(name)

			if err != nil {
				err = unlockErr
			}
		}()

		if index, found := indexCache.get(name); found {
			bsTRA := &bsTableReaderAt{name.String(), bs}
			tr, err := newTableReader(index, bsTRA, blockSize)
			if err != nil {
				return nil, err
			}
			return &chunkSourceAdapter{tr, name}, nil
		}
	}

	t1 := time.Now()
	indexBytes, tra, err := func() ([]byte, tableReaderAt, error) {
		size := int64(indexSize(chunkCount) + footerSize)
		key := name.String()
		buff, _, err := blobstore.GetBytes(ctx, bs, key, blobstore.NewBlobRange(-size, 0))

		if err != nil {
			return nil, nil, err
		}

		if size != int64(len(buff)) {
			return nil, nil, errors.New("failed to read all data")
		}

		return buff, &bsTableReaderAt{key, bs}, nil
	}()

	if err != nil {
		return nil, err
	}

	stats.IndexBytesPerRead.Sample(uint64(len(indexBytes)))
	stats.IndexReadLatency.SampleTimeSince(t1)

	index, err := parseTableIndex(indexBytes)

	if err != nil {
		return nil, err
	}

	if indexCache != nil {
		indexCache.put(name, index)
	}

	tr, err := newTableReader(index, tra, s3BlockSize)
	if err != nil {
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

func (bsp *blobstorePersister) PruneTableFiles(ctx context.Context, contents manifestContents) error {
	return chunks.ErrUnsupportedOperation
}
