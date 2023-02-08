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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/blobstore"
)

const (
	tableRecordsExt = ".records"
	tableTailExt    = ".tail"
)

type blobstorePersister struct {
	bs        blobstore.Blobstore
	blockSize uint64
	q         MemoryQuotaProvider
}

var _ tablePersister = &blobstorePersister{}
var _ tableFilePersister = &blobstorePersister{}

// Persist makes the contents of mt durable. Chunks already present in
// |haver| may be dropped in the process.
func (bsp *blobstorePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	address, data, chunkCount, err := mt.write(haver, stats)
	if err != nil {
		return emptyChunkSource{}, err
	} else if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}
	name := address.String()

	// persist this table in two parts to facilitate later conjoins
	records, tail := splitTableParts(data, chunkCount)

	// first write table records and tail (index+footer) as separate blobs
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() (err error) {
		_, err = bsp.bs.Put(ectx, name+tableRecordsExt, bytes.NewBuffer(records))
		return
	})
	eg.Go(func() (err error) {
		_, err = bsp.bs.Put(ectx, name+tableTailExt, bytes.NewBuffer(tail))
		return
	})
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	// then concatenate into a final blob
	if _, err = bsp.bs.Concatenate(ctx, name, []string{name + tableRecordsExt, name + tableTailExt}); err != nil {
		return emptyChunkSource{}, err
	}
	rdr := &bsTableReaderAt{name, bsp.bs}
	return newReaderFromIndexData(ctx, bsp.q, data, address, rdr, bsp.blockSize)
}

// ConjoinAll implements tablePersister.
func (bsp *blobstorePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	var sized []sourceWithSize
	for _, src := range sources {
		sized = append(sized, sourceWithSize{src, src.currentSize()})
	}

	plan, err := planConjoin(sized, stats)
	if err != nil {
		return nil, err
	}
	address := nameFromSuffixes(plan.suffixes())
	name := address.String()

	// conjoin must contiguously append the chunk records of |sources|, but the raw content
	// of each source contains a chunk index in the tail. Blobstore does not expose a range
	// copy (GCP Storage limitation), so we must create sub-objects from each source that
	// contain only chunk records. We make an effort to store these sub-objects on Persist(),
	// but we will create them in getRecordsSubObjects if necessary.

	conjoinees := make([]string, 0, len(sources)+1)
	for _, src := range plan.sources.sws {
		sub, err := bsp.getRecordsSubObject(ctx, src.source)
		if err != nil {
			return nil, err
		}
		conjoinees = append(conjoinees, sub)
	}

	// first concatenate all the sub-objects to create a composite sub-object
	if _, err = bsp.bs.Concatenate(ctx, name+tableRecordsExt, conjoinees); err != nil {
		return nil, err
	}
	if _, err = blobstore.PutBytes(ctx, bsp.bs, name+tableTailExt, plan.mergedIndex); err != nil {
		return nil, err
	}
	// then concatenate into a final blob
	if _, err = bsp.bs.Concatenate(ctx, name, []string{name + tableRecordsExt, name + tableTailExt}); err != nil {
		return emptyChunkSource{}, err
	}

	return newBSChunkSource(ctx, bsp.bs, address, plan.chunkCount, bsp.q, stats)
}

func (bsp *blobstorePersister) getRecordsSubObject(ctx context.Context, cs chunkSource) (name string, err error) {
	name = cs.hash().String() + tableRecordsExt
	// first check if we created this sub-object on Persist()
	ok, err := bsp.bs.Exists(ctx, name)
	if err != nil {
		return "", err
	} else if ok {
		return name, nil
	}

	// otherwise create the sub-object from |table|
	// (requires a round-trip for remote blobstores)
	cnt, err := cs.count()
	if err != nil {
		return "", err
	}
	off := tableTailOffset(cs.currentSize(), cnt)
	rng := blobstore.NewBlobRange(0, int64(off))

	rdr, _, err := bsp.bs.Get(ctx, cs.hash().String(), rng)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rdr.Close(); cerr != nil {
			err = cerr
		}
	}()

	if _, err = bsp.bs.Put(ctx, name, rdr); err != nil {
		return "", err
	}
	return name, nil
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *blobstorePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newBSChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, stats)
}

func (bsp *blobstorePersister) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return bsp.bs.Exists(ctx, name.String())
}

func (bsp *blobstorePersister) PruneTableFiles(ctx context.Context, contents manifestContents, t time.Time) error {
	return nil
}

func (bsp *blobstorePersister) Close() error {
	return nil
}

func (bsp *blobstorePersister) Path() string {
	return ""
}

func (bsp *blobstorePersister) CopyTableFile(ctx context.Context, r io.ReadCloser, name string, fileSz uint64, chunkCount uint32) (err error) {
	defer func() {
		if cerr := r.Close(); cerr != nil {
			err = cerr
		}
	}()

	// sanity check file size
	if fileSz < indexSize(chunkCount)+footerSize {
		return fmt.Errorf("table file size %d too small for chunk count %d", fileSz, chunkCount)
	}

	off := int64(tableTailOffset(fileSz, chunkCount))
	lr := io.LimitReader(r, off)

	// check if we can Put concurrently
	rr, ok := r.(io.ReaderAt)
	if !ok {
		// sequentially write chunk records then tail
		if _, err = bsp.bs.Put(ctx, name+tableRecordsExt, lr); err != nil {
			return err
		}
		if _, err = bsp.bs.Put(ctx, name+tableTailExt, r); err != nil {
			return err
		}
	} else {
		// on the push path, we expect to Put concurrently
		// see BufferedFileByteSink in byte_sink.go
		eg, ectx := errgroup.WithContext(ctx)
		eg.Go(func() (err error) {
			buf := make([]byte, indexSize(chunkCount)+footerSize)
			if _, err = rr.ReadAt(buf, off); err != nil {
				return err
			}
			_, err = bsp.bs.Put(ectx, name+tableTailExt, bytes.NewBuffer(buf))
			return
		})
		eg.Go(func() (err error) {
			_, err = bsp.bs.Put(ectx, name+tableRecordsExt, lr)
			return
		})
		if err = eg.Wait(); err != nil {
			return err
		}
	}

	// finally concatenate into the complete table
	_, err = bsp.bs.Concatenate(ctx, name, []string{name + tableRecordsExt, name + tableTailExt})
	return
}

type bsTableReaderAt struct {
	key string
	bs  blobstore.Blobstore
}

func (bsTRA *bsTableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	rc, _, err := bsTRA.bs.Get(ctx, bsTRA.key, blobstore.AllRange)
	return rc, err
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

	if chunkCount != index.chunkCount() {
		return nil, errors.New("unexpected chunk count")
	}

	tr, err := newTableReader(index, &bsTableReaderAt{name.String(), bs}, s3BlockSize)
	if err != nil {
		_ = index.Close()
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

// splitTableParts separates a table into chunk records and meta data.
//
//	              +----------------------+-------+--------+
//	table format: | Chunk Record 0 ... N | Index | Footer |
//	              +----------------------+-------+--------+
func splitTableParts(data []byte, count uint32) (records, tail []byte) {
	o := tableTailOffset(uint64(len(data)), count)
	records, tail = data[:o], data[o:]
	return
}

func tableTailOffset(size uint64, count uint32) uint64 {
	return size - (indexSize(count) + footerSize)
}
