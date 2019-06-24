package nbs

import (
	"context"
	"io"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/blobstore"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type blobstorePersister struct {
	bs         blobstore.Blobstore
	blockSize  uint64
	indexCache *indexCache
}

// Persist makes the contents of mt durable. Chunks already present in
// |haver| may be dropped in the process.
func (bsp *blobstorePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) chunkSource {
	name, data, chunkCount := mt.write(haver, stats)
	if chunkCount == 0 {
		return emptyChunkSource{}
	}

	_, err := blobstore.PutBytes(ctx, bsp.bs, name.String(), data)
	d.PanicIfError(err)

	bsTRA := &bsTableReaderAt{name.String(), bsp.bs}
	return newReaderFromIndexData(bsp.indexCache, data, name, bsTRA, bsp.blockSize)
}

// ConjoinAll (Not currently implemented) conjoins all chunks in |sources| into a single,
// new chunkSource.
func (bsp *blobstorePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) chunkSource {
	return emptyChunkSource{}
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *blobstorePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) chunkSource {
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

func newBSChunkSource(ctx context.Context, bs blobstore.Blobstore, name addr, chunkCount uint32, blockSize uint64, indexCache *indexCache, stats *Stats) chunkSource {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer indexCache.unlockEntry(name)
		if index, found := indexCache.get(name); found {
			bsTRA := &bsTableReaderAt{name.String(), bs}
			return &chunkSourceAdapter{newTableReader(index, bsTRA, blockSize), name}
		}
	}

	t1 := time.Now()
	indexBytes, tra := func() ([]byte, tableReaderAt) {
		size := int64(indexSize(chunkCount) + footerSize)
		key := name.String()
		buff, _, err := blobstore.GetBytes(ctx, bs, key, blobstore.NewBlobRange(-size, 0))
		d.PanicIfError(err)
		d.PanicIfFalse(size == int64(len(buff)))
		return buff, &bsTableReaderAt{key, bs}
	}()
	stats.IndexBytesPerRead.Sample(uint64(len(indexBytes)))
	stats.IndexReadLatency.SampleTimeSince(t1)

	index := parseTableIndex(indexBytes)
	if indexCache != nil {
		indexCache.put(name, index)
	}
	return &chunkSourceAdapter{newTableReader(index, tra, s3BlockSize), name}
}
