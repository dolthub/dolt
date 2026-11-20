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

	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	tableRecordsExt = ".records"
	tableTailExt    = ".tail"
)

type blobstorePersister struct {
	bs        blobstore.Blobstore
	q         MemoryQuotaProvider
	blockSize uint64
}

var _ tablePersister = &blobstorePersister{}
var _ tableFilePersister = &blobstorePersister{}

// Persist makes the contents of mt durable. Chunks already present in
// |haver| may be dropped in the process.
func (bsp *blobstorePersister) Persist(ctx context.Context, behavior dherrors.FatalBehavior, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	address, data, splitOffset, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	}
	if chunkCount == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}
	name := address.String()

	// persist this table in two parts to facilitate later conjoins
	records, tail := data[:splitOffset], data[splitOffset:]

	recordsName := name + tableRecordsExt
	tailName := name + tableTailExt

	// first write table records and tail (index+footer) as separate blobs
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		_, err := bsp.bs.Put(ectx, recordsName, int64(len(records)), bytes.NewBuffer(records))
		return err
	})
	eg.Go(func() error {
		_, err := bsp.bs.Put(ectx, tailName, int64(len(tail)), bytes.NewBuffer(tail))
		return err
	})
	if err = eg.Wait(); err != nil {
		return nil, gcBehavior_Continue, err
	}

	// then concatenate into a final blob
	if _, err = bsp.bs.Concatenate(ctx, name, []string{recordsName, tailName}); err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}

	src, err := newPersistedBSTableChunkSource(ctx, bsp.bs, bsp.q, data, address, bsp.blockSize)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	return src, gcBehavior_Continue, nil
}

// newPersistedBSTableChunkSource uses the table bytes already available at persist
// time to spool stores with expensive ranged reads, avoiding a blob readback.
func newPersistedBSTableChunkSource(ctx context.Context, bs blobstore.Blobstore, q MemoryQuotaProvider, data []byte, name hash.Hash, blockSize uint64) (chunkSource, error) {
	var rdr tableReaderAt = &bsTableReaderAt{key: name.String(), bs: bs}
	if shouldSpool(bs) {
		spooled, err := newPersistedSpoolingTableReaderAt(bs, name.String(), data)
		if err != nil {
			return nil, err
		}
		rdr = spooled
	}
	src, err := newReaderFromIndexData(ctx, q, data, name, rdr, blockSize)
	if err != nil {
		return nil, errors.Join(err, rdr.Close())
	}
	return src, nil
}

// ConjoinAll implements tablePersister.
func (bsp *blobstorePersister) ConjoinAll(ctx context.Context, behavior dherrors.FatalBehavior, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	plan, err := planRangeCopyConjoin(ctx, sources, bsp.q, stats)
	if err != nil {
		return nil, nil, err
	}
	defer plan.closer()

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, nil, nil
	}

	name := plan.name.String() + plan.suffix

	// conjoin must contiguously append the chunk records of |sources|, but the raw content
	// of each source contains a chunk index in the tail. Blobstore does not expose a range
	// copy (GCP Storage limitation), so we must create sub-objects from each source that
	// contain only chunk records. We make an effort to store these sub-objects on Persist(),
	// but we will create them in getRecordsSubObjects if necessary.

	conjoinees := make([]string, 0, len(sources)+1)
	for _, src := range plan.sources.sws {
		sub, err := bsp.getRecordsSubObject(ctx, src.source)
		if err != nil {
			return nil, nil, err
		}
		conjoinees = append(conjoinees, sub)
	}

	// first concatenate all the sub-objects to create a composite sub-object
	recordsName := name + tableRecordsExt
	tailName := name + tableTailExt
	if _, err = bsp.bs.Concatenate(ctx, recordsName, conjoinees); err != nil {
		return nil, nil, err
	}
	if _, err = blobstore.PutBytes(ctx, bsp.bs, tailName, plan.mergedIndex); err != nil {
		return nil, nil, err
	}
	// then concatenate into a final blob
	if _, err = bsp.bs.Concatenate(ctx, name, []string{recordsName, tailName}); err != nil {
		return emptyChunkSource{}, nil, err
	}

	var cs chunkSource
	if plan.suffix == ArchiveFileSuffix {
		cs, err = newBSArchiveChunkSource(ctx, bsp.bs, plan.name, plan.chunkCount, bsp.q, openOpts{deepValidate: true}, stats)
	} else {
		cs, err = newBSTableChunkSource(ctx, bsp.bs, plan.name, plan.chunkCount, bsp.q, openOpts{deepValidate: true}, stats)
	}

	return cs, func() {}, err
}

func (bsp *blobstorePersister) getRecordsSubObject(ctx context.Context, cs chunkSource) (name string, err error) {
	name = cs.hash().String() + cs.suffix() + tableRecordsExt
	// first check if we created this sub-object on Persist()
	ok, err := bsp.bs.Exists(ctx, name)
	if err != nil {
		return "", err
	} else if ok {
		return name, nil
	}

	// otherwise create the sub-object from |table|
	// (requires a round-trip for remote blobstores)
	if cs.suffix() == ArchiveFileSuffix {
		err = bsp.hotCreateArchiveRecords(ctx, cs)
	} else {
		err = bsp.hotCreateTableRecords(ctx, cs)
	}

	return name, err
}

func (bsp *blobstorePersister) hotCreateTableRecords(ctx context.Context, cs chunkSource) error {
	cnt := cs.count()
	off := tableTailOffset(cs.currentSize(), cnt)
	l := int64(off)
	rng := blobstore.NewBlobRange(0, l)

	rdr, _, _, err := bsp.bs.Get(ctx, cs.hash().String(), rng)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := rdr.Close(); cerr != nil {
			err = cerr
		}
	}()

	_, err = bsp.bs.Put(ctx, cs.hash().String()+tableRecordsExt, l, rdr)
	return err
}

func (bsp *blobstorePersister) hotCreateArchiveRecords(ctx context.Context, cs chunkSource) error {
	arch, ok := cs.(*archiveChunkSource)
	if !ok {
		return errors.New("runtime error: hotCreateArchiveRecords expected archiveChunkSource")
	}

	dataLen := int64(arch.aRdr.footer.dataSpan().length)
	rng := blobstore.NewBlobRange(0, dataLen)

	rdr, _, _, err := bsp.bs.Get(ctx, arch.hash().String()+ArchiveFileSuffix, rng)
	if err != nil {
		return err
	}
	defer rdr.Close()

	key := arch.hash().String() + ArchiveFileSuffix + tableRecordsExt
	_, err = bsp.bs.Put(ctx, key, dataLen, rdr)
	return err
}

// Open a table named |name|, containing |chunkCount| chunks.
func (bsp *blobstorePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, opts openOpts, stats *Stats) (chunkSource, error) {
	cs, err := newBSTableChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, opts, stats)
	if err == nil {
		return cs, nil
	}

	if blobstore.IsNotFoundError(err) {
		source, err := newBSArchiveChunkSource(ctx, bsp.bs, name, chunkCount, bsp.q, opts, stats)
		if err != nil {
			return nil, err
		}
		return source, nil
	}

	return nil, err
}

func (bsp *blobstorePersister) Exists(ctx context.Context, name string, _ uint32, _ *Stats) (bool, io.Closer, error) {
	exists, err := bsp.bs.Exists(ctx, name)
	if err != nil {
		return false, nil, err
	}
	if exists {
		return true, noopPendingHandle{}, nil
	}
	return false, nil, nil
}

func (bsp *blobstorePersister) PruneTableFiles(ctx context.Context) error {
	return nil
}

func (bsp *blobstorePersister) Close() error {
	if c, ok := bsp.bs.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (bsp *blobstorePersister) Teardown(ctx context.Context) error {
	return bsp.bs.Teardown(ctx)
}

func (bsp *blobstorePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func (bsp *blobstorePersister) Path() string {
	return ""
}

func (bsp *blobstorePersister) CopyTableFile(ctx context.Context, r io.Reader, name string, fileSz uint64, splitOffset uint64) (io.Closer, error) {
	if splitOffset > 0 {
		lr := io.LimitReader(r, int64(splitOffset))

		indexLen := int64(fileSz - splitOffset)

		recordsName := name + tableRecordsExt
		tailName := name + tableTailExt
		// check if we can Put concurrently
		rr, ok := r.(io.ReaderAt)
		if !ok {
			// sequentially write chunk records then tail
			if _, err := bsp.bs.Put(ctx, recordsName, int64(splitOffset), lr); err != nil {
				return nil, err
			}
			if _, err := bsp.bs.Put(ctx, tailName, indexLen, r); err != nil {
				return nil, err
			}
		} else {
			// on the push path, we expect to Put concurrently
			// see BufferedFileByteSink in byte_sink.go
			eg, ectx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				srdr := io.NewSectionReader(rr, int64(splitOffset), indexLen)
				_, err := bsp.bs.Put(ectx, tailName, indexLen, srdr)
				return err
			})
			eg.Go(func() error {
				_, err := bsp.bs.Put(ectx, recordsName, int64(splitOffset), lr)
				return err
			})
			if err := eg.Wait(); err != nil {
				return nil, err
			}
		}

		// finally concatenate into the complete table
		_, err := bsp.bs.Concatenate(ctx, name, []string{recordsName, tailName})
		if err != nil {
			return nil, err
		}
		return noopPendingHandle{}, nil
	} else {
		// no split offset, just copy the whole table. We will create the records object on demand if needed.
		_, err := bsp.bs.Put(ctx, name, int64(fileSz), r)
		if err != nil {
			return nil, err
		}
		return noopPendingHandle{}, nil
	}
}

type bsTableReaderAt struct {
	bs  blobstore.Blobstore
	key string
}

func (bsTRA *bsTableReaderAt) Close() error {
	return nil
}

func (bsTRA *bsTableReaderAt) clone() (tableReaderAt, error) {
	return bsTRA, nil
}

func (bsTRA *bsTableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	rc, _, _, err := bsTRA.bs.Get(ctx, bsTRA.key, blobstore.AllRange)
	return rc, err
}

// ReadAtWithStats is the bsTableReaderAt implementation of the tableReaderAt interface
func (bsTRA *bsTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (int, error) {
	br := blobstore.NewBlobRange(off, int64(len(p)))
	rc, _, _, err := bsTRA.bs.Get(ctx, bsTRA.key, br)

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

// persistBSArchive writes the chunks of |mt| that |haver| does not have to |bs|
// as an archive, and opens it from the bytes written.
func persistBSArchive(ctx context.Context, bs blobstore.Blobstore, q MemoryQuotaProvider, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	name, data, chunkCount, gcb, err := mt.writeArchive(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	} else if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	} else if chunkCount == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}

	key := name.String() + ArchiveFileSuffix
	if _, err = bs.Put(ctx, key, int64(len(data)), bytes.NewReader(data)); err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}

	aRdr, err := newArchiveReader(ctx, &archiveIndexReaderAt{buf: data}, name, uint64(len(data)), q, openOpts{}, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	blockSize := uint64(s3BlockSize)
	aRdr.reader = &bsTableReaderAt{key: key, bs: bs}
	if shouldSpool(bs) {
		blockSize = fileBlockSize
		aRdr.reader, err = newPersistedSpoolingTableReaderAt(bs, key, data)
		if err != nil {
			return emptyChunkSource{}, gcBehavior_Continue, errors.Join(err, aRdr.close())
		}
	}
	return &archiveChunkSource{aRdr: aRdr, refs: noopRefCounter{}, blockSize: blockSize}, gcBehavior_Continue, nil
}

// openBSArchiveFirst opens the table file |name| in |bs|, trying the archive
// format before the table format. Use it only where the persister writes
// archives, or every open of a table file costs a failed read.
func openBSArchiveFirst(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, opts openOpts, stats *Stats) (chunkSource, error) {
	cs, err := newBSArchiveChunkSource(ctx, bs, name, chunkCount, q, opts, stats)
	if err == nil || !blobstore.IsNotFoundError(err) {
		return cs, err
	}
	return newBSTableChunkSource(ctx, bs, name, chunkCount, q, opts, stats)
}

func newBSArchiveChunkSource(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, opts openOpts, stats *Stats) (cs chunkSource, err error) {
	if shouldSpool(bs) {
		return newSpooledBSArchiveChunkSource(ctx, bs, name, chunkCount, q, opts, stats)
	}

	key := name.String() + ArchiveFileSuffix
	aRdr, _, err := openBSArchiveIndex(ctx, bs, key, name, chunkCount, q, opts, stats)
	if err != nil {
		return emptyChunkSource{}, err
	}
	aRdr.reader = &bsTableReaderAt{key: key, bs: bs}
	return &archiveChunkSource{aRdr: aRdr, refs: noopRefCounter{}, blockSize: s3BlockSize}, nil
}

// archiveMetadataAllowance is how much metadata archiveTailEstimate assumes an
// archive holds. Typical metadata is a short JSON map; an archive with more,
// such as one listing many conjoined files, costs a second read to open.
const archiveMetadataAllowance = 1024

// archiveTailEstimate returns how many bytes from the end of an archive of
// |chunkCount| chunks are expected to hold its footer, metadata and index. It
// assumes the most byte spans the chunks can need, one per chunk plus one
// dictionary per chunk, so only metadata can push the index out of range.
func archiveTailEstimate(chunkCount uint32) uint64 {
	return archiveFooterSize + archiveMetadataAllowance + archiveIndexSize(2*chunkCount, chunkCount)
}

// openBSArchiveIndex reads the footer and index of the archive |key| and builds
// an archiveReader from them, returning it with the archive's size. The
// archiveReader has no reader for chunk data; the caller must set one.
//
// The footer, metadata and index are read with a single ranged read sized by
// archiveTailEstimate, falling back to a second read for the part of the index
// it missed.
func openBSArchiveIndex(ctx context.Context, bs blobstore.Blobstore, key string, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, opts openOpts, stats *Stats) (archiveReader, uint64, error) {
	want := archiveTailEstimate(chunkCount)
	rc, sz, _, err := bs.Get(ctx, key, blobstore.NewBlobRange(-int64(want), 0))
	if err != nil {
		return archiveReader{}, 0, err
	}
	tail, err := io.ReadAll(rc)
	if err = errors.Join(err, rc.Close()); err != nil {
		return archiveReader{}, 0, err
	}

	// Spans in the archive are addressed relative to the end of the file.
	// If we don't have a real size, we can't compute the right range reads.
	if sz == 0 {
		return archiveReader{}, 0, fmt.Errorf("%s: blobstore did not report the size of the archive", key)
	}
	tailLen := uint64(len(tail))
	if tailLen != min(want, sz) {
		return archiveReader{}, 0, fmt.Errorf("%s: read %d bytes from the end of a %d byte archive, expected %d", key, tailLen, sz, min(want, sz))
	}
	if tailLen < archiveFooterSize {
		return archiveReader{}, 0, fmt.Errorf("%s: %d byte archive is too small to hold a footer: %w", key, sz, ErrCorruptArchiveIndex)
	}

	footer, err := buildArchiveFooter(name, sz, tail[tailLen-archiveFooterSize:])
	if err != nil {
		return archiveReader{}, 0, err
	}

	index := footer.totalIndexSpan()
	indexEnd := index.offset + index.length
	tailOffset := sz - tailLen
	var indexBytes []byte
	switch {
	case index.offset >= tailOffset:
		indexBytes = tail[index.offset-tailOffset : indexEnd-tailOffset]
	case indexEnd <= tailOffset:
		indexBytes, _, err = readBlobRange(ctx, bs, key, blobstore.NewBlobRange(int64(index.offset), int64(index.length)), index.length)
	default:
		missing := tailOffset - index.offset
		indexBytes, _, err = readBlobRange(ctx, bs, key, blobstore.NewBlobRange(int64(index.offset), int64(missing)), missing)
		indexBytes = append(indexBytes, tail[:indexEnd-tailOffset]...)
	}
	if err != nil {
		return archiveReader{}, 0, err
	}

	aRdr, err := buildArchiveReader(ctx, &archiveIndexReaderAt{buf: indexBytes, off: int64(index.offset)}, footer, q, opts, stats)
	if err != nil {
		return archiveReader{}, 0, err
	}
	return aRdr, sz, nil
}

func newBSTableChunkSource(ctx context.Context, bs blobstore.Blobstore, name hash.Hash, chunkCount uint32, q MemoryQuotaProvider, opts openOpts, stats *Stats) (cs chunkSource, err error) {
	var fileSz uint64
	index, err := loadTableIndex(ctx, stats, name, chunkCount, q, opts, func(p []byte) (uint64, error) {
		rc, sz, _, err := bs.Get(ctx, name.String(), blobstore.NewBlobRange(-int64(len(p)), 0))
		if err != nil {
			return 0, err
		}
		defer rc.Close()

		_, err = io.ReadFull(rc, p)
		if err != nil {
			return 0, err
		}

		fileSz = sz
		return sz, nil
	})
	if err != nil {
		return nil, err
	}

	if chunkCount != index.chunkCount() {
		return nil, errors.New("unexpected chunk count")
	}

	var rdr tableReaderAt = &bsTableReaderAt{key: name.String(), bs: bs}
	if shouldSpool(bs) {
		rdr, err = newPinnedSpoolingTableReaderAt(bs, name.String(), int64(fileSz))
		if err != nil {
			_ = index.Close()
			return nil, err
		}
	}
	tr, err := newTableReader(ctx, index, rdr, s3BlockSize)
	if err != nil {
		_ = index.Close()
		return nil, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

func tableTailOffset(size uint64, count uint32) uint64 {
	return size - (indexSize(count) + footerSize)
}
