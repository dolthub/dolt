// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type archiveChunkSource struct {
	aRdr      archiveReader
	file      string
	refs      refCounter
	blockSize uint64
}

var _ chunkSource = &archiveChunkSource{}

func newArchiveChunkSource(ctx context.Context, dir string, h hash.Hash, chunkCount uint32, q MemoryQuotaProvider, mmapArchiveIndexes bool, refs refCounter, stats *Stats) (*archiveChunkSource, error) {
	archiveFile := filepath.Join(dir, h.String()+ArchiveFileSuffix)

	fra, err := newFileReaderAt(archiveFile, mmapArchiveIndexes)
	if err != nil {
		return nil, err
	}

	aRdr, err := newArchiveReader(ctx, fra, h, uint64(fra.sz), q, stats)
	if err != nil {
		return nil, err
	}
	return &archiveChunkSource{aRdr: aRdr, file: archiveFile, refs: refs, blockSize: fileBlockSize}, nil
}

func newAWSArchiveChunkSource(ctx context.Context,
	s3 *s3ObjectReader,
	al awsLimits,
	name string,
	chunkCount uint32,
	q MemoryQuotaProvider,
	stats *Stats) (cs chunkSource, err error) {

	footer, err := q.AcquireQuotaByteSlice(ctx, int(archiveFooterSize))
	if err != nil {
		return emptyChunkSource{}, err
	}
	defer q.ReleaseQuotaBytes(int(archiveFooterSize))
	// sz is what we are really after here, but we'll use the bytes to construct the footer to avoid another call.
	_, sz, err := s3.readS3ObjectFromEnd(ctx, name, footer, stats)
	if err != nil {
		return emptyChunkSource{}, err
	}

	id := strings.TrimSuffix(filepath.Base(name), ArchiveFileSuffix)
	hashId, ok := hash.MaybeParse(id)
	if !ok {
		return emptyChunkSource{}, fmt.Errorf("invalid archive file path: %s", name)
	}

	aRdr, err := newArchiveReaderFromFooter(ctx, &s3TableReaderAt{s3, name}, hashId, sz, footer, q, stats)
	if err != nil {
		return emptyChunkSource{}, err
	}
	return &archiveChunkSource{aRdr: aRdr, refs: noopRefCounter{}, blockSize: s3BlockSize}, nil
}

func (acs *archiveChunkSource) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	res := acs.aRdr.has(h)
	if res && keeper != nil && keeper(h) {
		return false, gcBehavior_Block, nil
	}
	return res, gcBehavior_Continue, nil
}

func (acs *archiveChunkSource) hasMany(records []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	// single threaded first pass.
	foundAll := true
	for i, req := range records {
		if req.has {
			continue
		}

		h := *req.a
		if acs.aRdr.has(h) {
			if keeper != nil && keeper(h) {
				return false, gcBehavior_Block, nil
			}
			records[i].has = true
		} else {
			foundAll = false
		}
	}
	return !foundAll, gcBehavior_Continue, nil
}

func (acs *archiveChunkSource) get(ctx context.Context, h hash.Hash, keeper keeperF, stats *Stats) ([]byte, gcBehavior, error) {
	res, err := acs.aRdr.get(ctx, h, stats)
	if err != nil {
		return nil, gcBehavior_Continue, err
	}
	if res != nil && keeper != nil && keeper(h) {
		return nil, gcBehavior_Block, nil
	}
	return res, gcBehavior_Continue, nil
}

func (acs *archiveChunkSource) getMany(ctx context.Context, eg *errgroup.Group, records []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	return acs.getManyResolved(ctx, eg, records, keeper, stats, func(ctx context.Context, rc resolvedChunk, data []byte) error {
		dict, err := acs.aRdr.dictFor(ctx, rc, stats)
		if err != nil {
			return err
		}
		raw, err := acs.aRdr.decompress(rc.h, dict, data)
		if err != nil {
			return err
		}
		chunk := chunks.NewChunk(raw)
		found(ctx, &chunk)
		return nil
	})
}

// getManyResolved locates every requested chunk in the index, groups their data
// spans into runs which are worth fetching in one request, and hands each run to
// |eg| so the requests run concurrently within the caller's io budget.
//
// |deliver| is where a chunk is finished. It is handed the bytes of one chunk's
// data span and turns them into what the caller asked for, running on the
// errgroup goroutine which read that batch. So it runs after this call has
// returned, and concurrently with the other batches.
func (acs *archiveChunkSource) getManyResolved(
	ctx context.Context,
	eg *errgroup.Group,
	records []getRecord,
	keeper keeperF,
	stats *Stats,
	deliver func(context.Context, resolvedChunk, []byte) error,
) (bool, gcBehavior, error) {
	resolved, remaining, gcb, err := acs.resolve(records, keeper)
	if err != nil || gcb != gcBehavior_Continue {
		return remaining, gcb, err
	}

	err = acs.loadDicts(ctx, resolved, stats)
	if err != nil {
		return remaining, gcBehavior_Continue, err
	}

	for _, batch := range acs.planReads(resolved) {
		eg.Go(func() error {
			return acs.fetchBatch(ctx, batch, stats, deliver)
		})
	}
	return remaining, gcBehavior_Continue, nil
}

// archiveReadBatch is a run of chunks whose data spans sit close enough together
// to fetch in a single read.
type archiveReadBatch struct {
	chunks []resolvedChunk
	start  uint64
	end    uint64
}

// resolvedByOffset orders resolved chunks by where their data sits in the file,
// keeping the spans alongside so they are not looked up again per comparison.
type resolvedByOffset struct {
	resolved []resolvedChunk
	spans    []byteSpan
}

func (s resolvedByOffset) Len() int           { return len(s.spans) }
func (s resolvedByOffset) Less(i, j int) bool { return s.spans[i].offset < s.spans[j].offset }
func (s resolvedByOffset) Swap(i, j int) {
	s.resolved[i], s.resolved[j] = s.resolved[j], s.resolved[i]
	s.spans[i], s.spans[j] = s.spans[j], s.spans[i]
}

// planReads groups |resolved| into the reads which will satisfy it, using the
// same span grouping the table reader uses so both formats tolerate the same gap
// and cap a read at the same size. |resolved| is reordered by offset.
//
// Dictionaries are not part of a batch. loadDicts has already fetched them, and
// there are far fewer of them than there are chunks.
func (acs *archiveChunkSource) planReads(resolved []resolvedChunk) []archiveReadBatch {
	spans := make([]byteSpan, len(resolved))
	for i, rc := range resolved {
		spans[i] = acs.aRdr.getByteSpanByID(rc.dataId)
	}
	sort.Sort(resolvedByOffset{resolved: resolved, spans: spans})

	runs := groupSpans(len(spans), func(i int) byteSpan { return spans[i] }, acs.blockSize)

	batches := make([]archiveReadBatch, 0, len(runs))
	for _, run := range runs {
		batches = append(batches, archiveReadBatch{
			chunks: resolved[run.first : run.first+run.count],
			start:  run.start,
			end:    run.end,
		})
	}
	return batches
}

// fetchBatch reads a batch in one request and hands each chunk its own slice of
// the result.
func (acs *archiveChunkSource) fetchBatch(
	ctx context.Context,
	batch archiveReadBatch,
	stats *Stats,
	deliver func(context.Context, resolvedChunk, []byte) error,
) error {
	buf := make([]byte, batch.end-batch.start)
	n, err := acs.aRdr.reader.ReadAtWithStats(ctx, buf, int64(batch.start), stats)
	if err != nil {
		return err
	}
	if uint64(n) != batch.end-batch.start {
		return errors.New("failed to read all data")
	}

	for _, rc := range batch.chunks {
		span := acs.aRdr.getByteSpanByID(rc.dataId)
		off := span.offset - batch.start
		err = deliver(ctx, rc, buf[off:off+span.length])
		if err != nil {
			return err
		}
	}
	return nil
}

// resolve looks up each not-yet-found record in the index.
//
// found flags are set only once the whole walk has succeeded. A keeper block part
// way through otherwise leaves earlier records marked found but never delivered,
// and the caller retries with the same slice.
func (acs *archiveChunkSource) resolve(records []getRecord, keeper keeperF) ([]resolvedChunk, bool, gcBehavior, error) {
	resolved := make([]resolvedChunk, 0, len(records))
	hits := make([]int, 0, len(records))

	foundAll := true
	for i, req := range records {
		if req.found {
			continue
		}

		h := *req.a
		rc, ok := acs.aRdr.resolveChunk(h)
		if !ok {
			foundAll = false
			continue
		}

		if keeper != nil && keeper(h) {
			return nil, true, gcBehavior_Block, nil
		}

		resolved = append(resolved, rc)
		hits = append(hits, i)
	}

	for _, i := range hits {
		records[i].found = true
	}
	return resolved, !foundAll, gcBehavior_Continue, nil
}

// loadDicts reads each distinct dictionary the resolved chunks need, before their
// reads fan out. Otherwise every concurrent reader sharing an uncached dictionary
// fetches its own copy.
//
// Currently dicts are requested serially. Generally one dict per archive is the norm,
// but that may not be true in the future. TODO: fan out.
func (acs *archiveChunkSource) loadDicts(ctx context.Context, resolved []resolvedChunk, stats *Stats) error {
	seen := make(map[uint32]struct{})
	for _, rc := range resolved {
		if rc.dictId == 0 {
			continue
		}
		if _, ok := seen[rc.dictId]; ok {
			continue
		}
		seen[rc.dictId] = struct{}{}

		_, err := acs.aRdr.loadDict(ctx, rc.dictId, stats)
		if err != nil {
			return err
		}
	}
	return nil
}

// iterate iterates over the archive chunks. The callback is called for each chunk in the archive. This is not optimized
// as currently is it only used for un-archiving, which should be uncommon.
func (acs *archiveChunkSource) iterate(ctx context.Context, cb func(chunks.Chunk) error, stats *Stats) error {
	return acs.aRdr.iterate(ctx, cb, stats)
}

func (acs *archiveChunkSource) count() uint32 {
	return acs.aRdr.count()
}

func (acs *archiveChunkSource) close() error {
	err := acs.aRdr.close()
	acs.refs.decRef()
	return err
}

func (acs *archiveChunkSource) hash() hash.Hash {
	return acs.aRdr.footer.hash
}

func (acs *archiveChunkSource) suffix() string {
	return ArchiveFileSuffix
}

func (acs *archiveChunkSource) currentSize() uint64 {
	return acs.aRdr.footer.fileSize
}

// reader returns a reader for the entire archive file.
func (acs *archiveChunkSource) reader(ctx context.Context, _ dherrors.FatalBehavior) (io.ReadCloser, uint64, error) {
	rd, err := acs.aRdr.reader.Reader(ctx)
	if err != nil {
		return nil, 0, err
	}
	return rd, acs.currentSize(), nil
}
func (acs *archiveChunkSource) uncompressedLen() (uint64, error) {
	return 0, errors.New("Archive chunk source does not support uncompressedLen")
}

func (acs *archiveChunkSource) index() (tableIndex, error) {
	return nil, errors.New("Archive chunk source does not expose table file indexes")
}

func (acs *archiveChunkSource) clone() (chunkSource, error) {
	reader, err := acs.aRdr.clone()
	if err != nil {
		return nil, err
	}
	acs.refs.addRef()
	return &archiveChunkSource{
		aRdr:      reader,
		file:      acs.file,
		refs:      acs.refs,
		blockSize: acs.blockSize,
	}, nil
}

func (acs *archiveChunkSource) getRecordRanges(_ context.Context, _ dherrors.FatalBehavior, records []getRecord, keeper keeperF) (map[hash.Hash]Range, gcBehavior, error) {
	resolved, _, gcb, err := acs.resolve(records, keeper)
	if err != nil || gcb != gcBehavior_Continue {
		return nil, gcb, err
	}

	result := make(map[hash.Hash]Range, len(resolved))
	for _, rc := range resolved {
		dataSpan := acs.aRdr.getByteSpanByID(rc.dataId)
		dictSpan := acs.aRdr.getByteSpanByID(rc.dictId)

		result[rc.h] = Range{
			Offset:     dataSpan.offset,
			Length:     uint32(dataSpan.length),
			DictOffset: dictSpan.offset,
			DictLength: uint32(dictSpan.length),
		}
	}
	return result, gcBehavior_Continue, nil
}

func (acs *archiveChunkSource) getManyCompressed(
	ctx context.Context,
	eg *errgroup.Group,
	reqs []getRecord,
	found func(context.Context, ToChunker),
	keeper keeperF,
	stats *Stats,
) (bool, gcBehavior, error) {
	return acs.getManyResolved(ctx, eg, reqs, keeper, stats, func(ctx context.Context, rc resolvedChunk, data []byte) error {
		dict, err := acs.aRdr.dictFor(ctx, rc, stats)
		if err != nil {
			return err
		}
		toChk, err := acs.aRdr.toChunker(rc.h, dict, data)
		if err != nil {
			return err
		}
		found(ctx, toChk)
		return nil
	})
}

func (acs *archiveChunkSource) iterateAllChunks(ctx context.Context, cb func(chunks.Chunk), stats *Stats) error {
	ncb := func(c chunks.Chunk) error {
		cb(c)
		return nil
	}

	return acs.aRdr.iterate(ctx, ncb, stats)
}

func (acs *archiveChunkSource) tolerantIterateAllChunks(ctx context.Context, cb func(chunks.Chunk), errCb func(error), stats *Stats) {
	acs.aRdr.tolerantIterate(ctx, cb, errCb, stats)
}
