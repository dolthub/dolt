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
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/gozstd"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// mixedArchive holds chunks under two dictionaries plus snappy chunks with no
// dictionary, so tests cover every path a resolved chunk can take.
type mixedArchive struct {
	data       []byte
	chunks     []*chunks.Chunk
	dictChunks []*chunks.Chunk
	name       hash.Hash
}

const mixedArchiveDictGroups = 2

func buildMixedArchive(t *testing.T) mixedArchive {
	t.Helper()

	sink := NewFixedBufferByteSink(make([]byte, 1<<20))
	aw := newArchiveWriterWithSink(sink)

	var all, dicted []*chunks.Chunk
	for _, seed := range []int64{42, 77} {
		chks, _, _ := generateSimilarChunks(seed, 8)

		samples := make([][]byte, len(chks))
		for i, c := range chks {
			samples[i] = c.Data()
		}
		raw := gozstd.BuildDict(samples, 2048)
		cDict, err := gozstd.NewCDict(raw)
		require.NoError(t, err)

		dictId, err := aw.writeByteSpan(gozstd.Compress(nil, raw))
		require.NoError(t, err)

		for _, chk := range chks {
			dataId, err := aw.writeByteSpan(gozstd.CompressDict(nil, chk.Data(), cDict))
			require.NoError(t, err)
			require.NoError(t, aw.stageZStdChunk(chk.Hash(), dictId, dataId))
			all = append(all, chk)
			dicted = append(dicted, chk)
		}
	}

	snappyChunks, _, _ := generateSimilarChunks(99, 6)
	for _, chk := range snappyChunks {
		dataId, err := aw.writeByteSpan(ChunkToCompressedChunk(*chk).FullCompressedChunk)
		require.NoError(t, err)
		require.NoError(t, aw.stageSnappyChunk(chk.Hash(), dataId))
		all = append(all, chk)
	}

	require.NoError(t, aw.finalizeByteSpans())
	require.NoError(t, aw.writeIndex())
	require.NoError(t, aw.writeMetadata([]byte("")))
	require.NoError(t, aw.writeFooter())

	return mixedArchive{data: sink.buff[:sink.pos], chunks: all, dictChunks: dicted, name: defaultId}
}

// countingReaderAt serves an archive from memory and records every read, so tests
// can assert how the archive reader uses its backing store and how many reads it
// keeps in flight.
type countingReaderAt struct {
	br    *bytes.Reader
	delay time.Duration

	mu       sync.Mutex
	reads    []spanRead
	inFlight int
	peak     int
	err      error
}

type spanRead struct {
	off int64
	len int
}

func newCountingReaderAt(data []byte) *countingReaderAt {
	return &countingReaderAt{br: bytes.NewReader(data)}
}

func (c *countingReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (int, error) {
	c.mu.Lock()
	c.reads = append(c.reads, spanRead{off: off, len: len(p)})
	c.inFlight++
	if c.inFlight > c.peak {
		c.peak = c.inFlight
	}
	failWith := c.err
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()

	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if failWith != nil {
		return 0, failWith
	}
	return c.br.ReadAt(p, off)
}

func (c *countingReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	r := *c.br
	return io.NopCloser(&r), nil
}

func (c *countingReaderAt) Close() error                  { return nil }
func (c *countingReaderAt) clone() (tableReaderAt, error) { return c, nil }

// failReads makes every subsequent read fail, after the archive has been opened.
func (c *countingReaderAt) failReads(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *countingReaderAt) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = nil
	c.peak = 0
}

func (c *countingReaderAt) peakInFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

// readsOf counts reads which start exactly at |off|, identifying a byte span.
func (c *countingReaderAt) readsOf(off uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.reads {
		if r.off == int64(off) {
			n++
		}
	}
	return n
}

func (c *countingReaderAt) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reads)
}

func openMixedChunkSource(t *testing.T, ctx context.Context, arc mixedArchive, rd tableReaderAt) *archiveChunkSource {
	t.Helper()
	ar, err := newArchiveReader(ctx, rd, arc.name, uint64(len(arc.data)), NewUnlimitedMemQuotaProvider(), &Stats{})
	require.NoError(t, err)
	acs := &archiveChunkSource{aRdr: ar, refs: noopRefCounter{}}
	t.Cleanup(func() { acs.close() })
	return acs
}

func recordsFor(chks []*chunks.Chunk) []getRecord {
	hs := hash.NewHashSet()
	for _, c := range chks {
		hs.Insert(c.Hash())
	}
	return toGetRecords(hs)
}

// collector gathers chunks delivered by a getMany callback, which is invoked from
// several goroutines once the reads fan out.
type collector struct {
	mu   sync.Mutex
	seen map[hash.Hash][]byte
}

func newCollector() *collector {
	return &collector{seen: map[hash.Hash][]byte{}}
}

func (c *collector) addChunk(_ context.Context, chk *chunks.Chunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[chk.Hash()] = chk.Data()
}

func (c *collector) addToChunker(_ context.Context, tc ToChunker) {
	chk, err := tc.ToChunk()
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[tc.Hash()] = chk.Data()
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// runGetMany drives a getMany style call the way NomsBlockStore does: an errgroup
// bounded by a limit, waited on after the call returns.
func runGetMany(
	t *testing.T,
	limit int,
	call func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error),
) (bool, gcBehavior, error) {
	t.Helper()
	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(limit)
	remaining, gcb, err := call(ctx, eg)
	return remaining, gcb, errors.Join(err, eg.Wait())
}

func TestArchiveChunkSourceGetManyCompressed(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	acs := openMixedChunkSource(t, ctx, arc, newCountingReaderAt(arc.data))

	reqs := recordsFor(arc.chunks)
	got := newCollector()

	remaining, gcb, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, reqs, got.addToChunker, nil, &Stats{})
	})
	require.NoError(t, err)
	require.Equal(t, gcBehavior_Continue, gcb)
	require.False(t, remaining)

	require.Equal(t, len(arc.chunks), got.count())
	for _, chk := range arc.chunks {
		require.Equal(t, chk.Data(), got.seen[chk.Hash()])
	}
	for _, r := range reqs {
		require.True(t, r.found)
	}
}

func TestArchiveChunkSourceGetMany(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	acs := openMixedChunkSource(t, ctx, arc, newCountingReaderAt(arc.data))

	reqs := recordsFor(arc.chunks)
	got := newCollector()

	remaining, gcb, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getMany(ctx, eg, reqs, got.addChunk, nil, &Stats{})
	})
	require.NoError(t, err)
	require.Equal(t, gcBehavior_Continue, gcb)
	require.False(t, remaining)

	require.Equal(t, len(arc.chunks), got.count())
	for _, chk := range arc.chunks {
		require.Equal(t, chk.Data(), got.seen[chk.Hash()])
	}
}

// TestArchiveChunkSourceGetManyReportsRemaining checks that absent chunks are
// reported before the reads finish, which is what lets the caller decide whether
// to consult the next chunk source.
func TestArchiveChunkSourceGetManyReportsRemaining(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	acs := openMixedChunkSource(t, ctx, arc, newCountingReaderAt(arc.data))

	absent, _, _ := generateSimilarChunks(1234, 3)
	reqs := recordsFor(append(append([]*chunks.Chunk{}, arc.chunks...), absent...))
	got := newCollector()

	remaining, gcb, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, reqs, got.addToChunker, nil, &Stats{})
	})
	require.NoError(t, err)
	require.Equal(t, gcBehavior_Continue, gcb)
	require.True(t, remaining)
	require.Equal(t, len(arc.chunks), got.count())

	present := hash.NewHashSet()
	for _, c := range arc.chunks {
		present.Insert(c.Hash())
	}
	for _, r := range reqs {
		require.Equal(t, present.Has(*r.a), r.found, "found flag disagrees with presence for %s", r.a.String())
	}
}

func TestArchiveChunkSourceGetManySkipsFoundRecords(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	acs := openMixedChunkSource(t, ctx, arc, rd)

	reqs := recordsFor(arc.chunks)
	for i := range reqs {
		if i%2 == 0 {
			reqs[i].found = true
		}
	}
	want := 0
	for _, r := range reqs {
		if !r.found {
			want++
		}
	}

	got := newCollector()
	rd.reset()
	_, _, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, reqs, got.addToChunker, nil, &Stats{})
	})
	require.NoError(t, err)
	require.Equal(t, want, got.count(), "already found records must not be fetched again")
}

// TestArchiveChunkSourceGetManyFansOut is the regression guard for the errgroup:
// the reads must run concurrently rather than one at a time.
func TestArchiveChunkSourceGetManyFansOut(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	acs := openMixedChunkSource(t, ctx, arc, rd)

	const limit = 4
	require.Greater(t, len(arc.chunks), limit, "need more chunks than slots to saturate")

	// The delay holds each read open long enough that concurrent reads overlap
	// observably; without it they retire faster than the next one is dispatched.
	rd.reset()
	rd.delay = 20 * time.Millisecond
	defer func() { rd.delay = 0 }()

	got := newCollector()
	_, _, err := runGetMany(t, limit, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, recordsFor(arc.chunks), got.addToChunker, nil, &Stats{})
	})
	require.NoError(t, err)
	require.Equal(t, len(arc.chunks), got.count())
	require.Equal(t, limit, rd.peakInFlight(), "chunk reads must saturate the errgroup")
}

// TestArchiveChunkSourceLoadsEachDictOnce checks that a dictionary shared by many
// chunks is read once, rather than once per concurrent reader that needs it.
func TestArchiveChunkSourceLoadsEachDictOnce(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	acs := openMixedChunkSource(t, ctx, arc, rd)

	dictSpans := map[uint64]struct{}{}
	for _, chk := range arc.dictChunks {
		idx := acs.aRdr.search(chk.Hash())
		require.GreaterOrEqual(t, idx, 0)
		dictId, _ := acs.aRdr.getChunkRef(idx)
		require.NotZero(t, dictId)
		dictSpans[acs.aRdr.getByteSpanByID(dictId).offset] = struct{}{}
	}
	require.Len(t, dictSpans, mixedArchiveDictGroups)

	rd.reset()
	got := newCollector()
	_, _, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, recordsFor(arc.chunks), got.addToChunker, nil, &Stats{})
	})
	require.NoError(t, err)

	for off := range dictSpans {
		require.Equal(t, 1, rd.readsOf(off), "dictionary at offset %d read more than once", off)
	}
}

// TestArchiveChunkSourceGetManyKeeperBlock checks that a blocked pass leaves no
// record marked found. The caller retries with the same slice, so a record marked
// found but never delivered would be skipped and its chunk lost.
func TestArchiveChunkSourceGetManyKeeperBlock(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	acs := openMixedChunkSource(t, ctx, arc, rd)

	reqs := recordsFor(arc.chunks)
	blocked := *reqs[len(reqs)/2].a
	keeper := func(h hash.Hash) bool { return h == blocked }

	rd.reset()
	got := newCollector()
	remaining, gcb, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, reqs, got.addToChunker, keeper, &Stats{})
	})
	require.NoError(t, err)
	// gcBehavior_Block is declared untyped, so it needs converting to compare.
	require.Equal(t, gcBehavior(gcBehavior_Block), gcb)
	require.True(t, remaining)

	require.Zero(t, got.count(), "a blocked pass must not deliver chunks")
	require.Zero(t, rd.readCount(), "a blocked pass must not read")
	for _, r := range reqs {
		require.False(t, r.found, "a blocked pass must leave every record unfound")
	}
}

// TestArchiveChunkSourceGetManyReadError checks that a read failure reaches the
// caller. The reads are dispatched to the errgroup, so the error surfaces from
// Wait rather than from the call itself.
func TestArchiveChunkSourceGetManyReadError(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	acs := openMixedChunkSource(t, ctx, arc, rd)

	boom := errors.New("boom")
	rd.failReads(boom)

	got := newCollector()
	_, _, err := runGetMany(t, 4, func(ctx context.Context, eg *errgroup.Group) (bool, gcBehavior, error) {
		return acs.getManyCompressed(ctx, eg, recordsFor(arc.chunks), got.addToChunker, nil, &Stats{})
	})
	require.ErrorIs(t, err, boom)
}
