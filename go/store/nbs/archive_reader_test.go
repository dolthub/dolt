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
	"crypto/rand"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestInMemoryArchiveIndexReaderQuota(t *testing.T) {
	// Write a simple archive file which has non-sense chunks which claim to be snappy encoded.
	dir := t.TempDir()
	writer, err := newArchiveWriter(dir)
	require.NoError(t, err)
	var bytes [1024]byte
	var h hash.Hash
	for i := 0; i < 1024; i++ {
		_, err := io.ReadFull(rand.Reader, bytes[:])
		require.NoError(t, err)
		spanID, err := writer.writeByteSpan(bytes[:])
		require.NoError(t, err)
		_, err = io.ReadFull(rand.Reader, h[:])
		require.NoError(t, err)
		err = writer.stageSnappyChunk(h, spanID)
		require.NoError(t, err)
	}
	_, err = io.ReadFull(rand.Reader, h[:])
	require.NoError(t, err)
	err = indexFinalizeFlushArchive(writer, dir, h)
	require.NoError(t, err)

	h, err = writer.getName()
	require.NoError(t, err)
	path := filepath.Join(dir, h.String()+".darc")

	t.Run("Success", func(t *testing.T) {
		// Build a tableReaderAt for the file we just wrote.
		tra, err := newFileReaderAt(path, false)
		require.NoError(t, err)

		// Load it as an archive reader.
		q := NewUnlimitedMemQuotaProvider()
		assert.Equal(t, uint64(0), q.Usage())
		ctx := context.Background()
		stats := &Stats{}
		reader, err := newArchiveReader(ctx, tra, h, uint64(tra.sz), q, stats)
		require.NoError(t, err)

		// It should have acquired quote.
		expectedBytes := (1024+1)*uint64Size /* byte span offsets */ +
			1024*uint64Size /* prefixes */ +
			1024*uint32Size*2 /* chunk spans */ +
			1024*hash.SuffixLen /* suffixes */
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// A clone should not change the acquired quota.
		readerClone, err := reader.clone()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// Closing the clone should not change the acquired quota.
		err = readerClone.close()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// We can clone again.
		readerClone, err = reader.clone()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// And clone a clone.
		anotherReaderClone, err := readerClone.clone()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())
		err = anotherReaderClone.close()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// Closing the original reader while there is a clone should not release the quota.
		err = reader.close()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// Closing the last reader should release the quota.
		err = readerClone.close()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), q.Usage())
	})
	t.Run("IO Errors", func(t *testing.T) {
		// If we have I/O errors while reading the index, all the acquired quota should be released.
		for _, afterBytes := range []int{
			1024,                                  /* while reading bytes span offsets */
			(1024+1)*8 + 1024,                     /* while reading prefixes */
			(1024+1)*8 + 1024*8 + 1024,            /* while reading chunk spans */
			(1024+1)*8 + 1024*8 + 1024*4*2 + 1024, /* while reading suffixes */
		} {
			t.Run(strconv.Itoa(afterBytes), func(t *testing.T) {
				// Build a tableReaderAt for the file we just wrote.
				tra, err := newFileReaderAt(path, false)
				// newArchiveReader typically takes ownership of the tableReaderAt
				// but because it is going to error, we need to close it here.
				require.NoError(t, err)

				// Load it as an archive reader.
				q := NewUnlimitedMemQuotaProvider()
				assert.Equal(t, uint64(0), q.Usage())
				ctx := context.Background()
				stats := &Stats{}
				_, err = newArchiveReader(ctx, &errorAfter{tra, afterBytes}, h, uint64(tra.sz), q, stats)
				require.Error(t, err)
				assert.Equal(t, uint64(0), q.Usage())
				require.NoError(t, tra.Close())
			})
		}
	})
	t.Run("Acquire Errors", func(t *testing.T) {
		// If we have error while acquiring memory for the index, all the acquired quota should be released.
		for _, afterBytes := range []int{
			1024,                                  /* while reading bytes span offsets */
			(1024+1)*8 + 1024,                     /* while reading prefixes */
			(1024+1)*8 + 1024*8 + 1024,            /* while reading chunk spans */
			(1024+1)*8 + 1024*8 + 1024*4*2 + 1024, /* while reading suffixes */
		} {
			t.Run(strconv.Itoa(afterBytes), func(t *testing.T) {
				// Build a tableReaderAt for the file we just wrote.
				tra, err := newFileReaderAt(path, false)
				require.NoError(t, err)
				// newArchiveReader typically takes ownership of the tableReaderAt
				// but because it is going to error, we need to close it here.
				defer tra.Close()

				// Load it as an archive reader.
				q := errorQuota{NewUnlimitedMemQuotaProvider(), afterBytes}
				assert.Equal(t, uint64(0), q.Usage())
				ctx := context.Background()
				stats := &Stats{}
				_, err = newArchiveReader(ctx, tra, h, uint64(tra.sz), &q, stats)
				require.Error(t, err)
				assert.Equal(t, uint64(0), q.Usage())
			})
		}
	})
}

type errorQuota struct {
	q     MemoryQuotaProvider
	after int
}

var _ MemoryQuotaProvider = (*errorQuota)(nil)

func (q *errorQuota) AcquireQuotaByteSlice(ctx context.Context, sz int) ([]byte, error) {
	if int(q.q.Usage())+sz > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaByteSlice(ctx, sz)
}
func (q *errorQuota) AcquireQuotaUint64Slice(ctx context.Context, sz int) ([]uint64, error) {
	if int(q.q.Usage())+(sz*8) > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaUint64Slice(ctx, sz)
}
func (q *errorQuota) AcquireQuotaUint32Slice(ctx context.Context, sz int) ([]uint32, error) {
	if int(q.q.Usage())+(sz*4) > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaUint32Slice(ctx, sz)
}
func (q *errorQuota) AcquireQuotaBytes(ctx context.Context, sz int) error {
	if int(q.q.Usage())+(sz) > q.after {
		return errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaBytes(ctx, sz)
}
func (q *errorQuota) ReleaseQuotaBytes(sz int) {
	q.q.ReleaseQuotaBytes(sz)
}
func (q *errorQuota) Usage() uint64 {
	return q.q.Usage()
}

type errorAfter struct {
	tra   tableReaderAt
	after int
}

var _ tableReaderAt = (*errorAfter)(nil)

func (e *errorAfter) Close() error {
	return e.tra.Close()
}
func (e *errorAfter) clone() (tableReaderAt, error) {
	// For now just return a cloned reader which has its own count and starts where we currently are.
	cloned, err := e.tra.clone()
	if err != nil {
		return cloned, err
	}
	return &errorAfter{cloned, e.after}, nil
}
func (e *errorAfter) Reader(ctx context.Context) (io.ReadCloser, error) {
	// Don't worry about erroring on this reader for now.
	return e.tra.Reader(ctx)
}
func (e *errorAfter) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	if e.after == 0 {
		return 0, errors.New("errorAfter i/o error")
	}
	if len(p) > e.after {
		n, err = e.tra.ReadAtWithStats(ctx, p[:e.after], off, stats)
		e.after -= n
		return n, err
	}
	n, err = e.tra.ReadAtWithStats(ctx, p, off, stats)
	e.after -= n
	return n, err
}

// resolveChunk locates |h| the way archiveChunkSource.resolve does, so the by-ref
// read paths can be exercised directly.
func resolveChunk(t *testing.T, ar archiveReader, h hash.Hash) resolvedChunk {
	t.Helper()
	idx := ar.search(h)
	require.GreaterOrEqual(t, idx, 0)
	dictId, dataId := ar.getChunkRef(idx)
	return resolvedChunk{h: h, dictId: dictId, dataId: dataId}
}

func openMixedReader(t *testing.T, ctx context.Context, arc mixedArchive, rd tableReaderAt) archiveReader {
	t.Helper()
	ar, err := newArchiveReader(ctx, rd, arc.name, uint64(len(arc.data)), NewUnlimitedMemQuotaProvider(), &Stats{})
	require.NoError(t, err)
	t.Cleanup(func() { ar.close() })
	return ar
}

// TestArchiveReaderByRefMatchesSearch checks the by-ref reads return exactly what
// the searching reads return, for dictionary and snappy chunks alike.
func TestArchiveReaderByRefMatchesSearch(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	ar := openMixedReader(t, ctx, arc, newCountingReaderAt(arc.data))

	for _, chk := range arc.chunks {
		ref := resolveChunk(t, ar, chk.Hash())

		want, err := ar.get(ctx, chk.Hash(), &Stats{})
		require.NoError(t, err)
		got, err := ar.getByRef(ctx, ref, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), want)
		require.Equal(t, want, got)

		wantTC, err := ar.getAsToChunker(ctx, chk.Hash(), &Stats{})
		require.NoError(t, err)
		gotTC, err := ar.getAsToChunkerByRef(ctx, ref, &Stats{})
		require.NoError(t, err)

		wantChk, err := wantTC.ToChunk()
		require.NoError(t, err)
		gotChk, err := gotTC.ToChunk()
		require.NoError(t, err)
		require.Equal(t, chk.Data(), gotChk.Data())
		require.Equal(t, wantChk.Data(), gotChk.Data())
	}
}

// TestArchiveReaderToChunkerFormat checks each compression format produces the
// ToChunker the consumer expects.
func TestArchiveReaderToChunkerFormat(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	ar := openMixedReader(t, ctx, arc, newCountingReaderAt(arc.data))

	dicted := arc.dictChunks[0]
	tc, err := ar.getAsToChunkerByRef(ctx, resolveChunk(t, ar, dicted.Hash()), &Stats{})
	require.NoError(t, err)
	require.IsType(t, &ArchiveToChunker{}, tc)

	// The snappy chunks are the ones written without a dictionary.
	var snappy *chunks.Chunk
	for _, chk := range arc.chunks {
		if resolveChunk(t, ar, chk.Hash()).dictId == 0 {
			snappy = chk
			break
		}
	}
	require.NotNil(t, snappy, "fixture must contain a chunk with no dictionary")

	tc, err = ar.getAsToChunkerByRef(ctx, resolveChunk(t, ar, snappy.Hash()), &Stats{})
	require.NoError(t, err)
	require.IsType(t, CompressedChunk{}, tc)
}

// TestArchiveReaderToChunkerRejectsMissingDict checks the pre-snappy format still
// refuses a chunk with no dictionary rather than mis-reading it.
func TestArchiveReaderToChunkerRejectsMissingDict(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	ar := openMixedReader(t, ctx, arc, newCountingReaderAt(arc.data))
	ar.footer.formatVersion = archiveVersionSnappySupport - 1

	_, err := ar.toChunker(hash.Hash{}, nil, []byte("data"))
	require.Error(t, err)

	_, err = ar.decompress(hash.Hash{}, nil, []byte("data"))
	require.Error(t, err)
}

// TestArchiveReaderLoadDictCaches checks a dictionary is fetched once and served
// from the cache thereafter.
func TestArchiveReaderLoadDictCaches(t *testing.T) {
	ctx := context.Background()
	arc := buildMixedArchive(t)
	rd := newCountingReaderAt(arc.data)
	ar := openMixedReader(t, ctx, arc, rd)

	ref := resolveChunk(t, ar, arc.dictChunks[0].Hash())
	require.NotZero(t, ref.dictId)
	dictOff := ar.getByteSpanByID(ref.dictId).offset

	rd.reset()
	first, err := ar.loadDict(ctx, ref.dictId, &Stats{})
	require.NoError(t, err)
	require.Equal(t, 1, rd.readsOf(dictOff))

	second, err := ar.loadDict(ctx, ref.dictId, &Stats{})
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Equal(t, 1, rd.readsOf(dictOff), "a cached dictionary must not be re-read")
}
