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
		expectedBytes := (1024+1)*8 /* byte span offsets */ +
			1024*8 /* prefixes */ +
			1024*4*2 /* chunk spans */ +
			1024*12 /* suffixes */
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// A clone should not change the acquired quota.
		readerClone, err := reader.clone()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// Closing the clone should not change the acquired quota.
		err = readerClone.close()
		require.NoError(t, err)
		assert.Equal(t, uint64(expectedBytes), q.Usage())

		// Closing the last reader should release the quota.
		err = reader.close()
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
				require.NoError(t, err)

				// Load it as an archive reader.
				q := NewUnlimitedMemQuotaProvider()
				assert.Equal(t, uint64(0), q.Usage())
				ctx := context.Background()
				stats := &Stats{}
				_, err = newArchiveReader(ctx, &errorAfter{tra, afterBytes}, h, uint64(tra.sz), q, stats)
				require.Error(t, err)
				assert.Equal(t, uint64(0), q.Usage())
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

func (q *errorQuota) AcquireQuotaBytes(ctx context.Context, sz int) ([]byte, error) {
	if int(q.q.Usage())+sz > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaBytes(ctx, sz)
}
func (q *errorQuota) AcquireQuotaUint64s(ctx context.Context, sz int) ([]uint64, error) {
	if int(q.q.Usage())+(sz*8) > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaUint64s(ctx, sz)
}
func (q *errorQuota) AcquireQuotaUint32s(ctx context.Context, sz int) ([]uint32, error) {
	if int(q.q.Usage())+(sz*4) > q.after {
		return nil, errors.New("quota acquire error")
	}
	return q.q.AcquireQuotaUint32s(ctx, sz)
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
