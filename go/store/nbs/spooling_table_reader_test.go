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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
)

// wholeBlobBlobstore wraps an InMemoryBlobstore and advertises the spool capability.
type wholeBlobBlobstore struct {
	*blobstore.InMemoryBlobstore
	spool bool
}

func (w wholeBlobBlobstore) RangeReadsWholeBlob() bool { return w.spool }

// fakeGetBlobstore overrides Get to return a canned reader and error.
type fakeGetBlobstore struct {
	*blobstore.InMemoryBlobstore
	rc  io.ReadCloser
	err error
}

func (f fakeGetBlobstore) Get(context.Context, string, blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return f.rc, 0, "", f.err
}

// countSpoolFiles counts leftover spool temp files in the provider's temp dir.
func countSpoolFiles(t *testing.T) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "nbs-spool-*"))
	require.NoError(t, err)
	return len(matches)
}

// spoolBytes stores |data| under key "table" in an in-memory blobstore, spools it, and
// returns the reader and its temp file path.
func spoolBytes(t *testing.T, ctx context.Context, data []byte) (*spoolingTableReaderAt, string) {
	t.Helper()
	bs := blobstore.NewInMemoryBlobstore("")
	_, err := bs.Put(ctx, "table", int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)
	ra, err := newSpoolingTableReaderAt(ctx, bs, "table")
	require.NoError(t, err)
	return ra, ra.f.Name()
}

func TestShouldSpool(t *testing.T) {
	inmem := blobstore.NewInMemoryBlobstore("")
	require.False(t, shouldSpool(inmem), "a blobstore without the capability must not spool")
	require.True(t, shouldSpool(wholeBlobBlobstore{inmem, true}))
	require.False(t, shouldSpool(wholeBlobBlobstore{inmem, false}))
}

func TestNewSpoolingTableReaderAt_GetError(t *testing.T) {
	before := countSpoolFiles(t)
	bs := fakeGetBlobstore{blobstore.NewInMemoryBlobstore(""), nil, errors.New("boom")}
	_, err := newSpoolingTableReaderAt(context.Background(), bs, "table")
	require.Error(t, err)
	require.Equal(t, before, countSpoolFiles(t), "no temp file when Get fails up front")
}

func TestNewSpoolingTableReaderAt_CopyMidStreamError(t *testing.T) {
	before := countSpoolFiles(t)
	// A reader that yields some bytes then errors makes io.Copy write a partial temp file and fail.
	partialThenErr := io.MultiReader(bytes.NewReader(make([]byte, 50)), iotest.ErrReader(errors.New("boom")))
	bs := fakeGetBlobstore{blobstore.NewInMemoryBlobstore(""), io.NopCloser(partialThenErr), nil}
	_, err := newSpoolingTableReaderAt(context.Background(), bs, "table")
	require.Error(t, err)
	require.Equal(t, before, countSpoolFiles(t), "partial temp file must be removed when io.Copy fails")
}

func TestNewSpooledBSTableChunkSource_RoundTrip(t *testing.T) {
	ctx := context.Background()
	data := [][]byte{[]byte("chunk one"), []byte("chunk two"), []byte("chunk three")}
	tableBytes, tableHash, err := buildTable(data)
	require.NoError(t, err)

	inmem := blobstore.NewInMemoryBlobstore("")
	_, err = inmem.Put(ctx, tableHash.String(), int64(len(tableBytes)), bytes.NewReader(tableBytes))
	require.NoError(t, err)
	bs := wholeBlobBlobstore{inmem, true}

	before := countSpoolFiles(t)
	cs, err := newSpooledBSTableChunkSource(ctx, bs, tableHash, uint32(len(data)), NewUnlimitedMemQuotaProvider(), &Stats{})
	require.NoError(t, err)
	require.Equal(t, before+1, countSpoolFiles(t), "opening the source spools exactly one temp file")

	for _, c := range data {
		got, _, err := cs.get(ctx, computeAddr(c), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, c, got)
	}

	require.NoError(t, cs.close())
	require.Equal(t, before, countSpoolFiles(t), "closing the source removes the spool temp file")
}

func TestNewSpooledBSTableChunkSource_ChunkCountMismatch(t *testing.T) {
	ctx := context.Background()
	data := [][]byte{[]byte("a"), []byte("b")}
	tableBytes, tableHash, err := buildTable(data)
	require.NoError(t, err)

	inmem := blobstore.NewInMemoryBlobstore("")
	_, err = inmem.Put(ctx, tableHash.String(), int64(len(tableBytes)), bytes.NewReader(tableBytes))
	require.NoError(t, err)
	bs := wholeBlobBlobstore{inmem, true}

	before := countSpoolFiles(t)
	_, err = newSpooledBSTableChunkSource(ctx, bs, tableHash, uint32(len(data))+1, NewUnlimitedMemQuotaProvider(), &Stats{})
	require.Error(t, err)
	require.Equal(t, before, countSpoolFiles(t), "the temp file is removed when open fails")
}

func TestSpoolingTableReaderAt_ConcurrentClonesAndClose(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("0123456789abcdef"), 4096)

	ra, path := spoolBytes(t, ctx, data)

	const (
		goroutines    = 16
		readsPerClone = 64
		offsetStride  = 37 // a prime, so a clone's own reads do not cluster at aligned offsets
	)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ra.clone()
			if err != nil {
				t.Errorf("clone: %v", err)
				return
			}
			defer c.Close()
			p := make([]byte, 8)
			span := int64(len(data) - len(p))
			// Start each clone at a different base offset so the clones read different
			// regions of the shared file at the same time.
			base := int64(i) * span / goroutines
			for j := 0; j < readsPerClone; j++ {
				off := (base + int64(j)*offsetStride) % span
				n, err := c.ReadAtWithStats(ctx, p, off, &Stats{})
				if err != nil {
					t.Errorf("read at %d: %v", off, err)
					return
				}
				if !bytes.Equal(p[:n], data[off:off+int64(n)]) {
					t.Errorf("byte mismatch at %d", off)
					return
				}
			}
		}()
	}
	wg.Wait()

	// The original still holds a reference, so the temp file survives every clone's Close.
	_, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, ra.Close())
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "temp file must be removed once the last reference closes")
}

func TestSpoolingTableReaderAt_ReaderOutlivesSource(t *testing.T) {
	ctx := context.Background()
	data := []byte("a reader must own its lifetime")

	ra, path := spoolBytes(t, ctx, data)

	rdr, err := ra.Reader(ctx)
	require.NoError(t, err)

	// Closing the source while the reader is open must not remove the spooled file.
	require.NoError(t, ra.Close())
	_, err = os.Stat(path)
	require.NoError(t, err, "the reader holds a reference, so the file survives the source close")

	got, err := io.ReadAll(rdr)
	require.NoError(t, err)
	require.Equal(t, data, got)

	require.NoError(t, rdr.Close())
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "closing the last reader removes the temp file")
}

func TestSpoolingTableReaderAt(t *testing.T) {
	ctx := context.Background()
	data := []byte("the quick brown fox jumps over the lazy dog")

	ra, path := spoolBytes(t, ctx, data)

	for _, tc := range []struct{ off, n int }{{0, 5}, {4, 6}, {len(data) - 3, 3}} {
		p := make([]byte, tc.n)
		got, err := ra.ReadAtWithStats(ctx, p, int64(tc.off), &Stats{})
		require.NoError(t, err)
		require.Equal(t, tc.n, got)
		require.Equal(t, data[tc.off:tc.off+tc.n], p)
	}

	rdr, err := ra.Reader(ctx)
	require.NoError(t, err)
	all, err := io.ReadAll(rdr)
	require.NoError(t, err)
	require.NoError(t, rdr.Close())
	require.Equal(t, data, all)

	clone, err := ra.clone()
	require.NoError(t, err)
	require.NoError(t, clone.Close())
	_, err = os.Stat(path)
	require.NoError(t, err, "temp file must survive while a clone is still open")

	require.NoError(t, ra.Close())
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "temp file must be removed once the last reference closes")
}
