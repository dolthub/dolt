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
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// wholeBlobBlobstore wraps an InMemoryBlobstore and advertises the spool capability.
type wholeBlobBlobstore struct {
	*blobstore.InMemoryBlobstore
	spool bool
}

func (w wholeBlobBlobstore) RangeReadsWholeBlob() bool { return w.spool }

func (w wholeBlobBlobstore) SpoolDir() *tempfiles.LockedDir { return testSpoolDir() }

// testSpoolParent holds the spool directory of the fake blobstores in this file.
var testSpoolParent = sync.OnceValue(func() string {
	parent, err := os.MkdirTemp("", "nbs-spool-test-")
	if err != nil {
		panic(err)
	}
	return parent
})

// testSpoolDir is the spool directory of the fake blobstores in this file.
var testSpoolDir = sync.OnceValue(func() *tempfiles.LockedDir {
	dir, err := tempfiles.NewLockedDir(testSpoolParent())
	if err != nil {
		panic(err)
	}
	return dir
})

// fakeGetBlobstore overrides Get to return a canned reader and error.
type fakeGetBlobstore struct {
	*blobstore.InMemoryBlobstore
	rc  io.ReadCloser
	err error
}

func (f fakeGetBlobstore) Get(context.Context, string, blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return f.rc, 0, "", f.err
}

func (f fakeGetBlobstore) SpoolDir() *tempfiles.LockedDir { return testSpoolDir() }

// countSpoolFiles counts leftover spool files in the fake blobstores' spool directory.
func countSpoolFiles(t *testing.T) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(testSpoolParent(), "*", "nbs-spool-*"))
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
	ra := newSpoolingTableReaderAt(wholeBlobBlobstore{bs, true}, "table", int64(len(data)))
	f, err := ra.spool(ctx)
	require.NoError(t, err)
	return ra, f.Name()
}

func TestShouldSpool(t *testing.T) {
	inmem := blobstore.NewInMemoryBlobstore("")
	require.False(t, shouldSpool(inmem), "a blobstore without the capability must not spool")
	require.True(t, shouldSpool(wholeBlobBlobstore{inmem, true}))
	require.False(t, shouldSpool(wholeBlobBlobstore{inmem, false}))
}

// spoolDirBlobstore is a blobstore with its own directory for spool files.
type spoolDirBlobstore struct {
	*blobstore.InMemoryBlobstore
	dir *tempfiles.LockedDir
}

func (s spoolDirBlobstore) SpoolDir() *tempfiles.LockedDir { return s.dir }

func TestSpoolingTableReaderAt_SpoolDir(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	dir, err := tempfiles.NewLockedDir(parent)
	require.NoError(t, err)
	defer dir.Close()
	bs := spoolDirBlobstore{blobstore.NewInMemoryBlobstore(""), dir}
	_, err = bs.Put(ctx, "table", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)

	ra := newSpoolingTableReaderAt(bs, "table", 5)
	f, err := ra.spool(ctx)
	require.NoError(t, err)
	require.Equal(t, parent, filepath.Dir(filepath.Dir(f.Name())))

	require.NoError(t, ra.Close())
	_, err = os.Stat(f.Name())
	require.True(t, os.IsNotExist(err), "closing the reader removes the spool")
}

func TestSpoolingTableReaderAt_NeverSpoolsToTempDir(t *testing.T) {
	ctx := context.Background()

	tempDir := t.TempDir()
	oldProvider := tempfiles.MovableTempFileProvider
	tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(tempDir)
	t.Cleanup(func() { tempfiles.MovableTempFileProvider = oldProvider })

	inmem := blobstore.NewInMemoryBlobstore("")
	_, err := inmem.Put(ctx, "table", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)

	for name, bs := range map[string]blobstore.Blobstore{
		"no spool dir method": inmem,
		"nil spool dir":       spoolDirBlobstore{inmem, nil},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newSpoolingTableReaderAt(bs, "table", 5).spool(ctx)
			require.ErrorIs(t, err, errNoSpoolDir)
		})
	}

	ra := newSpoolingTableReaderAt(wholeBlobBlobstore{inmem, true}, "table", 5)
	defer ra.Close()
	_, err = ra.spool(ctx)
	require.NoError(t, err)

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the temp dir holds no spools")
}

func TestSpoolingTableReaderAt_SpoolsOnceForConcurrentReads(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("0123456789abcdef"), 4096)
	inmem := blobstore.NewInMemoryBlobstore("")
	_, err := inmem.Put(ctx, "table", int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)
	bs := &persistReadCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{inmem, true}}

	ra := newSpoolingTableReaderAt(bs, "table", int64(len(data)))
	defer ra.Close()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := make([]byte, 16)
			_, err := ra.ReadAtWithStats(ctx, p, int64(i*16), &Stats{})
			if err != nil {
				t.Errorf("read: %v", err)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, bs.gets, "concurrent reads spool the blob once")
}

func TestSpoolingTableReaderAt_FailedSpoolCanBeRetried(t *testing.T) {
	ctx := context.Background()
	before := countSpoolFiles(t)
	inmem := blobstore.NewInMemoryBlobstore("")
	_, err := inmem.Put(ctx, "table", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)

	// The size is wrong, so the file must not be used, and must be removed.
	wrong := newSpoolingTableReaderAt(wholeBlobBlobstore{inmem, true}, "table", 6)
	_, err = wrong.ReadAtWithStats(ctx, make([]byte, 1), 0, &Stats{})
	require.ErrorContains(t, err, "expected 6 bytes, got 5")
	require.Equal(t, before, countSpoolFiles(t))

	// A cancelled read fails on its own, and the next read spools.
	ra := newSpoolingTableReaderAt(wholeBlobBlobstore{inmem, true}, "table", 5)
	defer ra.Close()
	ra.spooling <- struct{}{}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = ra.ReadAtWithStats(cancelled, make([]byte, 1), 0, &Stats{})
	require.ErrorIs(t, err, context.Canceled)
	<-ra.spooling
	p := make([]byte, 5)
	_, err = ra.ReadAtWithStats(ctx, p, 0, &Stats{})
	require.NoError(t, err)
	require.Equal(t, []byte("bytes"), p)
}

func TestSpoolingTableReaderAt_ReaderStreamsWithoutSpooling(t *testing.T) {
	ctx := context.Background()
	before := countSpoolFiles(t)
	inmem := blobstore.NewInMemoryBlobstore("")
	_, err := inmem.Put(ctx, "table", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)

	ra := newSpoolingTableReaderAt(wholeBlobBlobstore{inmem, true}, "table", 5)
	rc, err := ra.Reader(ctx)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, []byte("bytes"), got)
	require.Equal(t, before, countSpoolFiles(t), "a sequential read does not need a spool")
	require.NoError(t, ra.Close())
}

// TestNBS_GitStore_OpenReadsOnlyIndexes opens a store on a git remote holding archives and
// tables split into parts: nothing is spooled until chunks are read, and then only the tables
// which hold them are.
func TestNBS_GitStore_OpenReadsOnlyIndexes(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
	ctx := context.Background()
	remote, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	open := func(spoolParent string) *NomsBlockStore {
		spoolDir, err := tempfiles.NewLockedDir(spoolParent)
		require.NoError(t, err)
		cache, err := gitrepo.InitBare(ctx, t.TempDir()+"/cache.git")
		require.NoError(t, err)
		out, err := exec.CommandContext(ctx, "git", "--git-dir", cache.GitDir, "remote", "add", "origin", remote.GitDir).CombinedOutput()
		require.NoError(t, err, string(out))
		st, err := NewGitStore(ctx, types.Format_DOLT.VersionString(), cache.GitDir, blobstore.DoltDataRef, blobstore.GitBlobstoreOptions{MaxPartSize: 8 * 1024, SpoolDir: spoolDir}, 0, NewUnlimitedMemQuotaProvider())
		require.NoError(t, err)
		return st
	}
	randomChunk := func(r *rand.Rand) chunks.Chunk {
		data := make([]byte, 16*1024)
		_, err := r.Read(data)
		require.NoError(t, err)
		return chunks.NewChunk(data)
	}

	// A writer pushes an archive and a table.
	writer := open(t.TempDir())
	defer writer.Close()
	r := rand.New(rand.NewSource(1))
	wr, err := NewArchiveStreamWriter(t.TempDir())
	require.NoError(t, err)
	inArchive := randomChunk(r)
	_, err = wr.AddChunk(ChunkToCompressedChunk(inArchive))
	require.NoError(t, err)
	_, id, err := wr.Finish()
	require.NoError(t, err)
	chunkData, err := wr.ChunkDataLength()
	require.NoError(t, err)
	pending, err := writer.WriteTableFile(ctx, id, chunkData, wr.ChunkCount(), wr.GetMD5(), func() (io.ReadCloser, uint64, error) {
		rc, err := wr.Reader()
		return rc, wr.FullLength(), err
	})
	require.NoError(t, err)
	require.NoError(t, pending.Close())
	require.NoError(t, wr.Cancel())
	require.NoError(t, writer.AddTableFilesToManifest(ctx, map[string]int{strings.TrimSuffix(id, ArchiveFileSuffix): wr.ChunkCount()}, noopGetAddrs))
	inTable := randomChunk(r)
	require.NoError(t, writer.Put(ctx, inTable, noopGetAddrs))
	last, err := writer.Root(ctx)
	require.NoError(t, err)
	ok, err := writer.Commit(ctx, inTable.Hash(), last)
	require.NoError(t, err)
	require.True(t, ok)

	// A new store opens them without spooling either.
	spoolDir := t.TempDir()
	spools := func() int {
		matches, err := filepath.Glob(filepath.Join(spoolDir, "*", "nbs-spool-*"))
		require.NoError(t, err)
		return len(matches)
	}
	reader := open(spoolDir)
	root, err := reader.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, inTable.Hash(), root)
	require.Len(t, reader.tables.upstream, 2)
	require.Zero(t, spools(), "opening a store reads only indexes")

	has, err := reader.Has(ctx, inArchive.Hash())
	require.NoError(t, err)
	require.True(t, has)
	has, err = reader.Has(ctx, inTable.Hash())
	require.NoError(t, err)
	require.True(t, has)
	require.Zero(t, spools(), "looking in the indexes does not spool")

	got, err := reader.Get(ctx, inTable.Hash())
	require.NoError(t, err)
	require.Equal(t, inTable.Data(), got.Data())
	require.Equal(t, 1, spools(), "reading a chunk spools only the table holding it")

	got, err = reader.Get(ctx, inArchive.Hash())
	require.NoError(t, err)
	require.Equal(t, inArchive.Data(), got.Data())
	require.Equal(t, 2, spools())

	got, err = reader.Get(ctx, inArchive.Hash())
	require.NoError(t, err)
	require.Equal(t, inArchive.Data(), got.Data())
	require.Equal(t, 2, spools(), "later reads use the spool")

	require.NoError(t, reader.Close())
	entries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, entries, "closing the store gives up its spools and their directory")
}

// A read holding an older table set can reach a table after the store has conjoined it away.
// The table was never spooled, and its key is gone, but its chunks must still be readable.
func TestNBS_GitStore_ReadsTableConjoinedAway(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
	ctx := context.Background()
	remote, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	open := func() *NomsBlockStore {
		spoolDir, err := tempfiles.NewLockedDir(t.TempDir())
		require.NoError(t, err)
		cache, err := gitrepo.InitBare(ctx, t.TempDir()+"/cache.git")
		require.NoError(t, err)
		out, err := exec.CommandContext(ctx, "git", "--git-dir", cache.GitDir, "remote", "add", "origin", remote.GitDir).CombinedOutput()
		require.NoError(t, err, string(out))
		st, err := NewGitStore(ctx, types.Format_DOLT.VersionString(), cache.GitDir, blobstore.DoltDataRef, blobstore.GitBlobstoreOptions{MaxPartSize: 8 * 1024, SpoolDir: spoolDir}, 0, NewUnlimitedMemQuotaProvider())
		require.NoError(t, err)
		return st
	}
	commit := func(st *NomsBlockStore, c chunks.Chunk) {
		require.NoError(t, st.Put(ctx, c, noopGetAddrs))
		last, err := st.Root(ctx)
		require.NoError(t, err)
		ok, err := st.Commit(ctx, c.Hash(), last)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// A writer pushes some tables, and a new store opens them without spooling.
	writer := open()
	defer writer.Close()
	r := rand.New(rand.NewSource(1))
	var written []chunks.Chunk
	for i := 0; i < 3; i++ {
		data := make([]byte, 16*1024)
		_, err := r.Read(data)
		require.NoError(t, err)
		c := chunks.NewChunk(data)
		commit(writer, c)
		written = append(written, c)
	}
	st := open()
	defer st.Close()
	_, err = st.Root(ctx)
	require.NoError(t, err)
	require.Len(t, st.tables.upstream, 3)

	// A read takes a reference to the table set, as the store's reads do. The conjoin cannot
	// finish until the read releases it, but it removes the keys of the tables first.
	old := st.tables
	old.acquire()
	release := sync.OnceFunc(old.release)
	defer release()

	data := make([]byte, 16*1024)
	_, err = r.Read(data)
	require.NoError(t, err)
	st.conjoiner = inlineConjoiner{maxTables: 2}
	commit(st, chunks.NewChunk(data))
	bs := st.persister.(*singleBlobBSPersister).bs
	require.Eventually(t, func() bool {
		for _, source := range old.upstream {
			exists, err := bs.Exists(ctx, source.hash().String())
			if err != nil || exists {
				return false
			}
		}
		return true
	}, time.Minute, 10*time.Millisecond, "conjoining must remove the keys of the old tables")

	// The older table set can still read each chunk.
	for _, c := range written[:2] {
		got, gcb, err := old.get(ctx, c.Hash(), nil, st.stats)
		require.NoError(t, err)
		require.Equal(t, gcBehavior_Continue, gcb)
		require.Equal(t, c.Data(), got)
	}

	// So can a sequential read of a table which has not been spooled.
	var streamed int
	for _, source := range old.upstream {
		tr, ok := source.(*chunkSourceAdapter)
		require.True(t, ok)
		if tr.tableReader.r.(*spoolingTableReaderAt).f.Load() != nil {
			continue
		}
		rc, _, err := source.reader(ctx, dherrors.FatalBehaviorError)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		streamed++
	}
	require.Equal(t, 1, streamed)

	release()
	waitForConjoin(st)
}

// A closed store's directory takes no new spools, but a spool made before it closed stays usable.
func TestSpoolingTableReaderAt_ClosedSpoolDir(t *testing.T) {
	ctx := context.Background()
	dir, err := tempfiles.NewLockedDir(t.TempDir())
	require.NoError(t, err)
	bs := spoolDirBlobstore{blobstore.NewInMemoryBlobstore(""), dir}
	_, err = bs.Put(ctx, "spooled", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)
	_, err = bs.Put(ctx, "unspooled", 5, bytes.NewReader([]byte("bytes")))
	require.NoError(t, err)
	spooled := newSpoolingTableReaderAt(bs, "spooled", 5)
	f, err := spooled.spool(ctx)
	require.NoError(t, err)
	unspooled := newSpoolingTableReaderAt(bs, "unspooled", 5)

	require.NoError(t, dir.Close())

	_, err = unspooled.spool(ctx)
	require.ErrorIs(t, err, tempfiles.ErrLockedDirClosed)
	require.NoError(t, unspooled.Close())

	got := make([]byte, 5)
	_, err = f.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, "bytes", string(got))
	require.NoError(t, spooled.Close())
	entries, err := os.ReadDir(filepath.Dir(filepath.Dir(f.Name())))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNewSpoolingTableReaderAt_GetError(t *testing.T) {
	before := countSpoolFiles(t)
	bs := fakeGetBlobstore{blobstore.NewInMemoryBlobstore(""), nil, errors.New("boom")}
	_, err := newSpoolingTableReaderAt(bs, "table", 5).spool(context.Background())
	require.Error(t, err)
	require.Equal(t, before, countSpoolFiles(t), "no temp file when Get fails up front")
}

func TestNewSpoolingTableReaderAt_CopyMidStreamError(t *testing.T) {
	before := countSpoolFiles(t)
	// A reader that yields some bytes then errors makes io.Copy write a partial temp file and fail.
	partialThenErr := io.MultiReader(bytes.NewReader(make([]byte, 50)), iotest.ErrReader(errors.New("boom")))
	bs := fakeGetBlobstore{blobstore.NewInMemoryBlobstore(""), io.NopCloser(partialThenErr), nil}
	_, err := newSpoolingTableReaderAt(bs, "table", 50).spool(context.Background())
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
	cs, err := newBSTableChunkSource(ctx, bs, tableHash, uint32(len(data)), NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
	require.NoError(t, err)
	require.Equal(t, before, countSpoolFiles(t), "opening the source does not spool it")

	has, _, err := cs.has(computeAddr(data[0]), nil)
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, before, countSpoolFiles(t), "looking in the index does not spool it")

	for _, c := range data {
		got, _, err := cs.get(ctx, computeAddr(c), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, c, got)
		require.Equal(t, before+1, countSpoolFiles(t), "reading chunks spools it exactly once")
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
	_, err = newBSTableChunkSource(ctx, bs, tableHash, uint32(len(data))-1, NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
	require.Error(t, err)
	require.Equal(t, before, countSpoolFiles(t), "nothing is spooled when open fails")
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

// persistReadCountingBlobstore tracks blob reads after Persist returns.
type persistReadCountingBlobstore struct {
	wholeBlobBlobstore
	gets int
}

func (b *persistReadCountingBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	b.gets++
	return b.InMemoryBlobstore.Get(ctx, key, br)
}

func TestBlobstorePersistersPersistSpooling(t *testing.T) {
	// Regression coverage for https://github.com/dolthub/dolt/issues/11916.
	constructors := map[string]func(blobstore.Blobstore) tablePersister{
		"split": func(bs blobstore.Blobstore) tablePersister {
			return &blobstorePersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
		},
		"single": func(bs blobstore.Blobstore) tablePersister {
			return &singleBlobBSPersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
		},
		"no-conjoin": func(bs blobstore.Blobstore) tablePersister {
			return &noConjoinBlobstorePersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
		},
	}
	for name, constructor := range constructors {
		t.Run(name, func(t *testing.T) {
			for _, spool := range []bool{true, false} {
				t.Run(fmt.Sprint(spool), func(t *testing.T) {
					bs := &persistReadCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), spool}}
					data := [][]byte{[]byte("first chunk"), []byte("second chunk"), []byte("third chunk")}
					cs, err := persistTableData(constructor(bs), data...)
					require.NoError(t, err)
					sourceClosed := false
					t.Cleanup(func() {
						if !sourceClosed {
							require.NoError(t, cs.close())
						}
					})
					require.Zero(t, bs.gets, "Persist must not read back the table")
					var path string
					if spool {
						ra, ok := cs.(*chunkSourceAdapter).r.(*spoolingTableReaderAt)
						require.True(t, ok)
						path = ra.f.Load().Name()
					}
					clone, err := cs.clone()
					require.NoError(t, err)
					cloneClosed := false
					defer func() {
						if !cloneClosed {
							_ = clone.close()
						}
					}()
					for i := 0; i < 3; i++ {
						for _, c := range data {
							got, _, err := clone.get(context.Background(), computeAddr(c), nil, &Stats{})
							require.NoError(t, err)
							require.Equal(t, c, got)
						}
					}
					if spool {
						require.Zero(t, bs.gets, "chunk reads must use the spool")
					} else {
						require.Positive(t, bs.gets, "stores with cheap range reads still read the blobstore")
					}
					require.NoError(t, clone.close())
					cloneClosed = true
					if spool {
						_, err := os.Stat(path)
						require.NoError(t, err, "the original source still owns the spool")
						t.Cleanup(func() {
							require.NoError(t, cs.close())
							sourceClosed = true
							_, err := os.Stat(path)
							require.True(t, os.IsNotExist(err), "closing the last source removes the spool")
						})
					}
				})
			}
		})
	}
}

func TestSingleBlobBSPersisterConjoinReusesSpools(t *testing.T) {
	// Regression coverage for https://github.com/dolthub/dolt/issues/11916.
	for _, reopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen=%v", reopen), func(t *testing.T) {
			ctx := context.Background()
			bs := &persistReadCountingBlobstore{wholeBlobBlobstore: wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), true}}
			p := &singleBlobBSPersister{bs: bs, q: NewUnlimitedMemQuotaProvider(), blockSize: s3BlockSize}
			data := [][]byte{[]byte("first table chunk"), []byte("second table chunk")}
			var sources chunkSources
			var paths []string
			t.Cleanup(func() {
				for _, path := range paths {
					_, err := os.Stat(path)
					require.True(t, os.IsNotExist(err), "conjoin must not leak references to input spools")
				}
			})
			for _, c := range data {
				cs, err := persistTableData(p, c)
				require.NoError(t, err)
				if reopen {
					name, count := cs.hash(), cs.count()
					require.NoError(t, cs.close())
					cs, err = p.Open(ctx, name, count, openOpts{}, &Stats{})
					require.NoError(t, err)
				}
				sources = append(sources, cs)
				if f := cs.(*chunkSourceAdapter).r.(*spoolingTableReaderAt).f.Load(); f != nil {
					paths = append(paths, f.Name())
				}
				t.Cleanup(func() { require.NoError(t, cs.close()) })
			}
			bs.gets = 0
			cs, cleanup, err := p.ConjoinAll(ctx, dherrors.FatalBehaviorError, sources, &Stats{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cs.close()); cleanup() })
			// Conjoin reuses spools. Sources without one are streamed, not spooled. Opening the
			// output reads only its index.
			want := 1
			if reopen {
				want += len(data)
				require.Empty(t, paths, "reopened sources have not been spooled")
			}
			require.Equal(t, want, bs.gets)
			for _, c := range data {
				got, _, err := cs.get(ctx, computeAddr(c), nil, &Stats{})
				require.NoError(t, err)
				require.Equal(t, c, got)
			}
			require.Equal(t, want+1, bs.gets, "the first chunk read spools the output, and later ones use its spool")
			for _, src := range sources {
				ra := src.(*chunkSourceAdapter).r.(*spoolingTableReaderAt)
				require.Equal(t, int32(1), ra.cnt.Load(), "conjoin closes its readers without closing the source")
				require.Equal(t, !reopen, ra.f.Load() != nil, "conjoin does not spool its sources")
			}
			// Input spools must remain usable until their owning sources close.
			t.Cleanup(func() {
				for _, path := range paths {
					_, err := os.Stat(path)
					require.NoError(t, err)
				}
			})
		})
	}
}
