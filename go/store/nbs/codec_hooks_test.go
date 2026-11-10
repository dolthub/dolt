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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	filelocks "github.com/dolthub/file-locks"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var errHook = errors.New("hook reached")

// reached records the hooks a test reaches.
type reached struct {
	mu    sync.Mutex
	names map[string]bool
}

func (r *reached) reach(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names == nil {
		r.names = make(map[string]bool)
	}
	r.names[name] = true
}

func (r *reached) require(t *testing.T, names ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var missing []string
	for _, n := range names {
		if !r.names[n] {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "hooks not reached")
}

// hookVar replaces the hook |v| with |f| for the rest of the test.
func hookVar[T any](t *testing.T, v *T, f T) {
	orig := *v
	*v = f
	t.Cleanup(func() { *v = orig })
}

// recordingCodec is the plain codec, recording the hooks a store reaches. Like a codec that transforms records, it
// gives its persister to the store when it checks the store's first manifest.
type recordingCodec struct {
	plainCodec
	reached
	journal journalCodec
}

func withCodec(c storeCodec) StoreOption {
	return func(nbs *NomsBlockStore) { nbs.codec = c }
}

func (c *recordingCodec) openJournal(nbs *NomsBlockStore, dir, format string, lock *filelocks.Lock) (string, error) {
	c.reach("openJournal")
	return c.plainCodec.openJournal(nbs, dir, format, lock)
}

func (c *recordingCodec) checkManifest(nbs *NomsBlockStore, exists bool, contents manifestContents) error {
	c.reach("checkManifest")
	if p, ok := nbs.persister.(interface{ setCodec(storeCodec) }); ok {
		p.setCodec(c)
	}
	return nil
}

func (c *recordingCodec) checkWrite(nbs *NomsBlockStore) error {
	c.reach("checkWrite")
	return nil
}

func (c *recordingCodec) openAdded(ctx context.Context, nbs *NomsBlockStore, files map[hash.Hash]uint32, existing chunkSourceSet) (openChunkSourcesResult, error) {
	c.reach("openAdded")
	return c.plainCodec.openAdded(ctx, nbs, files, existing)
}

func (c *recordingCodec) checkAdded(nbs *NomsBlockStore, added chunkSourceSet) (func(*error, *bool), error) {
	c.reach("checkAdded")
	return func(*error, *bool) { c.reach("checkAdded.done") }, nil
}

func (c *recordingCodec) checkGC(src, dest *NomsBlockStore) error {
	c.reach("checkGC")
	return nil
}

func (c *recordingCodec) newGCWriter(level chunks.GCArchiveLevel) (GenericTableWriter, error) {
	c.reach("newGCWriter")
	return c.plainCodec.newGCWriter(level)
}

func (c *recordingCodec) readsRecords() bool {
	c.reach("readsRecords")
	return true
}

func (c *recordingCodec) plainTables() bool {
	c.reach("plainTables")
	return true
}

func (c *recordingCodec) writeMemTable(mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (hash.Hash, []byte, uint64, uint32, gcBehavior, error) {
	c.reach("writeMemTable")
	return mt.write(haver, keeper, stats)
}

func (c *recordingCodec) openWritten(cs chunkSource) (chunkSource, error) {
	c.reach("openWritten")
	return cs, nil
}

func (c *recordingCodec) newJournalCodec() journalCodec {
	c.reach("newJournalCodec")
	return c.journal
}

func hookTestChunks(n int) []chunks.Chunk {
	var cs []chunks.Chunk
	for _, cc := range randomCompressedChunks(n) {
		ch, _ := cc.ToChunk()
		cs = append(cs, ch)
	}
	return cs
}

func putAndCommit(t *testing.T, st *NomsBlockStore, cs []chunks.Chunk) {
	ctx := context.Background()
	for _, c := range cs {
		require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	}
	last, err := st.Root(ctx)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, cs[len(cs)-1].Hash(), last)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestStoreCodecHooks(t *testing.T) {
	ctx := context.Background()
	q := NewUnlimitedMemQuotaProvider()

	t.Run("local store", func(t *testing.T) {
		c := &recordingCodec{}
		dir := t.TempDir()
		st, err := NewLocalStore(ctx, types.Format_DOLT.VersionString(), dir, defaultMemTableSize, q, false, withCodec(c))
		require.NoError(t, err)
		defer st.Close()
		putAndCommit(t, st, hookTestChunks(8))
		populateLocalStore(t, st, 2)

		require.NoError(t, st.BeginGC(ctx, nil, chunks.GCMode_Full))
		gcConfig := chunks.NewGCConfig(chunks.GCMode_Full, chunks.NoArchive, chunks.IncrementalGCTablesDisabled)
		sweeper, err := st.MarkAndSweepChunks(ctx, noopWalkAddrs, func(_ context.Context, hs hash.HashSet) (hash.HashSet, error) { return hs, nil }, nil, gcConfig, false)
		require.NoError(t, err)
		require.NoError(t, sweeper.Close(ctx))
		st.EndGC(chunks.GCMode_Full)

		c.require(t, "checkManifest", "checkWrite", "writeMemTable", "plainTables", "openAdded", "checkAdded",
			"checkAdded.done", "readsRecords", "checkGC", "newGCWriter")
	})

	t.Run("blobstore store", func(t *testing.T) {
		c := &recordingCodec{}
		st, err := NewBSStore(ctx, types.Format_DOLT.VersionString(), blobstore.NewInMemoryBlobstore(""), defaultMemTableSize, q, withCodec(c))
		require.NoError(t, err)
		defer st.Close()
		putAndCommit(t, st, hookTestChunks(8))
		c.require(t, "writeMemTable", "openWritten")
	})

	t.Run("journaling store", func(t *testing.T) {
		c := &recordingCodec{}
		st, err := NewLocalJournalingStoreWithOptions(ctx, types.Format_DOLT.VersionString(), t.TempDir(), q, false, nil, JournalingStoreOptions{}, withCodec(c))
		require.NoError(t, err)
		defer st.Close()
		putAndCommit(t, st, hookTestChunks(8))
		c.require(t, "openJournal", "newJournalCodec")
	})
}

// hookJournal replaces the journal operations that read and write chunks with errHook, and records the rest.
type hookJournal struct {
	reached
}

func (j *hookJournal) replay(context.Context, *journalWriter, bool, int64, journalRec) (bool, error) {
	j.reach("replay")
	return false, nil
}

func (j *hookJournal) checkBatch(context.Context, *journalWriter, lookupMeta, []lookup) error {
	j.reach("checkBatch")
	return nil
}

func (j *hookJournal) reset() {
	j.reach("reset")
}

func (j *hookJournal) openChunk(*journalWriter, hash.Hash) (CompressedChunk, error) {
	return CompressedChunk{}, errHook
}

func (j *hookJournal) openChunkAtRange(*journalWriter, Range, hash.Hash) (CompressedChunk, error) {
	return CompressedChunk{}, errHook
}

func (j *hookJournal) chunkerAtRange(*journalWriter, Range, hash.Hash) (ToChunker, error) {
	return nil, errHook
}

func (j *hookJournal) chunker(hash.Hash, []byte) (ToChunker, error) {
	return nil, errHook
}

func (j *hookJournal) locate(context.Context, *journalWriter, hash.Hash, Range) (Range, error) {
	return Range{}, errHook
}

func (j *hookJournal) writeChunk(context.Context, *journalWriter, dherrors.FatalBehavior, CompressedChunk) error {
	return errHook
}

func (j *hookJournal) rootRecord(_ context.Context, _ *journalWriter, _ dherrors.FatalBehavior, root hash.Hash) (int, func([]byte) uint32, error) {
	j.reach("rootRecord")
	return rootHashRecordSize(), func(buf []byte) uint32 { return writeRootHashRecord(buf, root) }, nil
}

func (j *hookJournal) flushBatch(*journalWriter, hash.Hash, int64) (bool, error) {
	j.reach("flushBatch")
	return false, nil
}

func TestJournalCodecHooks(t *testing.T) {
	ctx := context.Background()
	behavior := dherrors.FatalBehaviorError
	path := newTestFilePath(t)
	hj := &hookJournal{}

	j := newTestJournalWriter(t, path)
	data := randomCompressedChunks(8)
	var last hash.Hash
	for h, cc := range data {
		require.NoError(t, j.writeCompressedChunk(ctx, behavior, cc))
		last = h
	}
	j.codec = hj
	o := j.offset()
	require.NoError(t, j.commitRootHash(ctx, behavior, last))
	require.NoError(t, j.flushIndexRecord(ctx, last, o))
	require.NoError(t, j.commitRootHash(ctx, behavior, last))
	require.NoError(t, j.Close())

	j, ok, err := openJournalWriter(ctx, path)
	require.NoError(t, err)
	require.True(t, ok)
	j.codec = hj
	_, err = j.bootstrapJournal(ctx, true, nil, nil)
	require.NoError(t, err)
	hj.require(t, "rootRecord", "flushBatch", "replay", "checkBatch")

	r, _ := j.ranges.get(last)
	_, err = j.getCompressedChunk(last)
	require.ErrorIs(t, err, errHook)
	_, err = j.getCompressedChunkAtRange(r, last)
	require.ErrorIs(t, err, errHook)
	_, _, err = j.getRange(ctx, behavior, last)
	require.ErrorIs(t, err, errHook)
	require.ErrorIs(t, j.writeCompressedChunk(ctx, behavior, ChunkToCompressedChunk(chunks.NewChunk([]byte("novel")))), errHook)

	eg, ectx := errgroup.WithContext(ctx)
	src := journalChunkSource{journal: j}
	_, _, err = src.getManyCompressed(ectx, eg, []getRecord{{a: &last, prefix: last.Prefix()}}, func(context.Context, ToChunker) {}, nil, &Stats{})
	require.NoError(t, err)
	require.ErrorIs(t, eg.Wait(), errHook)
	require.NoError(t, j.Close())

	t.Run("index recovery resets the codec", func(t *testing.T) {
		corruptJournalIndex(t, filepath.Join(filepath.Dir(path), journalIndexFileName))
		hj := &hookJournal{}
		j, _, err := openJournalWriter(ctx, path)
		require.NoError(t, err)
		j.codec = hj
		_, err = j.bootstrapJournal(ctx, true, nil, nil)
		require.NoError(t, err)
		require.NoError(t, j.Close())
		hj.require(t, "reset")
	})
}

// hookArchive replaces the archive operations that read chunks with errHook, and records the rest.
type hookArchive struct {
	*reached
}

func (a hookArchive) openDict(context.Context, *archiveReader, uint32, []byte) (*DecompBundle, error) {
	return nil, errHook
}

func (a hookArchive) openData(_ context.Context, _ *archiveReader, _ resolvedChunk, _ *DecompBundle, stored []byte) ([]byte, error) {
	a.reach("openData")
	return stored, nil
}

func (a hookArchive) decompress(hash.Hash, *DecompBundle, []byte) ([]byte, error) {
	return nil, errHook
}

func (a hookArchive) getRaw(context.Context, *archiveReader, resolvedChunk, *Stats) (*DecompBundle, []byte, error) {
	return nil, nil, errHook
}

func (a hookArchive) chunker(context.Context, *archiveReader, hash.Hash, *Stats) (ToChunker, error) {
	return nil, errHook
}

func (a hookArchive) found(context.Context, *archiveReader, resolvedChunk, *DecompBundle, []byte, func(context.Context, ToChunker)) error {
	return errHook
}

func (a hookArchive) locateSpans(context.Context, *archiveReader, []resolvedChunk) error {
	return errHook
}

func (a hookArchive) iterate(context.Context, *archiveReader, func(chunks.Chunk) error, *Stats) error {
	return errHook
}

func (a hookArchive) tolerantIterate(_ context.Context, _ *archiveReader, _ func(chunks.Chunk), errCb func(error), _ *Stats) {
	errCb(errHook)
}

// recordingSpans writes an archive's spans as they are, under a version only a codec reads.
type recordingSpans struct {
	reached
}

const testCodedArchiveVersion = 0x80

func (s *recordingSpans) writeDict(aw *archiveWriter, compressed []byte) (uint32, error) {
	s.reach("writeDict")
	return aw.writeByteSpan(compressed)
}

func (s *recordingSpans) writeZStdData(aw *archiveWriter, _ hash.Hash, _ uint32, data []byte) (uint32, error) {
	s.reach("writeZStdData")
	return aw.writeByteSpan(data)
}

func (s *recordingSpans) writeSnappyData(aw *archiveWriter, cc CompressedChunk) (uint32, error) {
	s.reach("writeSnappyData")
	return aw.writeByteSpan(cc.FullCompressedChunk)
}

func (s *recordingSpans) writeKeyBlock(*archiveWriter) error {
	s.reach("writeKeyBlock")
	return nil
}

func (s *recordingSpans) putKeyBlockLen([]byte) {
	s.reach("putKeyBlockLen")
}

func (s *recordingSpans) formatVersion() uint8 {
	return testCodedArchiveVersion
}

func TestArchiveCodecHooks(t *testing.T) {
	ctx := context.Background()
	var r reached
	hookVar(t, &codedArchiveVersion, func(v uint8) bool {
		r.reach("codedArchiveVersion")
		return v == testCodedArchiveVersion
	})
	hookVar(t, &archiveFooterExtra, func(*archiveFooter) uint64 {
		r.reach("archiveFooterExtra")
		return 0
	})
	hookVar(t, &addCodedChunk, func(*ArchiveStreamWriter, ToChunker) (uint32, bool, error) {
		r.reach("addCodedChunk")
		return 0, false, nil
	})

	spans := &recordingSpans{}
	asw, err := NewArchiveStreamWriter(t.TempDir())
	require.NoError(t, err)
	asw.writer.spans = spans
	src, hashes := createTestArchive(t, 1, [][]byte{[]byte("first"), []byte("second")}, "{}")
	for _, h := range hashes {
		tc, err := src.getAsToChunker(ctx, h, &Stats{})
		require.NoError(t, err)
		_, err = asw.AddChunk(tc)
		require.NoError(t, err)
	}
	snappy := ChunkToCompressedChunk(chunks.NewChunk([]byte("snappy")))
	_, err = asw.AddChunk(snappy)
	require.NoError(t, err)
	hashes = append(hashes, snappy.H)
	_, name, err := asw.Finish()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "coded"+ArchiveFileSuffix)
	require.NoError(t, asw.FlushToFile(path))
	spans.require(t, "writeDict", "writeZStdData", "writeSnappyData", "writeKeyBlock", "putKeyBlockLen")

	hookVar(t, &openArchiveCodec, func(context.Context, tableReaderAt, archiveFooter, openOpts, *Stats) (archiveCodec, error) {
		r.reach("openArchiveCodec")
		return hookArchive{&r}, nil
	})
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	ar, err := newArchiveReader(ctx, tableReaderAtAdapter{bytes.NewReader(data)}, hash.Parse(strings.TrimSuffix(name, ArchiveFileSuffix)), uint64(len(data)), NewUnlimitedMemQuotaProvider(), openOpts{}, &Stats{})
	require.NoError(t, err)
	r.require(t, "codedArchiveVersion", "archiveFooterExtra", "addCodedChunk", "openArchiveCodec")

	h := hashes[0]
	_, err = ar.get(ctx, h, &Stats{})
	require.ErrorIs(t, err, errHook)
	_, err = ar.getAsToChunker(ctx, h, &Stats{})
	require.ErrorIs(t, err, errHook)
	require.ErrorIs(t, ar.iterate(ctx, func(chunks.Chunk) error { return nil }, &Stats{}), errHook)
	var iterErr error
	ar.tolerantIterate(ctx, func(chunks.Chunk) {}, func(err error) { iterErr = err }, &Stats{})
	require.ErrorIs(t, iterErr, errHook)
	_, err = ar.loadDict(ctx, 1, &Stats{})
	require.ErrorIs(t, err, errHook)

	acs := &archiveChunkSource{aRdr: ar, blockSize: fileBlockSize}
	reqs := func(hs ...hash.Hash) []getRecord {
		var rs []getRecord
		for i := range hs {
			rs = append(rs, getRecord{a: &hs[i], prefix: hs[i].Prefix()})
		}
		sort.Sort(getRecordByPrefix(rs))
		return rs
	}
	snappyHash := hashes[len(hashes)-1]
	_, _, err = acs.getRecordRanges(ctx, dherrors.FatalBehaviorError, reqs(hashes...), nil)
	require.ErrorIs(t, err, errHook)
	getMany := func(get func(context.Context, *errgroup.Group) error) error {
		eg, ectx := errgroup.WithContext(ctx)
		return errors.Join(get(ectx, eg), eg.Wait())
	}
	require.ErrorIs(t, getMany(func(ctx context.Context, eg *errgroup.Group) error {
		_, _, err := acs.getManyCompressed(ctx, eg, reqs(snappyHash), func(context.Context, ToChunker) {}, nil, &Stats{})
		return err
	}), errHook)
	require.ErrorIs(t, getMany(func(ctx context.Context, eg *errgroup.Group) error {
		_, _, err := acs.getMany(ctx, eg, reqs(snappyHash), func(context.Context, *chunks.Chunk) {}, nil, &Stats{})
		return err
	}), errHook)
	r.require(t, "openData")

	t.Run("conjoin", func(t *testing.T) {
		hookVar(t, &conjoinSpanSealer, func([]sourceWithSize) (spanSealer, error) {
			return nil, errHook
		})
		a, _ := createTestArchive(t, 2, [][]byte{[]byte("third")}, "{}")
		b, _ := createTestArchive(t, 3, [][]byte{[]byte("fourth")}, "{}")
		var sources []sourceWithSize
		for _, cs := range readersToSource([]archiveReader{a, b}) {
			sources = append(sources, sourceWithSize{cs, cs.currentSize()})
		}
		_, err := planArchiveConjoin(ctx, sources, NewUnlimitedMemQuotaProvider(), &Stats{})
		require.ErrorIs(t, err, errHook)
	})
}

// hookRecords replaces the table operations that read records with errHook.
type hookRecords struct{}

func (hookRecords) chunkFromRecord(context.Context, tableReader, hash.Hash, uint64, []byte) (CompressedChunk, error) {
	return CompressedChunk{}, errHook
}

func (hookRecords) chunkerFromRead(context.Context, tableReader, readBatch, []byte, int) (ToChunker, error) {
	return nil, errHook
}

func (hookRecords) locateRecords(context.Context, tableReader, map[hash.Hash]Range) error {
	return errHook
}

func TestTableCodecHooks(t *testing.T) {
	ctx := context.Background()
	q := NewUnlimitedMemQuotaProvider()
	var r reached
	hookVar(t, &openTableCodec, func(tr tableReader, _ openOpts) (tableReader, error) {
		r.reach("openTableCodec")
		tr.codec = hookRecords{}
		return tr, nil
	})
	hookVar(t, &tableIndexExtra, func(tableIndex) uint64 {
		r.reach("tableIndexExtra")
		return 0
	})
	hookVar(t, &tableDataEnd, func(_ chunkSource, end uint64) uint64 {
		r.reach("tableDataEnd")
		return end
	})
	hookVar(t, &conjoinedTableTail, func([]sourceWithSize) ([]byte, error) {
		r.reach("conjoinedTableTail")
		return nil, nil
	})
	hookVar(t, &finishConjoinedTable, func(*compactionPlan, []byte) {
		r.reach("finishConjoinedTable")
	})

	bs := blobstore.NewInMemoryBlobstore("")
	var sources []sourceWithSize
	var h hash.Hash
	for i, chunkData := range [][][]byte{{[]byte("first")}, {[]byte("second")}} {
		data, name, err := buildTable(chunkData)
		require.NoError(t, err)
		_, err = bs.Put(ctx, name.String(), int64(len(data)), bytes.NewReader(data))
		require.NoError(t, err)
		cs, err := newBSTableChunkSource(ctx, bs, name, 1, q, openOpts{}, &Stats{})
		require.NoError(t, err)
		defer cs.close()
		sources = append(sources, sourceWithSize{cs, calcChunkRangeSize(mustIndex(t, cs))})
		if i == 0 {
			h = chunks.NewChunk(chunkData[0]).Hash()
		}
	}
	cs := sources[0].source
	_, _, err := cs.get(ctx, h, nil, &Stats{})
	require.ErrorIs(t, err, errHook)
	recs := []getRecord{{a: &h, prefix: h.Prefix()}}
	_, _, err = cs.getRecordRanges(ctx, dherrors.FatalBehaviorError, recs, nil)
	require.ErrorIs(t, err, errHook)
	eg, ectx := errgroup.WithContext(ctx)
	_, _, err = cs.getManyCompressed(ectx, eg, []getRecord{{a: &h, prefix: h.Prefix()}}, func(context.Context, ToChunker) {}, nil, &Stats{})
	require.NoError(t, err)
	require.ErrorIs(t, eg.Wait(), errHook)

	newTableFile(cs, tableSpec{name: hash.Hash{}, chunkCount: 1}, dherrors.FatalBehaviorError)
	plan, err := planTableConjoin(ctx, sources, q, &Stats{})
	require.NoError(t, err)
	plan.closer()
	r.require(t, "openTableCodec", "tableIndexExtra", "tableDataEnd", "conjoinedTableTail", "finishConjoinedTable")
}

func mustIndex(t *testing.T, cs chunkSource) tableIndex {
	idx, err := cs.index()
	require.NoError(t, err)
	return idx
}
