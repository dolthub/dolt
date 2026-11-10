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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var errHook = errors.New("hook reached")

// reached records the hooks a test reaches. hit records |name| and returns nil.
type reached struct {
	mu    sync.Mutex
	names map[string]bool
}

func (r *reached) hit(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names == nil {
		r.names = make(map[string]bool)
	}
	r.names[name] = true
	return nil
}

func (r *reached) require(t *testing.T, names ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range names {
		require.True(t, r.names[n], "hook %s not reached", n)
	}
}

// hookVar replaces the hook |v| with |f| for the rest of the test.
func hookVar[T any](t *testing.T, v *T, f T) {
	orig := *v
	*v = f
	t.Cleanup(func() { *v = orig })
}

// recordingCodec is the plain codec, recording the hooks a store reaches.
type recordingCodec struct {
	plainCodec
	reached
}

func withCodec(c storeCodec) StoreOption {
	return func(o *storeOptions) { o.codec = c }
}

func (c *recordingCodec) checkManifest(*NomsBlockStore, bool, manifestContents) error {
	return c.hit("checkManifest")
}
func (c *recordingCodec) journalFormat(_ *NomsBlockStore, _, format string) (string, error) {
	return format, c.hit("journalFormat")
}
func (c *recordingCodec) readsRecords() bool { return c.hit("readsRecords") == nil }
func (c *recordingCodec) plainTables() bool  { return c.hit("plainTables") == nil }

func (c *recordingCodec) openAdded(ctx context.Context, nbs *NomsBlockStore, files map[hash.Hash]uint32, existing chunkSourceSet) (openChunkSourcesResult, error) {
	c.hit("openAdded")
	return c.plainCodec.openAdded(ctx, nbs, files, existing)
}

func (c *recordingCodec) newGCWriter(ctx context.Context, level chunks.GCArchiveLevel) (GenericTableWriter, error) {
	c.hit("newGCWriter")
	return c.plainCodec.newGCWriter(ctx, level)
}

func (c *recordingCodec) writeMemTable(_ context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (hash.Hash, []byte, uint64, uint32, gcBehavior, error) {
	c.hit("writeMemTable")
	return mt.write(haver, keeper, stats)
}

func (c *recordingCodec) newJournalCodec() journalCodec {
	c.hit("newJournalCodec")
	return nil
}

func (c *recordingCodec) checkWrite(*NomsBlockStore) error   { return c.hit("checkWrite") }
func (c *recordingCodec) checkGC(_, _ *NomsBlockStore) error { return c.hit("checkGC") }
func (c *recordingCodec) openWritten(cs chunkSource) (chunkSource, error) {
	return cs, c.hit("openWritten")
}
func (c *recordingCodec) checkAdded(*NomsBlockStore, chunkSourceSet) (func(*error, *bool), error) {
	return func(*error, *bool) { c.hit("checkAdded.done") }, c.hit("checkAdded")
}

func putAndCommit(t *testing.T, st *NomsBlockStore) {
	ctx := context.Background()
	var last hash.Hash
	for _, cc := range randomCompressedChunks(8) {
		ch, _ := cc.ToChunk()
		require.NoError(t, st.Put(ctx, ch, noopGetAddrs))
		last = ch.Hash()
	}
	root, err := st.Root(ctx)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, last, root)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestStoreCodecHooks(t *testing.T) {
	ctx := context.Background()
	q := NewUnlimitedMemQuotaProvider()
	nbf := types.Format_DOLT.VersionString()

	c := &recordingCodec{}
	st, err := NewLocalStore(ctx, nbf, t.TempDir(), defaultMemTableSize, q, false, withCodec(c))
	require.NoError(t, err)
	defer st.Close()
	putAndCommit(t, st)
	populateLocalStore(t, st, 2)
	require.NoError(t, st.BeginGC(ctx, nil, chunks.GCMode_Full))
	gcConfig := chunks.NewGCConfig(chunks.GCMode_Full, chunks.NoArchive, chunks.IncrementalGCTablesDisabled)
	keepAll := func(_ context.Context, hs hash.HashSet) (hash.HashSet, error) { return hs, nil }
	sweeper, err := st.MarkAndSweepChunks(ctx, noopWalkAddrs, keepAll, nil, gcConfig, false)
	require.NoError(t, err)
	require.NoError(t, sweeper.Close(ctx))
	st.EndGC(chunks.GCMode_Full)
	c.require(t, "checkManifest", "checkWrite", "writeMemTable", "plainTables", "openAdded", "checkAdded",
		"checkAdded.done", "readsRecords", "checkGC", "newGCWriter")

	// A store gets its codec from newStoreCodec without an option, and gives it to its persister.
	bc := &recordingCodec{}
	hookVar(t, &newStoreCodec, func() storeCodec { return bc })
	bst, err := NewBSStore(ctx, nbf, blobstore.NewInMemoryBlobstore(""), defaultMemTableSize, q)
	require.NoError(t, err)
	defer bst.Close()
	putAndCommit(t, bst)
	bc.require(t, "writeMemTable", "openWritten")

	c = &recordingCodec{}
	dir := t.TempDir()
	jst, err := NewLocalJournalingStoreWithOptions(ctx, nbf, dir, q, false, nil, JournalingStoreOptions{}, withCodec(c))
	require.NoError(t, err)
	putAndCommit(t, jst)
	require.NoError(t, jst.Close())
	c.require(t, "journalFormat", "newJournalCodec")

	// A store whose codec refuses its format releases its lock.
	opts := JournalingStoreOptions{SkipLockFileTimeout: true, FailOnLockTimeout: true}
	_, err = NewLocalJournalingStoreWithOptions(ctx, nbf, dir, q, false, nil, opts, withCodec(refusingCodec{}))
	require.ErrorIs(t, err, errHook)
	jst, err = NewLocalJournalingStoreWithOptions(ctx, nbf, dir, q, false, nil, opts)
	require.NoError(t, err)
	require.NoError(t, jst.Close())
}

type refusingCodec struct {
	plainCodec
}

func (refusingCodec) journalFormat(*NomsBlockStore, string, string) (string, error) {
	return "", errHook
}

// hookJournal fails the journal operations that read and write chunks with errHook, and records the rest.
type hookJournal struct {
	reached
}

func (j *hookJournal) rootRecord(_ context.Context, _ *journalWriter, _ dherrors.FatalBehavior, root hash.Hash) (int, func([]byte) uint32, error) {
	return rootHashRecordSize(), func(buf []byte) uint32 { return writeRootHashRecord(buf, root) }, j.hit("rootRecord")
}

func (j *hookJournal) replay(context.Context, *journalWriter, bool, int64, journalRec) (bool, error) {
	return false, j.hit("replay")
}
func (j *hookJournal) checkBatch(context.Context, lookupMeta, []lookup) error {
	return j.hit("checkBatch")
}
func (j *hookJournal) flushBatch(*journalWriter, hash.Hash, int64) (bool, error) {
	return false, j.hit("flushBatch")
}
func (j *hookJournal) reset() { j.hit("reset") }
func (j *hookJournal) openChunk(*journalWriter, hash.Hash) (CompressedChunk, error) {
	return CompressedChunk{}, errHook
}
func (j *hookJournal) openChunkAtRange(*journalWriter, Range, hash.Hash) (CompressedChunk, error) {
	return CompressedChunk{}, errHook
}
func (j *hookJournal) chunkerAtRange(*journalWriter, Range, hash.Hash) (ToChunker, error) {
	j.hit("chunkerAtRange")
	return nil, errHook
}
func (j *hookJournal) chunker(hash.Hash, []byte) (ToChunker, error) { return nil, errHook }
func (j *hookJournal) locate(context.Context, *journalWriter, hash.Hash, Range) (Range, error) {
	return Range{}, errHook
}
func (j *hookJournal) writeChunk(context.Context, *journalWriter, dherrors.FatalBehavior, CompressedChunk) error {
	return errHook
}

func TestJournalCodecHooks(t *testing.T) {
	ctx := context.Background()
	behavior := dherrors.FatalBehaviorError
	path := newTestFilePath(t)
	hj := &hookJournal{}

	j := newTestJournalWriter(t, path)
	var last hash.Hash
	for h, cc := range randomCompressedChunks(8) {
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
	novel := ChunkToCompressedChunk(chunks.NewChunk([]byte("novel")))
	require.ErrorIs(t, j.writeCompressedChunk(ctx, behavior, novel), errHook)
	getMany := func(h hash.Hash) error {
		eg, ectx := errgroup.WithContext(ctx)
		recs := []getRecord{{a: &h, prefix: h.Prefix()}}
		_, _, err := journalChunkSource{journal: j}.getManyCompressed(ectx, eg, recs, func(context.Context, ToChunker) {}, nil, &Stats{})
		return errors.Join(err, eg.Wait())
	}
	require.ErrorIs(t, getMany(last), errHook)
	// A chunk still in the writer's buffer is read under the lock.
	j.codec = nil
	require.NoError(t, j.writeCompressedChunk(ctx, behavior, novel))
	j.codec = hj
	require.ErrorIs(t, getMany(novel.H), errHook)
	hj.require(t, "chunkerAtRange")
	require.NoError(t, j.Close())

	// Recovering a corrupt index resets the codec.
	corruptJournalIndex(t, filepath.Join(filepath.Dir(path), journalIndexFileName))
	hj = &hookJournal{}
	j, _, err = openJournalWriter(ctx, path)
	require.NoError(t, err)
	j.codec = hj
	_, err = j.bootstrapJournal(ctx, true, nil, nil)
	require.NoError(t, err)
	require.NoError(t, j.Close())
	hj.require(t, "reset")
}

// recode gives journal record |rec| kind |kind|.
func recode(rec []byte, kind journalRecKind) []byte {
	rec[journalRecLenSz+journalRecTagSz] = byte(kind)
	n := len(rec) - journalRecChecksumSz
	writeUint32(rec[n:], crc(rec[:n]))
	return rec
}

func TestJournalRecordKindHooks(t *testing.T) {
	ctx := context.Background()
	const codedChunk, codedRoot, pad = journalRecKind(0x80), journalRecKind(0x81), 8
	var r reached
	hookVar(t, &isChunkRecKind, func(k journalRecKind) bool { return k == chunkJournalRecKind || k == codedChunk })
	hookVar(t, &isRootHashRecKind, func(k journalRecKind) bool { return k == rootHashJournalRecKind || k == codedRoot })
	hookVar(t, &maxRootHashRecordSize, func() int { return rootHashRecordSize() + journalRecTagSz + pad })
	hookVar(t, &codedPayloadSize, func(rec journalRec) uint64 {
		r.hit("codedPayloadSize")
		return uint64(len(rec.payload))
	})

	cc := ChunkToCompressedChunk(chunks.NewChunk([]byte("coded")))
	sz, _ := chunkRecordSize(cc)
	chunkRec := make([]byte, sz)
	writeChunkRecord(chunkRec, cc)
	recode(chunkRec, codedChunk)
	// A coded root record is longer than a plain one.
	root := hash.Of([]byte("root"))
	rootRec := make([]byte, rootHashRecordSize())
	writeRootHashRecord(rootRec, root)
	rootRec = append(rootRec[:len(rootRec)-journalRecChecksumSz], byte(payloadJournalRecTag))
	rootRec = append(rootRec, make([]byte, pad+journalRecChecksumSz)...)
	writeUint32(rootRec, uint32(len(rootRec)))
	recode(rootRec, codedRoot)
	require.Equal(t, maxRootHashRecordSize(), len(rootRec))

	path := newTestFilePath(t)
	require.NoError(t, os.WriteFile(path, append(append([]byte{}, chunkRec...), rootRec...), 0644))
	j, ok, err := openJournalWriter(ctx, path)
	require.NoError(t, err)
	require.True(t, ok)
	defer j.Close()
	last, err := j.bootstrapJournal(ctx, false, nil, nil)
	require.NoError(t, err)
	require.Equal(t, root, last)
	require.True(t, j.hasAddr(cc.H))
	r.require(t, "codedPayloadSize")

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	peeked, err := peekRootHashAt(f, int64(len(chunkRec)))
	require.NoError(t, err)
	require.Equal(t, root, peeked)
	loss, err := possibleDataLossCheck(bufio.NewReader(bytes.NewReader(append(append([]byte{}, rootRec...), chunkRec...))))
	require.NoError(t, err)
	require.True(t, loss)
}

// hookArchive fails the archive operations that read chunks with errHook, and records the rest.
type hookArchive struct {
	*reached
}

func (a hookArchive) openData(_ context.Context, _ *archiveReader, _ resolvedChunk, _ *DecompBundle, stored []byte) ([]byte, error) {
	return stored, a.hit("openData")
}
func (a hookArchive) openDict(context.Context, *archiveReader, uint32, []byte) (*DecompBundle, error) {
	return nil, errHook
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

// recordingSpans writes an archive's spans as they are, with a key block before its footer, under a version only a
// codec reads.
type recordingSpans struct {
	reached
}

const testCodedArchiveVersion = 0x80

var testKeyBlock = []byte("KEYS")

func (s *recordingSpans) writeDict(aw *archiveWriter, compressed []byte) (uint32, error) {
	s.hit("writeDict")
	return aw.writeByteSpan(compressed)
}

func (s *recordingSpans) writeZStdData(aw *archiveWriter, _ hash.Hash, _ uint32, data []byte) (uint32, error) {
	s.hit("writeZStdData")
	return aw.writeByteSpan(data)
}

func (s *recordingSpans) writeSnappyData(aw *archiveWriter, cc CompressedChunk) (uint32, error) {
	s.hit("writeSnappyData")
	return aw.writeByteSpan(cc.FullCompressedChunk)
}

func (s *recordingSpans) writeKeyBlock(aw *archiveWriter) error {
	s.hit("writeKeyBlock")
	n, err := aw.output.Write(testKeyBlock)
	aw.bytesWritten += uint64(n)
	return err
}

func (s *recordingSpans) putKeyBlockLen([]byte) { s.hit("putKeyBlockLen") }
func (s *recordingSpans) formatVersion() uint8  { return testCodedArchiveVersion }

func TestArchiveCodecHooks(t *testing.T) {
	ctx := context.Background()
	q := NewUnlimitedMemQuotaProvider()
	var r reached
	hookVar(t, &codedArchiveVersion, func(v uint8) bool {
		r.hit("codedArchiveVersion")
		return v == testCodedArchiveVersion
	})
	hookVar(t, &archiveFooterExtra, func(f *archiveFooter) uint64 {
		if f.formatVersion != testCodedArchiveVersion {
			return 0
		}
		return uint64(len(testKeyBlock))
	})
	hookVar(t, &addCodedChunk, func(*ArchiveStreamWriter, ToChunker) (uint32, bool, error) {
		return 0, false, r.hit("addCodedChunk")
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
	_, fileName, err := asw.Finish()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), fileName)
	require.NoError(t, asw.FlushToFile(path))
	spans.require(t, "writeDict", "writeZStdData", "writeSnappyData", "writeKeyBlock", "putKeyBlockLen")

	// The codec of a coded archive reads its key block when the archive opens.
	hookVar(t, &openArchiveCodec, func(ctx context.Context, rd tableReaderAt, f archiveFooter, _ openOpts, stats *Stats) (archiveCodec, error) {
		if f.formatVersion != testCodedArchiveVersion {
			return nil, nil
		}
		kb := make([]byte, len(testKeyBlock))
		if _, err := rd.ReadAtWithStats(ctx, kb, int64(f.fileSize-archiveFooterSize)-int64(len(kb)), stats); err != nil {
			return nil, err
		} else if !bytes.Equal(kb, testKeyBlock) {
			return nil, fmt.Errorf("read key block %q", kb)
		}
		return hookArchive{&r}, r.hit("openArchiveCodec")
	})
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	name := hash.Parse(strings.TrimSuffix(fileName, ArchiveFileSuffix))
	bs := wholeBlobBlobstore{blobstore.NewInMemoryBlobstore(""), true}
	_, err = bs.Put(ctx, fileName, int64(len(data)), bytes.NewReader(data))
	require.NoError(t, err)
	spooled, err := newBSArchiveChunkSource(ctx, bs, name, q, openOpts{}, &Stats{})
	require.NoError(t, err)
	require.NoError(t, spooled.close())
	ar, err := newArchiveReader(ctx, tableReaderAtAdapter{bytes.NewReader(data)}, name, uint64(len(data)), q, openOpts{}, &Stats{})
	require.NoError(t, err)
	_, err = newArchiveMetadata(ctx, tableReaderAtAdapter{bytes.NewReader(data)}, name, uint64(len(data)), q, &Stats{})
	require.NoError(t, err)
	r.require(t, "codedArchiveVersion", "addCodedChunk", "openArchiveCodec")

	_, err = ar.get(ctx, hashes[0], &Stats{})
	require.ErrorIs(t, err, errHook)
	cl, err := ar.clone()
	require.NoError(t, err)
	_, err = cl.get(ctx, hashes[0], &Stats{})
	require.ErrorIs(t, err, errHook)
	require.NoError(t, cl.close())
	_, err = ar.getAsToChunker(ctx, hashes[0], &Stats{})
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
	_, _, err = acs.getRecordRanges(ctx, dherrors.FatalBehaviorError, reqs(append(hashes, snappy.H)...), nil)
	require.ErrorIs(t, err, errHook)
	eg, ectx := errgroup.WithContext(ctx)
	_, _, err = acs.getManyCompressed(ectx, eg, reqs(snappy.H), func(context.Context, ToChunker) {}, nil, &Stats{})
	require.ErrorIs(t, errors.Join(err, eg.Wait()), errHook)
	eg, ectx = errgroup.WithContext(ctx)
	_, _, err = acs.getMany(ectx, eg, reqs(snappy.H), func(context.Context, *chunks.Chunk) {}, nil, &Stats{})
	require.ErrorIs(t, errors.Join(err, eg.Wait()), errHook)
	r.require(t, "openData")

	hookVar(t, &conjoinSpanSealer, func(context.Context, []sourceWithSize) (spanSealer, error) { return nil, errHook })
	a, _ := createTestArchive(t, 2, [][]byte{[]byte("third")}, "{}")
	b, _ := createTestArchive(t, 3, [][]byte{[]byte("fourth")}, "{}")
	var sources []sourceWithSize
	for _, cs := range readersToSource([]archiveReader{a, b}) {
		sources = append(sources, sourceWithSize{cs, cs.currentSize()})
	}
	_, err = planArchiveConjoin(ctx, sources, q, &Stats{})
	require.ErrorIs(t, err, errHook)
}

// hookRecords fails the table operations that read records with errHook.
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
	refuse := false
	hookVar(t, &openTableCodec, func(tr tableReader, _ openOpts) (tableReader, error) {
		if refuse {
			return tableReader{}, errors.Join(tr.close(), errHook)
		}
		tr.codec = hookRecords{}
		return tr, r.hit("openTableCodec")
	})
	hookVar(t, &refusesFile, func(err error) bool { return errors.Is(err, errHook) })
	hookVar(t, &tableIndexExtra, func(tableIndex) uint64 {
		r.hit("tableIndexExtra")
		return 0
	})
	hookVar(t, &tableDataEnd, func(_ chunkSource, end uint64) uint64 {
		r.hit("tableDataEnd")
		return end
	})
	hookVar(t, &conjoinedTableTail, func([]sourceWithSize) ([]byte, error) { return nil, r.hit("conjoinedTableTail") })
	hookVar(t, &finishConjoinedTable, func(*compactionPlan, []byte) { r.hit("finishConjoinedTable") })

	bs := blobstore.NewInMemoryBlobstore("")
	var sources []sourceWithSize
	for _, chunkData := range [][]byte{[]byte("first"), []byte("second")} {
		data, name, err := buildTable([][]byte{chunkData})
		require.NoError(t, err)
		_, err = bs.Put(ctx, name.String(), int64(len(data)), bytes.NewReader(data))
		require.NoError(t, err)
		cs, err := newBSTableChunkSource(ctx, bs, name, 1, q, openOpts{}, &Stats{})
		require.NoError(t, err)
		defer cs.close()
		idx, err := cs.index()
		require.NoError(t, err)
		sources = append(sources, sourceWithSize{cs, calcChunkRangeSize(idx)})
	}
	cs, h := sources[0].source, chunks.NewChunk([]byte("first")).Hash()
	recs := func() []getRecord { return []getRecord{{a: &h, prefix: h.Prefix()}} }
	_, _, err := cs.get(ctx, h, nil, &Stats{})
	require.ErrorIs(t, err, errHook)
	cl, err := cs.clone()
	require.NoError(t, err)
	_, _, err = cl.get(ctx, h, nil, &Stats{})
	require.ErrorIs(t, err, errHook)
	require.NoError(t, cl.close())
	require.ErrorIs(t, cs.iterateAllChunks(ctx, func(chunks.Chunk) {}, &Stats{}), errHook)
	var iterErr error
	cs.tolerantIterateAllChunks(ctx, func(chunks.Chunk) {}, func(err error) { iterErr = err }, &Stats{})
	require.ErrorIs(t, iterErr, errHook)
	require.ErrorIs(t, cs.(*chunkSourceAdapter).extract(ctx, make(chan extractRecord, 1)), errHook)
	_, _, err = cs.getRecordRanges(ctx, dherrors.FatalBehaviorError, recs(), nil)
	require.ErrorIs(t, err, errHook)
	eg, ectx := errgroup.WithContext(ctx)
	_, _, err = cs.getManyCompressed(ectx, eg, recs(), func(context.Context, ToChunker) {}, nil, &Stats{})
	require.NoError(t, err)
	require.ErrorIs(t, eg.Wait(), errHook)

	newTableFile(cs, tableSpec{name: hash.Hash{}, chunkCount: 1}, dherrors.FatalBehaviorError)
	plan, err := planTableConjoin(ctx, sources, q, &Stats{})
	require.NoError(t, err)
	plan.closer()
	r.require(t, "openTableCodec", "tableIndexExtra", "tableDataEnd", "conjoinedTableTail", "finishConjoinedTable")

	// Table files open with their codec from a directory and from S3 too.
	data, name, err := buildTable([][]byte{[]byte("first")})
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()), data, 0644))
	fcs, err := newFSTablePersister(dir, q, false).Open(ctx, name, 1, openOpts{}, &Stats{})
	require.NoError(t, err)
	defer fcs.close()
	_, _, err = fcs.get(ctx, h, nil, &Stats{})
	require.ErrorIs(t, err, errHook)

	mt := newMemTable(defaultMemTableSize)
	mt.addChunk(h, []byte("first"))
	s3p := awsTablePersister{s3: makeFakeS3(t), bucket: "bucket", limits: awsLimits{partTarget: 1 << 20 * 64}, q: q}
	src, _, err := s3p.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	require.NoError(t, src.close())
	scs, err := s3p.Open(ctx, src.hash(), 1, openOpts{}, &Stats{})
	require.NoError(t, err)
	defer scs.close()
	_, _, err = scs.get(ctx, h, nil, &Stats{})
	require.ErrorIs(t, err, errHook)
	// A file that its codec refuses is not tried as an archive.
	refuse = true
	_, err = s3p.Open(ctx, src.hash(), 1, openOpts{}, &Stats{})
	require.ErrorIs(t, err, errHook)
}
