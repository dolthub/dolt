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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func toyChunks(n int) []chunks.Chunk {
	cs := make([]chunks.Chunk, n)
	for i := range cs {
		cs[i] = chunks.NewChunk([]byte(fmt.Sprintf("toy chunk %d: %s", i, strings.Repeat("plaintext ", 1+i%7))))
	}
	return cs
}

func toyHashes(cs []chunks.Chunk) hash.HashSet {
	hs := make(hash.HashSet)
	for _, c := range cs {
		hs.Insert(c.Hash())
	}
	return hs
}

func requireNoPlaintext(t *testing.T, data []byte, cs []chunks.Chunk) {
	for _, c := range cs {
		require.False(t, bytes.Contains(data, c.Data()[:12]), "found the plaintext of %s", c.Hash())
	}
}

// withPlainDefault makes stores opened without a StoreOption open only plaintext, as Dolt's do.
func withPlainDefault(t *testing.T) {
	prev := newStoreCodec
	newStoreCodec = plainStoreCodec
	t.Cleanup(func() { newStoreCodec = prev })
}

func writeToyTable(t *testing.T, c *toyCodec, cs []chunks.Chunk) (hash.Hash, []byte, uint64) {
	mt := newMemTable(1 << 20)
	mt.codec = c
	for _, chk := range cs {
		require.Equal(t, chunkAdded, mt.addChunk(chk.Hash(), chk.Data()))
	}
	name, data, splitOffset, _, _, err := mt.write(nil, nil, &Stats{})
	require.NoError(t, err)
	return name, data, splitOffset
}

func openToyTable(t *testing.T, data []byte, opts openOpts) (tableReader, error) {
	idx, err := parseTableIndexByCopy(t.Context(), data, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr, err := newTableReaderFor(t.Context(), idx, tableReaderAtFromBytes(data), fileBlockSize, opts)
	if err != nil {
		idx.Close()
	}
	return tr, err
}

func writeToyArchive(t *testing.T, c *toyCodec, cs []chunks.Chunk) ([]byte, hash.Hash) {
	asw, err := NewArchiveStreamWriter(t.TempDir())
	require.NoError(t, err)
	asw.writer.sealer, err = c.newSpanSealer(false)
	require.NoError(t, err)
	for _, chk := range cs {
		_, err := asw.AddChunk(ChunkToCompressedChunk(chk))
		require.NoError(t, err)
	}
	_, name, err := asw.Finish()
	require.NoError(t, err)
	rd, err := asw.Reader()
	require.NoError(t, err)
	defer rd.Close()
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	return data, hash.Parse(strings.TrimSuffix(name, ArchiveFileSuffix))
}

func openToyArchive(t *testing.T, data []byte, name hash.Hash, opts openOpts) (archiveReader, error) {
	return newArchiveReader(t.Context(), tableReaderAtFromBytes(data), name, uint64(len(data)), &UnlimitedQuotaProvider{}, opts, &Stats{})
}

// fillToyStore commits |rounds| batches of |per| chunks to |st|.
func fillToyStore(t *testing.T, st *NomsBlockStore, rounds, per int) []chunks.Chunk {
	ctx := t.Context()
	var all []chunks.Chunk
	for i := 0; i < rounds; i++ {
		cs := toyChunks(per * (i + 1))[per*i:]
		for _, chk := range cs {
			require.NoError(t, st.Put(ctx, chk, noopGetAddrs))
		}
		all = append(all, cs...)
		root, err := st.Root(ctx)
		require.NoError(t, err)
		ok, err := st.Commit(ctx, cs[0].Hash(), root)
		require.NoError(t, err)
		require.True(t, ok)
	}
	return all
}

func requireHas(t *testing.T, cr chunkReader, all []chunks.Chunk) {
	for _, chk := range all {
		got, _, err := cr.get(t.Context(), chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got, "chunk %s", chk.Hash())
	}
}

func requireStoreHas(t *testing.T, st *NomsBlockStore, all []chunks.Chunk) {
	for _, chk := range all {
		got, err := st.Get(t.Context(), chk.Hash())
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got.Data(), "chunk %s", chk.Hash())
	}
}

func TestToyCodecTableFile(t *testing.T) {
	c := &toyCodec{}
	cs := toyChunks(64)
	_, data, splitOffset := writeToyTable(t, c, cs)
	requireNoPlaintext(t, data, cs)
	require.True(t, isEncryptedFooter(data))

	tr, err := openToyTable(t, data, openOpts{codec: c})
	require.NoError(t, err)
	defer tr.close()
	require.Equal(t, calcChunkRangeSize(tr.idx), splitOffset)
	require.NoError(t, tr.idx.(onHeapTableIndex).deepValidate(hash.Hash{}))
	requireHas(t, &tr, cs)
	var n int
	require.NoError(t, tr.iterateAllChunks(t.Context(), func(chunks.Chunk) { n++ }, &Stats{}))
	require.Equal(t, len(cs), n)
	ranges, _, err := tr.getRecordRanges(t.Context(), dherrors.FatalBehaviorError, toGetRecords(toyHashes(cs)), nil)
	require.NoError(t, err)
	require.Len(t, ranges, len(cs))
	require.EqualValues(t, len(cs), c.located.Load())

	var sealed []ToChunker
	eg, ctx := errgroup.WithContext(t.Context())
	_, _, err = tr.getManyCompressed(ctx, eg, toGetRecords(toyHashes(cs)), func(_ context.Context, c ToChunker) { sealed = append(sealed, c) }, nil, &Stats{})
	require.NoError(t, err)
	require.NoError(t, eg.Wait())
	require.Len(t, sealed, len(cs))
	require.IsType(t, &toyChunk{}, sealed[0])

	t.Run("PlaintextStoreRefuses", func(t *testing.T) {
		_, err := openToyTable(t, data, openOpts{codec: plainCodec{}})
		require.ErrorIs(t, err, ErrEncryptionMismatch)
		err = IterChunks(t.Context(), bytes.NewReader(data), func(chunks.Chunk) (bool, error) { return false, nil })
		require.ErrorContains(t, err, "encrypted table file")
	})
	t.Run("EncryptedStoreRefusesPlaintext", func(t *testing.T) {
		_, plain, _, err := WriteChunks(cs)
		require.NoError(t, err)
		_, err = openToyTable(t, plain, openOpts{codec: c})
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
	t.Run("PlaintextWritersRefuseSealedChunks", func(t *testing.T) {
		tw, err := NewCmpChunkTableWriter("")
		require.NoError(t, err)
		defer tw.Cancel()
		_, err = tw.AddChunk(sealed[0])
		require.ErrorIs(t, err, ErrEncryptionMismatch)
		asw, err := NewArchiveStreamWriter(t.TempDir())
		require.NoError(t, err)
		defer asw.Cancel()
		_, err = asw.AddChunk(sealed[0])
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
}

func TestToyCodecArchive(t *testing.T) {
	// Fewer chunks than maxSamples are written snappy; more get a dictionary.
	for _, n := range []int{10, maxSamples + 20} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			c := &toyCodec{}
			cs := toyChunks(n)
			data, name := writeToyArchive(t, c, cs)
			requireNoPlaintext(t, data, cs)

			ar, err := openToyArchive(t, data, name, openOpts{codec: c, deepValidate: true})
			require.NoError(t, err)
			defer ar.close()
			require.True(t, ar.footer.encrypted())
			acs := &archiveChunkSource{aRdr: ar}
			requireHas(t, acs, cs)
			var n1, n2 int
			require.NoError(t, ar.iterate(t.Context(), func(chunks.Chunk) error { n1++; return nil }, &Stats{}))
			ar.tolerantIterate(t.Context(), func(chunks.Chunk) { n2++ }, func(err error) { require.NoError(t, err) }, &Stats{})
			require.Equal(t, len(cs), n1)
			require.Equal(t, len(cs), n2)
			ranges, _, err := acs.getRecordRanges(t.Context(), dherrors.FatalBehaviorError, toGetRecords(toyHashes(cs)), nil)
			require.NoError(t, err)
			require.Len(t, ranges, len(cs))
			require.EqualValues(t, len(cs), c.located.Load())

			_, err = openToyArchive(t, data, name, openOpts{codec: plainCodec{}})
			require.ErrorIs(t, err, ErrEncryptionMismatch)
		})
	}
}

func TestToyCodecJournal(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	c := &toyCodec{}
	q := NewUnlimitedMemQuotaProvider()
	open := func(opts ...StoreOption) (*NomsBlockStore, error) {
		st, err := NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, opts...)
		if err == nil {
			err = st.ensureLoad(ctx)
		}
		return st, err
	}

	st, err := open(withToy(c))
	require.NoError(t, err)
	fillToyStore(t, st, 1, 25)
	// Flush the journal index often, so it holds key and authentication records.
	st.persister.(*ChunkJournal).wr.maxNovel = 4
	all := fillToyStore(t, st, 4, 25)
	require.Equal(t, toyFormat, st.Version())
	require.NoError(t, st.Close())

	journal, err := os.ReadFile(filepath.Join(dir, chunkJournalName))
	require.NoError(t, err)
	requireNoPlaintext(t, journal, all)
	first, err := readJournalRecord(journal)
	require.NoError(t, err)
	require.Equal(t, keyJournalRecKind, first.kind)

	st, err = open(withToy(c))
	require.NoError(t, err)
	wr := st.persister.(*ChunkJournal).wr
	require.Greater(t, wr.indexed, int64(0), "the index was not used")
	require.Zero(t, wr.codec.(*toyJournal).resets)
	requireStoreHas(t, st, all)
	root, err := st.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, all[75].Hash(), root)
	locs, err := st.GetChunkLocationsWithPaths(ctx, toyHashes(all))
	require.NoError(t, err)
	var n int
	for _, byHash := range locs {
		n += len(byHash)
	}
	require.Equal(t, len(all), n)
	require.EqualValues(t, len(all), c.located.Load())
	n = 0
	require.NoError(t, st.IterateAllChunks(ctx, func(chunks.Chunk) { n++ }))
	require.Equal(t, len(all), n)
	require.NoError(t, st.Close())

	t.Run("PlaintextDolt", func(t *testing.T) {
		withPlainDefault(t)
		_, err := open()
		require.ErrorIs(t, err, ErrEncryptionUnsupported)
	})
	t.Run("VisitRefusesEncryptedKinds", func(t *testing.T) {
		err := VisitJournalReaderChunks(ctx, bytes.NewReader(journal), 0, func(CompressedChunk) error { return nil })
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
	t.Run("FormatLeftUnnamed", func(t *testing.T) {
		// Without a manifest, the store's format is the bare prefix its journal names, which the codec must name.
		require.NoError(t, os.Remove(filepath.Join(dir, manifestFileName)))
		unnamed := func(o *storeOptions) {
			o.newCodec = func(_ bool, format string) (string, storeCodec, error) { return format, c, nil }
		}
		_, err := open(unnamed)
		require.ErrorContains(t, err, "settled format")
		st, err := open(withToy(c))
		require.NoError(t, err)
		defer st.Close()
		require.Equal(t, toyFormat, st.Version())
	})
}

// A blobstore that spools opens a toy table or archive from its index, and reads its chunks from the spool.
func TestToyCodecSpooledBlobstore(t *testing.T) {
	ctx := t.Context()
	c := &toyCodec{}
	cs := toyChunks(maxSamples + 20)
	bs := blobstore.NewInMemoryBlobstore("")
	spooling := wholeBlobBlobstore{bs, true}

	name, table, _ := writeToyTable(t, c, cs)
	_, err := bs.Put(ctx, name.String(), int64(len(table)), bytes.NewReader(table))
	require.NoError(t, err)
	src, err := newBSTableChunkSource(ctx, spooling, name, uint32(len(cs)), &UnlimitedQuotaProvider{}, openOpts{codec: c}, &Stats{})
	require.NoError(t, err)
	defer src.close()
	requireHas(t, src, cs)

	archive, name := writeToyArchive(t, c, cs)
	_, err = bs.Put(ctx, name.String()+ArchiveFileSuffix, int64(len(archive)), bytes.NewReader(archive))
	require.NoError(t, err)
	src, err = newSpooledBSArchiveChunkSource(ctx, spooling, name, &UnlimitedQuotaProvider{}, openOpts{codec: c}, &Stats{})
	require.NoError(t, err)
	defer src.close()
	requireHas(t, src, cs)
}

func TestToyCodecConjoin(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	c := &toyCodec{}
	p := newFSTablePersister(dir, NewUnlimitedMemQuotaProvider(), false).(*fsTablePersister)
	p.setCodec(c)
	cs := toyChunks(maxSamples + 60)

	var sources chunkSources
	for i, part := range [][]chunks.Chunk{cs[:20], cs[20:40], cs[40:]} {
		var name hash.Hash
		var data []byte
		fileName := ""
		if i == 0 {
			name, data, _ = writeToyTable(t, c, part)
		} else {
			data, name = writeToyArchive(t, c, part)
			fileName = ArchiveFileSuffix
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()+fileName), data, 0644))
		src, err := p.Open(ctx, name, uint32(len(part)), openOpts{}, &Stats{})
		require.NoError(t, err)
		defer src.close()
		sources = append(sources, src)
	}

	conjoined, cleanup, err := p.ConjoinAll(ctx, dherrors.FatalBehaviorError, sources, &Stats{})
	require.NoError(t, err)
	defer cleanup()
	defer conjoined.close()
	require.IsType(t, &archiveChunkSource{}, conjoined)
	requireHas(t, conjoined, cs)

	t.Run("MixedSourcesRefused", func(t *testing.T) {
		_, plain, _, err := WriteChunks(toyChunks(3))
		require.NoError(t, err)
		idx, err := parseTableIndexByCopy(ctx, plain, &UnlimitedQuotaProvider{})
		require.NoError(t, err)
		tr, err := newTableReader(ctx, idx, tableReaderAtFromBytes(plain), fileBlockSize)
		require.NoError(t, err)
		defer tr.close()
		_, _, err = p.ConjoinAll(ctx, dherrors.FatalBehaviorError, chunkSources{sources[0], &chunkSourceAdapter{tr, hash.Hash{}}}, &Stats{})
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
}

// GC copies the sealed chunks of a toy store into new tables or archives.
func TestToyCodecGC(t *testing.T) {
	ctx := t.Context()
	for _, level := range []chunks.GCArchiveLevel{chunks.NoArchive, chunks.SimpleArchive} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			dir := t.TempDir()
			c := &toyCodec{}
			open := func() *NomsBlockStore {
				st, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<10, 8, NewUnlimitedMemQuotaProvider(), false, withToy(c))
				require.NoError(t, err)
				return st
			}
			st := open()
			all := fillToyStore(t, st, 3, maxSamples+10)

			require.NoError(t, st.BeginGC(ctx, nil, chunks.GCMode_Full))
			filter := func(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) { return hashes, nil }
			sweeper, err := st.MarkAndSweepChunks(ctx, noopWalkAddrs, filter, nil, chunks.NewGCConfig(chunks.GCMode_Full, level, chunks.IncrementalGCTablesDisabled), false)
			require.NoError(t, err)
			require.NoError(t, sweeper.SaveHashes(ctx, toyHashes(all)))
			finalizer, err := sweeper.Finalize(ctx)
			require.NoError(t, err)
			require.NoError(t, sweeper.Close(ctx))
			require.NoError(t, finalizer.SwapChunksInStore(ctx))
			require.NoError(t, finalizer.Close())
			st.EndGC(chunks.GCMode_Full)
			requireStoreHas(t, st, all)
			require.NoError(t, st.Close())

			st = open()
			defer st.Close()
			requireStoreHas(t, st, all)
		})
	}
}
