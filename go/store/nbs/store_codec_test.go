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
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
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

func requireNoPlaintext(t *testing.T, data []byte, cs []chunks.Chunk) {
	for _, c := range cs {
		require.False(t, bytes.Contains(data, c.Data()[:12]), "found the plaintext of %s", c.Hash())
	}
}

// withPlainDefault makes stores opened without a StoreOption open only plaintext, as Dolt's do.
func withPlainDefault(t *testing.T) {
	prev := newStoreCodec
	newStoreCodec = func(_ bool, format string) (string, storeCodec, error) {
		if format == constants.EncryptedFormatString {
			return "", nil, ErrEncryptionUnsupported
		}
		return format, plainCodec{}, nil
	}
	t.Cleanup(func() { newStoreCodec = prev })
}

func writeToyTable(t *testing.T, c *toyCodec, cs []chunks.Chunk) ([]byte, uint64) {
	_, data, splitOffset := writeNamedToyTable(t, c, cs)
	return data, splitOffset
}

func writeNamedToyTable(t *testing.T, c *toyCodec, cs []chunks.Chunk) (hash.Hash, []byte, uint64) {
	mt := newMemTable(1 << 20)
	mt.codec = c
	for _, chk := range cs {
		require.Equal(t, chunkAdded, mt.addChunk(chk.Hash(), chk.Data()))
	}
	name, data, splitOffset, count, _, err := mt.write(nil, nil, &Stats{})
	require.NoError(t, err)
	require.Equal(t, uint32(len(cs)), count)
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

func getManySealed(t *testing.T, cr chunkReader, cs []chunks.Chunk) []ToChunker {
	hs := make(hash.HashSet)
	for _, c := range cs {
		hs.Insert(c.Hash())
	}
	var found []ToChunker
	eg, ctx := errgroup.WithContext(t.Context())
	_, _, err := cr.getManyCompressed(ctx, eg, toGetRecords(hs), func(_ context.Context, c ToChunker) { found = append(found, c) }, nil, &Stats{})
	require.NoError(t, err)
	require.NoError(t, eg.Wait())
	require.Len(t, found, len(cs))
	return found
}

func TestToyCodecTableFile(t *testing.T) {
	c := newToyCodec()
	cs := toyChunks(64)
	data, splitOffset := writeToyTable(t, c, cs)
	requireNoPlaintext(t, data, cs)
	require.True(t, isEncryptedFooter(data))

	tr, err := openToyTable(t, data, openOpts{codec: c})
	require.NoError(t, err)
	defer tr.close()
	require.Equal(t, calcChunkRangeSize(tr.idx), splitOffset)
	require.NoError(t, tr.idx.(onHeapTableIndex).deepValidate(hash.Hash{}))
	for _, chk := range cs {
		got, _, err := tr.get(t.Context(), chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got)
	}
	for _, tc := range getManySealed(t, tr, cs) {
		require.IsType(t, &toyChunk{}, tc)
		chk, err := tc.ToChunk()
		require.NoError(t, err)
		require.Equal(t, tc.Hash(), chk.Hash())
	}
	var n int
	require.NoError(t, tr.iterateAllChunks(t.Context(), func(chunks.Chunk) { n++ }, &Stats{}))
	require.Equal(t, len(cs), n)

	t.Run("RangeKeys", func(t *testing.T) {
		keys := &LocatedKeys{keys: map[rangeLoc]RangeKeys{}}
		ctx := context.WithValue(t.Context(), locatedKeysKey{}, keys)
		hs := make(hash.HashSet)
		for _, chk := range cs {
			hs.Insert(chk.Hash())
		}
		ranges, _, err := tr.getRecordRanges(ctx, dherrors.FatalBehaviorError, toGetRecords(hs), nil)
		require.NoError(t, err)
		for h, r := range ranges {
			require.Len(t, keys.At(h, r.Offset, r.Length).Key, 1)
		}
	})
	t.Run("PlaintextStoreRefuses", func(t *testing.T) {
		_, err := openToyTable(t, data, openOpts{codec: plainCodec{}})
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
	t.Run("EncryptedStoreRefusesPlaintext", func(t *testing.T) {
		_, plain, _, err := WriteChunks(cs)
		require.NoError(t, err)
		_, err = openToyTable(t, plain, openOpts{codec: c})
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
	t.Run("IterChunksRefuses", func(t *testing.T) {
		require.Error(t, IterChunks(t.Context(), bytes.NewReader(data), func(chunks.Chunk) (bool, error) { return false, nil }))
	})
	t.Run("WrongKeyFails", func(t *testing.T) {
		oc := newToyCodec()
		oc.nextKey.Store(100)
		other, _ := writeToyTable(t, oc, cs)
		swapped := append(bytes.Clone(data[:splitOffset]), other[splitOffset:]...)
		tr, err := openToyTable(t, swapped, openOpts{codec: c})
		require.NoError(t, err)
		defer tr.close()
		_, _, err = tr.get(t.Context(), cs[0].Hash(), nil, &Stats{})
		require.Error(t, err)
	})
}

func TestToyCodecCopiesSealedRecords(t *testing.T) {
	c := newToyCodec()
	cs := toyChunks(16)
	data, _ := writeToyTable(t, c, cs)
	tr, err := openToyTable(t, data, openOpts{codec: c})
	require.NoError(t, err)
	defer tr.close()
	sealed := getManySealed(t, tr, cs)

	t.Run("PlaintextWriterRefuses", func(t *testing.T) {
		tw, err := NewCmpChunkTableWriter("")
		require.NoError(t, err)
		_, err = tw.AddChunk(sealed[0])
		require.ErrorIs(t, err, ErrEncryptionMismatch)
		asw, err := NewArchiveStreamWriter(t.TempDir())
		require.NoError(t, err)
		defer asw.Cancel()
		_, err = asw.AddChunk(sealed[0])
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})

	w, err := newTableWriterFromArchiveLevel(chunks.NoArchive, c)
	require.NoError(t, err)
	tw := w.(*CmpChunkTableWriter)
	for _, sc := range sealed {
		_, err = tw.AddChunk(sc)
		require.NoError(t, err)
	}
	extra := toyChunks(20)[16:]
	for _, chk := range extra {
		_, err = tw.AddChunk(ChunkToCompressedChunk(chk))
		require.NoError(t, err)
	}
	_, _, err = tw.Finish()
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, tw.Flush(&out))
	copied := out.Bytes()
	for _, sc := range sealed {
		require.True(t, bytes.Contains(copied, sc.(SealedChunk).sealedRecord()), "a sealed record was not copied verbatim")
	}
	requireNoPlaintext(t, copied, extra)

	tr2, err := openToyTable(t, copied, openOpts{codec: c})
	require.NoError(t, err)
	defer tr2.close()
	for _, chk := range append(cs, extra...) {
		got, _, err := tr2.get(t.Context(), chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got)
	}
}

func writeToyArchive(t *testing.T, sealer spanSealer, add func(*ArchiveStreamWriter)) ([]byte, hash.Hash) {
	asw, err := NewArchiveStreamWriter(t.TempDir())
	require.NoError(t, err)
	asw.writer.sealer = sealer
	add(asw)
	_, name, err := asw.Finish()
	require.NoError(t, err)
	rd, err := asw.Reader()
	require.NoError(t, err)
	defer rd.Close()
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	h, ok := hash.MaybeParse(strings.TrimSuffix(name, ArchiveFileSuffix))
	require.True(t, ok)
	return data, h
}

func openToyArchive(t *testing.T, data []byte, name hash.Hash, opts openOpts) (archiveReader, error) {
	return newArchiveReader(t.Context(), tableReaderAtFromBytes(data), name, uint64(len(data)), &UnlimitedQuotaProvider{}, opts, &Stats{})
}

func TestToyCodecArchive(t *testing.T) {
	// Fewer chunks than maxSamples are written snappy; more get a dictionary.
	for _, n := range []int{10, maxSamples + 20} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			c := newToyCodec()
			cs := toyChunks(n)
			sealer, err := c.newSpanSealer(false)
			require.NoError(t, err)
			data, name := writeToyArchive(t, sealer, func(asw *ArchiveStreamWriter) {
				for _, chk := range cs {
					_, err := asw.AddChunk(ChunkToCompressedChunk(chk))
					require.NoError(t, err)
				}
			})
			requireNoPlaintext(t, data, cs)

			ar, err := openToyArchive(t, data, name, openOpts{codec: c, deepValidate: true})
			require.NoError(t, err)
			defer ar.close()
			require.True(t, ar.footer.encrypted())
			for _, chk := range cs {
				got, err := ar.get(t.Context(), chk.Hash(), &Stats{})
				require.NoError(t, err)
				require.Equal(t, chk.Data(), got)
			}
			var n1, n2 int
			require.NoError(t, ar.iterate(t.Context(), func(chunks.Chunk) error { n1++; return nil }, &Stats{}))
			ar.tolerantIterate(t.Context(), func(chunks.Chunk) { n2++ }, func(err error) { require.NoError(t, err) }, &Stats{})
			require.Equal(t, len(cs), n1)
			require.Equal(t, len(cs), n2)

			_, err = openToyArchive(t, data, name, openOpts{codec: plainCodec{}})
			require.ErrorIs(t, err, ErrEncryptionMismatch)

			// Copy every chunk still sealed into a new archive.
			copier, err := c.newSpanSealer(false)
			require.NoError(t, err)
			copied, copiedName := writeToyArchive(t, copier, func(asw *ArchiveStreamWriter) {
				for _, chk := range cs {
					tc, err := ar.getAsToChunker(t.Context(), chk.Hash(), &Stats{})
					require.NoError(t, err)
					require.IsType(t, &toyChunk{}, tc)
					_, err = asw.AddChunk(tc)
					require.NoError(t, err)
				}
			})
			require.Len(t, copied, len(data))
			ar2, err := openToyArchive(t, copied, copiedName, openOpts{codec: c})
			require.NoError(t, err)
			defer ar2.close()
			for _, chk := range cs {
				got, err := ar2.get(t.Context(), chk.Hash(), &Stats{})
				require.NoError(t, err)
				require.Equal(t, chk.Data(), got)
			}
		})
	}
}

func TestToyCodecLocalStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newToyCodec()
	q := NewUnlimitedMemQuotaProvider()
	open := func(opts ...StoreOption) (*NomsBlockStore, error) {
		return newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<10, 3, q, false, opts...)
	}

	st, err := open(withToy(c))
	require.NoError(t, err)
	var all []chunks.Chunk
	for i := 0; i < 6; i++ {
		cs := toyChunks(40 * (i + 1))[40*i:]
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
	require.Equal(t, constants.EncryptedFormatString, st.Version())
	require.NoError(t, st.Close())

	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, f.Name()))
		require.NoError(t, err)
		requireNoPlaintext(t, b, all)
	}

	st, err = open(withToy(c))
	require.NoError(t, err)
	defer st.Close()
	for _, chk := range all {
		got, err := st.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got.Data())
	}
	hashesOf := func(cs []chunks.Chunk) hash.HashSet {
		hs := make(hash.HashSet)
		for _, chk := range cs {
			hs.Insert(chk.Hash())
		}
		return hs
	}
	locs, keys, err := GetChunkLocationsWithPathsAndKeys(ctx, st, hashesOf(all))
	require.NoError(t, err)
	for _, byHash := range locs {
		for h, r := range byHash {
			require.NotNil(t, keys.At(h, r.Offset, r.Length).Key)
		}
	}

	t.Run("PlaintextDolt", func(t *testing.T) {
		withPlainDefault(t)
		_, err := open()
		require.ErrorIs(t, err, ErrEncryptionUnsupported)
	})
	t.Run("Keyless", func(t *testing.T) {
		keyless := &toyCodec{keyless: true, nextKey: c.nextKey, added: c.added}
		st, err := open(withToy(keyless))
		require.NoError(t, err)
		defer st.Close()
		_, err = st.Get(ctx, all[0].Hash())
		require.ErrorIs(t, err, errToyKeyless)
		require.ErrorIs(t, st.Put(ctx, toyChunks(1)[0], noopGetAddrs), errToyKeyless)
		_, keys, err := GetChunkLocationsWithPathsAndKeys(ctx, st, hashesOf(all))
		require.NoError(t, err)
		require.Len(t, keys.keys, len(all))
	})
}

func TestToyCodecJournal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newToyCodec()
	q := NewUnlimitedMemQuotaProvider()
	open := func(opts ...StoreOption) (*NomsBlockStore, error) {
		return NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, opts...)
	}

	st, err := open(withToy(c))
	require.NoError(t, err)
	cs := toyChunks(100)
	for _, chk := range cs {
		require.NoError(t, st.Put(ctx, chk, noopGetAddrs))
	}
	ok, err := st.Commit(ctx, cs[0].Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, constants.EncryptedFormatString, st.Version())
	require.NoError(t, st.Close())

	journal, err := os.ReadFile(filepath.Join(dir, chunkJournalName))
	require.NoError(t, err)
	requireNoPlaintext(t, journal, cs)
	first, err := readJournalRecord(journal)
	require.NoError(t, err)
	require.Equal(t, keyJournalRecKind, first.kind)

	st, err = open(withToy(c))
	require.NoError(t, err)
	for _, chk := range cs {
		got, err := st.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got.Data())
	}
	root, err := st.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, cs[0].Hash(), root)
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
}

func TestToyCodecJournalIndex(t *testing.T) {
	ctx := context.Background()
	c := newToyCodec()
	path := newTestFilePath(t)
	j, err := createJournalWriter(ctx, path)
	require.NoError(t, err)
	j.codec = c.newJournalCodec()
	j.maxNovel = 4
	_, err = j.bootstrapJournal(ctx, true, nil, nil)
	require.NoError(t, err)
	data := randomCompressedChunks(32)
	var last hash.Hash
	for h, cc := range data {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
		require.NoError(t, j.commitRootHash(ctx, dherrors.FatalBehaviorError, h))
		last = h
	}
	require.NoError(t, j.Close())

	reopen := func() (*journalWriter, *toyJournal) {
		j, ok, err := openJournalWriter(ctx, path)
		require.NoError(t, err)
		require.True(t, ok)
		jc := c.newJournalCodec().(*toyJournal)
		j.codec = jc
		got, err := j.bootstrapJournal(ctx, true, nil, func(error) {})
		require.NoError(t, err)
		require.Equal(t, last, got)
		for h, cc := range data {
			got, err := j.getChunker(h)
			require.NoError(t, err)
			chk, err := got.ToChunk()
			require.NoError(t, err)
			want, err := cc.ToChunk()
			require.NoError(t, err)
			require.Equal(t, want.Data(), chk.Data())
		}
		return j, jc
	}

	j, jc := reopen()
	require.Greater(t, j.indexed, int64(0), "the index was not used")
	require.Zero(t, jc.resets)
	require.NoError(t, j.Close())

	// A forged authentication record makes the journal rebuild its index.
	idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)
	idx, err := os.ReadFile(idxPath)
	require.NoError(t, err)
	i := bytes.IndexByte(idx, indexRecAuth)
	require.GreaterOrEqual(t, i, 0)
	idx[i+uint16Size+1] ^= 0xff
	require.NoError(t, os.WriteFile(idxPath, idx, 0644))
	j, jc = reopen()
	require.Equal(t, 1, jc.resets)
	require.NoError(t, j.Close())
}

func TestToyCodecConjoin(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newToyCodec()
	p := newFSTablePersister(dir, NewUnlimitedMemQuotaProvider(), false).(*fsTablePersister)
	p.setCodec(c)
	cs := toyChunks(maxSamples + 60)
	parts := [][]chunks.Chunk{cs[:20], cs[20:40], cs[40:]}

	var sources chunkSources
	for i, part := range parts {
		var name hash.Hash
		var data []byte
		var fileName string
		if i == 0 {
			name, data, _ = writeNamedToyTable(t, c, part)
			fileName = name.String()
		} else {
			sealer, err := c.newSpanSealer(false)
			require.NoError(t, err)
			data, name = writeToyArchive(t, sealer, func(asw *ArchiveStreamWriter) {
				for _, chk := range part {
					_, err := asw.AddChunk(ChunkToCompressedChunk(chk))
					require.NoError(t, err)
				}
			})
			fileName = name.String() + ArchiveFileSuffix
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), data, 0644))
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
	for _, chk := range cs {
		got, _, err := conjoined.get(ctx, chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got)
	}

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

func TestToyCodecAWSStore(t *testing.T) {
	ctx := context.Background()
	c := newToyCodec()
	s3, ddb := makeFakeS3(t), makeFakeDDB(t)
	ddb.vers = constants.EncryptedFormatString
	open := func() *NomsBlockStore {
		st, err := NewAWSStore(ctx, constants.EncryptedFormatString, "table", "db", "bucket", s3, ddb, 1<<10, NewUnlimitedMemQuotaProvider(), withToy(c))
		require.NoError(t, err)
		// Small parts, so conjoin copies records with UploadPartCopy.
		st.persister.(*awsTablePersister).limits = awsLimits{partTarget: defaultS3PartSize, partMin: 1 << 10, partMax: 4 << 10}
		return st
	}
	st := open()
	var all []chunks.Chunk
	for i := 0; i < 4; i++ {
		cs := toyChunks(60 * (i + 1))[60*i:]
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
	for _, data := range s3.data {
		requireNoPlaintext(t, data, all)
	}
	var sources chunkSources
	for _, cs := range st.tables.upstream {
		sources = append(sources, cs)
	}
	require.Greater(t, len(sources), 1)
	conjoined, cleanup, err := st.persister.(*awsTablePersister).ConjoinAll(ctx, dherrors.FatalBehaviorError, sources, &Stats{})
	require.NoError(t, err)
	for _, chk := range all {
		got, _, err := conjoined.get(ctx, chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got)
	}
	require.NoError(t, conjoined.close())
	cleanup()
	require.NoError(t, st.Close())

	st = open()
	defer st.Close()
	for _, chk := range all {
		got, err := st.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got.Data())
	}
}
