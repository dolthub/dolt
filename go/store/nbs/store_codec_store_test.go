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
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// fillToyStore commits |rounds| batches of |per| chunks to |st|.
func fillToyStore(t *testing.T, st *NomsBlockStore, rounds, per int) []chunks.Chunk {
	ctx := context.Background()
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

func requireStoreHas(t *testing.T, st *NomsBlockStore, all []chunks.Chunk) {
	for _, chk := range all {
		got, err := st.Get(context.Background(), chk.Hash())
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got.Data(), "chunk %s", chk.Hash())
	}
}

func conjoinUpstream(t *testing.T, st *NomsBlockStore, all []chunks.Chunk) {
	ctx := context.Background()
	var sources chunkSources
	for _, cs := range st.tables.upstream {
		sources = append(sources, cs)
	}
	require.Greater(t, len(sources), 1)
	conjoined, cleanup, err := st.persister.ConjoinAll(ctx, dherrors.FatalBehaviorError, sources, &Stats{})
	require.NoError(t, err)
	defer cleanup()
	defer conjoined.close()
	for _, chk := range all {
		got, _, err := conjoined.get(ctx, chk.Hash(), nil, &Stats{})
		require.NoError(t, err)
		require.Equal(t, chk.Data(), got)
	}
}

func TestToyCodecBlobstoreStores(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"split", "no conjoin", "single blob"} {
		t.Run(kind, func(t *testing.T) {
			c := newToyCodec()
			bs := blobstore.NewInMemoryBlobstore("")
			q := NewUnlimitedMemQuotaProvider()
			open := func() *NomsBlockStore {
				var st *NomsBlockStore
				var err error
				switch kind {
				case "split":
					st, err = NewBSStore(ctx, types.Format_DOLT.VersionString(), bs, 1<<10, q, withToy(c))
				case "no conjoin":
					st, err = NewNoConjoinBSStore(ctx, types.Format_DOLT.VersionString(), bs, 1<<10, q, withToy(c))
				default:
					p := &singleBlobBSPersister{bs: bs, q: q, blockSize: s3BlockSize}
					st, err = newNomsBlockStore(ctx, types.Format_DOLT.VersionString(), manifest(blobstoreManifest{bs}), p, q, inlineConjoiner{defaultMaxTables}, 1<<10, withToy(c))
				}
				require.NoError(t, err)
				return st
			}
			st := open()
			all := fillToyStore(t, st, 4, 60)
			requireStoreHas(t, st, all)
			if kind != "no conjoin" {
				conjoinUpstream(t, st, all)
			}
			require.NoError(t, st.Close())
			st = open()
			defer st.Close()
			requireStoreHas(t, st, all)
		})
	}
}

func gcToyStore(t *testing.T, st *NomsBlockStore, keep []chunks.Chunk, level chunks.GCArchiveLevel) error {
	ctx := context.Background()
	require.NoError(t, st.BeginGC(ctx, nil, chunks.GCMode_Full))
	defer st.EndGC(chunks.GCMode_Full)
	filter := func(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) { return hashes, nil }
	sweeper, err := st.MarkAndSweepChunks(ctx, noopWalkAddrs, filter, nil, chunks.NewGCConfig(chunks.GCMode_Full, level, chunks.IncrementalGCTablesDisabled), false)
	if err != nil {
		return err
	}
	hs := make(hash.HashSet)
	for _, c := range keep {
		hs.Insert(c.Hash())
	}
	require.NoError(t, sweeper.SaveHashes(ctx, hs))
	finalizer, err := sweeper.Finalize(ctx)
	require.NoError(t, err)
	defer finalizer.Close()
	require.NoError(t, sweeper.Close(ctx))
	return finalizer.SwapChunksInStore(ctx)
}

func TestToyCodecGC(t *testing.T) {
	ctx := context.Background()
	for _, level := range []chunks.GCArchiveLevel{chunks.NoArchive, chunks.SimpleArchive} {
		for _, journal := range []bool{false, true} {
			t.Run(map[bool]string{false: "tables", true: "journal"}[journal]+map[chunks.GCArchiveLevel]string{chunks.NoArchive: "", chunks.SimpleArchive: " to archives"}[level], func(t *testing.T) {
				dir := t.TempDir()
				c := newToyCodec()
				q := NewUnlimitedMemQuotaProvider()
				open := func() *NomsBlockStore {
					var st *NomsBlockStore
					var err error
					if journal {
						st, err = NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, withToy(c))
					} else {
						st, err = newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<10, 8, q, false, withToy(c))
					}
					require.NoError(t, err)
					return st
				}
				st := open()
				all := fillToyStore(t, st, 3, maxSamples+10)
				require.NoError(t, gcToyStore(t, st, all, level))
				requireStoreHas(t, st, all)
				// A second GC copies the records the first one sealed.
				fillToyStore(t, st, 1, 5)
				require.NoError(t, gcToyStore(t, st, all, level))
				requireStoreHas(t, st, all)
				require.NoError(t, st.Close())
				st = open()
				defer st.Close()
				requireStoreHas(t, st, all)
			})
		}
	}
	t.Run("keyless", func(t *testing.T) {
		dir := t.TempDir()
		c := newToyCodec()
		st, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<10, 8, NewUnlimitedMemQuotaProvider(), false, withToy(c))
		require.NoError(t, err)
		all := fillToyStore(t, st, 1, 10)
		require.NoError(t, st.Close())
		keyless := &toyCodec{keyless: true, nextKey: c.nextKey, added: c.added}
		st, err = newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<10, 8, NewUnlimitedMemQuotaProvider(), false, withToy(keyless))
		require.NoError(t, err)
		defer st.Close()
		require.ErrorIs(t, gcToyStore(t, st, all, chunks.NoArchive), errToyKeyless)
	})
}

func copyTableFiles(t *testing.T, src, dst *NomsBlockStore) error {
	ctx := context.Background()
	srcs, err := src.Sources(ctx)
	require.NoError(t, err)
	files := map[string]int{}
	for _, tf := range srcs.TableFiles {
		id := tf.FileID() + tf.LocationSuffix()
		closer, err := dst.WriteTableFile(ctx, id, tf.SplitOffset(), tf.NumChunks(), nil, func() (io.ReadCloser, uint64, error) {
			return tf.Open(ctx)
		})
		if err != nil {
			return err
		}
		defer closer.Close()
		files[id] = tf.NumChunks()
	}
	return dst.AddTableFilesToManifest(ctx, files, noopGetAddrs)
}

func TestToyCodecAddTableFiles(t *testing.T) {
	ctx := context.Background()
	q := NewUnlimitedMemQuotaProvider()
	c := newToyCodec()
	src, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<10, 64, q, false, withToy(c))
	require.NoError(t, err)
	defer src.Close()
	all := fillToyStore(t, src, 3, 40)

	for _, kind := range []string{"local", "blobstore"} {
		t.Run(kind, func(t *testing.T) {
			var dst *NomsBlockStore
			if kind == "local" {
				dst, err = newLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<10, 64, q, false, withToy(c))
			} else {
				dst, err = NewBSStore(ctx, types.Format_DOLT.VersionString(), blobstore.NewInMemoryBlobstore(""), 1<<10, q, withToy(c))
			}
			require.NoError(t, err)
			defer dst.Close()
			before := c.added.Load()
			require.NoError(t, copyTableFiles(t, src, dst))
			require.Equal(t, before+1, c.added.Load(), "the add commits once")
			requireStoreHas(t, dst, all)
			if kind == "blobstore" {
				// Conjoin needs the records of the copied tables, found from their indexes.
				conjoinUpstream(t, dst, all)
			}
		})
	}
	t.Run("PlaintextIntoEncrypted", func(t *testing.T) {
		plain, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<10, 64, q, false, withPlainCodec())
		require.NoError(t, err)
		defer plain.Close()
		fillToyStore(t, plain, 1, 10)
		dst, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<10, 64, q, false, withToy(c))
		require.NoError(t, err)
		defer dst.Close()
		before := c.added.Load()
		require.ErrorIs(t, copyTableFiles(t, plain, dst), ErrEncryptionMismatch)
		require.Equal(t, before, c.added.Load())
	})
	t.Run("EncryptedIntoPlaintext", func(t *testing.T) {
		dst, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), 1<<10, 64, q, false, withPlainCodec())
		require.NoError(t, err)
		defer dst.Close()
		require.ErrorIs(t, copyTableFiles(t, src, dst), ErrEncryptionMismatch)
	})
}

// withPlainCodec opens a store plaintext, as Dolt does.
func withPlainCodec() StoreOption {
	return func(o *storeOptions) {
		o.newCodec = func(_ bool, format string) (string, storeCodec, error) { return format, plainCodec{}, nil }
	}
}

func TestToyCodecJournalReads(t *testing.T) {
	ctx := context.Background()
	st, err := NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), t.TempDir(), NewUnlimitedMemQuotaProvider(), false, nil, withToy(newToyCodec()))
	require.NoError(t, err)
	defer st.Close()
	cs := toyChunks(50)
	for _, chk := range cs {
		require.NoError(t, st.Put(ctx, chk, noopGetAddrs))
	}
	ok, err := st.Commit(ctx, cs[0].Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	hs := make(hash.HashSet)
	for _, chk := range cs {
		hs.Insert(chk.Hash())
	}

	var mu sync.Mutex
	var n int
	require.NoError(t, st.GetManyCompressed(ctx, hs, func(_ context.Context, tc ToChunker) {
		mu.Lock()
		defer mu.Unlock()
		require.IsType(t, &toyChunk{}, tc)
		chk, err := tc.ToChunk()
		require.NoError(t, err)
		require.Equal(t, tc.Hash(), chk.Hash())
		n++
	}))
	require.Equal(t, len(cs), n)

	locs, keys, err := GetChunkLocationsWithPathsAndKeys(ctx, st, hs)
	require.NoError(t, err)
	n = 0
	for _, byHash := range locs {
		for h, r := range byHash {
			require.Len(t, keys.At(h, r.Offset, r.Length).Key, 1)
			n++
		}
	}
	require.Equal(t, len(cs), n)

	n = 0
	require.NoError(t, st.IterateAllChunks(ctx, func(chunks.Chunk) { n++ }))
	require.Equal(t, len(cs), n)
}

// A journal written before its store's manifest opens in the format of its first record.
func TestToyCodecJournalWithoutManifest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newToyCodec()
	q := NewUnlimitedMemQuotaProvider()
	st, err := NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, withToy(c))
	require.NoError(t, err)
	fillToyStore(t, st, 1, 20)
	require.NoError(t, st.Close())
	require.NoError(t, os.Remove(filepath.Join(dir, manifestFileName)))

	t.Run("Plaintext", func(t *testing.T) {
		withPlainDefault(t)
		_, err := NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil)
		require.ErrorIs(t, err, ErrEncryptionUnsupported)
	})
	t.Run("PlaintextStore", func(t *testing.T) {
		st, err := NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, withPlainCodec())
		require.NoError(t, err)
		defer st.Close()
		_, err = st.Root(ctx)
		require.ErrorIs(t, err, ErrEncryptionMismatch)
	})
	st, err = NewLocalJournalingStore(ctx, types.Format_DOLT.VersionString(), dir, q, false, nil, withToy(c))
	require.NoError(t, err)
	defer st.Close()
	require.Equal(t, constants.EncryptedFormatString, st.Version())
	_, err = st.Root(ctx)
	require.NoError(t, err)
}
