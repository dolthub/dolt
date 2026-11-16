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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func requireCloseTwice(t *testing.T, c io.Closer) {
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

func openTestJournalingStore(t *testing.T, dir string) *NomsBlockStore {
	st, err := NewLocalJournalingStore(context.Background(), constants.FormatDoltString, dir, NewUnlimitedMemQuotaProvider(), false, nil)
	require.NoError(t, err)
	return st
}

// commitTestChunks commits |numChunks| new chunks to |st| and returns them.
func commitTestChunks(t *testing.T, st *NomsBlockStore, numChunks int) map[hash.Hash]chunks.Chunk {
	ctx := context.Background()
	chunkSet := makeChunkSet(numChunks, 64)
	var next hash.Hash
	for h, c := range chunkSet {
		require.NoError(t, st.Put(ctx, c, noopGetAddrs))
		next = h
	}
	last, err := st.Root(ctx)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, next, last)
	require.NoError(t, err)
	require.True(t, ok)
	return chunkSet
}

func runTestGC(t *testing.T, st *NomsBlockStore, keepers map[hash.Hash]chunks.Chunk) {
	ctx := context.Background()
	require.NoError(t, st.BeginGC(ctx, nil, chunks.GCMode_Full))
	noopFilter := func(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
		return hashes, nil
	}
	gcConfig := chunks.NewGCConfig(chunks.GCMode_Full, chunks.NoArchive, chunks.IncrementalGCTablesDisabled)
	sweeper, err := st.MarkAndSweepChunks(ctx, noopWalkAddrs, noopFilter, nil, gcConfig, false)
	require.NoError(t, err)
	keepersSet := make(hash.HashSet, len(keepers))
	for h := range keepers {
		keepersSet.Insert(h)
	}
	require.NoError(t, sweeper.SaveHashes(ctx, keepersSet))
	finalizer, err := sweeper.Finalize(ctx)
	require.NoError(t, err)
	require.NoError(t, sweeper.Close(ctx))
	require.NoError(t, finalizer.SwapChunksInStore(ctx))
	require.NoError(t, finalizer.Close())
	st.EndGC(chunks.GCMode_Full)
}

func TestNBSCloseTwice(t *testing.T) {
	kinds := []struct {
		name string
		open func(t *testing.T) *NomsBlockStore
	}{
		{"NonJournaled", func(t *testing.T) *NomsBlockStore {
			st, _, _ := makeTestLocalStore(t, defaultMaxTables)
			return st
		}},
		{"Journaled", func(t *testing.T) *NomsBlockStore {
			return openTestJournalingStore(t, t.TempDir())
		}},
	}
	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) {
			t.Run("Untouched", func(t *testing.T) {
				requireCloseTwice(t, kind.open(t))
			})
			t.Run("Committed", func(t *testing.T) {
				st := kind.open(t)
				commitTestChunks(t, st, 16)
				requireCloseTwice(t, st)
			})
			t.Run("TableFiles", func(t *testing.T) {
				st := kind.open(t)
				populateLocalStore(t, st, 4)
				requireCloseTwice(t, st)
			})
			t.Run("AfterGC", func(t *testing.T) {
				st := kind.open(t)
				keepers := commitTestChunks(t, st, 16)
				runTestGC(t, st, keepers)
				requireCloseTwice(t, st)
			})
			t.Run("Concurrent", func(t *testing.T) {
				st := kind.open(t)
				commitTestChunks(t, st, 16)
				var eg errgroup.Group
				for range 4 {
					eg.Go(st.Close)
				}
				require.NoError(t, eg.Wait())
			})
		})
	}
}

func TestNBSCloseTwiceAfterConjoin(t *testing.T) {
	st, _, _ := makeTestLocalStore(t, 2)
	for range 4 {
		commitTestChunks(t, st, 4)
	}
	waitForConjoin(st)
	requireCloseTwice(t, st)
}

// A second Close must not return while the first is still waiting for a
// conjoin, because the first has not yet released the store's files.
func TestNBSCloseTwiceDuringConjoin(t *testing.T) {
	st, _, _ := makeTestLocalStore(t, defaultMaxTables)
	// Simulate an in-flight conjoin, so the first Close parks on conjoinOpCond.
	st.conjoinOp = &conjoinOperation{}
	finishConjoin := func() {
		st.mu.Lock()
		st.conjoinOp = nil
		st.conjoinOpCond.Broadcast()
		st.mu.Unlock()
	}
	first := make(chan error, 1)
	go func() { first <- st.Close() }()
	defer func() {
		finishConjoin()
		require.NoError(t, <-first)
	}()

	require.Eventually(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.closed
	}, time.Second, time.Millisecond)

	second := make(chan error, 1)
	go func() { second <- st.Close() }()
	select {
	case err := <-second:
		t.Fatalf("second Close returned before the first finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	finishConjoin()
	require.NoError(t, <-second)
}

func TestJournalingStoreCloseTwice(t *testing.T) {
	ctx := context.Background()
	requireLockReleased := func(t *testing.T, dir string) {
		st, err := NewLocalJournalingStoreWithOptions(ctx, constants.FormatDoltString, dir, NewUnlimitedMemQuotaProvider(), false, nil, JournalingStoreOptions{FailOnLockTimeout: true})
		require.NoError(t, err)
		require.NoError(t, st.Close())
	}

	t.Run("NeverLoaded", func(t *testing.T) {
		dir := t.TempDir()
		requireCloseTwice(t, openTestJournalingStore(t, dir))
		requireLockReleased(t, dir)
	})
	t.Run("NeverLoadedConcurrent", func(t *testing.T) {
		dir := t.TempDir()
		st := openTestJournalingStore(t, dir)
		var eg errgroup.Group
		for range 4 {
			eg.Go(st.Close)
		}
		require.NoError(t, eg.Wait())
		requireLockReleased(t, dir)
	})
	t.Run("Reopened", func(t *testing.T) {
		dir := t.TempDir()
		st := openTestJournalingStore(t, dir)
		chunkSet := commitTestChunks(t, st, 16)
		require.NoError(t, st.Close())

		st = openTestJournalingStore(t, dir)
		for h, c := range chunkSet {
			out, err := st.Get(ctx, h)
			require.NoError(t, err)
			require.Equal(t, c, out)
		}
		requireCloseTwice(t, st)
		requireLockReleased(t, dir)
	})
	t.Run("LoadFailed", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, manifestFileName), []byte("not a manifest"), 0644))
		st := openTestJournalingStore(t, dir)
		_, err := st.Root(ctx)
		require.Error(t, err)
		requireCloseTwice(t, st)
		requireLockReleased(t, dir)
	})
}

func TestGenerationalNBSCloseTwice(t *testing.T) {
	oldGen, _, _ := makeTestLocalStore(t, defaultMaxTables)
	populateLocalStore(t, oldGen, 4)
	newGen := openTestJournalingStore(t, t.TempDir())
	commitTestChunks(t, newGen, 16)
	requireCloseTwice(t, NewGenerationalCS(oldGen, newGen, nil))
}

func TestNBSReopenAfterCloseTwice(t *testing.T) {
	ctx := context.Background()
	st, dir, q := makeTestLocalStore(t, defaultMaxTables)
	chunkSet := commitTestChunks(t, st, 16)
	requireCloseTwice(t, st)

	st, err := newLocalStore(ctx, types.Format_DOLT.VersionString(), dir, defaultMemTableSize, defaultMaxTables, q, false)
	require.NoError(t, err)
	defer st.Close()
	for h, c := range chunkSet {
		out, err := st.Get(ctx, h)
		require.NoError(t, err)
		require.Equal(t, c, out)
	}
}

func TestNBSReadsFailAfterClose(t *testing.T) {
	ctx := context.Background()
	requireClosedErr := func(t *testing.T, err error) {
		require.ErrorIs(t, err, chunks.ErrStoreClosed)
	}
	readers := []struct {
		name string
		read func(st *NomsBlockStore) error
	}{
		{"Root", func(st *NomsBlockStore) error {
			_, err := st.Root(ctx)
			return err
		}},
		{"Count", func(st *NomsBlockStore) error {
			_, err := st.Count(ctx)
			return err
		}},
		{"Rebase", func(st *NomsBlockStore) error {
			return st.Rebase(ctx)
		}},
		{"Sources", func(st *NomsBlockStore) error {
			_, err := st.Sources(ctx)
			return err
		}},
		{"Size", func(st *NomsBlockStore) error {
			_, err := st.Size(ctx)
			return err
		}},
		{"ChunkJournalSize", func(st *NomsBlockStore) error {
			_, _, err := st.ChunkJournalSize(ctx)
			return err
		}},
		{"IterateRoots", func(st *NomsBlockStore) error {
			return st.IterateRoots(ctx, func(string, *time.Time) error { return nil })
		}},
		{"IterateAllChunks", func(st *NomsBlockStore) error {
			return st.IterateAllChunks(ctx, func(chunks.Chunk) {})
		}},
		{"TolerantIterateAllChunks", func(st *NomsBlockStore) error {
			return st.TolerantIterateAllChunks(ctx, func(chunks.Chunk) {}, func(string, error) {})
		}},
		{"Get", func(st *NomsBlockStore) error {
			_, err := st.Get(ctx, hash.Hash{})
			return err
		}},
		{"Has", func(st *NomsBlockStore) error {
			_, err := st.Has(ctx, hash.Hash{})
			return err
		}},
	}
	stores := []struct {
		name string
		open func(t *testing.T) *NomsBlockStore
	}{
		{"NonJournaled", func(t *testing.T) *NomsBlockStore {
			st, _, _ := makeTestLocalStore(t, defaultMaxTables)
			commitTestChunks(t, st, 16)
			return st
		}},
		{"Journaled", func(t *testing.T) *NomsBlockStore {
			st := openTestJournalingStore(t, t.TempDir())
			commitTestChunks(t, st, 16)
			return st
		}},
		{"NeverLoaded", func(t *testing.T) *NomsBlockStore {
			return openTestJournalingStore(t, t.TempDir())
		}},
	}
	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			st := store.open(t)
			require.NoError(t, st.Close())
			for _, reader := range readers {
				t.Run(reader.name, func(t *testing.T) {
					requireClosedErr(t, reader.read(st))
				})
			}
		})
	}
	t.Run("Generational", func(t *testing.T) {
		oldGen, _, _ := makeTestLocalStore(t, defaultMaxTables)
		newGen := openTestJournalingStore(t, t.TempDir())
		commitTestChunks(t, newGen, 16)
		gen := NewGenerationalCS(oldGen, newGen, nil)
		require.NoError(t, gen.Close())
		_, err := gen.Root(ctx)
		requireClosedErr(t, err)
		requireClosedErr(t, gen.Rebase(ctx))
	})
}
