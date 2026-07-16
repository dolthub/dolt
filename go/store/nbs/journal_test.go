// Copyright 2022 Dolthub, Inc.
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
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func makeTestChunkJournal(t *testing.T) *ChunkJournal {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() { file.RemoveAll(dir) })
	l, _, err := newJournalLock(dir, lockFileTimeout, false)
	require.NoError(t, err)
	m, err := newJournalManifest(ctx, dir, l)
	require.NoError(t, err)
	q := NewUnlimitedMemQuotaProvider()
	p := newFSTablePersister(dir, q, false)
	nbf := types.Format_DOLT.VersionString()
	j, err := newChunkJournal(ctx, nbf, dir, m, p.(*fsTablePersister), dherrors.FatalBehaviorError, nil)
	require.NoError(t, err)
	// j.Close closes the journal writer; m.Close releases the backing manifest
	// lock. A NomsBlockStore drives these via the persister and (wrapped)
	// manifest Close paths, respectively.
	t.Cleanup(func() { j.Close(); m.Close() })
	return j
}

func openTestChunkJournal(t *testing.T, dir string) *ChunkJournal {
	l, _, err := newJournalLock(dir, lockFileTimeout, false)
	require.NoError(t, err)
	m, err := newJournalManifest(t.Context(), dir, l)
	require.NoError(t, err)
	q := NewUnlimitedMemQuotaProvider()
	p := newFSTablePersister(dir, q, false)
	nbf := types.Format_DOLT.VersionString()
	j, err := newChunkJournal(t.Context(), nbf, dir, m, p.(*fsTablePersister), dherrors.FatalBehaviorError, nil)
	require.NoError(t, err)
	t.Cleanup(func() { j.Close(); m.Close() })
	return j
}

func TestChunkJournalBlockStoreSuite(t *testing.T) {
	fn := func(ctx context.Context, dir string) (*NomsBlockStore, error) {
		q := NewUnlimitedMemQuotaProvider()
		nbf := types.Format_DOLT.VersionString()
		return NewLocalJournalingStore(ctx, nbf, dir, q, false, nil)
	}
	suite.Run(t, &BlockStoreSuite{
		factory:        fn,
		skipInterloper: true,
	})
}

func TestChunkJournalReadOnly(t *testing.T) {
	t.Run("ReadOnlyOpenNonExistantJournalFails", func(t *testing.T) {
		// If a read only ChunkJournal tries to open a journal,
		// and that journal does not exist, it should fail,
		// not try to create it.
		rw := makeTestChunkJournal(t)
		assert.Equal(t, chunks.ExclusiveAccessMode(chunks.ExclusiveAccessMode_Exclusive), rw.AccessMode())
		ro := openTestChunkJournal(t, rw.backing.dir)
		assert.Equal(t, chunks.ExclusiveAccessMode(chunks.ExclusiveAccessMode_ReadOnly), ro.AccessMode())

		// We start without a journal.
		assert.False(t, containsJournalSpec(rw.contents.specs))

		rosource, err := ro.Open(t.Context(), journalAddr, 0, &Stats{})
		require.Error(t, err)
		require.Nil(t, rosource)

		rwsource, err := rw.Open(t.Context(), journalAddr, 0, &Stats{})
		require.NoError(t, err)
		require.NotNil(t, rwsource)

		rosource, err = ro.Open(t.Context(), journalAddr, 0, &Stats{})
		require.NoError(t, err)
		require.NotNil(t, rosource)
	})

	t.Run("FailOnLockTimeoutReturnsErrDatabaseLocked", func(t *testing.T) {
		// A rw journal holds the journalManifest lock. With failOnTimeout enabled, a concurrent open should
		// return an error instead of falling back to read-only.
		rw := makeTestChunkJournal(t)
		assert.Equal(t, chunks.ExclusiveAccessMode(chunks.ExclusiveAccessMode_Exclusive), rw.AccessMode())

		_, _, err := newJournalLock(rw.backing.dir, lockFileTimeout, true)
		require.ErrorIs(t, err, ErrDatabaseLocked)
	})
}

// TestChunkJournalBootstrapMissingRootRecord verifies that when the journal file
// exists but contains no root hash record, bootstrapping leaves the root recorded
// in the manifest in place rather than overwriting it with the empty hash. This
// state can arise from a crash between createJournalWriter and the first
// commitRootHash, or between Persist flushing chunk records and Update committing
// the root.
func TestChunkJournalBootstrapMissingRootRecord(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_DOLT.VersionString()

	// setup creates a journaling store in |dir|, commits a root, and returns it.
	setup := func(t *testing.T, dir string) hash.Hash {
		store, err := NewLocalJournalingStore(ctx, nbf, dir, NewUnlimitedMemQuotaProvider(), false, nil)
		require.NoError(t, err)
		rootChunk := chunks.NewChunk([]byte("a commit root value"))
		require.NoError(t, store.Put(ctx, rootChunk, noopGetAddrs))
		ok, err := store.Commit(ctx, rootChunk.Hash(), hash.Hash{})
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, store.Close())
		require.False(t, rootChunk.Hash().IsEmpty())
		return rootChunk.Hash()
	}

	// reopen opens the journaling store in |dir|, returns its root, and closes it.
	reopen := func(t *testing.T, dir string) hash.Hash {
		store, err := NewLocalJournalingStore(ctx, nbf, dir, NewUnlimitedMemQuotaProvider(), false, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		root, err := store.Root(ctx)
		require.NoError(t, err)
		return root
	}

	// manifestRoot parses the on-disk manifest file directly and returns its root.
	manifestRoot := func(t *testing.T, dir string) hash.Hash {
		ok, mc, err := parseIfExists(ctx, dir, nil)
		require.NoError(t, err)
		require.True(t, ok)
		return mc.root
	}

	t.Run("EmptyJournal", func(t *testing.T) {
		dir := t.TempDir()
		root := setup(t, dir)

		// Simulate a crash between createJournalWriter and the first
		// commitRootHash: the journal file exists but is empty.
		require.NoError(t, os.Truncate(filepath.Join(dir, chunkJournalName), 0))
		require.NoError(t, os.Remove(filepath.Join(dir, journalIndexFileName)))

		// Bootstrapping must keep the manifest root, not overwrite it with 0000...
		assert.Equal(t, root, reopen(t, dir))
		assert.Equal(t, root, manifestRoot(t, dir))
		// The recovered root must survive a subsequent restart.
		assert.Equal(t, root, reopen(t, dir))
	})

	t.Run("ChunkRecordsNoRootRecord", func(t *testing.T) {
		dir := t.TempDir()
		root := setup(t, dir)

		// Simulate a crash after Persist flushed chunk records but before
		// Update committed the root: drop the trailing root hash record,
		// leaving a journal of only chunk records.
		jp := filepath.Join(dir, chunkJournalName)
		info, err := os.Stat(jp)
		require.NoError(t, err)
		require.NoError(t, os.Truncate(jp, info.Size()-int64(rootHashRecordSize())))
		require.NoError(t, os.Remove(filepath.Join(dir, journalIndexFileName)))

		assert.Equal(t, root, reopen(t, dir))
		assert.Equal(t, root, manifestRoot(t, dir))
		assert.Equal(t, root, reopen(t, dir))
	})
}

func TestChunkJournalPersist(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)
	const iters = 64
	stats := &Stats{}
	haver := emptyChunkSource{}
	for i := 0; i < iters; i++ {
		memTbl, chunkMap := randomMemTable(16)
		source, _, err := j.Persist(ctx, dherrors.FatalBehaviorError, memTbl, haver, nil, stats)
		assert.NoError(t, err)

		for h, ch := range chunkMap {
			ok, _, err := source.has(h, nil)
			assert.NoError(t, err)
			assert.True(t, ok)
			data, _, err := source.get(ctx, h, nil, stats)
			assert.NoError(t, err)
			assert.Equal(t, ch.Data(), data)
		}

		cs, err := j.Open(ctx, source.hash(), 16, stats)
		assert.NotNil(t, cs)
		assert.NoError(t, err)
	}
}

// protectedRefCount returns the current prune-protection ref count for |h| in
// the persister's protected set.
func protectedRefCount(ftp *fsTablePersister, h hash.Hash) int32 {
	ftp.mu.Lock()
	defer ftp.mu.Unlock()
	return ftp.protected[h]
}

// TestChunkJournalActiveFileProtectedFromPruning is a regression test for a bug
// where PruneTableFiles could delete the journal file out from under an active
// journal writer. The journal file lives in the table-file namespace (its name
// is journalAddr), so the generic pruner would treat it as a prunable table
// file. Protection used to be a racy, per-call `j.wr != nil` check inside
// ChunkJournal.PruneTableFiles, decided under the persister's pruneMu while the
// journal writer is created/dropped under the NomsBlockStore mutex — two
// disjoint lock domains. The fix registers the journal file in the persister's
// protected set for the entire lifetime of the writer, under pruneMu. This test
// asserts that lifetime protection; it fails against the pre-fix code, whose
// protection did not persist beyond a PruneTableFiles call.
func TestChunkJournalActiveFileProtectedFromPruning(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)
	journalPath := filepath.Join(j.persister.dir, chunkJournalName)

	// Before any writes there is no journal writer, file, or protection.
	require.Nil(t, j.wr)
	require.Zero(t, protectedRefCount(j.persister, journalAddr))
	ok, err := fileExists(journalPath)
	require.NoError(t, err)
	require.False(t, ok)

	// Persisting a memtable activates the journal writer, which must create the
	// journal file and register it as protected.
	mt, _ := randomMemTable(8)
	_, _, err = j.Persist(ctx, dherrors.FatalBehaviorError, mt, emptyChunkSource{}, nil, &Stats{})
	require.NoError(t, err)
	require.NotNil(t, j.wr)

	ok, err = fileExists(journalPath)
	require.NoError(t, err)
	require.True(t, ok, "journal file should exist while the writer is active")

	// The active journal file must be protected for the lifetime of the writer,
	// not just transiently during a prune call.
	require.Positive(t, protectedRefCount(j.persister, journalAddr),
		"active journal file must be protected from pruning")

	// A prune while the journal is active must not delete the journal file.
	require.NoError(t, j.PruneTableFiles(ctx))
	ok, err = fileExists(journalPath)
	require.NoError(t, err)
	require.True(t, ok, "PruneTableFiles must not delete the active journal file")

	// Dropping the writer releases the protection and deletes the file, so it
	// becomes prunable again.
	require.NoError(t, j.dropJournalWriter(ctx))
	require.Nil(t, j.wr)
	require.Zero(t, protectedRefCount(j.persister, journalAddr),
		"journal protection must be released when the writer is dropped")
	ok, err = fileExists(journalPath)
	require.NoError(t, err)
	require.False(t, ok, "dropped journal file should be deleted")
}

// TestChunkJournalPruneConcurrentWithJournalRecreation is a regression test for
// the race between PruneTableFiles and (re)creation of the journal file. A
// commit that recreates the journal (after a prior GC dropped it) runs under
// the NomsBlockStore mutex, while PruneTableFiles runs under the persister's
// pruneMu; before the fix these disjoint lock domains let a prune delete a live
// journal file, and left ChunkJournal.PruneTableFiles reading j.wr without
// synchronization. Run under `-race` this reliably catches the unsynchronized
// access; without it, it catches the erroneous deletion probabilistically.
func TestChunkJournalPruneConcurrentWithJournalRecreation(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)
	journalPath := filepath.Join(j.persister.dir, chunkJournalName)

	stop := make(chan struct{})
	var pruneErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := j.PruneTableFiles(ctx); err != nil {
				pruneErr.Store(err)
				return
			}
		}
	}()

	const iters = 300
	for i := 0; i < iters; i++ {
		// Recreate the journal, as a commit persisting after a GC drop would.
		mt, _ := randomMemTable(8)
		_, _, err := j.Persist(ctx, dherrors.FatalBehaviorError, mt, emptyChunkSource{}, nil, &Stats{})
		require.NoError(t, err)

		// While the writer is active the file must not have been pruned.
		ok, err := fileExists(journalPath)
		require.NoError(t, err)
		require.Truef(t, ok, "journal file was pruned while the writer was active (iter %d)", i)

		// Drop the journal, as a GC finalize dropping it would.
		require.NoError(t, j.dropJournalWriter(ctx))
	}

	close(stop)
	wg.Wait()
	require.Nil(t, pruneErr.Load(), "PruneTableFiles failed: %v", pruneErr.Load())
}

func TestReadRecordRanges(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)

	var buf []byte
	mt, data := randomMemTable(256)
	gets := make([]getRecord, 0, len(data))
	for h := range data {
		gets = append(gets, getRecord{a: &h, prefix: h.Prefix()})
	}

	jcs, _, err := j.Persist(ctx, dherrors.FatalBehaviorError, mt, emptyChunkSource{}, nil, &Stats{})
	require.NoError(t, err)

	rdr, sz, err := jcs.(journalChunkSource).journal.snapshot(context.Background(), dherrors.FatalBehaviorError)
	require.NoError(t, err)
	defer rdr.Close()

	buf = make([]byte, sz)
	n, err := rdr.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, int(sz), n)

	ranges, _, err := jcs.getRecordRanges(ctx, dherrors.FatalBehaviorError, gets, nil)
	require.NoError(t, err)

	for h, rng := range ranges {
		b, _, err := jcs.get(ctx, h, nil, &Stats{})
		assert.NoError(t, err)
		ch1 := chunks.NewChunkWithHash(h, b)
		assert.Equal(t, data[h], ch1)

		start, stop := rng.Offset, uint32(rng.Offset)+rng.Length
		cc2, err := NewCompressedChunk(h, buf[start:stop])
		assert.NoError(t, err)
		ch2, err := cc2.ToChunk()
		assert.NoError(t, err)
		assert.Equal(t, data[h], ch2)
	}
}

func randBuf(n int) (b []byte) {
	b = make([]byte, n)
	rand.Read(b)
	return
}
