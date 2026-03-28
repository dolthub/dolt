// Copyright 2025 Dolthub, Inc.
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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// failOnCloneSource is a chunkSource whose clone() always returns an error.
// It is used to simulate a rebase failure on the pre-opened conjoinedSrc path.
type failOnCloneSource struct {
	emptyChunkSource
	h hash.Hash
}

func (f failOnCloneSource) hash() hash.Hash { return f.h }

func (f failOnCloneSource) clone() (chunkSource, error) {
	return nil, errors.New("intentional clone failure")
}

func (f failOnCloneSource) close() error { return nil }

// fakeGCManifest extends fakeManifest with UpdateGCGen support, so it can be
// used as the backing manifest for stores that exercise the GC / swapTables path.
type fakeGCManifest struct {
	*fakeManifest
	gcGenUpdateCalled bool
}

func (fm *fakeGCManifest) UpdateGCGen(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	fm.fakeManifest.mu.Lock()
	defer fm.fakeManifest.mu.Unlock()
	fm.gcGenUpdateCalled = true
	if fm.fakeManifest.contents.lock == lastLock {
		updated := manifestContents{
			manifestVers: StorageVersion,
			nbfVers:      newContents.nbfVers,
			lock:         newContents.lock,
			root:         newContents.root,
			gcGen:        newContents.gcGen,
		}
		updated.specs = make([]tableSpec, len(newContents.specs))
		copy(updated.specs, newContents.specs)
		fm.fakeManifest.contents = updated
	}
	return fm.fakeManifest.contents, nil
}

// TestComputeNewContents_NoAppendix verifies computeNewContents with no appendix specs.
func TestComputeNewContents_NoAppendix(t *testing.T) {
	specA := tableSpec{name: computeAddr([]byte("a")), chunkCount: 1}
	specB := tableSpec{name: computeAddr([]byte("b")), chunkCount: 2}
	specC := tableSpec{name: computeAddr([]byte("c")), chunkCount: 3}
	conjoined := tableSpec{name: computeAddr([]byte("conjoined")), chunkCount: 6}

	upstream := manifestContents{
		nbfVers: constants.FormatDoltString,
		root:    hash.Of([]byte("root")),
		gcGen:   hash.Of([]byte("gc")),
		specs:   []tableSpec{specA, specB, specC},
	}

	op := &conjoinOperation{
		conjoinees: []tableSpec{specA, specB},
		conjoined:  conjoined,
	}

	result, ok := op.computeNewContents(upstream)
	require.True(t, ok)
	// With no appendix, conjoined is inserted at position 0 (i == len(nil) == 0),
	// then the non-conjoinee specs follow.
	require.Equal(t, 2, len(result.specs))
	assert.Equal(t, conjoined.name, result.specs[0].name)
	assert.Equal(t, specC.name, result.specs[1].name)
	assert.Equal(t, upstream.root, result.root)
	assert.Equal(t, upstream.gcGen, result.gcGen)
}

// TestComputeNewContents_WithAppendix verifies that when appendix specs are
// present, the conjoined file is placed immediately after the appendix entries
// and appendix specs are preserved unchanged.
func TestComputeNewContents_WithAppendix(t *testing.T) {
	appendixSpec := tableSpec{name: computeAddr([]byte("appendix")), chunkCount: 1}
	specA := tableSpec{name: computeAddr([]byte("a")), chunkCount: 2}
	specB := tableSpec{name: computeAddr([]byte("b")), chunkCount: 3}
	specC := tableSpec{name: computeAddr([]byte("c")), chunkCount: 4}
	conjoined := tableSpec{name: computeAddr([]byte("conjoined")), chunkCount: 9}

	// specs includes the appendix spec at the front, followed by non-appendix specs.
	upstream := manifestContents{
		nbfVers:  constants.FormatDoltString,
		root:     hash.Of([]byte("root")),
		gcGen:    hash.Of([]byte("gc")),
		specs:    []tableSpec{appendixSpec, specA, specB, specC},
		appendix: []tableSpec{appendixSpec},
	}

	op := &conjoinOperation{
		conjoinees: []tableSpec{specA, specB},
		conjoined:  conjoined,
	}

	result, ok := op.computeNewContents(upstream)
	require.True(t, ok)
	// Expected order: [appendixSpec, conjoined, specC]
	// conjoined inserted at i == len(appendix) == 1.
	require.Equal(t, 3, len(result.specs))
	assert.Equal(t, appendixSpec.name, result.specs[0].name)
	assert.Equal(t, conjoined.name, result.specs[1].name)
	assert.Equal(t, specC.name, result.specs[2].name)
	// Appendix must be preserved.
	require.Equal(t, 1, len(result.appendix))
	assert.Equal(t, appendixSpec.name, result.appendix[0].name)
}

// TestComputeNewContents_ConjoineesMissing verifies that computeNewContents
// returns canApply=false when the conjoinees are not in the upstream specs.
func TestComputeNewContents_ConjoineesMissing(t *testing.T) {
	specA := tableSpec{name: computeAddr([]byte("a")), chunkCount: 1}
	specB := tableSpec{name: computeAddr([]byte("b")), chunkCount: 2}
	specMissing := tableSpec{name: computeAddr([]byte("missing")), chunkCount: 3}

	upstream := manifestContents{
		nbfVers: constants.FormatDoltString,
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{specA, specB},
	}

	op := &conjoinOperation{
		conjoinees: []tableSpec{specA, specMissing},
		conjoined:  tableSpec{name: computeAddr([]byte("conjoined")), chunkCount: 4},
	}

	result, ok := op.computeNewContents(upstream)
	assert.False(t, ok)
	assert.Equal(t, upstream.specs, result.specs)
}

// TestComputeNewContents_EmptyConjoinees verifies that computeNewContents
// returns canApply=false when conjoinees is empty.
func TestComputeNewContents_EmptyConjoinees(t *testing.T) {
	specA := tableSpec{name: computeAddr([]byte("a")), chunkCount: 1}

	upstream := manifestContents{
		nbfVers: constants.FormatDoltString,
		root:    hash.Of([]byte("root")),
		specs:   []tableSpec{specA},
	}

	op := &conjoinOperation{
		conjoinees: nil,
		conjoined:  tableSpec{name: computeAddr([]byte("conjoined")), chunkCount: 1},
	}

	result, ok := op.computeNewContents(upstream)
	assert.False(t, ok)
	assert.Equal(t, upstream.specs, result.specs)
}

// TestUpdateManifestAddFiles_RebaseFailureDoesNotUpdateManifest verifies the core
// ordering invariant: if the tableSet rebase fails, the manifest is never written.
//
// Before the fix, updateManifestAddFiles wrote the manifest first and rebased
// afterward. A rebase failure would leave the manifest referencing table files
// that the in-process store could not load.
func TestUpdateManifestAddFiles_RebaseFailureDoesNotUpdateManifest(t *testing.T) {
	ctx := context.Background()
	fm, p, _, store := makeStoreWithFakes(t)
	defer store.Close()
	ftp := p.(fakeTablePersister)

	// Record the initial manifest lock (empty, since no manifest has been written yet).
	fm.mu.RLock()
	initialLock := fm.contents.lock
	fm.mu.RUnlock()

	// Register a table hash that cannot be opened by the persister. When
	// updateManifestAddFiles tries to rebase with this hash in the spec list,
	// the Open call will fail and the rebase will be aborted.
	failHash := computeAddr([]byte("table-that-cannot-be-opened"))
	ftp.mu.Lock()
	ftp.sourcesToFail[failHash] = true
	ftp.mu.Unlock()

	// Passing nil sources forces the rebase to call ts.p.Open, which will fail.
	_, _, err := store.updateManifestAddFiles(ctx, map[hash.Hash]uint32{failHash: 1}, nil, nil, nil)
	require.Error(t, err)

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	assert.Equal(t, initialLock, fm.contents.lock,
		"manifest must not be updated when the tableSet rebase fails")
}

// TestUpdateManifestAddFiles_OptimisticLockRetry verifies that when the first
// manifest update attempt loses the optimistic lock to a concurrent writer, the
// rebased tables from that attempt are discarded and the operation retries
// successfully, ultimately landing the correct table specs in the manifest.
func TestUpdateManifestAddFiles_OptimisticLockRetry(t *testing.T) {
	ctx := context.Background()
	fm := &fakeManifest{}

	updateCalls := 0
	upm := &updatePreemptManifest{
		manifest: fm,
		preUpdate: func() {
			updateCalls++
			if updateCalls == 1 {
				// On the first Update attempt, simulate a concurrent writer moving
				// the manifest lock, causing an optimistic lock failure.
				concurrentLock := computeAddr([]byte("concurrent-writer-lock"))
				fm.set(constants.FormatDoltString, concurrentLock, hash.Hash{}, nil, nil)
			}
		},
	}

	mm := manifestManager{upm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	// Persist a real table file so that the rebase can successfully open it on
	// both attempts (the data remains in ftp.sources after the first attempt's
	// tables are closed).
	mt := createMemTable([][]byte{[]byte("chunk1")})
	src, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	tableHash := src.hash()
	tableCount, _ := src.count()
	require.NoError(t, src.close())

	// Passing nil sources causes the rebase to call ts.p.Open each time. The
	// open succeeds on both attempts since the data is in ftp.sources.
	_, _, err = store.updateManifestAddFiles(ctx, map[hash.Hash]uint32{tableHash: tableCount}, nil, nil, nil)
	require.NoError(t, err)

	// Two manifest Update calls: the first was preempted and retried.
	assert.Equal(t, 2, updateCalls,
		"expected two manifest update attempts: first preempted, second succeeded")

	// The manifest and the store's in-memory upstream must both reflect the
	// table we added.
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, tableHash, fm.contents.specs[0].name)
}

// TestSwapTables_RebaseFailureDoesNotUpdateManifest verifies that when the
// tableSet rebase fails inside swapTables, UpdateGCGen is never called and the
// persisted manifest is left unchanged.
//
// Before the fix, swapTables wrote a new manifest (via UpdateGCGen) and only
// then attempted the rebase. A rebase failure would leave the manifest pointing
// at GC-compacted table files that the store could not open.
func TestSwapTables_RebaseFailureDoesNotUpdateManifest(t *testing.T) {
	ctx := context.Background()

	fm := &fakeManifest{}
	fgcm := &fakeGCManifest{fakeManifest: fm}
	mm := manifestManager{fgcm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	// Record the initial manifest lock (empty).
	fm.mu.RLock()
	initialLock := fm.contents.lock
	fm.mu.RUnlock()

	// The GC-compacted table file cannot be opened.
	gcTableHash := computeAddr([]byte("gc-compacted-table"))
	ftp.mu.Lock()
	ftp.sourcesToFail[gcTableHash] = true
	ftp.mu.Unlock()

	// swapTables should fail on the rebase before reaching UpdateGCGen.
	err = store.swapTables(ctx, []tableSpec{{name: gcTableHash, chunkCount: 1}}, chunks.GCMode_Full)
	require.Error(t, err)

	assert.False(t, fgcm.gcGenUpdateCalled,
		"UpdateGCGen must not be called when the tableSet rebase fails")

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	assert.Equal(t, initialLock, fm.contents.lock,
		"manifest must not be updated when the tableSet rebase fails")
}

// TestSwapTables_Success verifies the happy path: rebase succeeds, UpdateGCGen
// is called, the manifest and in-memory state are both updated.
func TestSwapTables_Success(t *testing.T) {
	ctx := context.Background()

	fm := &fakeManifest{}
	fgcm := &fakeGCManifest{fakeManifest: fm}
	mm := manifestManager{fgcm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	// Persist a table that will be the GC-compacted result.
	mt := createMemTable([][]byte{[]byte("chunk1"), []byte("chunk2")})
	src, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	gcTableHash := src.hash()
	gcTableCount, _ := src.count()
	require.NoError(t, src.close())

	err = store.swapTables(ctx, []tableSpec{{name: gcTableHash, chunkCount: gcTableCount}}, chunks.GCMode_Full)
	require.NoError(t, err)

	assert.True(t, fgcm.gcGenUpdateCalled, "UpdateGCGen must be called on successful swapTables")

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, gcTableHash, fm.contents.specs[0].name)
	assert.Equal(t, gcTableHash, store.upstream.specs[0].name)
	_, inUpstream := store.tables.upstream[gcTableHash]
	assert.True(t, inUpstream, "GC-compacted table must be present in the store's in-memory table set")
}

// TestFinalizeConjoin_OptimisticLockRetry verifies that when op.apply's first
// manifest update attempt is preempted by a concurrent out-of-process writer,
// finalizeConjoin retries successfully and the final state is correct.
func TestFinalizeConjoin_OptimisticLockRetry(t *testing.T) {
	ctx := context.Background()
	fm := &fakeManifest{}

	updateCalls := 0
	upm := &updatePreemptManifest{
		manifest: fm,
		preUpdate: func() {
			updateCalls++
			if updateCalls == 1 {
				// Simulate a concurrent out-of-process writer: change only the
				// manifest lock, leaving the conjoinees in place so the retry
				// can still apply the conjoin.
				concurrentLock := computeAddr([]byte("concurrent-lock"))
				fm.mu.RLock()
				specs := make([]tableSpec, len(fm.contents.specs))
				copy(specs, fm.contents.specs)
				nbfVers := fm.contents.nbfVers
				root := fm.contents.root
				fm.mu.RUnlock()
				fm.set(nbfVers, concurrentLock, root, specs, nil)
			}
		},
	}

	mm := manifestManager{upm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	// Conjoinee: a table that will be merged.
	mt1 := createMemTable([][]byte{[]byte("c1"), []byte("c2")})
	conjoinee, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt1, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoineeHash := conjoinee.hash()
	conjoineeCount, _ := conjoinee.count()
	require.NoError(t, conjoinee.close())

	// Conjoined output: a separate table standing in for the merged file.
	mt2 := createMemTable([][]byte{[]byte("c1"), []byte("c2"), []byte("c3")})
	conjoined, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt2, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoinedHash := conjoined.hash()
	conjoinedCount, _ := conjoined.count()
	// Keep conjoined open — it is the conjoinedSrc.

	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}}, nil)
	store.upstream = fm.contents

	// Mark conjoinedHash as failing to open via p.Open so the test also
	// verifies that the pre-opened conjoinedSrc survives both rebase attempts.
	ftp.mu.Lock()
	ftp.sourcesToFail[conjoinedHash] = true
	ftp.mu.Unlock()

	store.conjoinOp = &conjoinOperation{
		conjoinees:   []tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}},
		conjoined:    tableSpec{name: conjoinedHash, chunkCount: conjoinedCount},
		conjoinedSrc: conjoined,
		cleanup:      func() {},
	}

	store.finalizeConjoin(ctx, nil)

	assert.Equal(t, 2, updateCalls,
		"expected first attempt preempted by concurrent write, second to succeed")

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, conjoinedHash, fm.contents.specs[0].name,
		"manifest must reference the conjoined file after a successful retry")
	assert.Equal(t, conjoinedHash, store.upstream.specs[0].name)
	_, inUpstream := store.tables.upstream[conjoinedHash]
	assert.True(t, inUpstream, "conjoined table must be in the store's in-memory table set")
}

// TestConjoinTableFiles_Success verifies the happy path through the public
// ConjoinTableFiles API: two tables are merged, the manifest is updated with
// the conjoined file, the store's in-memory state is consistent, and the
// correct hash is returned.
func TestConjoinTableFiles_Success(t *testing.T) {
	ctx := context.Background()
	fm, p, _, store := makeStoreWithFakes(t)
	defer store.Close()
	ftp := p.(fakeTablePersister)

	// Persist two tables that will be conjoined.
	mt1 := createMemTable([][]byte{[]byte("chunk1")})
	src1, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt1, nil, nil, &Stats{})
	require.NoError(t, err)
	hash1 := src1.hash()
	count1, _ := src1.count()
	require.NoError(t, src1.close())

	mt2 := createMemTable([][]byte{[]byte("chunk2")})
	src2, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt2, nil, nil, &Stats{})
	require.NoError(t, err)
	hash2 := src2.hash()
	count2, _ := src2.count()
	require.NoError(t, src2.close())

	// Set the manifest and bring the store's upstream into sync by rebasing.
	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: hash1, chunkCount: count1}, {name: hash2, chunkCount: count2}}, nil)
	require.NoError(t, store.Rebase(ctx))

	conjoinedHash, err := store.ConjoinTableFiles(ctx, []hash.Hash{hash1, hash2})
	require.NoError(t, err)
	assert.NotEqual(t, hash.Hash{}, conjoinedHash)

	// Manifest must now contain only the conjoined file.
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, conjoinedHash, fm.contents.specs[0].name)

	// In-memory state must match.
	assert.Equal(t, conjoinedHash, store.upstream.specs[0].name)
	_, inUpstream := store.tables.upstream[conjoinedHash]
	assert.True(t, inUpstream, "conjoined table must be in the store's in-memory table set")
}

// TestConjoinTableFiles_OptimisticLockRetry verifies that when a concurrent
// out-of-process writer moves the manifest lock between ConjoinTableFiles'
// rebase and manifest-update steps, op.apply retries and the operation
// ultimately succeeds.
func TestConjoinTableFiles_OptimisticLockRetry(t *testing.T) {
	ctx := context.Background()
	fm := &fakeManifest{}

	updateCalls := 0
	upm := &updatePreemptManifest{
		manifest: fm,
		preUpdate: func() {
			updateCalls++
			if updateCalls == 1 {
				// Move the manifest lock without touching the specs, as a
				// concurrent writer that committed something unrelated would.
				concurrentLock := computeAddr([]byte("concurrent-lock"))
				fm.mu.RLock()
				specs := make([]tableSpec, len(fm.contents.specs))
				copy(specs, fm.contents.specs)
				nbfVers := fm.contents.nbfVers
				root := fm.contents.root
				fm.mu.RUnlock()
				fm.set(nbfVers, concurrentLock, root, specs, nil)
			}
		},
	}

	mm := manifestManager{upm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	mt1 := createMemTable([][]byte{[]byte("chunk1")})
	src1, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt1, nil, nil, &Stats{})
	require.NoError(t, err)
	hash1 := src1.hash()
	count1, _ := src1.count()
	require.NoError(t, src1.close())

	mt2 := createMemTable([][]byte{[]byte("chunk2")})
	src2, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt2, nil, nil, &Stats{})
	require.NoError(t, err)
	hash2 := src2.hash()
	count2, _ := src2.count()
	require.NoError(t, src2.close())

	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: hash1, chunkCount: count1}, {name: hash2, chunkCount: count2}}, nil)

	// newNomsBlockStore rebases against the manifest, loading hash1 and hash2
	// into nbs.tables.upstream so ConjoinTableFiles can open them for ConjoinAll.
	store, err = newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	conjoinedHash, err := store.ConjoinTableFiles(ctx, []hash.Hash{hash1, hash2})
	require.NoError(t, err)
	assert.NotEqual(t, hash.Hash{}, conjoinedHash)

	assert.Equal(t, 2, updateCalls,
		"expected first attempt preempted by concurrent write, second to succeed")

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, conjoinedHash, fm.contents.specs[0].name)
	assert.Equal(t, conjoinedHash, store.upstream.specs[0].name)
	_, inUpstream := store.tables.upstream[conjoinedHash]
	assert.True(t, inUpstream, "conjoined table must be in the store's in-memory table set")
}

// TestFinalizeConjoin_RebaseFailureDoesNotUpdateManifest verifies that if the
// tableSet rebase fails inside finalizeConjoin, the manifest is never written
// and the store's in-memory upstream is left unchanged.
//
// Before the fix, finalizeConjoin delegated the manifest update to
// conjoinOp.updateManifest (which performed the write internally) and only
// rebased afterward. A rebase failure left the manifest pointing at the
// conjoined file while the store still held open handles to the old conjoinees.
func TestFinalizeConjoin_RebaseFailureDoesNotUpdateManifest(t *testing.T) {
	ctx := context.Background()
	fm, p, _, store := makeStoreWithFakes(t)
	defer store.Close()
	ftp := p.(fakeTablePersister)

	// Persist a table that will play the role of the conjoinee.
	mt := createMemTable([][]byte{[]byte("c1"), []byte("c2")})
	conjoinee, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoineeHash := conjoinee.hash()
	conjoineeCount, _ := conjoinee.count()
	require.NoError(t, conjoinee.close())

	// Set the manifest to reference the conjoinee, and update the store's
	// in-memory upstream to match (bypassing the normal rebase, which is fine
	// because we only need the upstream spec list to be accurate for
	// computeNewContents to work correctly).
	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}}, nil)
	store.upstream = fm.contents

	// The conjoined output file cannot be opened; this simulates a corrupt or
	// incomplete conjoin output.
	conjoinedHash := computeAddr([]byte("conjoined-result"))
	ftp.mu.Lock()
	ftp.sourcesToFail[conjoinedHash] = true
	ftp.mu.Unlock()

	// Install the pending conjoin operation.
	store.conjoinOp = &conjoinOperation{
		conjoinees: []tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}},
		conjoined:  tableSpec{name: conjoinedHash, chunkCount: conjoineeCount},
		cleanup:    func() {},
	}

	// finalizeConjoin should abort on the rebase before touching the manifest.
	store.finalizeConjoin(ctx, nil)

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	assert.Equal(t, initialLock, fm.contents.lock,
		"manifest must not be updated when the tableSet rebase fails")
	assert.Equal(t, initialLock, store.upstream.lock,
		"store upstream must not be updated when the tableSet rebase fails")
}

// TestFinalizeConjoin_OpenSourceCloneFailureDoesNotUpdateManifest exercises the
// normal (non-nil conjoinedSrc) path through finalizeConjoin. It verifies that
// when the pre-opened conjoinedSrc is provided but clone() fails, the rebase
// aborts and the manifest is not updated.
func TestFinalizeConjoin_OpenSourceCloneFailureDoesNotUpdateManifest(t *testing.T) {
	ctx := context.Background()
	fm, p, _, store := makeStoreWithFakes(t)
	defer store.Close()
	ftp := p.(fakeTablePersister)

	mt := createMemTable([][]byte{[]byte("c1"), []byte("c2")})
	conjoinee, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoineeHash := conjoinee.hash()
	conjoineeCount, _ := conjoinee.count()
	require.NoError(t, conjoinee.close())

	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}}, nil)
	store.upstream = fm.contents

	// conjoinedSrc is non-nil but clone() returns an error. The rebase will
	// find it in sources and try to clone it, which fails.
	conjoinedHash := computeAddr([]byte("conjoined-result"))
	store.conjoinOp = &conjoinOperation{
		conjoinees:   []tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}},
		conjoined:    tableSpec{name: conjoinedHash, chunkCount: conjoineeCount},
		conjoinedSrc: failOnCloneSource{h: conjoinedHash},
		cleanup:      func() {},
	}

	store.finalizeConjoin(ctx, nil)

	fm.mu.RLock()
	defer fm.mu.RUnlock()
	assert.Equal(t, initialLock, fm.contents.lock,
		"manifest must not be updated when the tableSet rebase fails")
	assert.Equal(t, initialLock, store.upstream.lock,
		"store upstream must not be updated when the tableSet rebase fails")
}

// TestFinalizeConjoin_OpenSourceMakesItToTableSet verifies the happy path: the
// pre-opened conjoinedSrc is cloned directly into the new tableSet without a
// redundant round-trip through the persister.
func TestFinalizeConjoin_OpenSourceMakesItToTableSet(t *testing.T) {
	ctx := context.Background()
	fm, p, _, store := makeStoreWithFakes(t)
	defer store.Close()
	ftp := p.(fakeTablePersister)

	// Persist a conjoinee and add it to the store's upstream.
	mt := createMemTable([][]byte{[]byte("c1"), []byte("c2")})
	conjoinee, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoineeHash := conjoinee.hash()
	conjoineeCount, _ := conjoinee.count()
	require.NoError(t, conjoinee.close())

	initialLock := computeAddr([]byte("initial-lock"))
	fm.set(constants.FormatDoltString, initialLock, hash.Hash{},
		[]tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}}, nil)
	store.upstream = fm.contents

	// Persist the conjoined file and open a source for it — this is what
	// op.conjoin() would normally do. We open it ourselves in the test so we
	// can install it on conjoinOp.conjoinedSrc.
	mt2 := createMemTable([][]byte{[]byte("c1"), []byte("c2")})
	conjoined, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt2, nil, nil, &Stats{})
	require.NoError(t, err)
	conjoinedHash := conjoined.hash()
	conjoinedCount, _ := conjoined.count()

	store.conjoinOp = &conjoinOperation{
		conjoinees:   []tableSpec{{name: conjoineeHash, chunkCount: conjoineeCount}},
		conjoined:    tableSpec{name: conjoinedHash, chunkCount: conjoinedCount},
		conjoinedSrc: conjoined, // hand the open source to finalizeConjoin
		cleanup:      func() {},
	}

	// Mark the conjoined file as failing in the persister so that any attempt
	// to re-open it via p.Open would fail. If the source makes it through to
	// the rebase via sources, Open is never called and the test passes.
	ftp.mu.Lock()
	ftp.sourcesToFail[conjoinedHash] = true
	ftp.mu.Unlock()

	store.finalizeConjoin(ctx, nil)

	// The manifest and in-memory upstream must now reference the conjoined file.
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	require.Equal(t, 1, len(fm.contents.specs))
	assert.Equal(t, conjoinedHash, fm.contents.specs[0].name,
		"manifest must reference the conjoined file after a successful finalizeConjoin")
	assert.Equal(t, conjoinedHash, store.upstream.specs[0].name,
		"store upstream must reference the conjoined file after a successful finalizeConjoin")
	_, inUpstream := store.tables.upstream[conjoinedHash]
	assert.True(t, inUpstream,
		"conjoined table must be present in the store's in-memory table set")
}

// gcGenPreemptManifest wraps a fakeGCManifest, calling preUpdateGCGen before
// each UpdateGCGen. This allows tests to simulate concurrent manifest edits
// that happen between the rebase and the UpdateGCGen call in swapTables.
type gcGenPreemptManifest struct {
	*fakeGCManifest
	preUpdateGCGen func()
}

func (g *gcGenPreemptManifest) UpdateGCGen(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if g.preUpdateGCGen != nil {
		g.preUpdateGCGen()
	}
	return g.fakeGCManifest.UpdateGCGen(ctx, behavior, lastLock, newContents, stats, writeHook)
}

// TestSwapTables_ConcurrentManifestEdit verifies that when a concurrent writer
// moves the manifest lock between the rebase and UpdateGCGen calls in swapTables,
// the rebased tableSet is properly closed and the operation returns an error.
func TestSwapTables_ConcurrentManifestEdit(t *testing.T) {
	ctx := context.Background()

	fm := &fakeManifest{}
	fgcm := &fakeGCManifest{fakeManifest: fm}
	gpm := &gcGenPreemptManifest{
		fakeGCManifest: fgcm,
		preUpdateGCGen: func() {
			// Simulate a concurrent writer moving the manifest lock.
			concurrentLock := computeAddr([]byte("concurrent-lock"))
			fm.mu.RLock()
			specs := make([]tableSpec, len(fm.contents.specs))
			copy(specs, fm.contents.specs)
			nbfVers := fm.contents.nbfVers
			root := fm.contents.root
			fm.mu.RUnlock()
			fm.set(nbfVers, concurrentLock, root, specs, nil)
		},
	}

	mm := manifestManager{gpm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	ftp := newFakeTablePersister(q)
	store, err := newNomsBlockStore(ctx, constants.FormatDoltString, mm, ftp, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	defer store.Close()

	// Persist a table that will be the GC-compacted result.
	mt := createMemTable([][]byte{[]byte("chunk1"), []byte("chunk2")})
	src, _, err := ftp.Persist(ctx, dherrors.FatalBehaviorError, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	gcTableHash := src.hash()
	gcTableCount, _ := src.count()
	require.NoError(t, src.close())

	err = store.swapTables(ctx, []tableSpec{{name: gcTableHash, chunkCount: gcTableCount}}, chunks.GCMode_Full)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "concurrent manifest edit during GC")

	// UpdateGCGen was called (it just didn't match the lock).
	assert.True(t, fgcm.gcGenUpdateCalled)
}
