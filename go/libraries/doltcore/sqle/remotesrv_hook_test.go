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

package sqle

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// recordingCommitHook is a CommitHook that records the IDs of all datasets for
// which it was executed.
type recordingCommitHook struct {
	mu    sync.Mutex
	calls []string
}

func (h *recordingCommitHook) Execute(_ context.Context, ds datas.Dataset, _ *doltdb.DoltDB) (func(context.Context) error, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, ds.ID())
	return nil, nil
}

func (h *recordingCommitHook) ExecuteForWorkingSets() bool { return false }

func (h *recordingCommitHook) calledFor(datasetID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Contains(h.calls, datasetID)
}

func (h *recordingCommitHook) numCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

var _ doltdb.CommitHook = (*recordingCommitHook)(nil)

// makeTestCommit creates a second commit on the main branch and returns the
// old and new noms root hashes. The hook is installed before the commit is
// replayed via the low-level store, so only the store-level replay should
// trigger it.
func makeTestCommit(t *testing.T, ctx context.Context, ddb *doltdb.DoltDB) (oldRoot, newRoot hash.Hash) {
	t.Helper()
	rawDB := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(rawDB)

	// Record the noms root before the commit.
	var err error
	oldRoot, err = cs.Root(ctx)
	require.NoError(t, err)

	// Create a new commit on main.
	headCommit, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("main"))
	require.NoError(t, err)
	rootVal, err := headCommit.GetRootValue(ctx)
	require.NoError(t, err)
	valHash, err := rootVal.HashOf()
	require.NoError(t, err)
	meta, err := datas.NewCommitMeta("test", "test@test.com", "test commit")
	require.NoError(t, err)
	_, err = ddb.Commit(ctx, valHash, ref.NewBranchRef("main"), meta)
	require.NoError(t, err)

	// Record the noms root after the commit.
	newRoot, err = cs.Root(ctx)
	require.NoError(t, err)
	require.NotEqual(t, oldRoot, newRoot, "noms root must have advanced after commit")

	// Rewind the ChunkStore back to oldRoot so the test can replay the
	// commit via hooksFiringRemoteSrvStore.
	rewound, err := cs.Commit(ctx, oldRoot, newRoot)
	require.NoError(t, err)
	require.True(t, rewound, "rewind of ChunkStore to oldRoot must succeed")

	return oldRoot, newRoot
}

// TestHooksFiredOnRemoteSrvStoreCommit verifies that hooksFiringRemoteSrvStore
// fires the DoltDB's CommitHooks when a push commit succeeds.
func TestHooksFiredOnRemoteSrvStoreCommit(t *testing.T) {
	ctx := t.Context()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	ddb := dEnv.DoltDB(ctx)
	oldRoot, newRoot := makeTestCommit(t, ctx, ddb)

	// Install recording hook after the rewind so we only capture the
	// replay via the low-level store path.
	hook := &recordingCommitHook{}
	ddb.PrependCommitHooks(ctx, hook)

	rawDB := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(rawDB)
	rss, ok := cs.(remotesrv.RemoteSrvStore)
	require.True(t, ok, "in-memory NBS must implement RemoteSrvStore")

	store := hooksFiringRemoteSrvStore{rss, ddb}
	committed, err := store.Commit(ctx, newRoot, oldRoot)
	require.NoError(t, err)
	require.True(t, committed)

	assert.True(t, hook.calledFor("refs/heads/main"),
		"hook must be called for refs/heads/main after a successful push commit")
}

// TestHooksNotFiredOnFailedRemoteSrvCommit verifies that hooks are NOT fired
// when the underlying ChunkStore Commit returns false (e.g. CAS failure due
// to a concurrent update).
func TestHooksNotFiredOnFailedRemoteSrvCommit(t *testing.T) {
	ctx := t.Context()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	ddb := dEnv.DoltDB(ctx)
	oldRoot, newRoot := makeTestCommit(t, ctx, ddb)

	hook := &recordingCommitHook{}
	ddb.PrependCommitHooks(ctx, hook)

	rawDB := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(rawDB)
	rss, ok := cs.(remotesrv.RemoteSrvStore)
	require.True(t, ok)

	store := hooksFiringRemoteSrvStore{rss, ddb}

	// Pass a wrong last hash (zero hash is never a valid noms root for an
	// initialised repo). The NBS CAS check will reject this because the
	// actual current root is oldRoot, not hash.Hash{}.
	var wrongLast hash.Hash
	committed, err := store.Commit(ctx, newRoot, wrongLast)
	require.NoError(t, err)
	assert.False(t, committed, "commit must be rejected when last hash is wrong")
	assert.Equal(t, 0, hook.numCalls(), "hooks must not fire on a rejected commit")

	// Actual oldRoot → newRoot should still work afterwards.
	committed, err = store.Commit(ctx, newRoot, oldRoot)
	require.NoError(t, err)
	assert.True(t, committed)
	assert.Equal(t, 1, hook.numCalls(), "hook must fire exactly once after the successful commit")
}

// TestHooksFiredOnDeletedDataset verifies that a hook fires for a ref-typed
// dataset that was present at |last| but absent at |current| (i.e. a branch
// delete pushed via remotesrv). This mirrors the behaviour of
// hooksDatabase.Delete which also fires hooks for deleted datasets.
func TestHooksFiredOnDeletedDataset(t *testing.T) {
	ctx := t.Context()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	ddb := dEnv.DoltDB(ctx)

	// Create a feature branch so it exists in R_old.
	headCommit, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("main"))
	require.NoError(t, err)
	err = ddb.NewBranchAtCommit(ctx, ref.NewBranchRef("feature"), headCommit, nil)
	require.NoError(t, err)

	rawDB := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(rawDB)

	oldRoot, err := cs.Root(ctx)
	require.NoError(t, err)

	// Delete feature branch via DoltDB; this advances the noms root.
	err = ddb.DeleteBranch(ctx, ref.NewBranchRef("feature"), nil)
	require.NoError(t, err)

	newRoot, err := cs.Root(ctx)
	require.NoError(t, err)
	require.NotEqual(t, oldRoot, newRoot)

	// Rewind so we can replay via the low-level store path.
	rewound, err := cs.Commit(ctx, oldRoot, newRoot)
	require.NoError(t, err)
	require.True(t, rewound)

	hook := &recordingCommitHook{}
	ddb.PrependCommitHooks(ctx, hook)

	rss, ok := cs.(remotesrv.RemoteSrvStore)
	require.True(t, ok)

	store := hooksFiringRemoteSrvStore{rss, ddb}
	committed, err := store.Commit(ctx, newRoot, oldRoot)
	require.NoError(t, err)
	require.True(t, committed)

	assert.True(t, hook.calledFor("refs/heads/feature"),
		"hook must fire for a branch that was deleted by the push")
	assert.False(t, hook.calledFor("refs/heads/main"),
		"hook must not fire for a branch that was unchanged")
}

// TestOnlyChangedDatasetsFireHooks verifies that hooks are only fired for
// datasets whose head address actually changed between last and current.
func TestOnlyChangedDatasetsFireHooks(t *testing.T) {
	ctx := t.Context()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	ddb := dEnv.DoltDB(ctx)

	// Create a second branch before recording any hashes so it exists at oldRoot.
	headCommit, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("main"))
	require.NoError(t, err)
	err = ddb.NewBranchAtCommit(ctx, ref.NewBranchRef("feature"), headCommit, nil)
	require.NoError(t, err)

	// Now make a commit only on main; feature branch head stays the same.
	oldRoot, newRoot := makeTestCommit(t, ctx, ddb)

	hook := &recordingCommitHook{}
	ddb.PrependCommitHooks(ctx, hook)

	rawDB := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(rawDB)
	rss, ok := cs.(remotesrv.RemoteSrvStore)
	require.True(t, ok)

	store := hooksFiringRemoteSrvStore{rss, ddb}
	committed, err := store.Commit(ctx, newRoot, oldRoot)
	require.NoError(t, err)
	require.True(t, committed)

	assert.True(t, hook.calledFor("refs/heads/main"),
		"hook must fire for the branch whose head changed")
	assert.False(t, hook.calledFor("refs/heads/feature"),
		"hook must not fire for a branch whose head did not change")
}
