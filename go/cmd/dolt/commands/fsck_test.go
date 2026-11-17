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

package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// TestResolveStashRefsCorruptStashList exercises resolveStashRefs against damaged stash lists. fsck exists to inspect
// potentially corrupted repositories, so reading a stash list must report integrity errors rather than panic.
func TestResolveStashRefsCorruptStashList(t *testing.T) {
	ctx := context.Background()
	headHash := mkTestHash(1)
	rootHash := mkTestHash(2)

	t.Run("non-numeric address map key does not panic", func(t *testing.T) {
		// A stash's address map key is normally its numeric index. A corrupt, non-numeric key used to crash the
		// datas stash helpers (strconv.Atoi failure was swallowed, leaving a nil entry that was then dereferenced).
		// resolveStashRefs walks the address map directly and does not depend on the key, so it must still resolve.
		cs, ns := newTestChunkStore(t)
		stashAddr := writeTestChunk(t, ctx, cs, buildTestStash(headHash, rootHash))
		listAddr := writeTestChunk(t, ctx, cs, buildTestStashList(t, ctx, ns, "not-a-number", stashAddr))

		var errs Errs
		heads, roots := resolveStashRefs(ctx, cs, ns, listAddr, "refs/stashes/dolt", &errs)

		require.Empty(t, errs)
		require.Equal(t, []hash.Hash{headHash}, heads)
		require.Equal(t, []hash.Hash{rootHash}, roots)
	})

	t.Run("stash entry pointing to a missing object reports an error", func(t *testing.T) {
		cs, ns := newTestChunkStore(t)
		missingStash := mkTestHash(9)
		listAddr := writeTestChunk(t, ctx, cs, buildTestStashList(t, ctx, ns, "0", missingStash))

		var errs Errs
		heads, roots := resolveStashRefs(ctx, cs, ns, listAddr, "refs/stashes/dolt", &errs)

		require.Len(t, errs, 1)
		require.Empty(t, heads)
		require.Empty(t, roots)
	})
}

// TestFsckWorkingSets runs fsck over stores whose working sets are damaged. The damage is written directly to the
// store, so these tests do not depend on how the store could have ended up in that state.
func TestFsckWorkingSets(t *testing.T) {
	ctx := context.Background()
	const wsName = "workingSets/heads/feat"
	missing := mkTestHash(9)

	tests := []struct {
		name string
		ws   func(commit, root hash.Hash) testWorkingSet
		// wantField is the working set field that fsck should name.
		wantField string
	}{
		{
			name: "working root",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: missing, staged: root}
			},
			wantField: "working root",
		},
		{
			name: "staged root",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: missing}
			},
			wantField: "staged root",
		},
		{
			name: "pre-merge working root",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, merge: &testMergeState{preWorking: missing, from: commit}}
			},
			wantField: "pre-merge working root",
		},
		{
			name: "merge from commit",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, merge: &testMergeState{preWorking: root, from: missing}}
			},
			wantField: "merge from commit",
		},
		{
			name: "pre-merge head commit",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, merge: &testMergeState{preWorking: root, from: commit, preHead: missing}}
			},
			wantField: "pre-merge head commit",
		},
		{
			name: "pre-rebase working root",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, rebase: &testRebaseState{preWorking: missing, onto: commit}}
			},
			wantField: "pre-rebase working root",
		},
		{
			name: "rebase onto commit",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, rebase: &testRebaseState{preWorking: root, onto: missing}}
			},
			wantField: "rebase onto commit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newFsckTestStore(t, ctx)
			s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(test.ws(s.commit, s.root))))

			report := runFsck(t, ctx, s.gs)
			requireErrContains(t, report.CommitErrs, fmt.Sprintf("%s (%s)", wsName, test.wantField), missing.String())
		})
	}

	t.Run("a root shared by several fields names all of them", func(t *testing.T) {
		s := newFsckTestStore(t, ctx)
		s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(testWorkingSet{working: missing, staged: missing})))

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, wsName+" (working root)", wsName+" (staged root)")
	})

	t.Run("a chunk below the working root", func(t *testing.T) {
		s := newFsckTestStore(t, ctx)
		rootAddr := s.write(t, ctx, buildTestRootValue(t, ctx, s.ns, missing))
		s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(testWorkingSet{working: rootAddr, staged: s.root})))

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, wsName+" (working root)", missing.String())
	})

	t.Run("the working set object itself", func(t *testing.T) {
		s := newFsckTestStore(t, ctx)
		s.setRef(t, ctx, wsName, missing)

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, wsName, missing.String())
	})

	t.Run("a working set name containing a format verb", func(t *testing.T) {
		s := newFsckTestStore(t, ctx)
		name := "workingSets/heads/100%done"
		s.setRef(t, ctx, name, s.write(t, ctx, buildTestWorkingSet(testWorkingSet{working: missing, staged: s.root})))

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, name+" (working root)")
	})
}

// TestFsckHealthyWorkingSets checks that the working set walk does not fail a healthy store. The working set names do
// not have a matching branch, which is also what a working set left behind by a deleted branch looks like.
func TestFsckHealthyWorkingSets(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		ws   func(commit, root hash.Hash) testWorkingSet
	}{
		{
			name: "clean",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root}
			},
		},
		{
			name: "no staged root",
			ws: func(_, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root}
			},
		},
		{
			name: "merge in progress",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, merge: &testMergeState{preWorking: root, from: commit, preHead: commit}}
			},
		},
		{
			name: "merge in progress without a pre-merge head",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, merge: &testMergeState{preWorking: root, from: commit}}
			},
		},
		{
			name: "rebase in progress",
			ws: func(commit, root hash.Hash) testWorkingSet {
				return testWorkingSet{working: root, staged: root, rebase: &testRebaseState{preWorking: root, onto: commit}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newFsckTestStore(t, ctx)
			s.setRef(t, ctx, "workingSets/heads/feat", s.write(t, ctx, buildTestWorkingSet(test.ws(s.commit, s.root))))

			report := runFsck(t, ctx, s.gs)
			require.Empty(t, report.ScanErrs)
			require.Empty(t, report.CommitErrs)
		})
	}
}

// TestFsckGhostCommits checks that commits recorded as ghosts, which a shallow clone deliberately does not have, are not
// reported as missing wherever they are referenced from, and that this does not hide commits that really are missing.
func TestFsckGhostCommits(t *testing.T) {
	ctx := context.Background()
	const wsName = "workingSets/heads/feat"
	ghost := mkTestHash(8)
	missing := mkTestHash(9)

	newStore := func(t *testing.T) *fsckTestStore {
		s := newFsckTestStore(t, ctx)
		require.NoError(t, s.gs.PersistGhostHashes(ctx, hash.NewHashSet(ghost)))
		return s
	}

	t.Run("remote ref", func(t *testing.T) {
		s := newStore(t)
		s.setRef(t, ctx, "refs/remotes/origin/feat", ghost)

		report := runFsck(t, ctx, s.gs)
		require.Empty(t, report.CommitErrs)
	})

	t.Run("merge state", func(t *testing.T) {
		s := newStore(t)
		ws := testWorkingSet{working: s.root, staged: s.root, merge: &testMergeState{preWorking: s.root, from: ghost, preHead: ghost}}
		s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(ws)))

		report := runFsck(t, ctx, s.gs)
		require.Empty(t, report.CommitErrs)
	})

	t.Run("rebase state", func(t *testing.T) {
		s := newStore(t)
		ws := testWorkingSet{working: s.root, staged: s.root, rebase: &testRebaseState{preWorking: s.root, onto: ghost}}
		s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(ws)))

		report := runFsck(t, ctx, s.gs)
		require.Empty(t, report.CommitErrs)
	})

	t.Run("a missing remote ref commit is still reported", func(t *testing.T) {
		s := newStore(t)
		s.setRef(t, ctx, "refs/remotes/origin/feat", missing)

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, "refs/remotes/origin/feat", missing.String())
	})

	t.Run("a missing working set commit is still reported", func(t *testing.T) {
		s := newStore(t)
		ws := testWorkingSet{working: s.root, staged: s.root, merge: &testMergeState{preWorking: s.root, from: missing}}
		s.setRef(t, ctx, wsName, s.write(t, ctx, buildTestWorkingSet(ws)))

		report := runFsck(t, ctx, s.gs)
		requireErrContains(t, report.CommitErrs, wsName+" (merge from commit)", missing.String())
	})
}

// requireErrContains requires that a single error in |errs| contains all of |substrs|.
func requireErrContains(t *testing.T, errs Errs, substrs ...string) {
	t.Helper()
	for _, err := range errs {
		matched := true
		for _, substr := range substrs {
			if !strings.Contains(err.Error(), substr) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	require.Failf(t, "no matching fsck error", "wanted an error containing %q, got: %v", substrs, errs)
}

func runFsck(t *testing.T, ctx context.Context, gs *nbs.GenerationalNBS) *FsckReport {
	progress := make(chan FsckProgressMessage, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range progress {
		}
	}()

	var report FsckReport
	err := fsckOnChunkStore(ctx, gs, &report, progress)
	close(progress)
	<-done
	require.NoError(t, err)
	return &report
}

// fsckTestStore is an on-disk store with a single commit on main. Damage is added by writing chunks and store root
// entries directly, bypassing the dangling reference checks that a normal write goes through.
type fsckTestStore struct {
	gs     *nbs.GenerationalNBS
	ns     tree.NodeStore
	commit hash.Hash
	root   hash.Hash
}

func newFsckTestStore(t *testing.T, ctx context.Context) *fsckTestStore {
	nomsDir := filepath.Join(t.TempDir(), "noms")
	require.NoError(t, os.MkdirAll(nomsDir, os.ModePerm))
	u, err := earl.Parse(earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator))
	require.NoError(t, err)

	params := map[string]interface{}{dbfactory.ChunkJournalParam: struct{}{}}
	db, _, _, err := dbfactory.FileFactory{}.CreateDbNoCache(ctx, types.Format_DOLT, u, params, func(error) {})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	gs := datas.ChunkStoreFromDatabase(db).(*nbs.GenerationalNBS)
	ddb, err := doltdb.DoltDBFromCS(gs, "fsck_test")
	require.NoError(t, err)
	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", "fsck test", "fsck@test.com"))

	head, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("main"))
	require.NoError(t, err)
	commitAddr, err := head.HashOf()
	require.NoError(t, err)
	commitChunk, err := gs.Get(ctx, commitAddr)
	require.NoError(t, err)
	var commit serial.Commit
	require.NoError(t, serial.InitCommitRoot(&commit, commitChunk.Data(), serial.MessagePrefixSz))

	return &fsckTestStore{
		gs:     gs,
		ns:     tree.NewNodeStore(noRefCheckChunkStore{gs}),
		commit: commitAddr,
		root:   hash.New(commit.RootBytes()),
	}
}

func (s *fsckTestStore) write(t *testing.T, ctx context.Context, data []byte) hash.Hash {
	return writeTestChunk(t, ctx, s.gs, data)
}

// setRef points |name| at |addr| in the store root, whether or not |addr| exists.
func (s *fsckTestStore) setRef(t *testing.T, ctx context.Context, name string, addr hash.Hash) {
	oldRoot, err := s.gs.Root(ctx)
	require.NoError(t, err)
	rootChunk, err := s.gs.Get(ctx, oldRoot)
	require.NoError(t, err)
	sr, err := serial.TryGetRootAsStoreRoot(rootChunk.Data(), serial.MessagePrefixSz)
	require.NoError(t, err)
	am, err := addressMapFromBytes(sr.AddressMapBytes(), s.ns)
	require.NoError(t, err)

	editor := am.Editor()
	require.NoError(t, editor.Update(ctx, name, addr))
	am, err = editor.Flush(ctx)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(1024)
	amOff := b.CreateByteVector([]byte(tree.ValueFromNode(am.Node()).(types.SerialMessage)))
	serial.StoreRootStart(b)
	serial.StoreRootAddAddressMap(b, amOff)
	newRoot := s.write(t, ctx, []byte(serial.FinishMessage(b, serial.StoreRootEnd(b), []byte(serial.StoreRootFileID))))

	ok, err := s.gs.Commit(ctx, newRoot, oldRoot)
	require.NoError(t, err)
	require.True(t, ok)
}

// noRefCheckChunkStore writes chunks without recording their references, so that they are allowed to dangle.
type noRefCheckChunkStore struct {
	chunks.ChunkStore
}

func (cs noRefCheckChunkStore) Put(ctx context.Context, c chunks.Chunk, _ chunks.InsertAddrsCurry) error {
	return cs.ChunkStore.Put(ctx, c, func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	})
}

type testMergeState struct {
	preWorking, from, preHead hash.Hash
}

type testRebaseState struct {
	preWorking, onto hash.Hash
}

type testWorkingSet struct {
	working, staged hash.Hash
	merge           *testMergeState
	rebase          *testRebaseState
}

func buildTestWorkingSet(ws testWorkingSet) []byte {
	b := flatbuffers.NewBuilder(1024)
	workingOff := b.CreateByteVector(ws.working[:])

	var stagedOff flatbuffers.UOffsetT
	if !ws.staged.IsEmpty() {
		stagedOff = b.CreateByteVector(ws.staged[:])
	}

	var mergeOff flatbuffers.UOffsetT
	if ws.merge != nil {
		preWorkingOff := b.CreateByteVector(ws.merge.preWorking[:])
		fromOff := b.CreateByteVector(ws.merge.from[:])
		var preHeadOff flatbuffers.UOffsetT
		if !ws.merge.preHead.IsEmpty() {
			preHeadOff = b.CreateByteVector(ws.merge.preHead[:])
		}
		serial.MergeStateStart(b)
		serial.MergeStateAddPreWorkingRootAddr(b, preWorkingOff)
		serial.MergeStateAddFromCommitAddr(b, fromOff)
		if preHeadOff != 0 {
			serial.MergeStateAddPreMergeHeadCommitAddr(b, preHeadOff)
		}
		mergeOff = serial.MergeStateEnd(b)
	}

	var rebaseOff flatbuffers.UOffsetT
	if ws.rebase != nil {
		preWorkingOff := b.CreateByteVector(ws.rebase.preWorking[:])
		ontoOff := b.CreateByteVector(ws.rebase.onto[:])
		branchOff := b.CreateString("feat")
		serial.RebaseStateStart(b)
		serial.RebaseStateAddPreWorkingRootAddr(b, preWorkingOff)
		serial.RebaseStateAddOntoCommitAddr(b, ontoOff)
		serial.RebaseStateAddBranch(b, branchOff)
		rebaseOff = serial.RebaseStateEnd(b)
	}

	nameOff := b.CreateString("fsck test")
	emailOff := b.CreateString("fsck@test.com")
	descOff := b.CreateString("working set")

	serial.WorkingSetStart(b)
	serial.WorkingSetAddWorkingRootAddr(b, workingOff)
	if stagedOff != 0 {
		serial.WorkingSetAddStagedRootAddr(b, stagedOff)
	}
	if mergeOff != 0 {
		serial.WorkingSetAddMergeState(b, mergeOff)
	}
	if rebaseOff != 0 {
		serial.WorkingSetAddRebaseState(b, rebaseOff)
	}
	serial.WorkingSetAddName(b, nameOff)
	serial.WorkingSetAddEmail(b, emailOff)
	serial.WorkingSetAddDesc(b, descOff)
	return []byte(serial.FinishMessage(b, serial.WorkingSetEnd(b), []byte(serial.WorkingSetFileID)))
}

// buildTestRootValue builds a root value holding one table stored at |tableAddr|.
func buildTestRootValue(t *testing.T, ctx context.Context, ns tree.NodeStore, tableAddr hash.Hash) []byte {
	am, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)
	editor := am.Editor()
	require.NoError(t, editor.Add(ctx, "t", tableAddr))
	am, err = editor.Flush(ctx)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(1024)
	tablesOff := b.CreateByteVector([]byte(tree.ValueFromNode(am.Node()).(types.SerialMessage)))
	var noFKs hash.Hash
	fkOff := b.CreateByteVector(noFKs[:])
	serial.RootValueStart(b)
	serial.RootValueAddTables(b, tablesOff)
	serial.RootValueAddForeignKeyAddr(b, fkOff)
	return []byte(serial.FinishMessage(b, serial.RootValueEnd(b), []byte(serial.RootValueFileID)))
}

func newTestChunkStore(t *testing.T) (chunks.ChunkStore, tree.NodeStore) {
	stg := &chunks.MemoryStorage{}
	cs := stg.NewViewWithDefaultFormat()
	return cs, tree.NewNodeStore(cs)
}

func writeTestChunk(t *testing.T, ctx context.Context, cs chunks.ChunkStore, data []byte) hash.Hash {
	c := chunks.NewChunk(data)
	noRefs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}
	require.NoError(t, cs.Put(ctx, c, noRefs))
	return c.Hash()
}

func mkTestHash(b byte) hash.Hash {
	var h hash.Hash
	h[hash.ByteLen-1] = b
	return h
}

func buildTestStash(head, root hash.Hash) []byte {
	b := flatbuffers.NewBuilder(1024)
	rootOff := b.CreateByteVector(root[:])
	headOff := b.CreateByteVector(head[:])
	branchOff := b.CreateString("main")
	descOff := b.CreateString("WIP")
	serial.StashStart(b)
	serial.StashAddStashRootAddr(b, rootOff)
	serial.StashAddHeadCommitAddr(b, headOff)
	serial.StashAddBranchName(b, branchOff)
	serial.StashAddDesc(b, descOff)
	return []byte(serial.FinishMessage(b, serial.StashEnd(b), []byte(serial.StashFileID)))
}

func buildTestStashList(t *testing.T, ctx context.Context, ns tree.NodeStore, key string, stashAddr hash.Hash) []byte {
	am, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)
	editor := am.Editor()
	require.NoError(t, editor.Add(ctx, key, stashAddr))
	am, err = editor.Flush(ctx)
	require.NoError(t, err)

	amBytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	b := flatbuffers.NewBuilder(1024)
	amOff := b.CreateByteVector(amBytes)
	serial.StashListStart(b)
	serial.StashListAddAddressMap(b, amOff)
	return []byte(serial.FinishMessage(b, serial.StashListEnd(b), []byte(serial.StashListFileID)))
}
