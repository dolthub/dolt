// Copyright 2021 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type RebaseState struct {
	preRebaseWorking *RootValue
	ontoCommit       *Commit
}

func (rs RebaseState) OntoCommit() *Commit {
	return rs.ontoCommit
}

func (rs RebaseState) PreRebaseWorkingRoot() *RootValue {
	return rs.preRebaseWorking
}

type MergeState struct {
	// the source commit
	commit *Commit
	// the spec string that was used to specify |commit|
	commitSpecStr    string
	preMergeWorking  *RootValue
	unmergableTables []string
	mergedTables     []string
	// isCherryPick is set to true when the in-progress merge is a cherry-pick. This is needed so that
	// commit knows to NOT create a commit with multiple parents when creating a commit for a cherry-pick.
	isCherryPick bool
}

// todo(andy): this might make more sense in pkg merge
type SchemaConflict struct {
	ToSch, FromSch    schema.Schema
	ToFks, FromFks    []ForeignKey
	ToParentSchemas   map[string]schema.Schema
	FromParentSchemas map[string]schema.Schema
	toTbl, fromTbl    *Table
}

func (sc SchemaConflict) GetConflictingTables() (ours, theirs *Table) {
	return sc.toTbl, sc.fromTbl
}

// TodoWorkingSetMeta returns an incomplete WorkingSetMeta, suitable for methods that don't have the means to construct
// a real one. These should be considered temporary and cleaned up when possible, similar to Context.TODO
func TodoWorkingSetMeta() *datas.WorkingSetMeta {
	return &datas.WorkingSetMeta{
		Name:        "TODO",
		Email:       "TODO",
		Timestamp:   uint64(time.Now().Unix()),
		Description: "TODO",
	}
}

// MergeStateFromCommitAndWorking returns a new MergeState.
// Most clients should not construct MergeState objects directly, but instead use WorkingSet.StartMerge
func MergeStateFromCommitAndWorking(commit *Commit, preMergeWorking *RootValue) *MergeState {
	return &MergeState{commit: commit, preMergeWorking: preMergeWorking}
}

func (m MergeState) Commit() *Commit {
	return m.commit
}

func (m MergeState) CommitSpecStr() string {
	return m.commitSpecStr
}

// IsCherryPick returns true if the current merge state is for a cherry-pick operation. Cherry-picks use the same
// code as merge, but need slightly different behavior (e.g. only recording one commit parent, instead of two).
func (m MergeState) IsCherryPick() bool {
	return m.isCherryPick
}

func (m MergeState) PreMergeWorkingRoot() *RootValue {
	return m.preMergeWorking
}

type SchemaConflictFn func(table string, conflict SchemaConflict) error

func (m MergeState) HasSchemaConflicts() bool {
	return len(m.unmergableTables) > 0
}

func (m MergeState) TablesWithSchemaConflicts() []string {
	return m.unmergableTables
}

func (m MergeState) MergedTables() []string {
	return m.mergedTables
}

func (m MergeState) IterSchemaConflicts(ctx context.Context, ddb *DoltDB, cb SchemaConflictFn) (err error) {
	var to, from *RootValue

	to = m.preMergeWorking
	if from, err = m.commit.GetRootValue(ctx); err != nil {
		return err
	}

	toFKs, err := to.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	toSchemas, err := to.GetAllSchemas(ctx)
	if err != nil {
		return err
	}

	fromFKs, err := from.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	fromSchemas, err := from.GetAllSchemas(ctx)
	if err != nil {
		return err
	}

	for _, name := range m.unmergableTables {
		var sc SchemaConflict
		if sc.toTbl, _, err = to.GetTable(ctx, name); err != nil {
			return err
		}
		// todo: handle schema conflicts for renamed tables
		if sc.fromTbl, _, err = from.GetTable(ctx, name); err != nil {
			return err
		}

		if sc.ToSch, err = sc.toTbl.GetSchema(ctx); err != nil {
			return err
		}
		if sc.FromSch, err = sc.fromTbl.GetSchema(ctx); err != nil {
			return err
		}

		sc.ToFks, _ = toFKs.KeysForTable(name)
		sc.ToParentSchemas = toSchemas

		sc.FromFks, _ = fromFKs.KeysForTable(name)
		sc.FromParentSchemas = fromSchemas

		if err = cb(name, sc); err != nil {
			return err
		}
	}
	return nil
}

type WorkingSet struct {
	Name        string
	meta        *datas.WorkingSetMeta
	addr        *hash.Hash
	workingRoot *RootValue
	stagedRoot  *RootValue
	mergeState  *MergeState
	rebaseState *RebaseState
}

var _ Rootish = &WorkingSet{}

// TODO: remove this, require working and staged
func EmptyWorkingSet(wsRef ref.WorkingSetRef) *WorkingSet {
	return &WorkingSet{
		Name: wsRef.GetPath(),
	}
}

func (ws WorkingSet) WithStagedRoot(stagedRoot *RootValue) *WorkingSet {
	ws.stagedRoot = stagedRoot
	return &ws
}

func (ws WorkingSet) WithWorkingRoot(workingRoot *RootValue) *WorkingSet {
	ws.workingRoot = workingRoot
	return &ws
}

func (ws WorkingSet) WithMergeState(mergeState *MergeState) *WorkingSet {
	ws.mergeState = mergeState
	return &ws
}

func (ws WorkingSet) WithRebaseState(rebaseState *RebaseState) *WorkingSet {
	ws.rebaseState = rebaseState
	return &ws
}

func (ws WorkingSet) WithUnmergableTables(tables []string) *WorkingSet {
	ws.mergeState.unmergableTables = tables
	return &ws
}

func (ws WorkingSet) WithMergedTables(tables []string) *WorkingSet {
	ws.mergeState.mergedTables = tables
	return &ws
}

func (ws WorkingSet) StartMerge(commit *Commit, commitSpecStr string) *WorkingSet {
	ws.mergeState = &MergeState{
		commit:          commit,
		commitSpecStr:   commitSpecStr,
		preMergeWorking: ws.workingRoot,
	}

	return &ws
}

func (ws WorkingSet) StartRebase(ctx *sql.Context, ontoCommit *Commit) (*WorkingSet, error) {
	ws.rebaseState = &RebaseState{
		ontoCommit:       ontoCommit,
		preRebaseWorking: ws.workingRoot,
	}

	ontoRoot, err := ontoCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	ws.workingRoot = ontoRoot
	ws.stagedRoot = ontoRoot

	return &ws, nil
}

// StartCherryPick creates and returns a new working set based off of the current |ws| with the specified |commit|
// and |commitSpecStr| referring to the commit being cherry-picked. The returned WorkingSet records that a cherry-pick
// operation is in progress (i.e. conflicts being resolved). Note that this function does not update the current
// session – the returned WorkingSet must still be set using DoltSession.SetWorkingSet().
func (ws WorkingSet) StartCherryPick(commit *Commit, commitSpecStr string) *WorkingSet {
	ws.mergeState = &MergeState{
		commit:          commit,
		commitSpecStr:   commitSpecStr,
		preMergeWorking: ws.workingRoot,
		isCherryPick:    true,
	}
	return &ws
}

func (ws WorkingSet) AbortMerge() *WorkingSet {
	ws.workingRoot = ws.mergeState.PreMergeWorkingRoot()
	ws.stagedRoot = ws.workingRoot
	ws.mergeState = nil
	return &ws
}

func (ws WorkingSet) AbortRebase() *WorkingSet {
	ws.workingRoot = ws.rebaseState.preRebaseWorking
	ws.stagedRoot = ws.workingRoot
	ws.rebaseState = nil
	return &ws
}

func (ws WorkingSet) ClearMerge() *WorkingSet {
	ws.mergeState = nil
	return &ws
}

func (ws WorkingSet) ClearRebase() *WorkingSet {
	ws.rebaseState = nil
	return &ws
}

func (ws *WorkingSet) WorkingRoot() *RootValue {
	return ws.workingRoot
}

func (ws *WorkingSet) StagedRoot() *RootValue {
	return ws.stagedRoot
}

func (ws *WorkingSet) MergeState() *MergeState {
	return ws.mergeState
}

func (ws *WorkingSet) RebaseState() *RebaseState {
	return ws.rebaseState
}

func (ws *WorkingSet) MergeActive() bool {
	return ws.mergeState != nil
}

func (ws *WorkingSet) RebaseActive() bool {
	return ws.rebaseState != nil
}

// MergeCommitParents returns true if there is an active merge in progress and
// the recorded commit being merged into the active branch should be included as
// a second parent of the created commit. This is the expected behavior for a
// normal merge, but not for other pseudo-merges, like cherry-picks or reverts,
// where the created commit should only have one parent.
func (ws *WorkingSet) MergeCommitParents() bool {
	if !ws.MergeActive() {
		return false
	}
	return ws.MergeState().IsCherryPick() == false
}

func (ws WorkingSet) Meta() *datas.WorkingSetMeta {
	return ws.meta
}

// newWorkingSet creates a new WorkingSet object.
func newWorkingSet(ctx context.Context, name string, vrw types.ValueReadWriter, ns tree.NodeStore, ds datas.Dataset) (*WorkingSet, error) {
	dsws, err := ds.HeadWorkingSet()
	if err != nil {
		return nil, err
	}

	meta := dsws.Meta
	if meta == nil {
		meta = &datas.WorkingSetMeta{
			Name:        "not present",
			Email:       "not present",
			Timestamp:   0,
			Description: "not present",
		}
	}

	workingRootVal, err := vrw.ReadValue(ctx, dsws.WorkingAddr)
	if err != nil {
		return nil, err
	}
	workingRoot, err := newRootValue(vrw, ns, workingRootVal)
	if err != nil {
		return nil, err
	}

	var stagedRoot *RootValue
	if dsws.StagedAddr != nil {
		stagedRootVal, err := vrw.ReadValue(ctx, *dsws.StagedAddr)
		if err != nil {
			return nil, err
		}

		stagedRoot, err = newRootValue(vrw, ns, stagedRootVal)
		if err != nil {
			return nil, err
		}
	}

	var mergeState *MergeState
	if dsws.MergeState != nil {
		preMergeWorkingAddr, err := dsws.MergeState.PreMergeWorkingAddr(ctx, vrw)
		if err != nil {
			return nil, err
		}
		fromDCommit, err := dsws.MergeState.FromCommit(ctx, vrw)
		if err != nil {
			return nil, err
		}
		commitSpec, err := dsws.MergeState.FromCommitSpec(ctx, vrw)
		if err != nil {
			return nil, err
		}

		commit, err := NewCommit(ctx, vrw, ns, fromDCommit)
		if err != nil {
			return nil, err
		}

		preMergeWorkingV, err := vrw.ReadValue(ctx, preMergeWorkingAddr)
		if err != nil {
			return nil, err
		}

		preMergeWorkingRoot, err := newRootValue(vrw, ns, preMergeWorkingV)
		if err != nil {
			return nil, err
		}

		unmergableTables, err := dsws.MergeState.UnmergableTables(ctx, vrw)
		if err != nil {
			return nil, err
		}

		isCherryPick, err := dsws.MergeState.IsCherryPick(ctx, vrw)
		if err != nil {
			return nil, err
		}

		mergeState = &MergeState{
			commit:           commit,
			commitSpecStr:    commitSpec,
			preMergeWorking:  preMergeWorkingRoot,
			unmergableTables: unmergableTables,
			isCherryPick:     isCherryPick,
		}
	}

	var rebaseState *RebaseState
	if dsws.RebaseState != nil {
		preRebaseWorkingAddr, err := dsws.RebaseState.PreRebaseWorkingAddr(ctx, vrw)
		if err != nil {
			return nil, err
		}

		preRebaseWorkingV, err := vrw.ReadValue(ctx, preRebaseWorkingAddr)
		if err != nil {
			return nil, err
		}

		preRebaseWorkingRoot, err := newRootValue(vrw, ns, preRebaseWorkingV)
		if err != nil {
			return nil, err
		}

		ontoCommit, err := dsws.RebaseState.OntoCommit(ctx, vrw)
		if err != nil {
			return nil, err
		}

		// TODO: rename
		ontoCommit2, err := NewCommit(ctx, vrw, ns, ontoCommit)
		if err != nil {
			return nil, err
		}

		rebaseState = &RebaseState{
			preRebaseWorking: preRebaseWorkingRoot,
			ontoCommit:       ontoCommit2,
		}
	}

	addr, _ := ds.MaybeHeadAddr()

	return &WorkingSet{
		Name:        name,
		meta:        meta,
		addr:        &addr,
		workingRoot: workingRoot,
		stagedRoot:  stagedRoot,
		mergeState:  mergeState,
		rebaseState: rebaseState,
	}, nil
}

// ResolveRootValue implements Rootish.
func (ws *WorkingSet) ResolveRootValue(context.Context) (*RootValue, error) {
	return ws.WorkingRoot(), nil
}

// HashOf returns the hash of the workingset struct, which is not the same as the hash of the root value stored in the
// working set. This value is used for optimistic locking when updating a working set for a head ref.
func (ws *WorkingSet) HashOf() (hash.Hash, error) {
	if ws == nil || ws.addr == nil {
		return hash.Hash{}, nil
	}
	return *ws.addr, nil
}

// Ref returns a WorkingSetRef for this WorkingSet.
func (ws *WorkingSet) Ref() ref.WorkingSetRef {
	return ref.NewWorkingSetRef(ws.Name)
}

// writeValues write the values in this working set to the database and returns them
func (ws *WorkingSet) writeValues(ctx context.Context, db *DoltDB) (
	workingRoot types.Ref,
	stagedRoot types.Ref,
	mergeState *datas.MergeState,
	rebaseState *datas.RebaseState,
	err error,
) {
	if ws.stagedRoot == nil || ws.workingRoot == nil {
		return types.Ref{}, types.Ref{}, nil, nil, fmt.Errorf("StagedRoot and workingRoot must be set. This is a bug.")
	}

	var r *RootValue
	r, workingRoot, err = db.writeRootValue(ctx, ws.workingRoot)
	if err != nil {
		return types.Ref{}, types.Ref{}, nil, nil, err
	}
	ws.workingRoot = r

	r, stagedRoot, err = db.writeRootValue(ctx, ws.stagedRoot)
	if err != nil {
		return types.Ref{}, types.Ref{}, nil, nil, err
	}
	ws.stagedRoot = r

	if ws.mergeState != nil {
		r, preMergeWorking, err := db.writeRootValue(ctx, ws.mergeState.preMergeWorking)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}
		ws.mergeState.preMergeWorking = r

		h, err := ws.mergeState.commit.HashOf()
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}
		dCommit, err := datas.LoadCommitAddr(ctx, db.vrw, h)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}

		mergeState, err = datas.NewMergeState(ctx, db.vrw, preMergeWorking, dCommit, ws.mergeState.commitSpecStr, ws.mergeState.unmergableTables, ws.mergeState.isCherryPick)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}
	}

	if ws.rebaseState != nil {
		r, preRebaseWorking, err := db.writeRootValue(ctx, ws.rebaseState.preRebaseWorking)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}
		ws.rebaseState.preRebaseWorking = r

		h, err := ws.rebaseState.ontoCommit.HashOf()
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}
		dCommit, err := datas.LoadCommitAddr(ctx, db.vrw, h)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, nil, err
		}

		// TODO: is this the right signature for this function?
		rebaseState = datas.NewRebaseState(preRebaseWorking.TargetHash(), dCommit.Addr())
	}

	return workingRoot, stagedRoot, mergeState, rebaseState, nil
}
