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

// EmptyCommitHandling describes how a cherry-pick action should handle empty commits. This applies to commits that
// start off as empty, as well as commits whose changes are applied, but are redundant, and become empty. Note that
// cherry-pick and rebase treat these two cases separately – commits that start as empty versus commits that become
// empty while being rebased or cherry-picked.
type EmptyCommitHandling int

const (
	// ErrorOnEmptyCommit instructs a cherry-pick or rebase operation to fail with an error when an empty commit
	// is encountered.
	ErrorOnEmptyCommit = iota

	// DropEmptyCommit instructs a cherry-pick or rebase operation to drop empty commits and to not create new
	// commits for them.
	DropEmptyCommit

	// KeepEmptyCommit instructs a cherry-pick or rebase operation to keep empty commits.
	KeepEmptyCommit

	// StopOnEmptyCommit instructs a cherry-pick or rebase operation to stop and let the user take additional action
	// to decide how to handle an empty commit.
	StopOnEmptyCommit
)

// RebaseState tracks the state of an in-progress rebase action. It records the name of the branch being rebased, the
// commit onto which the new commits will be rebased, and the root value of the previous working set, which is used if
// the rebase is aborted and the working set needs to be restored to its previous state.
type RebaseState struct {
	preRebaseWorking RootValue
	ontoCommit       *Commit
	branch           string

	// commitBecomesEmptyHandling specifies how to handle a commit that contains changes, but when cherry-picked,
	// results in no changes being applied.
	commitBecomesEmptyHandling EmptyCommitHandling

	// emptyCommitHandling specifies how to handle empty commits that contain no changes.
	emptyCommitHandling EmptyCommitHandling

	// lastAttemptedStep records the last rebase plan step that was attempted, whether it completed successfully, or
	// resulted in conflicts for the user to manually resolve. This field is not valid unless rebasingStarted is set
	// to true.
	lastAttemptedStep float32

	// rebasingStarted is true once the rebase plan has been started to execute. Once rebasingStarted is true, the
	// value in lastAttemptedStep has been initialized and is valid to read.
	rebasingStarted bool
}

// Branch returns the name of the branch being actively rebased. This is the branch that will be updated to point
// at the new commits created by the rebase operation.
func (rs RebaseState) Branch() string {
	return rs.branch
}

// OntoCommit returns the commit onto which new commits are being rebased by the active rebase operation.
func (rs RebaseState) OntoCommit() *Commit {
	return rs.ontoCommit
}

// PreRebaseWorkingRoot stores the RootValue of the working set immediately before the current rebase operation was
// started. This value is used when a rebase is aborted, so that the working set can be restored to its previous state.
func (rs RebaseState) PreRebaseWorkingRoot() RootValue {
	return rs.preRebaseWorking
}

func (rs RebaseState) EmptyCommitHandling() EmptyCommitHandling {
	return rs.emptyCommitHandling
}

func (rs RebaseState) CommitBecomesEmptyHandling() EmptyCommitHandling {
	return rs.commitBecomesEmptyHandling
}

func (rs RebaseState) LastAttemptedStep() float32 {
	return rs.lastAttemptedStep
}

func (rs RebaseState) WithLastAttemptedStep(step float32) *RebaseState {
	rs.lastAttemptedStep = step
	return &rs
}

func (rs RebaseState) RebasingStarted() bool {
	return rs.rebasingStarted
}

func (rs RebaseState) WithRebasingStarted(rebasingStarted bool) *RebaseState {
	rs.rebasingStarted = rebasingStarted
	return &rs
}

type MergeState struct {
	// the source commit
	commit *Commit
	// the spec string that was used to specify |commit|
	commitSpecStr    string
	preMergeWorking  RootValue
	unmergableTables []TableName
	mergedTables     []TableName
	// isCherryPick is set to true when the in-progress merge is a cherry-pick. This is needed so that
	// commit knows to NOT create a commit with multiple parents when creating a commit for a cherry-pick.
	isCherryPick bool
}

// todo(andy): this might make more sense in pkg merge
type SchemaConflict struct {
	ToSch, FromSch    schema.Schema
	ToFks, FromFks    []ForeignKey
	ToParentSchemas   map[TableName]schema.Schema
	FromParentSchemas map[TableName]schema.Schema
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
func MergeStateFromCommitAndWorking(commit *Commit, preMergeWorking RootValue) *MergeState {
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

func (m MergeState) PreMergeWorkingRoot() RootValue {
	return m.preMergeWorking
}

type SchemaConflictFn func(table TableName, conflict SchemaConflict) error

func (m MergeState) HasSchemaConflicts() bool {
	return len(m.unmergableTables) > 0
}

func (m MergeState) TablesWithSchemaConflicts() []TableName {
	return m.unmergableTables
}

func (m MergeState) MergedTables() []TableName {
	return m.mergedTables
}

func (m MergeState) IterSchemaConflicts(ctx context.Context, ddb *DoltDB, cb SchemaConflictFn) (err error) {
	var to, from RootValue

	to = m.preMergeWorking
	if from, err = m.commit.GetRootValue(ctx); err != nil {
		return err
	}

	toFKs, err := to.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	toSchemas, err := GetAllSchemas(ctx, to)
	if err != nil {
		return err
	}

	fromFKs, err := from.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	fromSchemas, err := GetAllSchemas(ctx, from)
	if err != nil {
		return err
	}

	for _, name := range m.unmergableTables {
		var sc SchemaConflict
		var hasToTable bool
		if sc.toTbl, hasToTable, err = to.GetTable(ctx, name); err != nil {
			return err
		}
		if hasToTable {
			if sc.ToSch, err = sc.toTbl.GetSchema(ctx); err != nil {
				return err
			}
		}

		var hasFromTable bool
		// todo: handle schema conflicts for renamed tables
		if sc.fromTbl, hasFromTable, err = from.GetTable(ctx, name); err != nil {
			return err
		}
		if hasFromTable {
			if sc.FromSch, err = sc.fromTbl.GetSchema(ctx); err != nil {
				return err
			}
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
	workingRoot RootValue
	stagedRoot  RootValue
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

func (ws WorkingSet) WithStagedRoot(stagedRoot RootValue) *WorkingSet {
	ws.stagedRoot = stagedRoot
	return &ws
}

func (ws WorkingSet) WithWorkingRoot(workingRoot RootValue) *WorkingSet {
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

func (ws WorkingSet) WithUnmergableTables(tables []TableName) *WorkingSet {
	ws.mergeState.unmergableTables = tables
	return &ws
}

func (ws WorkingSet) WithMergedTables(tables []TableName) *WorkingSet {
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

// StartRebase adds rebase tracking metadata to a new working set instance and returns it. Callers must then persist
// the returned working set in a session in order for the new working set to be recorded. |ontoCommit| specifies the
// commit that serves as the base commit for the new commits that will be created by the rebase process, |branch| is
// the branch that is being rebased, and |previousRoot| is root value of the branch being rebased. The HEAD and STAGED
// root values of the branch being rebased must match |previousRoot|; WORKING may be a different root value, but ONLY
// if it contains only ignored tables.
func (ws WorkingSet) StartRebase(ctx *sql.Context, ontoCommit *Commit, branch string, previousRoot RootValue, commitBecomesEmptyHandling EmptyCommitHandling, emptyCommitHandling EmptyCommitHandling) (*WorkingSet, error) {
	ws.rebaseState = &RebaseState{
		ontoCommit:                 ontoCommit,
		preRebaseWorking:           previousRoot,
		branch:                     branch,
		commitBecomesEmptyHandling: commitBecomesEmptyHandling,
		emptyCommitHandling:        emptyCommitHandling,
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

func (ws *WorkingSet) WorkingRoot() RootValue {
	return ws.workingRoot
}

func (ws *WorkingSet) StagedRoot() RootValue {
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
	workingRoot, err := NewRootValue(ctx, vrw, ns, workingRootVal)
	if err != nil {
		return nil, err
	}

	var stagedRoot RootValue
	if dsws.StagedAddr != nil {
		stagedRootVal, err := vrw.ReadValue(ctx, *dsws.StagedAddr)
		if err != nil {
			return nil, err
		}

		stagedRoot, err = NewRootValue(ctx, vrw, ns, stagedRootVal)
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

		if fromDCommit.IsGhost() {
			return nil, ErrGhostCommitEncountered
		}

		commit, err := NewCommit(ctx, vrw, ns, fromDCommit)
		if err != nil {
			return nil, err
		}

		preMergeWorkingV, err := vrw.ReadValue(ctx, preMergeWorkingAddr)
		if err != nil {
			return nil, err
		}

		preMergeWorkingRoot, err := NewRootValue(ctx, vrw, ns, preMergeWorkingV)
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

		unmergableTableNames := ToTableNames(unmergableTables, DefaultSchemaName)

		mergeState = &MergeState{
			commit:           commit,
			commitSpecStr:    commitSpec,
			preMergeWorking:  preMergeWorkingRoot,
			unmergableTables: unmergableTableNames,
			isCherryPick:     isCherryPick,
		}
	}

	var rebaseState *RebaseState
	if dsws.RebaseState != nil {
		preRebaseWorkingAddr := dsws.RebaseState.PreRebaseWorkingAddr()
		preRebaseWorkingV, err := vrw.ReadValue(ctx, preRebaseWorkingAddr)
		if err != nil {
			return nil, err
		}

		preRebaseWorkingRoot, err := NewRootValue(ctx, vrw, ns, preRebaseWorkingV)
		if err != nil {
			return nil, err
		}

		datasOntoCommit, err := dsws.RebaseState.OntoCommit(ctx, vrw)
		if err != nil {
			return nil, err
		}

		if datasOntoCommit.IsGhost() {
			return nil, ErrGhostCommitEncountered
		}

		ontoCommit, err := NewCommit(ctx, vrw, ns, datasOntoCommit)
		if err != nil {
			return nil, err
		}

		rebaseState = &RebaseState{
			preRebaseWorking:           preRebaseWorkingRoot,
			ontoCommit:                 ontoCommit,
			branch:                     dsws.RebaseState.Branch(ctx),
			commitBecomesEmptyHandling: EmptyCommitHandling(dsws.RebaseState.CommitBecomesEmptyHandling(ctx)),
			emptyCommitHandling:        EmptyCommitHandling(dsws.RebaseState.EmptyCommitHandling(ctx)),
			lastAttemptedStep:          dsws.RebaseState.LastAttemptedStep(ctx),
			rebasingStarted:            dsws.RebaseState.RebasingStarted(ctx),
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
func (ws *WorkingSet) ResolveRootValue(context.Context) (RootValue, error) {
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

// writeValues write the values in this working set to the database and returns a datas.WorkingSetSpec with the
// new values in it.
func (ws *WorkingSet) writeValues(ctx context.Context, db *DoltDB, meta *datas.WorkingSetMeta) (spec *datas.WorkingSetSpec, err error) {
	if ws.stagedRoot == nil || ws.workingRoot == nil {
		return nil, fmt.Errorf("StagedRoot and workingRoot must be set. This is a bug.")
	}

	r, workingRoot, err := db.writeRootValue(ctx, ws.workingRoot)
	if err != nil {
		return nil, err
	}
	ws.workingRoot = r

	r, stagedRoot, err := db.writeRootValue(ctx, ws.stagedRoot)
	if err != nil {
		return nil, err
	}
	ws.stagedRoot = r

	var mergeState *datas.MergeState
	if ws.mergeState != nil {
		r, preMergeWorking, err := db.writeRootValue(ctx, ws.mergeState.preMergeWorking)
		if err != nil {
			return nil, err
		}
		ws.mergeState.preMergeWorking = r

		h, err := ws.mergeState.commit.HashOf()
		if err != nil {
			return nil, err
		}
		dCommit, err := datas.LoadCommitAddr(ctx, db.vrw, h)
		if err != nil {
			return nil, err
		}

		// TODO: Serialize the full TableName
		mergeState, err = datas.NewMergeState(ctx, db.vrw, preMergeWorking, dCommit, ws.mergeState.commitSpecStr, FlattenTableNames(ws.mergeState.unmergableTables), ws.mergeState.isCherryPick)
		if err != nil {
			return nil, err
		}
	}

	var rebaseState *datas.RebaseState
	if ws.rebaseState != nil {
		r, preRebaseWorking, err := db.writeRootValue(ctx, ws.rebaseState.preRebaseWorking)
		if err != nil {
			return nil, err
		}
		ws.rebaseState.preRebaseWorking = r

		h, err := ws.rebaseState.ontoCommit.HashOf()
		if err != nil {
			return nil, err
		}
		dCommit, err := datas.LoadCommitAddr(ctx, db.vrw, h)
		if err != nil {
			return nil, err
		}

		rebaseState = datas.NewRebaseState(preRebaseWorking.TargetHash(), dCommit.Addr(), ws.rebaseState.branch,
			uint8(ws.rebaseState.commitBecomesEmptyHandling), uint8(ws.rebaseState.emptyCommitHandling),
			ws.rebaseState.lastAttemptedStep, ws.rebaseState.rebasingStarted)
	}

	return &datas.WorkingSetSpec{
		Meta:        meta,
		WorkingRoot: workingRoot,
		StagedRoot:  stagedRoot,
		MergeState:  mergeState,
		RebaseState: rebaseState,
	}, nil
}
