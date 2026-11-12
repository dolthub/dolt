// Copyright 2020 Dolthub, Inc.
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

package dsess

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
	"github.com/dolthub/dolt/go/libraries/utils/dynassert"
)

// InitialDbState is the initial state of a database, as returned by SessionDatabase.InitialDBState. It is used to
// establish the in memory state of the session for every new transaction.
type InitialDbState struct {
	DbData env.DbData[*sql.Context]
	Db     sql.Database
	// HeadRoot is the root value for databases without a HeadCommit. Nil for databases with a HeadCommit.
	HeadRoot doltdb.RootValue
	// If err is set, this InitialDbState is partially invalid, but may be
	// usable to initialize a database at a revision specifier, for
	// example. Adding this InitialDbState to a session will return this
	// error.
	Err error
	// WorkingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case HeadCommit must be set
	WorkingSet *doltdb.WorkingSet
	// The head commit for this database. May be nil for databases tied to a detached root value, in which case
	// RootValue must be set.
	HeadCommit *doltdb.Commit
	Remotes    *concurrentmap.Map[string, env.Remote]
	Branches   *concurrentmap.Map[string, env.BranchConfig]
	Backups    *concurrentmap.Map[string, env.Remote]
	ReadOnly   bool
}

// SessionDatabase is a database that can be managed by a dsess.Session. It has methods to return its initial state in
// order for the session to manage it.
type SessionDatabase interface {
	sql.Database
	InitialDBState(ctx *sql.Context) (InitialDbState, error)
}

// DatabaseSessionState is the state of a database that lasts for the whole session. The branch states loaded in the
// current transaction are in the session's branchStates.
type DatabaseSessionState struct {
	// globalState is the global state of this session (shared by all sessions for a particular db)
	globalState globalstate.GlobalState
	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
	// headCache records the session-caches for every branch head accessed by the session
	// This is managed separately from the branch states themselves because it persists across transactions (which is
	// safe because it's keyed by immutable hashes)
	headCache map[doltdb.RevisionKey]*SessionCache
	// dbName is the name of the database this state applies to. This is always the base name of the database, without
	// a revision qualifier.
	dbName string
	// checkedOutRevSpec is the revision of the database when referred to by its base name. Changes only when a
	// `dolt_checkout` occurs.
	checkedOutRevSpec string
	// tmpFileDir is the directory to use for temporary files for this database
	tmpFileDir string
	// ddb is the provider's DoltDB for this database, shared by every session and revision. Dropping the database
	// removes this state from every session, so it can't outlive the DoltDB.
	ddb *doltdb.DoltDB
}

func newEmptyDatabaseSessionState() *DatabaseSessionState {
	return &DatabaseSessionState{
		headCache: make(map[doltdb.RevisionKey]*SessionCache),
	}
}

// SessionState is the public interface for dealing with session state outside this package. Session-state is always
// branch-specific.
type SessionState interface {
	// WorkingSet returns the working set, or nil for a detached head, with any pending edits materialized into it.
	WorkingSet(ctx *sql.Context) (*doltdb.WorkingSet, error)
	WorkingRoot(ctx *sql.Context) (doltdb.RootValue, error)
	// UnmaterializedWorkingRoot returns the working root without materializing pending edits. Its tables and schemas
	// are current, but the rows of a table with pending edits are not; read those through the WriteSession's
	// PendingRows. Only reads that resolve tables and schemas use it.
	UnmaterializedWorkingRoot() doltdb.RootValue
	WriteSession() WriteSession
	EditOpts() editor.Options
	SessionCache() *SessionCache
}

// branchState records all the in-memory session state for a particular branch head
type branchState struct {
	// dbData is an accessor for the underlying doltDb
	dbData env.DbData[*sql.Context]
	// HeadRoot is the root value for databases without a headCommit. Nil for databases with a headCommit.
	headRoot     doltdb.RootValue
	writeSession WriteSession
	// dbState is the session-wide state of this branch state's database
	dbState *DatabaseSessionState
	// headCommit is the head commit for this database. May be nil for databases tied to a detached root value, in which
	// case headRoot must be set.
	headCommit *doltdb.Commit
	// workingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case headCommit must be set. It doesn't include the write session's pending edits. Read it through WorkingSet,
	// which materializes them; only code that sets it, and GC, reads the field.
	workingSet *doltdb.WorkingSet
	// head is the name of the branch head for this state
	head string
	// revisionType is the type of revision this branchState tracks
	revisionType RevisionType
	// readOnly is true if this database is read only
	readOnly bool
	// dirty is true if this branch state has uncommitted changes
	dirty bool
}

// ErrOpenTableWriters is returned when a branch state would be dropped while its write session has open table writers.
var ErrOpenTableWriters = errors.New("cannot drop session state with open table writers")

// checkNoOpenWriters returns an error if this branch state's write session has open table writers. Those writers
// flush through the write session into this branch state, so it must outlive them. Panics when dynamic asserts are
// enabled.
func (bs *branchState) checkNoOpenWriters() error {
	if bs.writeSession == nil || !bs.writeSession.HasOpenTableWriters() {
		return nil
	}
	err := fmt.Errorf("%w: %s", ErrOpenTableWriters, bs.RevisionDbName())
	dynassert.Assert(false, "%s", err.Error())
	return err
}

// replaceWorkingSet sets this branch state's working set from outside its write session, and rebuilds the write
// session's table writers if the working root changed.
func (bs *branchState) replaceWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	prev := bs.workingSet
	bs.workingSet = ws
	if bs.writeSession == nil || (prev != nil && rootsEqual(prev.WorkingRoot(), ws.WorkingRoot())) {
		return nil
	}
	return bs.writeSession.ResetTableWriters(ctx)
}

// branchWorkingSetOwner is the WorkingSetOwner of a branch state's own write session.
type branchWorkingSetOwner struct {
	sess *DoltSession
	bs   *branchState
}

var _ WorkingSetOwner = branchWorkingSetOwner{}

func (o branchWorkingSetOwner) WorkingSet() *doltdb.WorkingSet {
	return o.bs.workingSet
}

func (o branchWorkingSetOwner) SetFlushedWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	return o.sess.setFlushedWorkingSet(ctx, o.bs, ws)
}

// forkedWorkingSetOwner is the WorkingSetOwner returned by DoltSession.ForkWorkingSet.
type forkedWorkingSetOwner struct {
	sess          *DoltSession
	bs            *branchState
	ws            *doltdb.WorkingSet
	targetStaging bool
	// base is the root of |bs| that the next flush replaces, as of the fork's creation or its last flush.
	base doltdb.RootValue
}

// target returns the root of |roots| that this fork's flushes replace.
func (o *forkedWorkingSetOwner) target(roots doltdb.Roots) doltdb.RootValue {
	if o.targetStaging {
		return roots.Staged
	}
	return roots.Working
}

var _ WorkingSetOwner = (*forkedWorkingSetOwner)(nil)

func (o *forkedWorkingSetOwner) WorkingSet() *doltdb.WorkingSet {
	return o.ws
}

// SetFlushedWorkingSet implements WorkingSetOwner. It refuses to flush into a branch state that has been dropped,
// which would lose the writes, or whose root has changed since |base|, which the flush would revert. Panics when
// dynamic asserts are enabled.
func (o *forkedWorkingSetOwner) SetFlushedWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	o.ws = ws
	if !o.sess.isCurrentBranchState(o.bs) {
		err := fmt.Errorf("%w: %s", ErrBranchStateDropped, o.bs.RevisionDbName())
		dynassert.Assert(false, "%s", err.Error())
		return err
	}
	// Edits the branch state's write session left pending since the fork count as a change.
	roots, err := o.bs.roots(ctx)
	if err != nil {
		return err
	}
	if !rootsEqual(o.target(roots), o.base) {
		err = fmt.Errorf("%w: %s", ErrBranchStateChanged, o.bs.RevisionDbName())
	}
	if err != nil {
		dynassert.Assert(false, "%s", err.Error())
		return err
	}

	root := o.target(doltdb.Roots{Working: ws.WorkingRoot(), Staged: ws.StagedRoot()})
	if o.targetStaging {
		err = o.sess.setStagingRootOf(ctx, o.bs, root)
	} else {
		err = o.sess.setWorkingRootOf(ctx, o.bs, root)
	}
	if err != nil {
		return err
	}
	o.base = root
	return nil
}

// ErrBranchStateDropped is returned when a write session flushes into a branch state that has been dropped.
var ErrBranchStateDropped = errors.New("cannot flush into dropped session state")

// ErrBranchStateChanged is returned when a forked write session flushes into a branch state whose root something else
// has changed since the fork was created or last flushed.
var ErrBranchStateChanged = errors.New("cannot flush a forked working set into session state that changed after the fork")

// RevisionDbName returns the revision-qualified database name for this branch state
func (bs *branchState) RevisionDbName() string {
	return doltdb.RevisionDbName(bs.dbState.dbName, bs.head)
}

func (bs *branchState) name() doltdb.QualifiedDbName {
	return doltdb.NewQualifiedDbName(bs.dbState.dbName, bs.head)
}

func (bs *branchState) key() doltdb.QualifiedDbKey {
	return bs.name().Key()
}

var _ SessionState = (*branchState)(nil)

// WorkingSet returns this branch state's working set, or nil for a detached head, with its write session's pending
// edits materialized into it. Everything that reads the working set's roots, or derives a new working set from it,
// goes through WorkingSet, WorkingRoot or roots.
func (bs *branchState) WorkingSet(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	if err := bs.materializePending(ctx); err != nil {
		return nil, err
	}
	return bs.workingSet, nil
}

// materializePending materializes the edits pending in this branch state's write session into its working set.
func (bs *branchState) materializePending(ctx *sql.Context) error {
	if bs.writeSession == nil || bs.workingSet == nil || !bs.writeSession.HasPendingEdits() {
		return nil
	}
	return bs.writeSession.MaterializePending(ctx)
}

// UnmaterializedWorkingRoot implements SessionState.
func (bs *branchState) UnmaterializedWorkingRoot() doltdb.RootValue {
	return bs.storedRoots().Working
}

func (bs *branchState) WorkingRoot(ctx *sql.Context) (doltdb.RootValue, error) {
	roots, err := bs.roots(ctx)
	if err != nil {
		return nil, err
	}
	return roots.Working, nil
}

// roots returns this branch state's roots. A detached head's working and staged roots are its head root.
func (bs *branchState) roots(ctx *sql.Context) (doltdb.Roots, error) {
	ws, err := bs.WorkingSet(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}
	return bs.rootsOf(ws), nil
}

// storedRoots returns the roots of this branch state's working set as last set, without going through WorkingSet. Only
// the code that sets the working set, and code that reports what's set (GC roots, session variables), uses it.
func (bs *branchState) storedRoots() doltdb.Roots {
	return bs.rootsOf(bs.workingSet)
}

// isDirty returns true if this branch state has uncommitted changes, including edits that have not been
// materialized into its working set.
func (bs *branchState) isDirty() bool {
	return bs.dirty || (bs.writeSession != nil && bs.writeSession.HasPendingEdits())
}

func (bs *branchState) rootsOf(ws *doltdb.WorkingSet) doltdb.Roots {
	if ws == nil {
		return doltdb.Roots{
			Head:    bs.headRoot,
			Working: bs.headRoot,
			Staged:  bs.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    bs.headRoot,
		Working: ws.WorkingRoot(),
		Staged:  ws.StagedRoot(),
	}
}

// workingSetRef returns the ref of this branch state's working set, and false for a detached head. Edits to tables don't
// change it, so it doesn't go through WorkingSet.
func (bs *branchState) workingSetRef() (ref.WorkingSetRef, bool) {
	if bs.workingSet == nil {
		return ref.WorkingSetRef{}, false
	}
	return bs.workingSet.Ref(), true
}

func (bs *branchState) WriteSession() WriteSession {
	return bs.writeSession
}

func (bs *branchState) SessionCache() *SessionCache {
	return bs.dbState.headCache[doltdb.NewRevisionKey(bs.head)]
}

func (bs branchState) EditOpts() editor.Options {
	if bs.writeSession == nil {
		return editor.Options{}
	}
	return bs.WriteSession().GetOptions()
}
