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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

// InitialDbState is the initial state of a database, as returned by SessionDatabase.InitialDBState. It is used to
// establish the in memory state of the session for every new transaction.
type InitialDbState struct {
	Db sql.Database
	// WorkingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case HeadCommit must be set
	WorkingSet *doltdb.WorkingSet
	// The head commit for this database. May be nil for databases tied to a detached root value, in which case
	// RootValue must be set.
	HeadCommit *doltdb.Commit
	// HeadRoot is the root value for databases without a HeadCommit. Nil for databases with a HeadCommit.
	HeadRoot doltdb.RootValue
	ReadOnly bool
	DbData   env.DbData
	Remotes  *concurrentmap.Map[string, env.Remote]
	Branches *concurrentmap.Map[string, env.BranchConfig]
	Backups  *concurrentmap.Map[string, env.Remote]

	// If err is set, this InitialDbState is partially invalid, but may be
	// usable to initialize a database at a revision specifier, for
	// example. Adding this InitialDbState to a session will return this
	// error.
	Err error
}

// SessionDatabase is a database that can be managed by a dsess.Session. It has methods to return its initial state in
// order for the session to manage it.
type SessionDatabase interface {
	sql.Database
	InitialDBState(ctx *sql.Context) (InitialDbState, error)
}

// DatabaseSessionState is the set of all information for a given database in this session.
type DatabaseSessionState struct {
	// dbName is the name of the database this state applies to. This is always the base name of the database, without
	// a revision qualifier.
	dbName string
	// checkedOutRevSpec is the revision of the database when referred to by its base name. Changes only when a
	// `dolt_checkout` occurs.
	checkedOutRevSpec string
	// heads records the in-memory DB state for every branch head accessed by the session
	heads map[string]*branchState
	// headCache records the session-caches for every branch head accessed by the session
	// This is managed separately from the branch states themselves because it persists across transactions (which is
	// safe because it's keyed by immutable hashes)
	headCache map[string]*SessionCache
	// globalState is the global state of this session (shared by all sessions for a particular db)
	globalState globalstate.GlobalState
	// tmpFileDir is the directory to use for temporary files for this database
	tmpFileDir string

	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
}

func newEmptyDatabaseSessionState() *DatabaseSessionState {
	return &DatabaseSessionState{
		heads:     make(map[string]*branchState),
		headCache: make(map[string]*SessionCache),
	}
}

// SessionState is the public interface for dealing with session state outside this package. Session-state is always
// branch-specific.
type SessionState interface {
	WorkingSet() *doltdb.WorkingSet
	WorkingRoot() doltdb.RootValue
	WriteSession() WriteSession
	EditOpts() editor.Options
	SessionCache() *SessionCache
}

// branchState records all the in-memory session state for a particular branch head
type branchState struct {
	// dbState is the parent database state for this branch head state
	dbState *DatabaseSessionState
	// head is the name of the branch head for this state
	head string
	// revisionType is the type of revision this branchState tracks
	revisionType RevisionType
	// headCommit is the head commit for this database. May be nil for databases tied to a detached root value, in which
	// case headRoot must be set.
	headCommit *doltdb.Commit
	// HeadRoot is the root value for databases without a headCommit. Nil for databases with a headCommit.
	headRoot doltdb.RootValue
	// workingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case headCommit must be set
	workingSet *doltdb.WorkingSet
	// dbData is an accessor for the underlying doltDb
	dbData env.DbData
	// writeSession is this head's write session
	writeSession WriteSession
	// readOnly is true if this database is read only
	readOnly bool
	// dirty is true if this branch state has uncommitted changes
	dirty bool
}

// NewEmptyBranchState creates a new branch state for the given head name with the head provided, adds it to the db
// state, and returns it. The state returned is empty except for its identifiers and must be filled in by the caller.
func (dbState *DatabaseSessionState) NewEmptyBranchState(head string, revisionType RevisionType) *branchState {
	b := &branchState{
		dbState:      dbState,
		head:         head,
		revisionType: revisionType,
	}

	lowerHead := strings.ToLower(head)
	dbState.heads[lowerHead] = b
	_, ok := dbState.headCache[lowerHead]
	if !ok {
		dbState.headCache[lowerHead] = newSessionCache()
	}

	return b
}

// RevisionDbName returns the revision-qualified database name for this branch state
func (bs *branchState) RevisionDbName() string {
	return RevisionDbName(bs.dbState.dbName, bs.head)
}

func (bs *branchState) WorkingRoot() doltdb.RootValue {
	return bs.roots().Working
}

var _ SessionState = (*branchState)(nil)

func (bs *branchState) WorkingSet() *doltdb.WorkingSet {
	return bs.workingSet
}

func (bs *branchState) WriteSession() WriteSession {
	return bs.writeSession
}

func (bs *branchState) SessionCache() *SessionCache {
	return bs.dbState.headCache[strings.ToLower(bs.head)]
}

func (bs branchState) EditOpts() editor.Options {
	if bs.writeSession == nil {
		return editor.Options{}
	}
	return bs.WriteSession().GetOptions()
}

func (bs *branchState) roots() doltdb.Roots {
	if bs.WorkingSet() == nil {
		return doltdb.Roots{
			Head:    bs.headRoot,
			Working: bs.headRoot,
			Staged:  bs.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    bs.headRoot,
		Working: bs.WorkingSet().WorkingRoot(),
		Staged:  bs.WorkingSet().StagedRoot(),
	}
}
