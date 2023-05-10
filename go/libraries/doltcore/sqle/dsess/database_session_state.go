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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
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
	HeadRoot    *doltdb.RootValue
	ReadOnly    bool
	DbData      env.DbData
	Remotes     map[string]env.Remote
	Branches    map[string]env.BranchConfig
	Backups     map[string]env.Remote

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
	InitialDBState(ctx *sql.Context, branch string) (InitialDbState, error)
}

// DatabaseSessionState is the set of all information for a given database in this session.
type DatabaseSessionState struct {
	// dbName is the name of the database this state applies to. This includes a revision specifier in some cases.
	dbName       string
	// db is the database this state applies to
	db           SqlDatabase
	// currRevSpec is the current revision spec of the database when referred to by its base name. Changes when a 
	// `dolt_checkout` or `use` statement is executed.
	currRevSpec string
	// currRevType is the current revision type of the database when referred to by its base name. Changes when a
	// `dolt_checkout` or `use` statement is executed.
	currRevType RevisionType
	// checkedOutRevSpec is the checked out revision specifier of the database. Changes only when a `dolt_checkout` 
	// occurs.
	checkedOutRevSpec string
	// heads records the in-memory DB state for every branch head accessed by the session
	heads 			map[string]*branchState
	// globalState is the global state of this session (shared by all sessions for a particular db)
	globalState  globalstate.GlobalState
	// dirty is true if this session has uncommitted changes
	dirty        bool
	// tmpFileDir is the directory to use for temporary files for this database
	tmpFileDir   string
	
	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
}

func NewEmptyDatabaseSessionState() *DatabaseSessionState {
	return &DatabaseSessionState{
		heads: make(map[string]*branchState),
	}
}

// SessionState is the public interface for dealing with session state outside this package. Session-state is always
// branch-specific. 
type SessionState interface {
	WorkingSet() *doltdb.WorkingSet
	WorkingRoot() *doltdb.RootValue
	WriteSession() writer.WriteSession
	EditOpts() editor.Options
	SessionCache() *SessionCache
}

// branchState records all the in-memory session state for a particular branch head
type branchState struct {
	// dbState is the parent database state for this branch head state
	dbState *DatabaseSessionState
	// headCommit is the head commit for this database. May be nil for databases tied to a detached root value, in which 
	// case headRoot must be set.
	headCommit   *doltdb.Commit
	// HeadRoot is the root value for databases without a headCommit. Nil for databases with a headCommit.
	headRoot     *doltdb.RootValue
	// workingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case headCommit must be set
	workingSet *doltdb.WorkingSet
	// dbData is an accessor for the underlying doltDb
	// TODO: move this to DatabaseSessionState only
	dbData       env.DbData
	// writeSession is this head's write session
	writeSession writer.WriteSession
	// readOnly is true if this database is read only
	readOnly     bool
	// sessionCache is a collection of cached values used to speed up performance
	sessionCache *SessionCache
}

func NewEmptyBranchState(dbState *DatabaseSessionState) *branchState {
	return &branchState{
		dbState: dbState,
		sessionCache: newSessionCache(),
	}
}

func (d *branchState) WorkingRoot() *doltdb.RootValue {
	return d.roots().Working
}

var _ SessionState = (*branchState)(nil)

func (d *branchState) WorkingSet() *doltdb.WorkingSet {
	return d.workingSet
}

func (d *branchState) WriteSession() writer.WriteSession {
	return d.writeSession
}

func (d *branchState) SessionCache() *SessionCache {
	return d.sessionCache
}

func (d branchState) EditOpts() editor.Options {
	return d.WriteSession().GetOptions()
}

func (d *branchState) roots() doltdb.Roots {
	if d.WorkingSet() == nil {
		return doltdb.Roots{
			Head:    d.headRoot,
			Working: d.headRoot,
			Staged:  d.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.WorkingSet().WorkingRoot(),
		Staged:  d.WorkingSet().StagedRoot(),
	}
}
