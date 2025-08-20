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
	DbData     env.DbData[*sql.Context]
	Db         sql.Database
	HeadRoot   doltdb.RootValue
	Err        error
	WorkingSet *doltdb.WorkingSet
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

// DatabaseSessionState is the set of all information for a given database in this session.
type DatabaseSessionState struct {
	globalState       globalstate.GlobalState
	Err               error
	heads             map[string]*branchState
	headCache         map[string]*SessionCache
	dbName            string
	checkedOutRevSpec string
	tmpFileDir        string
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
	dbData       env.DbData[*sql.Context]
	headRoot     doltdb.RootValue
	writeSession WriteSession
	dbState      *DatabaseSessionState
	headCommit   *doltdb.Commit
	workingSet   *doltdb.WorkingSet
	head         string
	revisionType RevisionType
	readOnly     bool
	dirty        bool
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
