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

type InitialDbState struct {
	Db          sql.Database
	HeadCommit  *doltdb.Commit
	ReadOnly    bool
	WorkingSet  *doltdb.WorkingSet
	DbData      env.DbData
	ReadReplica *env.Remote
	Remotes     map[string]env.Remote
	Branches    map[string]env.BranchConfig

	// If err is set, this InitialDbState is partially invalid, but may be
	// usable to initialize a database at a revision specifier, for
	// example. Adding this InitialDbState to a session will return this
	// error.
	Err error
}

type DatabaseSessionState struct {
	dbName       string
	headCommit   *doltdb.Commit
	headRoot     *doltdb.RootValue
	WorkingSet   *doltdb.WorkingSet
	dbData       env.DbData
	WriteSession writer.WriteSession
	globalState  globalstate.GlobalState
	readOnly     bool
	dirty        bool
	readReplica  *env.Remote
	tmpFileDir   string

	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
}

func (d DatabaseSessionState) GetRoots() doltdb.Roots {
	if d.WorkingSet == nil {
		return doltdb.Roots{
			Head:    d.headRoot,
			Working: d.headRoot,
			Staged:  d.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.WorkingSet.WorkingRoot(),
		Staged:  d.WorkingSet.StagedRoot(),
	}
}

func (d DatabaseSessionState) EditOpts() editor.Options {
	return d.WriteSession.GetOptions()
}
