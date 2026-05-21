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

package dsess

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// VersionedDatabase is the data-layer surface a dsess.Session needs from
// a database. It contains no SQL-engine concepts (no sql.Database,
// sql.SchemaDatabase, sql.DatabaseSchema, or sql.AliasedDatabase
// embedding); implementations may be SQL-queryable (sqle.Database) or
// not (a mongo-compatible server's branch-backed document store).
//
// Compare with SqlDatabase, the composite that bundles this interface
// together with the SQL-engine surface for callers of the SQL engine.
type VersionedDatabase interface {
	// Name is the bare database name. Identity, not SQL.
	Name() string

	// RevisionDatabase is the revision coordinates (branch / commit /
	// tag) of this database.
	RevisionDatabase

	// InitialDBState returns the head commit and working set this
	// database is attached to. Called once when the session first
	// attaches the database.
	InitialDBState(ctx *sql.Context) (InitialDbState, error)

	// DbData provides access to the underlying DoltDB and its repo-state
	// reader/writer.
	DbData() env.DbData[*sql.Context]

	// DoltDatabases returns all underlying DoltDBs for this database.
	DoltDatabases() []*doltdb.DoltDB

	// GetTableResolver returns the table-name resolver used on the
	// write/commit path.
	GetTableResolver() doltdb.TableResolver

	// EditOptions returns the editor configuration used to build the
	// WriteSession for branch states.
	//
	// Promoted from a structural type assertion previously made inside
	// dsess.Session.addDB.
	EditOptions() editor.Options

	// GetGlobalState returns the shared per-database state (currently
	// only the auto-increment tracker).
	//
	// Promoted from globalstate.GlobalStateProvider, which was an
	// optional structural type assertion. Implementations that do not
	// maintain per-database global state (e.g. read-only detached-head
	// databases) may return globalstate.NoOp{}.
	GetGlobalState() globalstate.GlobalState
}
