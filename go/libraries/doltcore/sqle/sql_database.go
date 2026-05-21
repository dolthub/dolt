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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dsess"
)

// SqlDatabase is the composite interface for databases exposed through
// the SQL engine. It bundles dsess.VersionedDatabase (the data-layer
// surface dsess itself uses) with the go-mysql-server interfaces a
// SQL-queryable database must satisfy.
//
// Lives in the sqle package, not dsess, because the SQL-engine half
// of the composite (sql.Database, sql.SchemaDatabase, etc.) is a
// SQL-engine concern. dsess depends on go-mysql-server only for the
// *sql.Context type; it does not embed any of the database-shaped
// interfaces.
type SqlDatabase interface {
	dsess.VersionedDatabase

	sql.Database
	sql.SchemaDatabase
	sql.DatabaseSchema
	sql.AliasedDatabase

	// WithBranchRevision returns a copy of this database with the revision set to the given branch revision, and the
	// database name set to the given name.
	WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (SqlDatabase, error)

	// TODO: get rid of this, it's managed by the session, not the DB
	GetRoot(*sql.Context) (doltdb.RootValue, error)

	// Schema returns the schema of the database.
	Schema() string

	// Clean up any global resources associated with the
	// SqlDatabase itself.  For DoltDatabases, this notably does
	// not close the DoltDB, for example, but should shut down
	// background threads not managed through
	// sql.BackgroundThreads but which could be accessing or
	// mutating database state.
	Close()
}
