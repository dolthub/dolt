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

package dsess

import (
	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ErrRevisionDbNotFound is thrown when a RevisionDatabaseProvider cannot find a specified revision database.
var ErrRevisionDbNotFound = errors.NewKind("revision database not found: '%s'")

// RevisionDatabaseProvider provides revision databases.
// In Dolt, commits and branches can be accessed as discrete databases
// using a Dolt-specific syntax: `my_database/my_branch`. Revision databases
// corresponding to historical commits in the repository will be read-only
// databases. Revision databases for branches will be read/write.
type RevisionDatabaseProvider interface {
	// RevisionDbState provides the InitialDbState for a revision database.
	RevisionDbState(ctx *sql.Context, revDB string) (InitialDbState, error)
	// DropRevisionDb removes the specified revision database from the databases this provider is tracking.
	DropRevisionDb(ctx *sql.Context, revDB string) error
}
type DoltDatabaseProvider interface {
	RevisionDatabaseProvider
	// FileSystem returns the filesystem used by this provider, rooted at the data directory for all databases
	FileSystem() filesys.Filesys
	// GetRemoteDB returns the remote database for given env.Remote object using the local database's vrw, and
	// withCaching defines whether the remoteDB gets cached or not.
	// This function replaces env.Remote's GetRemoteDB method during SQL session to access dialer in order
	// to get remote database associated to the env.Remote object.
	GetRemoteDB(ctx *sql.Context, srcDB *doltdb.DoltDB, r env.Remote, withCaching bool) (*doltdb.DoltDB, error)
	// CloneDatabaseFromRemote clones the database from the specified remoteURL as a new database in this provider.
	// dbName is the name for the new database, branch is an optional parameter indicating which branch to clone
	// (otherwise all branches are cloned), remoteName is the name for the remote created in the new database, and
	// remoteUrl is a URL (e.g. "file:///dbs/db1") or an <org>/<database> path indicating a database hosted on DoltHub.
	CloneDatabaseFromRemote(ctx *sql.Context, dbName, branch, remoteName, remoteUrl string, remoteParams map[string]string) error
}

func EmptyDatabaseProvider() DoltDatabaseProvider {
	return emptyRevisionDatabaseProvider{}
}

type emptyRevisionDatabaseProvider struct{}

func (e emptyRevisionDatabaseProvider) GetRemoteDB(ctx *sql.Context, srcDB *doltdb.DoltDB, r env.Remote, withCaching bool) (*doltdb.DoltDB, error) {
	return nil, nil
}

func (e emptyRevisionDatabaseProvider) FileSystem() filesys.Filesys {
	return nil
}

func (e emptyRevisionDatabaseProvider) CloneDatabaseFromRemote(ctx *sql.Context, dbName, branch, remoteName, remoteUrl string, remoteParams map[string]string) error {
	return nil
}

func (e emptyRevisionDatabaseProvider) DropRevisionDb(ctx *sql.Context, revDB string) error {
	return nil
}

func (e emptyRevisionDatabaseProvider) RevisionDbState(_ *sql.Context, revDB string) (InitialDbState, error) {
	return InitialDbState{}, sql.ErrDatabaseNotFound.New(revDB)
}
