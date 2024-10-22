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
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

// RevisionDatabase allows callers to query a revision database for the commit, branch, or tag it is pinned to. For
// example, when using a database with a branch revision specification, that database is only able to use that branch
// and cannot change branch heads. Calling `Revision` on that database will return the branch name. Similarly, for
// databases using a commit revision spec or a tag revision spec, Revision will return the commit or tag, respectively.
// Currently, only explicit branch names, commit hashes, and tag names are allowed as database revision specs. Other
// refspecs, such as "HEAD~2" are not supported yet.
type RevisionDatabase interface {
	// Revision returns the specific branch, commit, or tag to which this revision database has been pinned. Other
	// revision specifications (e.g. "HEAD~2") are not supported. If a database implements RevisionDatabase, but
	// is not pinned to a specific revision, the empty string is returned.
	Revision() string
	// RevisionType returns the type of revision this database is pinned to.
	RevisionType() RevisionType
	// RevisionQualifiedName returns the fully qualified name of the database, which includes the revision if one is
	// specified.
	RevisionQualifiedName() string
	// RequestedName returns the name of the database as requested by the user when the name was resolved to this
	// database.
	RequestedName() string
	// Versioned returns whether this database implementation supports more than a single revision.
	// TODO: This shouldn't be a necessary part of the interface, but it's required to differentiate between dolt-backed
	//  databases and others that we serve for custom purposes with similar pieces of functionality, and the session
	//  management logic intermixes these concerns.
	Versioned() bool
}

// RevisionType represents the type of revision a database is pinned to. For branches and tags, the revision is a
// string naming that branch or tag. For other revision specs, e.g. "HEAD~2", the revision is a commit hash.
type RevisionType int

const (
	RevisionTypeNone RevisionType = iota
	RevisionTypeBranch
	RevisionTypeTag
	RevisionTypeCommit
)

// RemoteReadReplicaDatabase is a database that pulls from a connected remote when a transaction begins.
type RemoteReadReplicaDatabase interface {
	// ValidReplicaState returns whether this read replica is in a valid state to pull from the remote
	ValidReplicaState(ctx *sql.Context) bool
	// PullFromRemote performs a pull from the remote and returns any error encountered
	PullFromRemote(ctx *sql.Context) error
}

type DoltDatabaseProvider interface {
	sql.MutableDatabaseProvider
	// FileSystem returns the filesystem used by this provider, rooted at the data directory for all databases.
	FileSystem() filesys.Filesys
	DbFactoryUrl() string
	// FileSystemForDatabase returns a filesystem, with the working directory set to the root directory
	// of the requested database. If the requested database isn't found, a database not found error
	// is returned.
	FileSystemForDatabase(dbname string) (filesys.Filesys, error)
	// GetRemoteDB returns the remote database for given env.Remote object using the local database's vrw, and
	// withCaching defines whether the remoteDB gets cached or not.
	// This function replaces env.Remote's GetRemoteDB method during SQL session to access dialer in order
	// to get remote database associated to the env.Remote object.
	GetRemoteDB(ctx context.Context, format *types.NomsBinFormat, r env.Remote, withCaching bool) (*doltdb.DoltDB, error)
	// CloneDatabaseFromRemote clones the database from the specified remoteURL as a new database in this provider.
	// dbName is the name for the new database, branch is an optional parameter indicating which branch to clone
	// (otherwise all branches are cloned), remoteName is the name for the remote created in the new database, and
	// remoteUrl is a URL (e.g. "file:///dbs/db1") or an <org>/<database> path indicating a database hosted on DoltHub.
	CloneDatabaseFromRemote(ctx *sql.Context, dbName, branch, remoteName, remoteUrl string, depth int, remoteParams map[string]string) error
	// SessionDatabase returns the SessionDatabase for the specified database, which may name a revision of a base
	// database.
	SessionDatabase(ctx *sql.Context, dbName string) (SqlDatabase, bool, error)
	// BaseDatabase returns the base database for the specified database name. Meant for informational purposes when
	// managing the session initialization only. Use SessionDatabase for normal database retrieval.
	BaseDatabase(ctx *sql.Context, dbName string) (SqlDatabase, bool)
	// DoltDatabases returns all databases known to this provider.
	DoltDatabases() []SqlDatabase
	// UndropDatabase attempts to restore the database |dbName| that was previously dropped.
	// The restored database will appear identically when accessed through the SQL
	// interface, but may be stored in a slightly different location on disk
	// (e.g. a root database will be restored as a regular/non-root database,
	// databases original stored with hyphens in their directory name will be rewritten
	// to underscores to match their SQL database name).
	// If the database is unable to be restored, an error is returned explaining why.
	UndropDatabase(ctx *sql.Context, dbName string) error
	// ListDroppedDatabases returns a list of the database names for dropped databases that are still
	// available on disk and can be restored with dolt_undrop().
	ListDroppedDatabases(ctx *sql.Context) ([]string, error)
	// PurgeDroppedDatabases permanently deletes any dropped databases that are being held in temporary storage
	// in case they need to be restored. This operation is not reversible, so use with caution!
	PurgeDroppedDatabases(ctx *sql.Context) error
}

type SessionDatabaseBranchSpec struct {
	RepoState env.RepoStateReadWriter
	Branch    string
}

type SqlDatabase interface {
	sql.Database
	sql.SchemaDatabase
	sql.DatabaseSchema
	SessionDatabase
	RevisionDatabase

	// WithBranchRevision returns a copy of this database with the revision set to the given branch revision, and the
	// database name set to the given name.
	WithBranchRevision(requestedName string, branchSpec SessionDatabaseBranchSpec) (SqlDatabase, error)

	// TODO: get rid of this, it's managed by the session, not the DB
	GetRoot(*sql.Context) (doltdb.RootValue, error)
	// TODO: remove ddb from the below, it's separable and is 95% of the uses of this method
	DbData() env.DbData
	// DoltDatabases returns all underlying DoltDBs for this database.
	DoltDatabases() []*doltdb.DoltDB
	// Schema returns the schema of the database.
	Schema() string
}
