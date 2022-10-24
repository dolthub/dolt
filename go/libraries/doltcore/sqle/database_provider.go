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

package sqle

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	dbRevisionDelimiter = "/"
)

type DoltDatabaseProvider struct {
	// dbLocations maps a database name to its file system root
	dbLocations        map[string]filesys.Filesys
	databases          map[string]sql.Database
	functions          map[string]sql.Function
	externalProcedures sql.ExternalStoredProcedureRegistry
	InitDatabaseHook   InitDatabaseHook
	mu                 *sync.RWMutex

	defaultBranch string
	fs            filesys.Filesys
	remoteDialer  dbfactory.GRPCDialProvider // TODO: why isn't this a method defined on the remote object

	dbFactoryUrl string
	isStandby    *bool
}

var _ sql.DatabaseProvider = (*DoltDatabaseProvider)(nil)
var _ sql.FunctionProvider = (*DoltDatabaseProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DoltDatabaseProvider)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DoltDatabaseProvider)(nil)
var _ sql.TableFunctionProvider = (*DoltDatabaseProvider)(nil)
var _ dsess.DoltDatabaseProvider = (*DoltDatabaseProvider)(nil)

// NewDoltDatabaseProvider returns a new provider, initialized without any databases, along with any
// errors that occurred while trying to create the database provider.
func NewDoltDatabaseProvider(defaultBranch string, fs filesys.Filesys) (DoltDatabaseProvider, error) {
	return NewDoltDatabaseProviderWithDatabases(defaultBranch, fs, nil, nil)
}

// NewDoltDatabaseProviderWithDatabase returns a new provider, initialized with one database at the
// specified location, and any error that occurred along the way.
func NewDoltDatabaseProviderWithDatabase(defaultBranch string, fs filesys.Filesys, database sql.Database, dbLocation filesys.Filesys) (DoltDatabaseProvider, error) {
	return NewDoltDatabaseProviderWithDatabases(defaultBranch, fs, []sql.Database{database}, []filesys.Filesys{dbLocation})
}

// NewDoltDatabaseProviderWithDatabases returns a new provider, initialized with the specified databases,
// at the specified locations. For every database specified, there must be a corresponding filesystem
// specified that represents where the database is located. If the number of specified databases is not the
// same as the number of specified locations, an error is returned.
func NewDoltDatabaseProviderWithDatabases(defaultBranch string, fs filesys.Filesys, databases []sql.Database, locations []filesys.Filesys) (DoltDatabaseProvider, error) {
	if len(databases) != len(locations) {
		return DoltDatabaseProvider{}, fmt.Errorf("unable to create DoltDatabaseProvider: "+
			"incorrect number of databases (%d) and database locations (%d) specified", len(databases), len(locations))
	}

	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[strings.ToLower(db.Name())] = db
	}

	dbLocations := make(map[string]filesys.Filesys, len(locations))
	for i, dbLocation := range locations {
		dbLocations[databases[i].Name()] = dbLocation
	}

	funcs := make(map[string]sql.Function, len(dfunctions.DoltFunctions))
	for _, fn := range dfunctions.DoltFunctions {
		funcs[strings.ToLower(fn.FunctionName())] = fn
	}

	externalProcedures := sql.NewExternalStoredProcedureRegistry()
	for _, esp := range dprocedures.DoltProcedures {
		externalProcedures.Register(esp)
	}

	return DoltDatabaseProvider{
		dbLocations:        dbLocations,
		databases:          dbs,
		functions:          funcs,
		externalProcedures: externalProcedures,
		mu:                 &sync.RWMutex{},
		fs:                 fs,
		defaultBranch:      defaultBranch,
		dbFactoryUrl:       doltdb.LocalDirDoltDB,
		InitDatabaseHook:   ConfigureReplicationDatabaseHook,
		isStandby:          new(bool),
	}, nil
}

// WithFunctions returns a copy of this provider with the functions given. Any previous functions are removed.
func (p DoltDatabaseProvider) WithFunctions(fns []sql.Function) DoltDatabaseProvider {
	funcs := make(map[string]sql.Function, len(dfunctions.DoltFunctions))
	for _, fn := range fns {
		funcs[strings.ToLower(fn.FunctionName())] = fn
	}

	p.functions = funcs
	return p
}

// WithDbFactoryUrl returns a copy of this provider with the DbFactoryUrl set as provided.
// The URL is used when creating new databases.
// See doltdb.InMemDoltDB, doltdb.LocalDirDoltDB
func (p DoltDatabaseProvider) WithDbFactoryUrl(url string) DoltDatabaseProvider {
	p.dbFactoryUrl = url
	return p
}

// WithRemoteDialer returns a copy of this provider with the dialer provided
func (p DoltDatabaseProvider) WithRemoteDialer(provider dbfactory.GRPCDialProvider) DoltDatabaseProvider {
	p.remoteDialer = provider
	return p
}

func (p DoltDatabaseProvider) FileSystem() filesys.Filesys {
	return p.fs
}

// If this DatabaseProvider is set to standby |true|, it returns every dolt
// database as a read only database. Set back to |false| to get read-write
// behavior from dolt databases again.
func (p DoltDatabaseProvider) SetIsStandby(standby bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*p.isStandby = standby
}

// FileSystemForDatabase returns a filesystem, with the working directory set to the root directory
// of the requested database. If the requested database isn't found, a database not found error
// is returned.
func (p DoltDatabaseProvider) FileSystemForDatabase(dbname string) (filesys.Filesys, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	dbLocation, ok := p.dbLocations[dbname]
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbname)
	}

	return dbLocation, nil
}

// Database implements the sql.DatabaseProvider interface
func (p DoltDatabaseProvider) Database(ctx *sql.Context, name string) (db sql.Database, err error) {
	var ok bool
	p.mu.RLock()
	db, ok = p.databases[formatDbMapKeyName(name)]
	standby := *p.isStandby
	p.mu.RUnlock()
	if ok {
		return wrapForStandby(db, standby), nil
	}

	db, _, ok, err = p.databaseForRevision(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		db, err = p.databaseForClone(ctx, name)
		if err != nil {
			return nil, err
		}

		if db == nil {
			return nil, sql.ErrDatabaseNotFound.New(name)
		}
	}

	// Don't track revision databases, just instantiate them on demand
	return wrapForStandby(db, standby), nil
}

func wrapForStandby(db sql.Database, standby bool) sql.Database {
	if !standby {
		return db
	}
	if _, ok := db.(ReadOnlyDatabase); ok {
		return db
	}
	if db, ok := db.(Database); ok {
		// :-/. Hopefully it's not too sliced.
		return ReadOnlyDatabase{db}
	}
	return db
}

// attemptCloneReplica attempts to clone a database from the configured replication remote URL template, returning an error
// if it cannot be found
// TODO: distinct error for not found v. others
func (p DoltDatabaseProvider) attemptCloneReplica(ctx *sql.Context, dbName string) error {
	// TODO: these need some reworking, they don't make total sense together
	_, readReplicaRemoteName, _ := sql.SystemVariables.GetGlobal(dsess.ReadReplicaRemote)
	if readReplicaRemoteName == "" {
		// not a read replica DB
		return nil
	}

	remoteName := readReplicaRemoteName.(string)

	// TODO: error handling when not set
	_, remoteUrlTemplate, _ := sql.SystemVariables.GetGlobal(dsess.ReplicationRemoteURLTemplate)
	if remoteUrlTemplate == "" {
		return nil
	}

	urlTemplate, ok := remoteUrlTemplate.(string)
	if !ok {
		return nil
	}

	// TODO: url sanitize
	// TODO: SQL identifiers aren't case sensitive, but URLs are, need a plan for this
	remoteUrl := strings.Replace(urlTemplate, dsess.URLTemplateDatabasePlaceholder, dbName, -1)

	// TODO: remote params for AWS, others
	// TODO: this needs to be robust in the face of the DB not having the default branch
	// TODO: this treats every database not found error as a clone error, need to tighten
	err := p.CloneDatabaseFromRemote(ctx, dbName, p.defaultBranch, remoteName, remoteUrl, nil)
	if err != nil {
		return err
	}

	return nil
}

func (p DoltDatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	_, err := p.Database(ctx, name)
	return err == nil
}

func (p DoltDatabaseProvider) AllDatabases(ctx *sql.Context) (all []sql.Database) {
	p.mu.RLock()

	all = make([]sql.Database, 0, len(p.databases))
	var foundDatabase bool
	for _, db := range p.databases {
		if db.Name() == ctx.GetCurrentDatabase() {
			foundDatabase = true
		}
		all = append(all, db)
	}
	p.mu.RUnlock()

	// If the current database is not one of the primary databases, it must be a transitory revision database
	if !foundDatabase && ctx.GetCurrentDatabase() != "" {
		revDb, _, ok, err := p.databaseForRevision(ctx, ctx.GetCurrentDatabase())
		if err != nil {
			// We can't return an error from this interface function, so just log a message
			ctx.GetLogger().Warnf("unable to load %q as a database revision: %s", ctx.GetCurrentDatabase(), err.Error())
		} else if !ok {
			ctx.GetLogger().Warnf("unable to load %q as a database revision", ctx.GetCurrentDatabase())
		} else {
			all = append(all, revDb)
		}
	}

	return
}

func (p DoltDatabaseProvider) GetRemoteDB(ctx *sql.Context, srcDB *doltdb.DoltDB, r env.Remote, withCaching bool) (*doltdb.DoltDB, error) {
	if withCaching {
		return r.GetRemoteDB(ctx, srcDB.ValueReadWriter().Format(), p.remoteDialer)
	}
	return r.GetRemoteDBWithoutCaching(ctx, srcDB.ValueReadWriter().Format(), p.remoteDialer)
}

func (p DoltDatabaseProvider) CreateDatabase(ctx *sql.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	exists, isDir := p.fs.Exists(name)
	if exists && isDir {
		return sql.ErrDatabaseExists.New(name)
	} else if exists {
		return fmt.Errorf("Cannot create DB, file exists at %s", name)
	}

	err := p.fs.MkDirs(name)
	if err != nil {
		return err
	}

	newFs, err := p.fs.WithWorkingDir(name)
	if err != nil {
		return err
	}

	// TODO: fill in version appropriately
	sess := dsess.DSessFromSess(ctx.Session)
	newEnv := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, p.dbFactoryUrl, "TODO")

	// if currentDB is empty, it will create the database with the default format which is the old format
	newDbStorageFormat := types.Format_Default
	if curDB := sess.GetCurrentDatabase(); curDB != "" {
		if ddb, ok := sess.GetDoltDB(ctx, curDB); ok {
			newDbStorageFormat = ddb.ValueReadWriter().Format()
		}
	} else {
		dbs := sess.GetDbStates()
		var formats = make(map[*types.NomsBinFormat]int)
		for dbName, _ := range dbs {
			if ddb, ok := sess.GetDoltDB(ctx, dbName); ok {
				formats[ddb.ValueReadWriter().Format()] += 1
			}
		}
		if len(formats) > 1 {
			return fmt.Errorf("multiple formats in the same server is not supported")
		}
		if len(formats) == 1 {
			for f, _ := range formats {
				newDbStorageFormat = f
			}
		}
	}
	err = newEnv.InitRepo(ctx, newDbStorageFormat, sess.Username(), sess.Email(), p.defaultBranch)
	if err != nil {
		return err
	}

	// if calling process has a lockfile, also create one for new database
	if env.FsIsLocked(p.fs) {
		err := newEnv.Lock()
		if err != nil {
			ctx.GetLogger().Warnf("Failed to lock newly created database: %s", err.Error())
		}
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}

	opts := editor.Options{
		Deaf: newEnv.DbEaFactory(),
		// TODO: this doesn't seem right, why is this getting set in the constructor to the DB
		ForeignKeyChecksDisabled: fkChecks.(int8) == 0,
	}

	db, err := NewDatabase(ctx, name, newEnv.DbData(), opts)
	if err != nil {
		return err
	}

	// If we have an initialization hook, invoke it.  By default, this will
	// be ConfigureReplicationDatabaseHook, which will setup replication
	// for the new database if a remote url template is set.
	err = p.InitDatabaseHook(ctx, p, name, newEnv)
	if err != nil {
		return err
	}

	formattedName := formatDbMapKeyName(db.Name())
	p.databases[formattedName] = db
	p.dbLocations[formattedName] = newEnv.FS

	dbstate, err := GetInitialDBState(ctx, db)
	if err != nil {
		return err
	}

	return sess.AddDB(ctx, dbstate)
}

type InitDatabaseHook func(ctx *sql.Context, pro DoltDatabaseProvider, name string, env *env.DoltEnv) error

// configureReplication sets up replication for a newly created database as necessary
// TODO: consider the replication heads / all heads setting
func ConfigureReplicationDatabaseHook(ctx *sql.Context, p DoltDatabaseProvider, name string, newEnv *env.DoltEnv) error {
	_, replicationRemoteName, _ := sql.SystemVariables.GetGlobal(dsess.ReplicateToRemote)
	if replicationRemoteName == "" {
		return nil
	}

	remoteName, ok := replicationRemoteName.(string)
	if !ok {
		return nil
	}

	_, remoteUrlTemplate, _ := sql.SystemVariables.GetGlobal(dsess.ReplicationRemoteURLTemplate)
	if remoteUrlTemplate == "" {
		return nil
	}

	urlTemplate, ok := remoteUrlTemplate.(string)
	if !ok {
		return nil
	}

	// TODO: url sanitize name
	remoteUrl := strings.Replace(urlTemplate, dsess.URLTemplateDatabasePlaceholder, name, -1)

	// TODO: params for AWS, others that need them
	r := env.NewRemote(remoteName, remoteUrl, nil)
	err := r.Prepare(ctx, newEnv.DoltDB.Format(), p.remoteDialer)
	if err != nil {
		return err
	}

	err = newEnv.AddRemote(r)
	if err != env.ErrRemoteAlreadyExists && err != nil {
		return err
	}

	// TODO: get background threads from the engine
	commitHooks, err := GetCommitHooks(ctx, sql.NewBackgroundThreads(), newEnv, cli.CliErr)
	if err != nil {
		return err
	}

	newEnv.DoltDB.SetCommitHooks(ctx, commitHooks)

	// After setting hooks on the newly created DB, we need to do the first push manually
	branchRef := ref.NewBranchRef(p.defaultBranch)
	return newEnv.DoltDB.ExecuteCommitHooks(ctx, branchRef.String())
}

// CloneDatabaseFromRemote implements DoltDatabaseProvider interface
func (p DoltDatabaseProvider) CloneDatabaseFromRemote(
	ctx *sql.Context,
	dbName, branch, remoteName, remoteUrl string,
	remoteParams map[string]string,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	exists, isDir := p.fs.Exists(dbName)
	if exists && isDir {
		return sql.ErrDatabaseExists.New(dbName)
	} else if exists {
		return fmt.Errorf("cannot create DB, file exists at %s", dbName)
	}

	dEnv, err := p.cloneDatabaseFromRemote(ctx, dbName, remoteName, branch, remoteUrl, remoteParams)
	if err != nil {
		// Make a best effort to clean up any artifacts on disk from a failed clone
		// before we return the error
		exists, _ := p.fs.Exists(dbName)
		if exists {
			deleteErr := p.fs.Delete(dbName, true)
			if deleteErr != nil {
				err = fmt.Errorf("%s: unable to clean up failed clone in directory '%s'", err.Error(), dbName)
			}
		}
		return err
	}

	return ConfigureReplicationDatabaseHook(ctx, p, dbName, dEnv)
}

// cloneDatabaseFromRemote encapsulates the inner logic for cloning a database so that if any error
// is returned by this function, the caller can capture the error and safely clean up the failed
// clone directory before returning the error to the user. This function should not be used directly;
// use CloneDatabaseFromRemote instead.
func (p DoltDatabaseProvider) cloneDatabaseFromRemote(
	ctx *sql.Context,
	dbName, remoteName, branch, remoteUrl string,
	remoteParams map[string]string,
) (*env.DoltEnv, error) {
	if p.remoteDialer == nil {
		return nil, fmt.Errorf("unable to clone remote database; no remote dialer configured")
	}

	// TODO: params for AWS, others that need them
	r := env.NewRemote(remoteName, remoteUrl, nil)
	srcDB, err := r.GetRemoteDB(ctx, types.Format_Default, p.remoteDialer)
	if err != nil {
		return nil, err
	}

	dEnv, err := actions.EnvForClone(ctx, srcDB.ValueReadWriter().Format(), r, dbName, p.fs, "VERSION", env.GetCurrentUserHomeDir)
	if err != nil {
		return nil, err
	}

	err = actions.CloneRemote(ctx, srcDB, remoteName, branch, dEnv)
	if err != nil {
		return nil, err
	}

	err = dEnv.RepoStateWriter().UpdateBranch(dEnv.RepoState.CWBHeadRef().GetPath(), env.BranchConfig{
		Merge:  dEnv.RepoState.Head,
		Remote: remoteName,
	})

	sess := dsess.DSessFromSess(ctx.Session)
	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}

	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
		// TODO: this doesn't seem right, why is this getting set in the constructor to the DB
		ForeignKeyChecksDisabled: fkChecks.(int8) == 0,
	}

	db, err := NewDatabase(ctx, dbName, dEnv.DbData(), opts)
	if err != nil {
		return nil, err
	}

	p.databases[formatDbMapKeyName(db.Name())] = db

	dbstate, err := GetInitialDBState(ctx, db)
	if err != nil {
		return nil, err
	}

	err = sess.AddDB(ctx, dbstate)
	if err != nil {
		return nil, err
	}

	return dEnv, nil
}

// DropDatabase implements the sql.MutableDatabaseProvider interface
func (p DoltDatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	isRevisionDatabase, err := p.IsRevisionDatabase(ctx, name)
	if err != nil {
		return err
	}
	if isRevisionDatabase {
		return fmt.Errorf("unable to drop revision database: %s", name)
	}

	// get the case-sensitive name for case-sensitive file systems
	// TODO: there are still cases (not server-first) where we rename databases because the directory name would need
	//  quoting if used as a database name, and that breaks here. We either need the database name to match the directory
	//  name in all cases, or else keep a mapping from database name to directory on disk.
	p.mu.Lock()
	defer p.mu.Unlock()

	dbKey := formatDbMapKeyName(name)
	db := p.databases[dbKey]

	// get location of database that's being dropped
	dbLoc := p.dbLocations[dbKey]
	if dbLoc == nil {
		return sql.ErrDatabaseNotFound.New(db.Name())
	}
	dropDbLoc, err := dbLoc.Abs("")
	if err != nil {
		return err
	}
	rootDbLoc, err := p.fs.Abs("")
	if err != nil {
		return err
	}
	dirToDelete := ""
	// if the database is in the directory itself, we remove '.dolt' directory rather than
	// the whole directory itself because it can have other databases that are nested.
	if rootDbLoc == dropDbLoc {
		doltDirExists, _ := p.fs.Exists(dbfactory.DoltDir)
		if !doltDirExists {
			return sql.ErrDatabaseNotFound.New(db.Name())
		}
		dirToDelete = dbfactory.DoltDir
	} else {
		exists, isDir := p.fs.Exists(dropDbLoc)
		// Get the DB's directory
		if !exists {
			// engine should already protect against this
			return sql.ErrDatabaseNotFound.New(db.Name())
		} else if !isDir {
			return fmt.Errorf("unexpected error: %s exists but is not a directory", dbKey)
		}
		dirToDelete = dropDbLoc
	}

	err = p.fs.Delete(dirToDelete, true)
	if err != nil {
		return err
	}

	// We not only have to delete this database, but any derivative ones that we've stored as a result of USE or
	// connection strings
	derivativeNamePrefix := dbKey + dbRevisionDelimiter
	for dbName := range p.databases {
		if strings.HasPrefix(dbName, derivativeNamePrefix) {
			delete(p.databases, dbName)
		}
	}

	delete(p.databases, dbKey)
	return nil
}

// TODO: databaseForRevision should call checkout on the given branch/commit, returning a non-mutable session
// only if a non-branch revspec was indicated.
func (p DoltDatabaseProvider) databaseForRevision(ctx *sql.Context, revDB string) (sql.Database, dsess.InitialDbState, bool, error) {
	if !strings.Contains(revDB, dbRevisionDelimiter) {
		return nil, dsess.InitialDbState{}, false, nil
	}

	parts := strings.SplitN(revDB, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	p.mu.RLock()
	candidate, ok := p.databases[formatDbMapKeyName(dbName)]
	p.mu.RUnlock()
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}

	srcDb, ok := candidate.(SqlDatabase)
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}

	var err error
	revSpec, err = p.resolveAncestorSpec(ctx, revSpec, srcDb.DbData().Ddb)
	if err != nil {
		return nil, dsess.InitialDbState{}, false, err
	}

	isBranch, err := isBranch(ctx, srcDb, revSpec, p.remoteDialer)
	if err != nil {
		return nil, dsess.InitialDbState{}, false, err
	}

	if isBranch {
		// fetch the upstream head if this is a replicated db
		if replicaDb, ok := srcDb.(ReadReplicaDatabase); ok {
			// TODO move this out of analysis phase, should only happen at read time
			err := switchAndFetchReplicaHead(ctx, revSpec, replicaDb)
			if err != nil {
				return nil, dsess.InitialDbState{}, false, err
			}
		}

		db, init, err := dbRevisionForBranch(ctx, srcDb, revSpec)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}

		return db, init, true, nil
	}

	isTag, err := isTag(ctx, srcDb, revSpec, p.remoteDialer)
	if err != nil {
		return nil, dsess.InitialDbState{}, false, err
	}

	if isTag {
		// TODO: this should be an interface, not a struct
		replicaDb, ok := srcDb.(ReadReplicaDatabase)
		if ok {
			srcDb = replicaDb.Database
		}

		srcDb, ok = srcDb.(Database)
		if !ok {
			return nil, dsess.InitialDbState{}, false, nil
		}

		db, init, err := dbRevisionForTag(ctx, srcDb.(Database), revSpec)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}
		return db, init, true, nil
	}

	if doltdb.IsValidCommitHash(revSpec) {
		// TODO: this should be an interface, not a struct
		replicaDb, ok := srcDb.(ReadReplicaDatabase)
		if ok {
			srcDb = replicaDb.Database
		}

		srcDb, ok = srcDb.(Database)
		if !ok {
			return nil, dsess.InitialDbState{}, false, nil
		}
		db, init, err := dbRevisionForCommit(ctx, srcDb.(Database), revSpec)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}
		return db, init, true, nil
	}

	return nil, dsess.InitialDbState{}, false, nil
}

// databaseForClone returns a newly cloned database if read replication is enabled and a remote DB exists, or an error
// otherwise
func (p DoltDatabaseProvider) databaseForClone(ctx *sql.Context, revDB string) (sql.Database, error) {
	if !readReplicationActive(ctx) {
		return nil, nil
	}

	var dbName string
	if strings.Contains(revDB, dbRevisionDelimiter) {
		parts := strings.SplitN(revDB, dbRevisionDelimiter, 2)
		dbName = parts[0]
	} else {
		dbName = revDB
	}

	err := p.attemptCloneReplica(ctx, dbName)
	if err != nil {
		ctx.GetLogger().Warnf("couldn't clone database %s: %s", dbName, err.Error())
		return nil, nil
	}

	// now that the database has been cloned, retry the Database call
	return p.Database(ctx, revDB)
}

// TODO: figure out the right contract: which variables must be set? What happens if they aren't all set?
func readReplicationActive(ctx *sql.Context) bool {
	_, readReplicaRemoteName, _ := sql.SystemVariables.GetGlobal(dsess.ReadReplicaRemote)
	if readReplicaRemoteName == "" {
		return false
	}

	_, remoteUrlTemplate, _ := sql.SystemVariables.GetGlobal(dsess.ReplicationRemoteURLTemplate)
	if remoteUrlTemplate == "" {
		return false
	}

	return true
}

// resolveAncestorSpec resolves the specified revSpec to a specific commit hash if it contains an ancestor reference
// such as ~ or ^. If no ancestor reference is present, the specified revSpec is returned as is. If any unexpected
// problems are encountered, an error is returned.
func (p DoltDatabaseProvider) resolveAncestorSpec(ctx *sql.Context, revSpec string, ddb *doltdb.DoltDB) (string, error) {
	refname, ancestorSpec, err := doltdb.SplitAncestorSpec(revSpec)
	if err != nil {
		return "", err
	}
	if ancestorSpec == nil || ancestorSpec.SpecStr == "" {
		return revSpec, nil
	}

	ref, err := ddb.GetRefByNameInsensitive(ctx, refname)
	if err != nil {
		return "", err
	}

	cm, err := ddb.ResolveCommitRef(ctx, ref)
	if err != nil {
		return "", err
	}

	cm, err = cm.GetAncestor(ctx, ancestorSpec)
	if err != nil {
		return "", err
	}

	hash, err := cm.HashOf()
	if err != nil {
		return "", err
	}

	return hash.String(), nil
}

func (p DoltDatabaseProvider) RevisionDbState(ctx *sql.Context, revDB string) (dsess.InitialDbState, error) {
	_, init, ok, err := p.databaseForRevision(ctx, revDB)
	if err != nil {
		return dsess.InitialDbState{}, err
	} else if !ok {
		return dsess.InitialDbState{}, sql.ErrDatabaseNotFound.New(revDB)
	}

	return init, nil
}

// Function implements the FunctionProvider interface
func (p DoltDatabaseProvider) Function(_ *sql.Context, name string) (sql.Function, error) {
	fn, ok := p.functions[strings.ToLower(name)]
	if !ok {
		return nil, sql.ErrFunctionNotFound.New(name)
	}
	return fn, nil
}

func (p DoltDatabaseProvider) Register(d sql.ExternalStoredProcedureDetails) {
	p.externalProcedures.Register(d)
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (p DoltDatabaseProvider) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return p.externalProcedures.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (p DoltDatabaseProvider) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return p.externalProcedures.LookupByName(name)
}

// TableFunction implements the sql.TableFunctionProvider interface
func (p DoltDatabaseProvider) TableFunction(_ *sql.Context, name string) (sql.TableFunction, error) {
	// currently, only one table function is supported, if we extend this, we should clean this up
	// and store table functions in a map, similar to regular functions.
	switch strings.ToLower(name) {
	case "dolt_diff":
		dtf := &DiffTableFunction{}
		return dtf, nil
	case "dolt_diff_summary":
		dtf := &DiffSummaryTableFunction{}
		return dtf, nil
	case "dolt_log":
		dtf := &LogTableFunction{}
		return dtf, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
}

// GetRevisionForRevisionDatabase implements dsess.RevisionDatabaseProvider
func (p DoltDatabaseProvider) GetRevisionForRevisionDatabase(ctx *sql.Context, dbName string) (string, string, error) {
	db, err := p.Database(ctx, dbName)
	if err != nil {
		return "", "", err
	}

	sqldb, ok := db.(dsess.RevisionDatabase)
	if !ok {
		return db.Name(), "", nil
	}

	if sqldb.Revision() != "" {
		dbName = strings.TrimSuffix(dbName, dbRevisionDelimiter+sqldb.Revision())
	}

	return dbName, sqldb.Revision(), nil
}

// IsRevisionDatabase returns true if the specified dbName represents a database that is tied to a specific
// branch or commit from a database (e.g. "dolt/branch1").
func (p DoltDatabaseProvider) IsRevisionDatabase(ctx *sql.Context, dbName string) (bool, error) {
	dbName, revision, err := p.GetRevisionForRevisionDatabase(ctx, dbName)
	if err != nil {
		return false, err
	}

	return revision != "", nil
}

// switchAndFetchReplicaHead tries to pull the latest version of a branch. Will fail if the branch
// does not exist on the ReadReplicaDatabase's remote. If the target branch is not a replication
// head, the new branch will not be continuously fetched.
func switchAndFetchReplicaHead(ctx *sql.Context, branch string, db ReadReplicaDatabase) error {
	branchRef := ref.NewBranchRef(branch)

	var branchExists bool
	branches, err := db.ddb.GetBranches(ctx)
	if err != nil {
		return err
	}

	for _, br := range branches {
		if br.String() == branch {
			branchExists = true
			break
		}
	}

	// check whether branch is on remote before creating local tracking branch
	cm, err := actions.FetchRemoteBranch(ctx, db.tmpDir, db.remote, db.srcDB, db.DbData().Ddb, branchRef, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		return err
	}

	// create refs/heads/branch dataset
	if !branchExists {
		err = db.ddb.NewBranchAtCommit(ctx, branchRef, cm)
		if err != nil {
			return err
		}
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	currentBranchRef, err := dSess.CWBHeadRef(ctx, db.name)
	if err != nil {
		return err
	}

	// create workingSets/heads/branch and update the working set
	err = pullBranches(ctx, db, []string{branch}, currentBranchRef, pullBehavior_fastForward)
	if err != nil {
		return err
	}

	return nil
}

// isBranch returns whether a branch with the given name is in scope for the database given
func isBranch(ctx context.Context, db SqlDatabase, branchName string, dialer dbfactory.GRPCDialProvider) (bool, error) {
	var ddbs []*doltdb.DoltDB

	if rdb, ok := db.(ReadReplicaDatabase); ok {
		remoteDB, err := rdb.remote.GetRemoteDB(ctx, rdb.ddb.Format(), dialer)
		if err != nil {
			return false, err
		}
		ddbs = append(ddbs, rdb.ddb, remoteDB)
	} else if ddb, ok := db.(Database); ok {
		ddbs = append(ddbs, ddb.ddb)
	} else {
		return false, fmt.Errorf("unrecognized type of database %T", db)
	}

	for _, ddb := range ddbs {
		branchExists, err := ddb.HasBranch(ctx, branchName)
		if err != nil {
			return false, err
		}

		if branchExists {
			return true, nil
		}
	}

	return false, nil
}

// isTag returns whether a tag with the given name is in scope for the database given
func isTag(ctx context.Context, db SqlDatabase, tagName string, dialer dbfactory.GRPCDialProvider) (bool, error) {
	var ddbs []*doltdb.DoltDB

	if rdb, ok := db.(ReadReplicaDatabase); ok {
		remoteDB, err := rdb.remote.GetRemoteDB(ctx, rdb.ddb.Format(), dialer)
		if err != nil {
			return false, err
		}
		ddbs = append(ddbs, rdb.ddb, remoteDB)
	} else if ddb, ok := db.(Database); ok {
		ddbs = append(ddbs, ddb.ddb)
	} else {
		return false, fmt.Errorf("unrecognized type of database %T", db)
	}

	for _, ddb := range ddbs {
		tagExists, err := ddb.HasTag(ctx, tagName)
		if err != nil {
			return false, err
		}

		if tagExists {
			return true, nil
		}
	}

	return false, nil
}

func dbRevisionForBranch(ctx context.Context, srcDb SqlDatabase, revSpec string) (SqlDatabase, dsess.InitialDbState, error) {
	branch := ref.NewBranchRef(revSpec)
	cm, err := srcDb.DbData().Ddb.ResolveCommitRef(ctx, branch)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	wsRef, err := ref.WorkingSetRefForHead(branch)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	ws, err := srcDb.DbData().Ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	dbName := srcDb.Name() + dbRevisionDelimiter + revSpec

	static := staticRepoState{
		branch:          branch,
		RepoStateWriter: srcDb.DbData().Rsw,
		RepoStateReader: srcDb.DbData().Rsr,
	}

	var db SqlDatabase

	switch v := srcDb.(type) {
	case Database:
		db = Database{
			name:     dbName,
			ddb:      v.ddb,
			rsw:      static,
			rsr:      static,
			gs:       v.gs,
			editOpts: v.editOpts,
			revision: revSpec,
		}
	case ReadReplicaDatabase:
		db = ReadReplicaDatabase{
			Database: Database{
				name:     dbName,
				ddb:      v.ddb,
				rsw:      static,
				rsr:      static,
				gs:       v.gs,
				editOpts: v.editOpts,
				revision: revSpec,
			},
			remote:  v.remote,
			srcDB:   v.srcDB,
			tmpDir:  v.tmpDir,
			limiter: newLimiter(),
		}
	}

	remotes, err := static.GetRemotes()
	if err != nil {
		return nil, dsess.InitialDbState{}, err
	}

	branches, err := static.GetBranches()
	if err != nil {
		return nil, dsess.InitialDbState{}, err
	}

	backups, err := static.GetBackups()
	if err != nil {
		return nil, dsess.InitialDbState{}, err
	}

	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		WorkingSet: ws,
		DbData: env.DbData{
			Ddb: srcDb.DbData().Ddb,
			Rsw: static,
			Rsr: static,
		},
		Remotes:  remotes,
		Branches: branches,
		Backups:  backups,
		//ReadReplica: //todo
	}

	return db, init, nil
}

func dbRevisionForTag(ctx context.Context, srcDb Database, revSpec string) (ReadOnlyDatabase, dsess.InitialDbState, error) {
	tag := ref.NewTagRef(revSpec)

	cm, err := srcDb.DbData().Ddb.ResolveCommitRef(ctx, tag)
	if err != nil {
		return ReadOnlyDatabase{}, dsess.InitialDbState{}, err
	}

	name := srcDb.Name() + dbRevisionDelimiter + revSpec
	db := ReadOnlyDatabase{Database: Database{
		name:     name,
		ddb:      srcDb.DbData().Ddb,
		rsw:      srcDb.DbData().Rsw,
		rsr:      srcDb.DbData().Rsr,
		editOpts: srcDb.editOpts,
		revision: revSpec,
	}}
	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		ReadOnly:   true,
		DbData: env.DbData{
			Ddb: srcDb.DbData().Ddb,
			Rsw: srcDb.DbData().Rsw,
			Rsr: srcDb.DbData().Rsr,
		},
		// todo: should we initialize
		//  - Remotes
		//  - Branches
		//  - Backups
		//  - ReadReplicas
	}

	return db, init, nil
}

func dbRevisionForCommit(ctx context.Context, srcDb Database, revSpec string) (ReadOnlyDatabase, dsess.InitialDbState, error) {
	spec, err := doltdb.NewCommitSpec(revSpec)
	if err != nil {
		return ReadOnlyDatabase{}, dsess.InitialDbState{}, err
	}

	cm, err := srcDb.DbData().Ddb.Resolve(ctx, spec, srcDb.DbData().Rsr.CWBHeadRef())
	if err != nil {
		return ReadOnlyDatabase{}, dsess.InitialDbState{}, err
	}

	name := srcDb.Name() + dbRevisionDelimiter + revSpec
	db := ReadOnlyDatabase{Database: Database{
		name:     name,
		ddb:      srcDb.DbData().Ddb,
		rsw:      srcDb.DbData().Rsw,
		rsr:      srcDb.DbData().Rsr,
		editOpts: srcDb.editOpts,
		revision: revSpec,
	}}
	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		ReadOnly:   true,
		DbData: env.DbData{
			Ddb: srcDb.DbData().Ddb,
			Rsw: srcDb.DbData().Rsw,
			Rsr: srcDb.DbData().Rsr,
		},
		// todo: should we initialize
		//  - Remotes
		//  - Branches
		//  - Backups
		//  - ReadReplicas
	}

	return db, init, nil
}

type staticRepoState struct {
	branch ref.DoltRef
	env.RepoStateWriter
	env.RepoStateReader
}

func (s staticRepoState) CWBHeadRef() ref.DoltRef {
	return s.branch
}

// formatDbMapKeyName returns formatted string of database name and/or branch name. Database name is case-insensitive,
// so it's stored in lower case name. Branch name is case-sensitive, so not changed.
func formatDbMapKeyName(name string) string {
	if !strings.Contains(name, dbRevisionDelimiter) {
		return strings.ToLower(name)
	}

	parts := strings.SplitN(name, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	return strings.ToLower(dbName) + dbRevisionDelimiter + revSpec
}
