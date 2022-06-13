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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	dbRevisionDelimiter = "/"
)

type DoltDatabaseProvider struct {
	databases map[string]sql.Database
	functions map[string]sql.Function
	mu        *sync.RWMutex

	defaultBranch string
	dataRootDir   string
	fs            filesys.Filesys

	dbFactoryUrl string
}

var _ sql.DatabaseProvider = (*DoltDatabaseProvider)(nil)
var _ sql.FunctionProvider = (*DoltDatabaseProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DoltDatabaseProvider)(nil)
var _ dsess.RevisionDatabaseProvider = (*DoltDatabaseProvider)(nil)

// NewDoltDatabaseProvider returns a provider for the databases given
func NewDoltDatabaseProvider(defaultBranch string, fs filesys.Filesys, databases ...sql.Database) DoltDatabaseProvider {
	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[strings.ToLower(db.Name())] = db
	}

	funcs := make(map[string]sql.Function, len(dfunctions.DoltFunctions))
	for _, fn := range dfunctions.DoltFunctions {
		funcs[strings.ToLower(fn.FunctionName())] = fn
	}

	return DoltDatabaseProvider{
		databases:     dbs,
		functions:     funcs,
		mu:            &sync.RWMutex{},
		fs:            fs,
		defaultBranch: defaultBranch,
		dbFactoryUrl:  doltdb.LocalDirDoltDB,
	}
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

func (p DoltDatabaseProvider) Database(ctx *sql.Context, name string) (db sql.Database, err error) {
	var ok bool
	p.mu.RLock()
	db, ok = p.databases[formatDbMapKeyName(name)]
	p.mu.RUnlock()
	if ok {
		return db, nil
	}

	db, _, ok, err = p.databaseForRevision(ctx, name)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(name)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if found, ok := p.databases[formatDbMapKeyName(name)]; !ok {
		p.databases[formatDbMapKeyName(name)] = db
		return db, nil
	} else {
		return found, nil
	}

}

func (p DoltDatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	_, err := p.Database(ctx, name)
	return err == nil
}

func (p DoltDatabaseProvider) AllDatabases(ctx *sql.Context) (all []sql.Database) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	i := 0
	all = make([]sql.Database, len(p.databases))
	for _, db := range p.databases {
		all[i] = db
		i++
	}
	return
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
	dsess := dsess.DSessFromSess(ctx.Session)
	newEnv := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, p.dbFactoryUrl, "TODO")
	err = newEnv.InitRepo(ctx, types.Format_Default, dsess.Username(), dsess.Email(), p.defaultBranch)
	if err != nil {
		return err
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

	db := NewDatabase(name, newEnv.DbData(), opts)
	p.databases[formatDbMapKeyName(db.Name())] = db

	dbstate, err := GetInitialDBState(ctx, db)
	if err != nil {
		return err
	}

	return dsess.AddDB(ctx, dbstate)
}

func (p DoltDatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// get the case-sensitive name for case-sensitive file systems
	// TODO: there are still cases (not server-first) where we rename databases because the directory name would need
	//  quoting if used as a database name, and that breaks here. We either need the database name to match the directory
	//  name in all cases, or else keep a mapping from database name to directory on disk.
	dbKey := formatDbMapKeyName(name)
	db := p.databases[dbKey]

	// Get the DB's directory
	exists, isDir := p.fs.Exists(db.Name())
	if !exists {
		// engine should already protect against this
		return sql.ErrDatabaseNotFound.New(db.Name())
	} else if !isDir {
		return fmt.Errorf("unexpected error: %s exists but is not a directory", dbKey)
	}

	err := p.fs.Delete(db.Name(), true)
	if err != nil {
		return err
	}

	// TODO: delete database in current dir

	// We not only have to delete this database, but any derivative ones that we've stored as a result of USE or
	// connection strings
	derivativeNamePrefix := dbKey + "/"
	for dbName := range p.databases {
		if strings.HasPrefix(dbName, derivativeNamePrefix) {
			delete(p.databases, dbName)
		}
	}

	delete(p.databases, dbKey)
	return nil
}

//TODO: databaseForRevision should call checkout on the given branch/commit, returning a non-mutable session
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

	isBranch, err := isBranch(ctx, srcDb, revSpec)
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

// TableFunction implements the TableFunctionProvider interface
func (p DoltDatabaseProvider) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, error) {
	// currently, only one table function is supported, if we extend this, we should clean this up
	// and store table functions in a map, similar to regular functions.
	if strings.ToLower(name) == "dolt_diff" {
		dtf := &DiffTableFunction{}
		return dtf, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
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

	// create workingSets/heads/branch and update the working set
	err = pullBranches(ctx, db, []string{branch})
	if err != nil {
		return err
	}

	return nil
}

// isBranch returns whether a branch with the given name is in scope for the database given
func isBranch(ctx context.Context, db SqlDatabase, branchName string) (bool, error) {
	var ddbs []*doltdb.DoltDB

	if rdb, ok := db.(ReadReplicaDatabase); ok {
		remoteDB, err := rdb.remote.GetRemoteDB(ctx, rdb.ddb.Format())
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
		DocsReadWriter:  srcDb.DbData().Drw,
	}

	var db SqlDatabase

	switch v := srcDb.(type) {
	case Database:
		db = Database{
			name:     dbName,
			ddb:      v.ddb,
			rsw:      static,
			rsr:      static,
			drw:      static,
			gs:       v.gs,
			editOpts: v.editOpts,
		}
	case ReadReplicaDatabase:
		db = ReadReplicaDatabase{
			Database: Database{
				name:     dbName,
				ddb:      v.ddb,
				rsw:      static,
				rsr:      static,
				drw:      static,
				gs:       v.gs,
				editOpts: v.editOpts,
			},
			remote: v.remote,
			srcDB:  v.srcDB,
			tmpDir: v.tmpDir,
		}
	}

	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		WorkingSet: ws,
		DbData: env.DbData{
			Ddb: srcDb.DbData().Ddb,
			Rsw: static,
			Rsr: static,
			Drw: static,
		},
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
		drw:      srcDb.DbData().Drw,
		editOpts: srcDb.editOpts,
	}}
	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		ReadOnly:   true,
		DbData: env.DbData{
			Ddb: srcDb.DbData().Ddb,
			Rsw: srcDb.DbData().Rsw,
			Rsr: srcDb.DbData().Rsr,
			Drw: srcDb.DbData().Drw,
		},
	}

	return db, init, nil
}

type staticRepoState struct {
	branch ref.DoltRef
	env.RepoStateWriter
	env.RepoStateReader
	env.DocsReadWriter
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
