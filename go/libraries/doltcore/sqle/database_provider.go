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
	"github.com/dolthub/dolt/go/libraries/utils/config"
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

	dataRootDir string
	fs          filesys.Filesys
	cfg         config.ReadableConfig

	dbFactoryUrl string
}

var _ sql.DatabaseProvider = DoltDatabaseProvider{}
var _ sql.FunctionProvider = DoltDatabaseProvider{}
var _ sql.MutableDatabaseProvider = DoltDatabaseProvider{}
var _ dsess.RevisionDatabaseProvider = DoltDatabaseProvider{}

const createDbWC = 1105 // 1105 represents an unknown error.

// NewDoltDatabaseProvider returns a provider for the databases given
func NewDoltDatabaseProvider(config config.ReadableConfig, fs filesys.Filesys, databases ...sql.Database) DoltDatabaseProvider {
	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[strings.ToLower(db.Name())] = db
	}

	funcs := make(map[string]sql.Function, len(dfunctions.DoltFunctions))
	for _, fn := range dfunctions.DoltFunctions {
		funcs[strings.ToLower(fn.FunctionName())] = fn
	}

	return DoltDatabaseProvider{
		databases:    dbs,
		functions:    funcs,
		mu:           &sync.RWMutex{},
		fs:           fs,
		cfg:          config,
		dbFactoryUrl: doltdb.LocalDirDoltDB,
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
	name = strings.ToLower(name)
	var ok bool
	p.mu.RLock()
	db, ok = p.databases[name]
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
	if found, ok := p.databases[name]; !ok {
		p.databases[name] = db
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

	dsess := dsess.DSessFromSess(ctx.Session)
	branch := env.GetDefaultInitBranch(p.cfg)

	// TODO: fill in version appropriately
	newEnv := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, p.dbFactoryUrl, "TODO")
	err = newEnv.InitRepo(ctx, types.Format_Default, dsess.Username(), dsess.Email(), branch)
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
	p.databases[strings.ToLower(db.Name())] = db

	dbstate, err := GetInitialDBState(ctx, db)
	if err != nil {
		return err
	}

	return dsess.AddDB(ctx, dbstate)
}

func (p DoltDatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get the DB's directory
	exists, isDir := p.fs.Exists(name)
	if !exists {
		// engine should already protect against this
		return sql.ErrDatabaseNotFound.New(name)
	} else if !isDir {
		return fmt.Errorf("unexpected error: %s exists but is not a directory", name)
	}

	err := p.fs.Delete(name, true)
	if err != nil {
		return err
	}

	// TODO: delete database in current dir

	delete(p.databases, strings.ToLower(name))
	return nil
}

func (p DoltDatabaseProvider) databaseForRevision(ctx context.Context, revDB string) (sql.Database, dsess.InitialDbState, bool, error) {
	revDB = strings.ToLower(revDB)
	if !strings.Contains(revDB, dbRevisionDelimiter) {
		return nil, dsess.InitialDbState{}, false, nil
	}

	parts := strings.SplitN(revDB, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	p.mu.RLock()
	candidate, ok := p.databases[dbName]
	p.mu.RUnlock()
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}

	srcDb, ok := candidate.(SqlDatabase)
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}

	if replicaDb, ok := srcDb.(ReadReplicaDatabase); ok {
		// TODO move this out of analysis phase, should only happen at read time
		err := switchAndFetchReplicaHead(ctx, revSpec, replicaDb)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}
	}

	if isBranch(ctx, srcDb.DbData().Ddb, revSpec) {
		db, init, err := dbRevisionForBranch(ctx, srcDb, revSpec)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}
		return db, init, true, nil
	}

	if doltdb.IsValidCommitHash(revSpec) {
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

func (p DoltDatabaseProvider) RevisionDbState(ctx context.Context, revDB string) (dsess.InitialDbState, error) {
	_, init, ok, err := p.databaseForRevision(ctx, revDB)
	if err != nil {
		return dsess.InitialDbState{}, err
	} else if !ok {
		return dsess.InitialDbState{}, sql.ErrDatabaseNotFound.New(revDB)
	}

	return init, nil
}

func (p DoltDatabaseProvider) Function(ctx *sql.Context, name string) (sql.Function, error) {
	fn, ok := p.functions[strings.ToLower(name)]
	if !ok {
		return nil, sql.ErrFunctionNotFound.New(name)
	}
	return fn, nil
}

// switchAndFetchReplicaHead tries to pull the latest version of a branch. Will fail if the branch
// does not exist on the ReadReplicaDatabase's remote. If the target branch is not a replication
// head, the new branch will not be continuously fetched.
func switchAndFetchReplicaHead(ctx context.Context, branch string, db sql.Database) error {
	destDb, ok := db.(ReadReplicaDatabase)
	if !ok {
		return ErrFailedToCastToReplicaDb
	}

	branchRef := ref.NewBranchRef(branch)

	var branchExists bool
	branches, err := destDb.ddb.GetBranches(ctx)
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
	cm, err := actions.FetchRemoteBranch(ctx, destDb.tmpDir, destDb.remote, destDb.srcDB, destDb.DbData().Ddb, branchRef, nil, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		return err
	}

	// create refs/heads/branch dataset
	if !branchExists {
		err = destDb.ddb.NewBranchAtCommit(ctx, branchRef, cm)
		if err != nil {
			return err
		}
	}

	// update ReadReplicaRemote with new HEAD
	// dolt_replicate_heads configuration remains unchanged
	destDb, err = destDb.SetHeadRef(branchRef)
	if err != nil {
		return err
	}

	// create workingSets/heads/branch and update the working set
	err = pullBranches(ctx, destDb, []string{branch})
	if err != nil {
		return err
	}

	return nil
}

func isBranch(ctx context.Context, ddb *doltdb.DoltDB, revSpec string) bool {
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return false
	}

	for _, br := range branches {
		if revSpec == br.GetPath() {
			return true
		}
	}

	return false
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
			headRef:        v.headRef,
			remoteTrackRef: v.remoteTrackRef,
			remote:         v.remote,
			srcDB:          v.srcDB,
			tmpDir:         v.tmpDir,
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
		gs:       nil,
		editOpts: srcDb.editOpts,
	}}
	init := dsess.InitialDbState{
		Db:           db,
		HeadCommit:   cm,
		ReadOnly:     true,
		DetachedHead: true,
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
