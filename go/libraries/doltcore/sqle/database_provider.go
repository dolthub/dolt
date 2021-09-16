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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

const (
	dbRevisionDelimiter = "/"
)

type DoltDatabaseProvider struct {
	databases map[string]sql.Database
	mu        *sync.RWMutex

	cfg config.ReadableConfig
}

var _ sql.DatabaseProvider = DoltDatabaseProvider{}
var _ sql.MutableDatabaseProvider = DoltDatabaseProvider{}
var _ dsess.RevisionDatabaseProvider = DoltDatabaseProvider{}

func NewDoltDatabaseProvider(config config.ReadableConfig, databases ...sql.Database) DoltDatabaseProvider {
	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[strings.ToLower(db.Name())] = db
	}

	return DoltDatabaseProvider{
		databases: dbs,
		mu:        &sync.RWMutex{},
		cfg:       config,
	}
}

func (p DoltDatabaseProvider) Database(name string) (db sql.Database, err error) {
	name = strings.ToLower(name)
	var ok bool
	func() {
		p.mu.RLock()
		defer p.mu.RUnlock()

		db, ok = p.databases[name]
	}()
	if ok {
		return db, nil
	}

	db, _, ok, err = p.databaseForRevision(context.Background(), name)
	if err != nil {
		return nil, err
	}
	if ok {
		p.mu.Lock()
		defer p.mu.Unlock()

		p.databases[name] = db
		return db, nil
	}

	return nil, sql.ErrDatabaseNotFound.New(name)
}

func (p DoltDatabaseProvider) HasDatabase(name string) bool {
	_, err := p.Database(name)
	return err == nil
}

func (p DoltDatabaseProvider) AllDatabases() (all []sql.Database) {
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

	mem, err := env.NewMemoryDbData(ctx, p.cfg)
	if err != nil {
		return err
	}
	opts := editor.Options{
		Deaf: editor.NewDbEaFactory(
			mem.Rsw.TempTableFilesDir(),
			mem.Ddb.ValueReadWriter()),
	}

	db := NewDatabase(name, mem, opts)
	p.databases[strings.ToLower(db.Name())] = db

	return nil
}

func (p DoltDatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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

	candidate, ok := p.databases[dbName]
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}
	srcDb, ok := candidate.(Database)
	if !ok {
		return nil, dsess.InitialDbState{}, false, nil
	}

	if isBranch(ctx, srcDb.ddb, revSpec) {
		// if the requested revision is a br we can
		// write to it, otherwise make read-only
		db, init, err := dbRevisionForBranch(ctx, srcDb, revSpec)
		if err != nil {
			return nil, dsess.InitialDbState{}, false, err
		}
		return db, init, true, nil
	}

	if doltdb.IsValidCommitHash(revSpec) {
		db, init, err := dbRevisionForCommit(ctx, srcDb, revSpec)
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

func dbRevisionForBranch(ctx context.Context, srcDb Database, revSpec string) (Database, dsess.InitialDbState, error) {
	branch := ref.NewBranchRef(revSpec)
	cm, err := srcDb.ddb.ResolveCommitRef(ctx, branch)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	wsRef, err := ref.WorkingSetRefForHead(branch)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	ws, err := srcDb.ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return Database{}, dsess.InitialDbState{}, err
	}

	dbName := srcDb.Name() + dbRevisionDelimiter + revSpec

	static := staticRepoState{
		branch:          branch,
		RepoStateWriter: srcDb.rsw,
		RepoStateReader: srcDb.rsr,
		DocsReadWriter:  srcDb.drw,
	}
	db := Database{
		name:     dbName,
		ddb:      srcDb.ddb,
		rsw:      static,
		rsr:      static,
		drw:      static,
		gs:       srcDb.gs,
		editOpts: srcDb.editOpts,
	}
	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
		WorkingSet: ws,
		DbData: env.DbData{
			Ddb: srcDb.ddb,
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

	cm, err := srcDb.ddb.Resolve(ctx, spec, srcDb.rsr.CWBHeadRef())
	if err != nil {
		return ReadOnlyDatabase{}, dsess.InitialDbState{}, err
	}

	name := srcDb.Name() + dbRevisionDelimiter + revSpec
	db := ReadOnlyDatabase{Database: Database{
		name:     name,
		ddb:      srcDb.ddb,
		rsw:      srcDb.rsw,
		rsr:      srcDb.rsr,
		drw:      srcDb.drw,
		gs:       nil,
		editOpts: srcDb.editOpts,
	}}
	init := dsess.InitialDbState{
		Db:           db,
		HeadCommit:   cm,
		ReadOnly:     true,
		DetachedHead: true,
		DbData: env.DbData{
			Ddb: srcDb.ddb,
			Rsw: srcDb.rsw,
			Rsr: srcDb.rsr,
			Drw: srcDb.drw,
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
