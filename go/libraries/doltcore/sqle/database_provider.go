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
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	dbRevisionDelimiter = "/"

	enableDbRevisionsEnvKey = "DOLT_ENABLE_DB_REVISIONS"
)

var dbRevisionsEnabled = false

func init() {
	val, ok := os.LookupEnv(enableDbRevisionsEnvKey)
	if ok {
		if strings.ToLower(val) == "true" {
			dbRevisionsEnabled = true
		}
	}
}

type RevisionDatabaseProvider interface {
	RevisionDbState(ctx context.Context, revDB string) (dsess.InitialDbState, error)
}

type DoltDatabaseProvider struct {
	databases map[string]sql.Database
}

var _ sql.DatabaseProvider = DoltDatabaseProvider{}
var _ sql.MutableDatabaseProvider = DoltDatabaseProvider{}
var _ RevisionDatabaseProvider = DoltDatabaseProvider{}

func NewDoltDatabaseProvider(databases ...Database) DoltDatabaseProvider {
	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[db.Name()] = db
	}

	return DoltDatabaseProvider{databases: dbs}
}

func (p DoltDatabaseProvider) Database(name string) (db sql.Database, err error) {
	var ok bool
	if db, ok = p.databases[name]; ok {
		return db, nil
	}

	if dbRevisionsEnabled { // feature flagged
		db, ok, err = p.databaseForRevision(context.Background(), name)
		if err != nil {
			return nil, err
		}
		if ok {
			p.databases[name] = db
			return db, nil
		}
	}

	return nil, sql.ErrDatabaseNotFound.New(name)
}

func (p DoltDatabaseProvider) HasDatabase(name string) bool {
	_, err := p.Database(name)
	return err == nil
}

func (p DoltDatabaseProvider) AllDatabases() (all []sql.Database) {
	i := 0
	all = make([]sql.Database, len(p.databases))
	for _, db := range p.databases {
		all[i] = db
		i++
	}
	return
}

func (p DoltDatabaseProvider) AddDatabase(db sql.Database) {
	p.databases[db.Name()] = db
}

func (p DoltDatabaseProvider) DropDatabase(name string) {
	delete(p.databases, name)
}

func (p DoltDatabaseProvider) databaseForRevision(ctx context.Context, revDB string) (sql.Database, bool, error) {
	if !strings.Contains(revDB, dbRevisionDelimiter) {
		return nil, false, nil
	}

	parts := strings.SplitN(revDB, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	candidate, ok := p.databases[dbName]
	if !ok {
		return nil, false, nil
	}
	srcDb, ok := candidate.(Database)
	if !ok {
		return nil, false, nil
	}

	if isBranch(ctx, srcDb.ddb, revSpec) {
		// if the requested revision is a br we can
		// write to it, otherwise make read-only
		db, _, err := dbRevisionForBranch(ctx, srcDb, revSpec)
		if err != nil {
			return nil, false, err
		}
		return db, true, nil
	}

	if doltdb.IsValidCommitHash(revSpec) {
		db, _, err := dbRevisionForCommit(ctx, srcDb, revSpec)
		if err != nil {
			return nil, false, err
		}
		return db, true, nil
	}

	return nil, false, nil
}

func (p DoltDatabaseProvider) RevisionDbState(ctx context.Context, revDB string) (dsess.InitialDbState, error) {
	err := sql.ErrDatabaseNotFound.New(revDB)
	if !strings.Contains(revDB, dbRevisionDelimiter) {
		return dsess.InitialDbState{}, err
	}

	parts := strings.SplitN(revDB, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	candidate, ok := p.databases[dbName]
	if !ok {
		return dsess.InitialDbState{}, err
	}
	srcDb, ok := candidate.(Database)
	if !ok {
		return dsess.InitialDbState{}, err
	}

	if isBranch(ctx, srcDb.ddb, revSpec) {
		// if the requested revision is a br we can
		// write to it, otherwise make read-only
		_, init, err := dbRevisionForBranch(ctx, srcDb, revSpec)
		if err != nil {
			return dsess.InitialDbState{}, err
		}
		return init, nil
	}

	if doltdb.IsValidCommitHash(revSpec) {
		_, init, err := dbRevisionForCommit(ctx, srcDb, revSpec)
		if err != nil {
			return dsess.InitialDbState{}, err
		}
		return init, nil
	}

	return dsess.InitialDbState{}, err
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
		name: dbName,
		ddb:  srcDb.ddb,
		rsw:  static,
		rsr:  static,
		drw:  static,
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
		name: name,
		ddb:  srcDb.ddb,
		rsw:  srcDb.rsw,
		rsr:  srcDb.rsr,
		drw:  srcDb.drw,
	}}
	init := dsess.InitialDbState{
		Db:         db,
		HeadCommit: cm,
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
