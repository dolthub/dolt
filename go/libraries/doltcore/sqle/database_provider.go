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
	DatabaseAtRevision(name string) (sql.Database, InitialDbState, bool)
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
		db, _, ok = p.DatabaseAtRevision(name)
		if ok {
			p.databases[name] = db
			return db, err
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

func (p DoltDatabaseProvider) DatabaseAtRevision(name string) (db sql.Database, init InitialDbState, ok bool) {
	ctx := context.Background()

	if !strings.Contains(name, dbRevisionDelimiter) {
		return nil, InitialDbState{}, false
	}

	parts := strings.SplitN(name, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	candidate, ok := p.databases[dbName]
	if !ok {
		return nil, InitialDbState{}, false
	}
	srcDb, ok := candidate.(Database)
	if !ok {
		return nil, InitialDbState{}, false
	}

	var err error
	if br, ok := branchFromRevSpec(ctx, srcDb.ddb, revSpec); ok {
		// if the requested revision is a branch we can
		// write to it, otherwise make read-only
		db, init, err = dbRevisionForBranch(ctx, srcDb, br)
	} else if doltdb.IsValidCommitHash(revSpec) {
		db, init, err = dbRevisionForCommit(ctx, srcDb, revSpec)
	}
	if err != nil {
		return nil, InitialDbState{}, false
	}

	return db, init, ok
}

func branchFromRevSpec(ctx context.Context, ddb *doltdb.DoltDB, revSpec string) (ref.BranchRef, bool) {
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return ref.BranchRef{}, false
	}

	for _, br := range branches {
		if revSpec == br.String() {
			return br.(ref.BranchRef), true
		}
	}

	return ref.BranchRef{}, false
}

func dbRevisionForBranch(ctx context.Context, srcDb Database, branch ref.BranchRef) (Database, InitialDbState, error) {
	cm, err := srcDb.ddb.ResolveCommitRef(ctx, branch)
	if err != nil {
		return Database{}, InitialDbState{}, err
	}

	wsRef, err := ref.WorkingSetRefForHead(branch)
	if err != nil {
		return Database{}, InitialDbState{}, err
	}

	ws, err := srcDb.ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return Database{}, InitialDbState{}, err
	}
	root := ws.RootValue()

	name := srcDb.Name() + dbRevisionDelimiter + branch.String()
	db := Database{
		name: name,
		ddb:  srcDb.ddb,
		rsr:  srcDb.rsr,
		rsw:  srcDb.rsw,
		drw:  srcDb.drw,
	}

	init := InitialDbState{
		Db:          db,
		HeadCommit:  cm,
		WorkingRoot: root,
		DbData: env.DbData{
			Ddb: srcDb.ddb,
			Rsw: srcDb.rsw,
			Rsr: srcDb.rsr,
			Drw: srcDb.drw,
		},
	}

	return db, init, nil
}

func dbRevisionForCommit(ctx context.Context, srcDb Database, revSpec string) (ReadOnlyDatabase, InitialDbState, error) {
	spec, err := doltdb.NewCommitSpec(revSpec)
	if err != nil {
		return ReadOnlyDatabase{}, InitialDbState{}, err
	}

	cm, err := srcDb.ddb.Resolve(ctx, spec, srcDb.rsr.CWBHeadRef())
	if err != nil {
		return ReadOnlyDatabase{}, InitialDbState{}, err
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return ReadOnlyDatabase{}, InitialDbState{}, err
	}

	name := srcDb.Name() + dbRevisionDelimiter + revSpec
	db := ReadOnlyDatabase{Database: Database{
		name: name,
		ddb:  srcDb.ddb,
		rsr:  srcDb.rsr,
		rsw:  srcDb.rsw,
		drw:  srcDb.drw,
	}}

	init := InitialDbState{
		Db:          db,
		HeadCommit:  cm,
		WorkingRoot: root,
		DbData: env.DbData{
			Ddb: srcDb.ddb,
			Rsw: srcDb.rsw,
			Rsr: srcDb.rsr,
			Drw: srcDb.drw,
		},
	}

	return db, init, nil
}
