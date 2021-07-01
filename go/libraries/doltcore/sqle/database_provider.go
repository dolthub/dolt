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
)

const (
	dbRevisionDelimiter = "/"

	dbRevisionsEnabledSysVar = "dolt_db_revisions_enabled"
)

var dbRevisionsEnabled = false

func init() {
	val, ok := os.LookupEnv(dbRevisionsEnabledSysVar)
	if ok {
		if strings.ToLower(val) == "true" {
			dbRevisionsEnabled = true
		}
	}
}

type DoltDatabaseProvider struct {
	databases   map[string]sql.Database
	revisionDBs map[string]Database
}

var _ sql.DatabaseProvider = DoltDatabaseProvider{}

func NewDoltDatabaseProvider(databases ...Database) sql.MutableDatabaseProvider {
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
		db, _, ok = p.databaseAtRevision(name)
		if ok {
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

func (p DoltDatabaseProvider) databaseAtRevision(name string) (sql.Database, *doltdb.RootValue, bool) {
	ctx := context.Background()

	if !strings.Contains(name, dbRevisionDelimiter) {
		return nil, nil, false
	}

	parts := strings.SplitN(name, dbRevisionDelimiter, 2)
	dbName, revSpec := parts[0], parts[1]

	candidate, ok := p.databases[dbName]
	if !ok {
		return nil, nil, false
	}
	db, ok := candidate.(Database)
	if !ok {
		return nil, nil, false
	}

	readOnly := true
	if isBranch(ctx, db.ddb, revSpec) {
		// if the requested revision is a branch we can
		// write to it, otherwise make read-only
		readOnly = false
	}

	var root *doltdb.RootValue
	db, root, ok = makeDatabaseAtRevision(ctx, db, revSpec)
	if !ok {
		return nil, nil, false
	}

	if readOnly {
		return ReadOnlyDatabase{Database: db}, root, ok
	}
	return db, nil, ok
}

func isBranch(ctx context.Context, ddb *doltdb.DoltDB, name string) bool {
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return false
	}

	for _, branch := range branches {
		if name == branch.String() {
			return true
		}
	}
	return false
}

func makeDatabaseAtRevision(ctx context.Context, srcDb Database, revSpec string) (Database, *doltdb.RootValue, bool) {
	spec, err := doltdb.NewCommitSpec(revSpec)
	if err != nil {
		return Database{}, nil, false
	}

	cm, err := srcDb.ddb.Resolve(ctx, spec, srcDb.rsr.CWBHeadRef())
	if err != nil {
		return Database{}, nil, false
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return Database{}, nil, false
	}

	db := Database{
		name: srcDb.Name() + dbRevisionDelimiter + revSpec,
		ddb:  srcDb.ddb,
		rsr:  srcDb.rsr,
		rsw:  srcDb.rsw,
		drw:  srcDb.drw,
	}

	return db, root, true
}
