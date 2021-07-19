// Copyright 2020 Dolthub, Inc.
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

package enginetest

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
)

type DoltHarness struct {
	t                    *testing.T
	env                  *env.DoltEnv
	session              *dsess.Session
	databases            []sqle.Database
	databaseGlobalStates []globalstate.GlobalState
	parallelism          int
	skippedQueries       []string
}

var _ enginetest.Harness = (*DoltHarness)(nil)
var _ enginetest.SkippingHarness = (*DoltHarness)(nil)
var _ enginetest.IndexHarness = (*DoltHarness)(nil)
var _ enginetest.VersionedDBHarness = (*DoltHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DoltHarness)(nil)
var _ enginetest.KeylessTableHarness = (*DoltHarness)(nil)
var _ enginetest.ReadOnlyDatabaseHarness = (*DoltHarness)(nil)

func newDoltHarness(t *testing.T) *DoltHarness {
	session, err := dsess.NewSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(t, err)
	return &DoltHarness{
		t:              t,
		session:        session,
		skippedQueries: defaultSkippedQueries,
	}
}

var defaultSkippedQueries = []string{
	"show variables",           // we set extra variables
	"show create table fk_tbl", // we create an extra key for the FK that vanilla gms does not
	"show indexes from",        // we create / expose extra indexes (for foreign keys)
	"json_arrayagg",            // TODO: aggregation ordering
	"json_objectagg",           // TODO: aggregation ordering
	"typestable",               // Bit type isn't working?
	"dolt_commit_diff_",        // see broken queries in `dolt_system_table_queries.go`
}

// WithParallelism returns a copy of the harness with parallelism set to the given number of threads. A value of 0 or
// less means to use the system parallelism settings.
func (d DoltHarness) WithParallelism(parallelism int) *DoltHarness {
	d.parallelism = parallelism
	return &d
}

// WithSkippedQueries returns a copy of the harness with the given queries skipped
func (d DoltHarness) WithSkippedQueries(queries []string) *DoltHarness {
	d.skippedQueries = queries
	return &d
}

// SkipQueryTest returns whether to skip a query
func (d *DoltHarness) SkipQueryTest(query string) bool {
	lowerQuery := strings.ToLower(query)
	for _, skipped := range d.skippedQueries {
		if strings.Contains(lowerQuery, strings.ToLower(skipped)) {
			return true
		}
	}

	return false
}

func (d *DoltHarness) Parallelism() int {
	if d.parallelism <= 0 {

		// always test with some parallelism
		parallelism := runtime.NumCPU()

		if parallelism <= 1 {
			parallelism = 2
		}

		return parallelism
	}

	return d.parallelism
}

func (d *DoltHarness) NewContext() *sql.Context {
	return sql.NewContext(
		context.Background(),
		sql.WithSession(d.session))
}

func (d DoltHarness) NewSession() *sql.Context {
	session, err := dsess.NewSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(d.t, err)

	ctx := sql.NewContext(
		context.Background(),
		sql.WithSession(session))

	for i, db := range d.databases {
		dbState := getDbState(d.t, db, d.env, d.databaseGlobalStates[i])
		err := session.AddDB(ctx, dbState)
		require.NoError(d.t, err)
	}

	return ctx
}

func (d *DoltHarness) SupportsNativeIndexCreation() bool {
	return true
}

func (d *DoltHarness) SupportsForeignKeys() bool {
	return true
}

func (d *DoltHarness) SupportsKeylessTables() bool {
	return true
}

func (d *DoltHarness) NewDatabase(name string) sql.Database {
	return d.NewDatabases(name)[0]
}

func (d *DoltHarness) NewDatabases(names ...string) []sql.Database {
	dEnv := dtestutils.CreateTestEnv()
	d.env = dEnv

	// TODO: it should be safe to reuse a session with a new database, but it isn't in all cases. Particularly, if you
	//  have a database that only ever receives read queries, and then you re-use its session for a new database with
	//  the same name, the first write query will panic on dangling references in the noms layer. Not sure why this is
	//  happening, but it only happens as a result of this test setup.
	var err error
	d.session, err = dsess.NewSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(d.t, err)

	var dbs []sql.Database
	d.databases = nil
	d.databaseGlobalStates = nil
	for _, name := range names {
		db := sqle.NewDatabase(name, dEnv.DbData())
		globalState := globalstate.NewGlobalStateStore()
		dbState := getDbState(d.t, db, dEnv, globalState)
		require.NoError(d.t, d.session.AddDB(enginetest.NewContext(d), dbState))
		dbs = append(dbs, db)
		d.databases = append(d.databases, db)
		d.databaseGlobalStates = append(d.databaseGlobalStates, globalState)
	}

	return dbs
}

func (d *DoltHarness) NewReadOnlyDatabases(names ...string) (dbs []sql.ReadOnlyDatabase) {
	for _, db := range d.NewDatabases(names...) {
		dbs = append(dbs, sqle.ReadOnlyDatabase{Database: db.(sqle.Database)})
	}
	return
}

func getDbState(t *testing.T, db sqle.Database, dEnv *env.DoltEnv, globalState globalstate.GlobalState) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)

	ws, err := dEnv.WorkingSet(ctx)
	require.NoError(t, err)

	return dsess.InitialDbState{
		Db:          db,
		HeadCommit:  headCommit,
		WorkingSet:  ws,
		DbData:      dEnv.DbData(),
		GlobalState: globalState,
	}
}

func (d *DoltHarness) NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error) {
	var err error
	if ro, ok := db.(sqle.ReadOnlyDatabase); ok {
		err = ro.CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema)
	} else {
		err = db.(sqle.Database).CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema)
	}
	if err != nil {
		return nil, err
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d).WithCurrentDB(db.Name()), name)
	require.NoError(d.t, err)
	require.True(d.t, ok, "table %s not found after creation", name)
	return table, nil
}

// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.Schema, asOf interface{}) sql.Table {
	table, err := d.NewTable(db, name, schema)
	if err != nil {
		require.True(d.t, sql.ErrTableAlreadyExists.Is(err))
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d), name)
	require.NoError(d.t, err)
	require.True(d.t, ok)

	return table
}

// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error {
	switch db.(type) {
	case sqle.ReadOnlyDatabase:
		// TODO: insert query to dolt_branches table (below)
		// can't be performed against ReadOnlyDatabase
		d.t.Skip("can't create SnaphotTables for ReadOnlyDatabases")
	case sqle.Database:
	default:
		panic("not a Dolt SQL Database")
	}

	e := enginetest.NewEngineWithDbs(d.t, d, []sql.Database{db}, nil)

	if _, err := e.Catalog.FunctionRegistry.Function(dfunctions.CommitFuncName); sql.ErrFunctionNotFound.Is(err) {
		require.NoError(d.t,
			e.Catalog.FunctionRegistry.Register(sql.FunctionN{Name: dfunctions.CommitFuncName, Fn: dfunctions.NewCommitFunc}))
	}

	asOfString, ok := asOf.(string)
	require.True(d.t, ok)

	ctx := enginetest.NewContext(d)
	_, iter, err := e.Query(ctx,
		"set @@"+dsess.HeadKey(db.Name())+" = COMMIT('-m', 'test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(d.t, err)

	headHash, err := ctx.GetSessionVariable(ctx, dsess.HeadKey(db.Name()))
	require.NoError(d.t, err)

	ctx = enginetest.NewContext(d)
	// TODO: there's a bug in test setup with transactions, where the HEAD session var gets overwritten on transaction
	//  start, so we quote it here instead
	// query := "insert into dolt_branches (name, hash) values ('" + asOfString + "', @@" + dsess.HeadKey(ddb.Name()) + ")"
	query := "insert into dolt_branches (name, hash) values ('" + asOfString + "', '" + headHash.(string) + "')"

	_, iter, err = e.Query(ctx,
		query)
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(d.t, err)

	return nil
}
