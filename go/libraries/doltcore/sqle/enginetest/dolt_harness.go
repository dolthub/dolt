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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
)

type DoltHarness struct {
	t              *testing.T
	session        *sqle.DoltSession
	databases      []sqle.Database
	parallelism    int
	skippedQueries []string
}

var _ enginetest.Harness = (*DoltHarness)(nil)
var _ enginetest.SkippingHarness = (*DoltHarness)(nil)
var _ enginetest.IndexHarness = (*DoltHarness)(nil)
var _ enginetest.VersionedDBHarness = (*DoltHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DoltHarness)(nil)
var _ enginetest.KeylessTableHarness = (*DoltHarness)(nil)

func newDoltHarness(t *testing.T) *DoltHarness {
	session, err := sqle.NewDoltSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
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
	session, err := sqle.NewDoltSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(d.t, err)

	ctx := sql.NewContext(
		context.Background(),
		sql.WithSession(session))

	for _, db := range d.databases {
		err := session.AddDB(ctx, db, db.DbData())
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

	// TODO: it should be safe to reuse a session with a new database, but it isn't in all cases. Particularly, if you
	//  have a database that only ever receives read queries, and then you re-use its session for a new database with
	//  the same name, the first write query will panic on dangling references in the noms layer. Not sure why this is
	//  happening, but it only happens as a result of this test setup.
	var err error
	d.session, err = sqle.NewDoltSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(d.t, err)

	var dbs []sql.Database
	for _, name := range names {
		db := sqle.NewDatabase(name, dEnv.DbData())
		require.NoError(d.t, d.session.AddDB(enginetest.NewContext(d), db, db.DbData()))
		dbs = append(dbs, db)
	}
	return dbs
}

func (d *DoltHarness) NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error) {
	doltDatabase := db.(sqle.Database)
	err := doltDatabase.CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema)
	if err != nil {
		return nil, err
	}

	table, ok, err := doltDatabase.GetTableInsensitive(enginetest.NewContext(d).WithCurrentDB(db.Name()), name)

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
	ddb := db.(sqle.Database)
	e := enginetest.NewEngineWithDbs(d.t, d, []sql.Database{db}, nil)

	if _, err := e.Catalog.FunctionRegistry.Function(dfunctions.CommitFuncName); sql.ErrFunctionNotFound.Is(err) {
		require.NoError(d.t,
			e.Catalog.FunctionRegistry.Register(sql.FunctionN{Name: dfunctions.CommitFuncName, Fn: dfunctions.NewCommitFunc}))
	}

	asOfString, ok := asOf.(string)
	require.True(d.t, ok)
	ctx := enginetest.NewContext(d)
	_, iter, err := e.Query(ctx,
		"set @@"+sqle.HeadKey(ddb.Name())+" = COMMIT('-m', 'test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(d.t, err)

	ctx = enginetest.NewContext(d)
	_, iter, err = e.Query(ctx,
		"insert into dolt_branches (name, hash) values ('"+asOfString+"', @@"+sqle.HeadKey(ddb.Name())+")")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(d.t, err)

	return nil
}
