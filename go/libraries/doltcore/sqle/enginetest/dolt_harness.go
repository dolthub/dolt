// Copyright 2020 Liquidata, Inc.
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
)

type DoltHarness struct {
	t           *testing.T
	session     *sqle.DoltSession
	mrEnv       env.MultiRepoEnv
	parallelism int
}

var _ enginetest.Harness = (*DoltHarness)(nil)
var _ enginetest.SkippingHarness = (*DoltHarness)(nil)
var _ enginetest.IndexHarness = (*DoltHarness)(nil)
var _ enginetest.VersionedDBHarness = (*DoltHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DoltHarness)(nil)

func newDoltHarness(t *testing.T) *DoltHarness {
	session, err := sqle.NewDoltSession(context.Background(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(t, err)
	return &DoltHarness{
		t:       t,
		session: session,
		mrEnv:   make(env.MultiRepoEnv),
	}
}

// WithParallelism returns a copy of the harness with parallelism set to the given number of threads. A value of 0 or
// less means to use the system parallelism settings.
func (d *DoltHarness) WithParallelism(parallelism int) *DoltHarness {
	nd := *d
	nd.parallelism = parallelism
	return &nd
}

// Logic to skip unsupported queries
func (d *DoltHarness) SkipQueryTest(query string) bool {
	lowerQuery := strings.ToLower(query)
	return strings.Contains(lowerQuery, "typestable") || // we don't support all the required types
		strings.Contains(lowerQuery, "show full columns") || // we set extra comment info
		lowerQuery == "show variables" || // we set extra variables
		strings.Contains(lowerQuery, "show create table") || // we set extra comment info
		strings.Contains(lowerQuery, "show indexes from") || // we create / expose extra indexes (for foreign keys)
		strings.Contains(lowerQuery, "on duplicate key update") // not working yet
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
		sql.WithSession(d.session),
		sql.WithViewRegistry(sql.NewViewRegistry()),
	)
}

func (d *DoltHarness) SupportsNativeIndexCreation() bool {
	return true
}

func (d *DoltHarness) SupportsForeignKeys() bool {
	return true
}

func (d *DoltHarness) NewDatabase(name string) sql.Database {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(enginetest.NewContext(d))
	require.NoError(d.t, err)

	d.mrEnv.AddEnv(name, dEnv)
	db := sqle.NewDatabase(name, dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	require.NoError(d.t, d.session.AddDB(enginetest.NewContext(d), db))
	require.NoError(d.t, db.SetRoot(enginetest.NewContext(d).WithCurrentDB(db.Name()), root))
	return db
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
			e.Catalog.FunctionRegistry.Register(sql.Function1{Name: dfunctions.CommitFuncName, Fn: dfunctions.NewCommitFunc}))
	}

	asOfString, ok := asOf.(string)
	require.True(d.t, ok)

	_, iter, err := e.Query(enginetest.NewContext(d),
		"set @@"+ddb.HeadKey()+" = COMMIT('test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(d.t, err)

	_, iter, err = e.Query(enginetest.NewContext(d),
		"insert into dolt_branches (name, hash) values ('"+asOfString+"', @@"+ddb.HeadKey()+")")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(d.t, err)

	return nil
}
