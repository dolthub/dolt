// Copyright 2019 Dolthub, Inc.
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
	"testing"

	sql "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleDeleteQueryTest = "" //"Natural join with join clause"

// Structure for a test of a delete query
type DeleteTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The delete query to run
	DeleteQuery string
	// The select query to run to verify the results
	SelectQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows this query should return, nil if an error is expected
	ExpectedRows []sql.Row
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
}

// BasicDeleteTests cover basic delete statement features and error handling
var BasicDeleteTests = []DeleteTest{
	{
		Name:           "delete everything",
		DeleteQuery:    "delete from people",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id equals",
		DeleteQuery:    "delete from people where id = 2",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id less than",
		DeleteQuery:    "delete from people where id < 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id greater than",
		DeleteQuery:    "delete from people where id > 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id less than or equal",
		DeleteQuery:    "delete from people where id <= 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id greater than or equal",
		DeleteQuery:    "delete from people where id >= 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where id equals nothing",
		DeleteQuery:    "delete from people where id = 9999",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some =",
		DeleteQuery:    "delete from people where last_name = 'Simpson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some <>",
		DeleteQuery:    "delete from people where last_name <> 'Simpson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete where last_name matches some like",
		DeleteQuery:    "delete from people where last_name like '%pson'",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by",
		DeleteQuery:    "delete from people order by id",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by asc limit",
		DeleteQuery:    "delete from people order by id asc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by desc limit",
		DeleteQuery:    "delete from people order by id desc limit 3",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "delete order by desc limit",
		DeleteQuery:    "delete from people order by id desc limit 3 offset 1",
		SelectQuery:    "select * from people",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "delete invalid table",
		DeleteQuery: "delete from nobody",
		ExpectedErr: "invalid table",
	},
	{
		Name:        "delete invalid column",
		DeleteQuery: "delete from people where z = 'dne'",
		ExpectedErr: "invalid column",
	},
	{
		Name:        "delete negative limit",
		DeleteQuery: "delete from people limit -1",
		ExpectedErr: "invalid limit number",
	},
	{
		Name:        "delete negative offset",
		DeleteQuery: "delete from people limit 1 offset -1",
		ExpectedErr: "invalid limit number",
	},
}

func TestExecuteDelete(t *testing.T) {
	for _, test := range BasicDeleteTests {
		t.Run(test.Name, func(t *testing.T) {
			testDeleteQuery(t, test)
		})
	}
}

func TestExecuteDeleteSystemTables(t *testing.T) {
	for _, test := range systemTableDeleteTests {
		t.Run(test.Name, func(t *testing.T) {
			testDeleteQuery(t, test)
		})
	}
}

var systemTableDeleteTests = []DeleteTest{
	{
		Name: "delete dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			doltdocs.DocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		DeleteQuery: "delete from dolt_docs",
		ExpectedErr: "cannot delete from table",
	},
	{
		Name: "delete dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			dtables.DoltQueryCatalogSchema,
			NewRow(types.String("abc123"), types.Uint(1), types.String("example"), types.String("select 2+2 from dual"), types.String("description"))),
		DeleteQuery:    "delete from dolt_query_catalog",
		SelectQuery:    "select * from dolt_query_catalog",
		ExpectedRows:   ToSqlRows(dtables.DoltQueryCatalogSchema),
		ExpectedSchema: CompressSchema(dtables.DoltQueryCatalogSchema),
	},
	{
		Name: "delete dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			SchemasTableSchema(),
			NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual"))),
		DeleteQuery:    "delete from dolt_schemas",
		SelectQuery:    "select * from dolt_schemas",
		ExpectedRows:   ToSqlRows(dtables.DoltQueryCatalogSchema),
		ExpectedSchema: SchemasTableSchema(),
	},
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testDeleteQuery(t *testing.T, test DeleteTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleDeleteQueryTest) > 0 && test.Name != singleDeleteQueryTest {
		t.Skip("Skipping tests until " + singleDeleteQueryTest)
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	var err error
	root, _ := dEnv.WorkingRoot(context.Background())
	root, err = executeModify(t, context.Background(), dEnv, root, test.DeleteQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(t, context.Background(), dEnv, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, test.ExpectedRows, actualRows)
	assertSchemasEqual(t, mustSqlSchema(test.ExpectedSchema), sch)
}
