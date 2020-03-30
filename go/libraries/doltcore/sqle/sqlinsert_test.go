// Copyright 2019 Liquidata, Inc.
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

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleInsertQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenInsert = true

func TestExecuteInsert(t *testing.T) {
	for _, test := range BasicInsertTests {
		t.Run(test.Name, func(t *testing.T) {
			testInsertQuery(t, test)
		})
	}
}

var systemTableInsertTests = []InsertTest{
	{
		Name: "insert into dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			env.DoltDocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		InsertQuery: "insert into dolt_docs (doc_name, doc_text) values ('README.md', 'Some text')",
		ExpectedErr: "cannot insert into table",
	},
	{
		Name: "insert into dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			DoltQueryCatalogSchema,
			NewRowWithSchema(row.TaggedValues{
				doltdb.QueryCatalogIdTag:          types.String("existingEntry"),
				doltdb.QueryCatalogOrderTag:       types.Uint(2),
				doltdb.QueryCatalogNameTag:        types.String("example"),
				doltdb.QueryCatalogQueryTag:       types.String("select 2+2 from dual"),
				doltdb.QueryCatalogDescriptionTag: types.String("description")},
				DoltQueryCatalogSchema)),
		InsertQuery: "insert into dolt_query_catalog (id, display_order, name, query, description) values ('abc123', 1, 'example', 'select 1+1 from dual', 'description')",
		SelectQuery: "select * from dolt_query_catalog",
		ExpectedRows: CompressRows(DoltQueryCatalogSchema,
			NewRowWithSchema(row.TaggedValues{
				doltdb.QueryCatalogIdTag:          types.String("abc123"),
				doltdb.QueryCatalogOrderTag:       types.Uint(1),
				doltdb.QueryCatalogNameTag:        types.String("example"),
				doltdb.QueryCatalogQueryTag:       types.String("select 1+1 from dual"),
				doltdb.QueryCatalogDescriptionTag: types.String("description")},
				DoltQueryCatalogSchema),
			NewRowWithSchema(row.TaggedValues{
				doltdb.QueryCatalogIdTag:          types.String("existingEntry"),
				doltdb.QueryCatalogOrderTag:       types.Uint(2),
				doltdb.QueryCatalogNameTag:        types.String("example"),
				doltdb.QueryCatalogQueryTag:       types.String("select 2+2 from dual"),
				doltdb.QueryCatalogDescriptionTag: types.String("description")},
				DoltQueryCatalogSchema),
		),
		ExpectedSchema: CompressSchema(DoltQueryCatalogSchema),
	},
	{
		Name:            "insert into dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName, schemasTableDoltSchema()),
		InsertQuery:     "insert into dolt_schemas (type, name, fragment) values ('view', 'name', 'select 2+2 from dual')",
		SelectQuery:     "select * from dolt_schemas",
		ExpectedRows: []row.Row{NewRowWithSchema(row.TaggedValues{
			doltdb.DoltSchemasTypeTag:     types.String("view"),
			doltdb.DoltSchemasNameTag:     types.String("name"),
			doltdb.DoltSchemasFragmentTag: types.String("select 2+2 from dual"),
		}, schemasTableDoltSchema())},
		ExpectedSchema: schemasTableDoltSchema(),
	},
}

func mustGetDoltSchema(sch sql.Schema, tableName string, testEnv *env.DoltEnv) schema.Schema {
	wrt, err := testEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}

	doltSchema, err := SqlSchemaToDoltSchema(context.Background(), wrt, tableName, sch)
	if err != nil {
		panic(err)
	}
	return doltSchema
}

func TestInsertIntoSystemTables(t *testing.T) {
	for _, test := range systemTableInsertTests {
		t.Run(test.Name, func(t *testing.T) {
			testInsertQuery(t, test)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testInsertQuery(t *testing.T, test InsertTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleInsertQueryTest) > 0 && test.Name != singleInsertQueryTest {
		t.Skip("Skipping tests until " + singleInsertQueryTest)
	}

	if len(singleInsertQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenInsert {
		t.Skip("Skipping test broken on SQL engine")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateEmptyTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	var err error
	root, _ := dEnv.WorkingRoot(context.Background())
	root, err = executeModify(context.Background(), root, test.InsertQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(context.Background(), dEnv, test.ExpectedSchema, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, test.ExpectedRows, actualRows)
	assert.Equal(t, test.ExpectedSchema, sch)
}
