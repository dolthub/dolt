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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleUpdateQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenUpdate = true

func TestExecuteUpdate(t *testing.T) {
	for _, test := range BasicUpdateTests {
		t.Run(test.Name, func(t *testing.T) {
			testUpdateQuery(t, test)
		})
	}
}

func TestExecuteUpdateSystemTables(t *testing.T) {
	for _, test := range systemTableUpdateTests {
		t.Run(test.Name, func(t *testing.T) {
			testUpdateQuery(t, test)
		})
	}
}

var systemTableUpdateTests = []UpdateTest{
	{
		Name: "update dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			env.DoltDocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		UpdateQuery: "update dolt_docs set doc_text = 'Some text')",
		ExpectedErr: "cannot insert into table",
	},
	{
		Name: "update dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			DoltQueryCatalogSchema,
			NewRowWithSchema(row.TaggedValues{
				schema.QueryCatalogIdTag: types.String("abc123"),
				schema.QueryCatalogOrderTag: types.Uint(1),
				schema.QueryCatalogNameTag: types.String("example"),
				schema.QueryCatalogQueryTag: types.String("select 2+2 from dual"),
				schema.QueryCatalogDescriptionTag: types.String("description")},
				DoltQueryCatalogSchema)),
		UpdateQuery: "update dolt_query_catalog set display_order = display_order + 1",
		SelectQuery: "select * from dolt_query_catalog",
		ExpectedRows: CompressRows(DoltQueryCatalogSchema,
			NewRowWithSchema(row.TaggedValues{
				schema.QueryCatalogIdTag: types.String("abc123"),
				schema.QueryCatalogOrderTag: types.Uint(2),
				schema.QueryCatalogNameTag: types.String("example"),
				schema.QueryCatalogQueryTag: types.String("select 2+2 from dual"),
				schema.QueryCatalogDescriptionTag: types.String("description")},
				DoltQueryCatalogSchema)),
		ExpectedSchema: CompressSchema(DoltQueryCatalogSchema),
	},
	{
		Name: "update dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			schemasTableDoltSchema(),
			NewRowWithSchema(row.TaggedValues{
				schema.DoltSchemasTypeTag: types.String("view"),
				schema.DoltSchemasNameTag: types.String("name"),
				schema.DoltSchemasFragmentTag: types.String("select 2+2 from dual"),
			}, schemasTableDoltSchema())),
		UpdateQuery: "update dolt_schemas set type = 'not a view'",
		SelectQuery: "select * from dolt_schemas",
		ExpectedRows: []row.Row{NewRowWithSchema(row.TaggedValues{
				schema.DoltSchemasTypeTag: types.String("not a view"),
				schema.DoltSchemasNameTag: types.String("name"),
				schema.DoltSchemasFragmentTag: types.String("select 2+2 from dual"),
			}, schemasTableDoltSchema())},
		ExpectedSchema: schemasTableDoltSchema(),
	},
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testUpdateQuery(t *testing.T, test UpdateTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleUpdateQueryTest) > 0 && test.Name != singleUpdateQueryTest {
		t.Skip("Skipping tests until " + singleUpdateQueryTest)
	}

	if len(singleUpdateQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenUpdate {
		t.Skip("Skipping test broken on SQL engine")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	var err error
	root, _ := dEnv.WorkingRoot(context.Background())
	root, err = executeModify(context.Background(), root, test.UpdateQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(context.Background(), dEnv, test.ExpectedSchema, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, test.ExpectedSchema, sch)
	assert.Equal(t, test.ExpectedRows, actualRows)
}
