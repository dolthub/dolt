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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleDeleteQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenDelete = true

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
			env.DoltDocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		DeleteQuery: "delete from dolt_docs",
		ExpectedErr: "cannot delete from table",
	},
	{
		Name: "delete dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			DoltQueryCatalogSchema,
			NewRow(types.String("abc123"), types.Uint(1), types.String("example"), types.String("select 2+2 from dual"), types.String("description"))),
		DeleteQuery:    "delete from dolt_query_catalog",
		SelectQuery:    "select * from dolt_query_catalog",
		ExpectedRows:   CompressRows(DoltQueryCatalogSchema),
		ExpectedSchema: CompressSchema(DoltQueryCatalogSchema),
	},
	{
		Name: "delete dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			mustGetDoltSchema(SchemasTableSchema()),
			NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual"))),
		DeleteQuery:    "delete from dolt_schemas",
		SelectQuery:    "select * from dolt_schemas",
		ExpectedRows:   CompressRows(mustGetDoltSchema(SchemasTableSchema())),
		ExpectedSchema: CompressSchema(mustGetDoltSchema(SchemasTableSchema())),
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

	if len(singleDeleteQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenDelete {
		t.Skip("Skipping test broken on SQL engine")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	var err error
	root, _ := dEnv.WorkingRoot(context.Background())
	root, err = executeModify(context.Background(), dEnv.DoltDB, root, test.DeleteQuery)
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
