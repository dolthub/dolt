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
const singleReplaceQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenReplace = true

func TestExecuteReplace(t *testing.T) {
	for _, test := range BasicReplaceTests {
		t.Run(test.Name, func(t *testing.T) {
			testReplaceQuery(t, test)
		})
	}
}

var systemTableReplaceTests = []ReplaceTest{
	{
		Name: "replace into dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			env.DoltDocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		ReplaceQuery: "replace into dolt_docs (doc_name, doc_text) values ('README.md', 'Some text')",
		ExpectedErr:  "cannot insert into table",
	},
	{
		Name: "replace into dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			DoltQueryCatalogSchema,
			NewRow(types.String("existingEntry"), types.Uint(1), types.String("example"), types.String("select 2+2 from dual"), types.String("description"))),
		ReplaceQuery: "replace into dolt_query_catalog (id, display_order, name, query, description) values ('existingEntry', 1, 'example', 'select 1+1 from dual', 'description')",
		SelectQuery:  "select * from dolt_query_catalog",
		ExpectedRows: CompressRows(DoltQueryCatalogSchema,
			NewRow(types.String("existingEntry"), types.Uint(1), types.String("example"), types.String("select 1+1 from dual"), types.String("description")),
		),
		ExpectedSchema: CompressSchema(DoltQueryCatalogSchema),
	},
	{
		Name: "replace into dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			schemasTableDoltSchema(),
			NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual"))),
		ReplaceQuery: "replace into dolt_schemas (type, name, fragment) values ('view', 'name', 'select 1+1 from dual')",
		SelectQuery:  "select * from dolt_schemas",
		ExpectedRows: CompressRows(schemasTableDoltSchema(),
			NewRow(types.String("view"), types.String("name"), types.String("select 1+1 from dual")),
		),
		ExpectedSchema: CompressSchema(schemasTableDoltSchema()),
	},
}

func TestReplaceIntoSystemTables(t *testing.T) {
	for _, test := range systemTableInsertTests {
		t.Run(test.Name, func(t *testing.T) {
			testInsertQuery(t, test)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testReplaceQuery(t *testing.T, test ReplaceTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleReplaceQueryTest) > 0 && test.Name != singleReplaceQueryTest {
		t.Skip("Skipping tests until " + singleReplaceQueryTest)
	}

	if len(singleReplaceQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenReplace {
		t.Skip("Skipping test broken on SQL engine")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateEmptyTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	var err error
	root, _ := dEnv.WorkingRoot(context.Background())
	root, err = executeModify(context.Background(), dEnv.DoltDB, root, test.ReplaceQuery)
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
