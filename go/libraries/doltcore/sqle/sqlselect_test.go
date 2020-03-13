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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/envtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleSelectQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenSelect = true

func TestSelect(t *testing.T) {
	for _, test := range BasicSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectQuery(t, test)
		})
	}
}

func TestDiffQueries(t *testing.T) {
	for _, test := range SelectDiffTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectDiffQuery(t, test)
		})
	}
}

func TestAsOfQueries(t *testing.T) {
	for _, test := range AsOfTests {
		t.Run(test.Name, func(t *testing.T) {
			// AS OF queries use the same history as the diff tests, so exercise the same test setup
			testSelectDiffQuery(t, test)
		})
	}
}


func TestJoins(t *testing.T) {
	for _, tt := range JoinTests {
		t.Run(tt.Name, func(t *testing.T) {
			testSelectQuery(t, tt)
		})
	}
}

// Tests of case sensitivity handling
func TestCaseSensitivity(t *testing.T) {
	for _, tt := range CaseSensitivityTests {
		t.Run(tt.Name, func(t *testing.T) {
			testSelectQuery(t, tt)
		})
	}
}

var systemTableSelectTests = []SelectTest{
	{
		Name: "select from dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			env.DoltDocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		Query: "select * from dolt_docs",
		ExpectedRows: CompressRows(CompressSchema(env.DoltDocsSchema),
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		ExpectedSchema: CompressSchema(env.DoltDocsSchema),
	},
	{
		Name: "select from dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			DoltQueryCatalogSchema,
			NewRow(types.String("existingEntry"), types.Uint(2), types.String("example"), types.String("select 2+2 from dual"), types.String("description"))),
		Query: "select * from dolt_query_catalog",
		ExpectedRows: CompressRows(DoltQueryCatalogSchema,
			NewRow(types.String("existingEntry"), types.Uint(2), types.String("example"), types.String("select 2+2 from dual"), types.String("description")),
		),
		ExpectedSchema: CompressSchema(DoltQueryCatalogSchema),
	},
	{
		Name: "select from dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			mustGetDoltSchema(SchemasTableSchema()),
			NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual"))),
		Query: "select * from dolt_schemas",
		ExpectedRows: CompressRows(mustGetDoltSchema(SchemasTableSchema()),
			NewRow(types.String("view"), types.String("name"), types.String("select 2+2 from dual")),
		),
		ExpectedSchema: CompressSchema(mustGetDoltSchema(SchemasTableSchema())),
	},
}

func TestSelectSystemTables(t *testing.T) {
	for _, test := range systemTableSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectQuery(t, test)
		})
	}
}

type testCommitClock struct {
	unixNano int64
}

func (tcc *testCommitClock) Now() time.Time {
	now := time.Unix(0, tcc.unixNano)
	tcc.unixNano += int64(time.Millisecond)
	return now
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testSelectQuery(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleSelectQueryTest) > 0 && test.Name != singleSelectQueryTest {
		t.Skip("Skipping tests until " + singleSelectQueryTest)
	}

	if len(singleSelectQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenSelect {
		t.Skip("Skipping test broken on SQL engine")
	}

	tcc := &testCommitClock{}
	doltdb.CommitNowFunc = tcc.Now
	doltdb.CommitLoc = time.UTC

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	actualRows, sch, err := executeSelect(context.Background(), dEnv, test.ExpectedSchema, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		// Too much work to synchronize error messages between the two implementations, so for now we'll just assert that an error occurred.
		// require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedRows, actualRows)

	// this is meaningless as executeSelect just returns the schema that is passed in.
	assert.Equal(t, test.ExpectedSchema, sch)
}

func testSelectDiffQuery(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleSelectQueryTest) > 0 && test.Name != singleSelectQueryTest {
		t.Skip("Skipping tests until " + singleSelectQueryTest)
	}

	if len(singleSelectQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenSelect {
		t.Skip("Skipping test broken on SQL engine")
	}

	ctx := context.Background()
	tcc := &testCommitClock{}
	doltdb.CommitNowFunc = tcc.Now
	doltdb.CommitLoc = time.UTC

	dEnv := dtestutils.CreateTestEnv()
	envtestutils.InitializeWithHistory(t, ctx, dEnv, CreateHistory(ctx, dEnv, t)...)
	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	cs, err := doltdb.NewCommitSpec("HEAD", "master")
	require.NoError(t, err)

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)
	require.NoError(t, err)

	root, err := cm.GetRootValue()
	require.NoError(t, err)

	_, err = dEnv.UpdateStagedRoot(ctx, root)
	require.NoError(t, err)

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	root, err = dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)

	root = envtestutils.UpdateTables(t, ctx, root, CreateWorkingRootUpdate())

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	actualRows, sch, err := executeSelect(ctx, dEnv, test.ExpectedSchema, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		// Too much work to synchronize error messages between the two implementations, so for now we'll just assert that an error occurred.
		// require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedSchema, sch)
	require.Equal(t, len(test.ExpectedRows), len(actualRows))
	for i := 0; i < len(test.ExpectedRows); i++ {
		eq := row.AreEqual(test.ExpectedRows[i], actualRows[i], test.ExpectedSchema)

		if !eq {
			expVal, err := test.ExpectedRows[i].NomsMapValue(test.ExpectedSchema).Value(ctx)
			require.NoError(t, err)

			expValStr, err := types.EncodedValue(ctx, expVal)
			require.NoError(t, err)

			actVal, err := actualRows[i].NomsMapValue(test.ExpectedSchema).Value(ctx)
			require.NoError(t, err)

			actValStr, err := types.EncodedValue(ctx, actVal)
			require.NoError(t, err)

			assert.Fail(t, fmt.Sprintf("%s\n\t!=\n%s", expValStr, actValStr))
		}
	}
}
