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
	"fmt"
	"os"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

var skipPrepared bool

// SkipPreparedsCount is used by the "ci-check-repo CI workflow
// as a reminder to consider prepareds when adding a new
// enginetest suite.
const SkipPreparedsCount = 84

const skipPreparedFlag = "DOLT_SKIP_PREPARED_ENGINETESTS"

func init() {
	sqle.MinRowsPerPartition = 8
	sqle.MaxRowsPerPartition = 1024

	if v := os.Getenv(skipPreparedFlag); v != "" {
		skipPrepared = true
	}
}

func TestQueries(t *testing.T) {
	enginetest.TestQueries(t, newDoltHarness(t))
}

func TestSingleQuery(t *testing.T) {
	t.Skip()

	harness := newDoltHarness(t)
	harness.Setup(setup.SimpleSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	setupQueries := []string{
		// "create table t1 (pk int primary key, c int);",
		// "insert into t1 values (1,2), (3,4)",
		// "call dolt_add('.')",
		// "set @Commit1 = dolt_commit('-am', 'initial table');",
		// "insert into t1 values (5,6), (7,8)",
		// "set @Commit2 = dolt_commit('-am', 'two more rows');",
	}

	for _, q := range setupQueries {
		enginetest.RunQuery(t, engine, harness, q)
	}

	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	var test queries.QueryTest
	test = queries.QueryTest{
		Query: `show create table mytable`,
		Expected: []sql.Row{
			{"mytable",
				"CREATE TABLE `mytable` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20) NOT NULL COMMENT 'column s',\n" +
					"  PRIMARY KEY (`i`),\n" +
					"  KEY `idx_si` (`s`,`i`),\n" +
					"  KEY `mytable_i_s` (`i`,`s`),\n" +
					"  UNIQUE KEY `mytable_s` (`s`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	}

	enginetest.TestQueryWithEngine(t, harness, engine, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()
	var scripts = []queries.ScriptTest{
		{
			Name: "dolt_history table filter correctness",
			SetUpScript: []string{
				"create table xy (x int primary key, y int);",
				"call dolt_add('.');",
				"call dolt_commit('-m', 'creating table');",
				"insert into xy values (0, 1);",
				"call dolt_commit('-am', 'add data');",
				"insert into xy values (2, 3);",
				"call dolt_commit('-am', 'add data');",
				"insert into xy values (4, 5);",
				"call dolt_commit('-am', 'add data');",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "select count(*) from dolt_history_xy where commit_hash = (select dolt_log.commit_hash from dolt_log limit 1 offset 1)",
					Expected: []sql.Row{
						{2},
					},
				},
			},
		},
	}

	harness := newDoltHarness(t)
	for _, test := range scripts {
		enginetest.TestScript(t, harness, test)
	}
}

func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()

	harness := newDoltHarness(t)
	//engine := enginetest.NewEngine(t, harness)
	//enginetest.CreateIndexes(t, harness, engine)
	//engine := enginetest.NewSpatialEngine(t, harness)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	setupQueries := []string{
		"create table t1 (pk int primary key, c int);",
		"call dolt_add('.')",
		"insert into t1 values (1,2), (3,4)",
		"set @Commit1 = dolt_commit('-am', 'initial table');",
		"insert into t1 values (5,6), (7,8)",
		"set @Commit2 = dolt_commit('-am', 'two more rows');",
	}

	for _, q := range setupQueries {
		enginetest.RunQuery(t, engine, harness, q)
	}

	//engine.Analyzer.Debug = true
	//engine.Analyzer.Verbose = true

	var test queries.QueryTest
	test = queries.QueryTest{
		Query: "explain select pk, c from dolt_history_t1 where pk = 3 and committer = 'someguy'",
		Expected: []sql.Row{
			{"Exchange"},
			{" └─ Project(dolt_history_t1.pk, dolt_history_t1.c)"},
			{"     └─ Filter((dolt_history_t1.pk = 3) AND (dolt_history_t1.committer = 'someguy'))"},
			{"         └─ IndexedTableAccess(dolt_history_t1)"},
			{"             ├─ index: [dolt_history_t1.pk]"},
			{"             ├─ filters: [{[3, 3]}]"},
			{"             └─ columns: [pk c committer]"},
		},
	}

	enginetest.TestPreparedQuery(t, harness, test.Query, test.Expected, nil)
}

func TestSingleScriptPrepared(t *testing.T) {
	t.Skip()
	s := []setup.SetupScript{
		{
			"create table test (pk int primary key, c1 int)",
			"call dolt_add('.')",
			"insert into test values (0,0), (1,1);",
			"set @Commit1 = dolt_commit('-am', 'creating table');",
			"call dolt_branch('-c', 'main', 'newb')",
			"alter table test add column c2 int;",
			"set @Commit2 = dolt_commit('-am', 'alter table');",
		},
	}
	tt := queries.QueryTest{
		Query: "select * from test as of 'HEAD~2' where pk=?",
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(0, gmstypes.Int8),
		},
		Expected: []sql.Row{{0, 0}},
	}

	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData, s)

	e, err := harness.NewEngine(t)
	defer e.Close()
	require.NoError(t, err)
	ctx := harness.NewContext()

	//e.Analyzer.Debug = true
	//e.Analyzer.Verbose = true

	// full impl
	pre1, sch1, rows1 := enginetest.MustQueryWithPreBindings(ctx, e, tt.Query, tt.Bindings)
	fmt.Println(pre1, sch1, rows1)

	// inline bindings
	sch2, rows2 := enginetest.MustQueryWithBindings(ctx, e, tt.Query, tt.Bindings)
	fmt.Println(sch2, rows2)

	// no bindings
	//sch3, rows3 := enginetest.MustQuery(ctx, e, rawQuery)
	//fmt.Println(sch3, rows3)

	enginetest.TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings)
}

func TestVersionedQueries(t *testing.T) {
	enginetest.TestVersionedQueries(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	// Dolt supports partial keys, so the index matched is different for some plans
	// TODO: Fix these differences by implementing partial key matching in the memory tables, or the engine itself
	skipped := []string{
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1",
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1",
		"SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2",
	}
	// Parallelism introduces Exchange nodes into the query plans, so disable.
	// TODO: exchange nodes should really only be part of the explain plan under certain debug settings
	harness := newDoltHarness(t).WithParallelism(1).WithSkippedQueries(skipped)
	enginetest.TestQueryPlans(t, harness, queries.PlanTests)
}

func TestIntegrationQueryPlans(t *testing.T) {
	harness := newDoltHarness(t).WithParallelism(1)
	enginetest.TestIntegrationPlans(t, harness)
}

func TestDoltDiffQueryPlans(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	harness := newDoltHarness(t).WithParallelism(2) // want Exchange nodes
	harness.Setup(setup.SimpleSetup...)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	for _, tt := range DoltDiffPlanTests {
		enginetest.TestQueryPlan(t, harness, e, tt.Query, tt.ExpectedPlan, false)
	}
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newDoltHarness(t))
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newDoltHarness(t))
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, newDoltHarness(t))
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, newDoltHarness(t))
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, newDoltHarness(t))
}

func TestInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, newDoltHarness(t))
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, newDoltHarness(t))
}

// todo: merge this into the above test when https://github.com/dolthub/dolt/issues/3836 is fixed
func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		// todo: fix https://github.com/dolthub/dolt/issues/3836
		t.Skip()
	}
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, newDoltHarness(t))
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, newDoltHarness(t))
}

func TestSpatialQueries(t *testing.T) {
	enginetest.TestSpatialQueries(t, newDoltHarness(t))
}

func TestReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, newDoltHarness(t))
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, newDoltHarness(t))
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newDoltHarness(t))
}

func TestUpdateIgnore(t *testing.T) {
	enginetest.TestUpdateIgnore(t, newDoltHarness(t))
}

func TestUpdateErrors(t *testing.T) {
	enginetest.TestUpdateErrors(t, newDoltHarness(t))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, newDoltHarness(t))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, newDoltHarness(t))
}

func TestSpatialDelete(t *testing.T) {
	enginetest.TestSpatialDelete(t, newDoltHarness(t))
}

func TestSpatialScripts(t *testing.T) {
	enginetest.TestSpatialScripts(t, newDoltHarness(t))
}

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, newDoltHarness(t))
}

func TestScripts(t *testing.T) {
	var skipped []string
	if types.IsFormat_DOLT(types.Format_Default) {
		skipped = append(skipped, newFormatSkippedScripts...)
	}
	enginetest.TestScripts(t, newDoltHarness(t).WithSkippedQueries(skipped))
}

// TestDoltUserPrivileges tests Dolt-specific code that needs to handle user privilege checking
func TestDoltUserPrivileges(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltUserPrivTests {
		t.Run(script.Name, func(t *testing.T) {
			harness.Setup(setup.MydbData)
			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			ctx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})

			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range script.SetUpScript {
				if sh, ok := interface{}(harness).(enginetest.SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				enginetest.RunQueryWithContext(t, engine, harness, ctx, statement)
			}
			for _, assertion := range script.Assertions {
				if sh, ok := interface{}(harness).(enginetest.SkippingHarness); ok {
					if sh.SkipQueryTest(assertion.Query) {
						t.Skipf("Skipping query %s", assertion.Query)
					}
				}

				user := assertion.User
				host := assertion.Host
				if user == "" {
					user = "root"
				}
				if host == "" {
					host = "localhost"
				}
				ctx := enginetest.NewContextWithClient(harness, sql.Client{
					User:    user,
					Address: host,
				})

				if assertion.ExpectedErr != nil {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.TestQueryWithContext(t, ctx, engine, harness, assertion.Query, assertion.Expected, nil, nil)
					})
				}
			}
		})
	}
}

func TestJoinOps(t *testing.T) {
	if types.IsFormat_DOLT_DEV(types.Format_Default) || types.IsFormat_LD(types.Format_Default) {
		t.Skip("DOLT_LD keyless indexes are not sorted")
	}

	enginetest.TestJoinOps(t, newDoltHarness(t))
}

func TestJoinOpsPrepared(t *testing.T) {
	if types.IsFormat_DOLT_DEV(types.Format_Default) || types.IsFormat_LD(types.Format_Default) {
		t.Skip("DOLT_LD keyless indexes are not sorted")
	}

	enginetest.TestJoinOpsPrepared(t, newDoltHarness(t))
}

func TestJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, newDoltHarness(t))
}

func TestJoinQueriesPrepared(t *testing.T) {
	enginetest.TestJoinQueriesPrepared(t, newDoltHarness(t))
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	enginetest.TestJSONTableQueries(t, newDoltHarness(t))
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	enginetest.TestJSONTableScripts(t, newDoltHarness(t))
}

func TestUserPrivileges(t *testing.T) {
	enginetest.TestUserPrivileges(t, newDoltHarness(t))
}

func TestUserAuthentication(t *testing.T) {
	t.Skip("Unexpected panic, need to fix")
	enginetest.TestUserAuthentication(t, newDoltHarness(t))
}

func TestComplexIndexQueries(t *testing.T) {
	enginetest.TestComplexIndexQueries(t, newDoltHarness(t))
}

func TestCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, newDoltHarness(t))
}

func TestPkOrdinalsDDL(t *testing.T) {
	enginetest.TestPkOrdinalsDDL(t, newDoltHarness(t))
}

func TestPkOrdinalsDML(t *testing.T) {
	enginetest.TestPkOrdinalsDML(t, newDoltHarness(t))
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newDoltHarness(t))
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, newDoltHarness(t))
}

func TestRenameColumn(t *testing.T) {
	enginetest.TestRenameColumn(t, newDoltHarness(t))
}

func TestAddColumn(t *testing.T) {
	enginetest.TestAddColumn(t, newDoltHarness(t))
}

func TestModifyColumn(t *testing.T) {
	enginetest.TestModifyColumn(t, newDoltHarness(t))
}

func TestDropColumn(t *testing.T) {
	enginetest.TestDropColumn(t, newDoltHarness(t))
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, newDoltHarness(t))
}

func TestBlobs(t *testing.T) {
	skipOldFormat(t)
	enginetest.TestBlobs(t, newDoltHarness(t))
}

func TestIndexes(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestIndexes(t, harness)
}

func TestIndexPrefix(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	enginetest.TestIndexPrefix(t, harness)
	for _, script := range DoltIndexPrefixScripts {
		enginetest.TestScript(t, harness, script)
	}
}

func TestBigBlobs(t *testing.T) {
	skipOldFormat(t)

	h := newDoltHarness(t)
	h.Setup(setup.MydbData, setup.BlobData)
	for _, tt := range BigBlobQueries {
		enginetest.RunWriteQueryTest(t, h, tt)
	}
}

func TestDropDatabase(t *testing.T) {
	enginetest.TestScript(t, newDoltHarness(t), queries.ScriptTest{
		Name: "Drop database engine tests for Dolt only",
		SetUpScript: []string{
			"CREATE DATABASE Test1db",
			"CREATE DATABASE TEST2db",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "DROP DATABASE TeSt2DB",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1}}},
			},
			{
				Query:       "USE test2db",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:    "USE TEST1DB",
				Expected: []sql.Row{},
			},
			{
				Query:    "DROP DATABASE IF EXISTS test1DB",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1}}},
			},
			{
				Query:       "USE Test1db",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
		},
	})

	t.Skip("Dolt doesn't yet support dropping the primary database, which these tests do")
	enginetest.TestDropDatabase(t, newDoltHarness(t))
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, newDoltHarness(t))
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, newDoltHarness(t))
}

func TestForeignKeys(t *testing.T) {
	enginetest.TestForeignKeys(t, newDoltHarness(t))
}

func TestCreateCheckConstraints(t *testing.T) {
	enginetest.TestCreateCheckConstraints(t, newDoltHarness(t))
}

func TestChecksOnInsert(t *testing.T) {
	enginetest.TestChecksOnInsert(t, newDoltHarness(t))
}

func TestChecksOnUpdate(t *testing.T) {
	enginetest.TestChecksOnUpdate(t, newDoltHarness(t))
}

func TestDisallowedCheckConstraints(t *testing.T) {
	enginetest.TestDisallowedCheckConstraints(t, newDoltHarness(t))
}

func TestDropCheckConstraints(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, newDoltHarness(t))
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, newDoltHarness(t))
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, newDoltHarness(t))
}

func TestVersionedViews(t *testing.T) {
	enginetest.TestVersionedViews(t, newDoltHarness(t))
}

func TestWindowFunctions(t *testing.T) {
	enginetest.TestWindowFunctions(t, newDoltHarness(t))
}

func TestWindowRowFrames(t *testing.T) {
	enginetest.TestWindowRowFrames(t, newDoltHarness(t))
}

func TestWindowRangeFrames(t *testing.T) {
	enginetest.TestWindowRangeFrames(t, newDoltHarness(t))
}

func TestNamedWindows(t *testing.T) {
	enginetest.TestNamedWindows(t, newDoltHarness(t))
}

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, newDoltHarness(t))
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, newDoltHarness(t))
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, newDoltHarness(t))
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, newDoltHarness(t))
}

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, newDoltHarness(t))
}

func TestAlterTable(t *testing.T) {
	enginetest.TestAlterTable(t, newDoltHarness(t))
}

func TestVariables(t *testing.T) {
	enginetest.TestVariables(t, newDoltHarness(t))
}

func TestVariableErrors(t *testing.T) {
	enginetest.TestVariableErrors(t, newDoltHarness(t))
}

func TestLoadDataPrepared(t *testing.T) {
	t.Skip("feature not supported")
	skipPreparedTests(t)
	enginetest.TestLoadDataPrepared(t, newDoltHarness(t))
}

func TestLoadData(t *testing.T) {
	t.Skip()
	enginetest.TestLoadData(t, newDoltHarness(t))
}

func TestLoadDataErrors(t *testing.T) {
	enginetest.TestLoadDataErrors(t, newDoltHarness(t))
}

func TestJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, newDoltHarness(t))
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newDoltHarness(t))
}

func TestRollbackTriggers(t *testing.T) {
	enginetest.TestRollbackTriggers(t, newDoltHarness(t))
}

func TestStoredProcedures(t *testing.T) {
	tests := make([]queries.ScriptTest, 0, len(queries.ProcedureLogicTests))
	for _, test := range queries.ProcedureLogicTests {
		//TODO: this passes locally but SOMETIMES fails tests on GitHub, no clue why
		if test.Name != "ITERATE and LEAVE loops" {
			tests = append(tests, test)
		}
	}
	queries.ProcedureLogicTests = tests

	enginetest.TestStoredProcedures(t, newDoltHarness(t))
}

func TestCallAsOf(t *testing.T) {
	for _, script := range DoltCallAsOf {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestLargeJsonObjects(t *testing.T) {
	SkipByDefaultInCI(t)
	harness := newDoltHarness(t)
	for _, script := range LargeJsonObjectScriptTests {
		enginetest.TestScript(t, harness, script)
	}
}

func SkipByDefaultInCI(t *testing.T) {
	if os.Getenv("CI") != "" && os.Getenv("DOLT_TEST_RUN_NON_RACE_TESTS") == "" {
		t.Skip()
	}
}

func TestTransactions(t *testing.T) {
	for _, script := range queries.TransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
	for _, script := range DoltTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
	for _, script := range DoltSqlFuncTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
	for _, script := range DoltConflictHandlingTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
	for _, script := range DoltConstraintViolationTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	enginetest.TestConcurrentTransactions(t, newDoltHarness(t))
}

func TestDoltScripts(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltScripts {
		enginetest.TestScript(t, harness, script)
	}
}

func TestDoltRevisionDbScripts(t *testing.T) {
	for _, script := range DoltRevisionDbScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}

	// Testing a commit-qualified database revision spec requires
	// a little extra work to get the generated commit hash
	harness := newDoltHarness(t)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()
	ctx := harness.NewContext()

	setupScripts := []setup.SetupScript{
		{"create table t01 (pk int primary key, c1 int)"},
		{"call dolt_add('.');"},
		{"call dolt_commit('-am', 'creating table t01 on main');"},
		{"insert into t01 values (1, 1), (2, 2);"},
		{"call dolt_commit('-am', 'adding rows to table t01 on main');"},
		{"insert into t01 values (3, 3);"},
		{"call dolt_commit('-am', 'adding another row to table t01 on main');"},
	}
	_, err = enginetest.RunSetupScripts(ctx, harness.engine, setupScripts, true)
	require.NoError(t, err)

	sch, iter, err := harness.engine.Query(ctx, "select hashof('HEAD~2');")
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, sch, iter)
	require.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	commithash := rows[0][0].(string)

	scriptTest := queries.ScriptTest{
		Name: "database revision specs: commit-qualified revision spec",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "use mydb/" + commithash,
				Expected: []sql.Row{},
			},
			{
				Query: "select active_branch();",
				Expected: []sql.Row{
					{nil},
				},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/" + commithash}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mydb/" + commithash}, {"mysql"}},
			},
			{
				Query:    "select * from t01",
				Expected: []sql.Row{},
			},
			{
				Query:          "call dolt_reset();",
				ExpectedErrStr: "unable to reset HEAD in read-only databases",
			},
			{
				Query:    "call dolt_checkout('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb"}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
		},
	}

	enginetest.TestScript(t, harness, scriptTest)
}

func TestDoltRevisionDbScriptsPrepared(t *testing.T) {
	for _, script := range DoltRevisionDbScripts {
		enginetest.TestScriptPrepared(t, newDoltHarness(t), script)
	}
}

func TestDoltDdlScripts(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup()

	for _, script := range ModifyAndChangeColumnScripts {
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		enginetest.TestScriptWithEngine(t, e, harness, script)
	}

	for _, script := range ModifyColumnTypeScripts {
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		enginetest.TestScriptWithEngine(t, e, harness, script)
	}

	for _, script := range DropColumnScripts {
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		enginetest.TestScriptWithEngine(t, e, harness, script)
	}
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("not fixing unique index on keyless tables for old format")
	}
	for _, script := range AddIndexScripts {
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		enginetest.TestScriptWithEngine(t, e, harness, script)
	}
}

func TestBrokenDdlScripts(t *testing.T) {
	for _, script := range BrokenDDLScripts {
		t.Skip(script.Name)
	}
}

func TestDescribeTableAsOf(t *testing.T) {
	enginetest.TestScript(t, newDoltHarness(t), DescribeTableAsOfScriptTest)
}

func TestShowCreateTableAsOf(t *testing.T) {
	enginetest.TestScript(t, newDoltHarness(t), ShowCreateTableAsOfScriptTest)
}

func TestViewsWithAsOf(t *testing.T) {
	enginetest.TestScript(t, newDoltHarness(t), ViewsWithAsOfScriptTest)
}

func TestViewsWithAsOfPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestScriptPrepared(t, newDoltHarness(t), ViewsWithAsOfScriptTest)
}

func TestDoltMerge(t *testing.T) {
	for _, script := range MergeScripts {
		// dolt versioning conflicts with reset harness -- use new harness every time
		enginetest.TestScript(t, newDoltHarness(t), script)
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, script := range Dolt1MergeScripts {
			enginetest.TestScript(t, newDoltHarness(t), script)
		}
	}
}

func TestDoltAutoIncrement(t *testing.T) {
	for _, script := range DoltAutoIncrementTests {
		// doing commits on different branches is antagonistic to engine reuse, use a new engine on each script
		enginetest.TestScript(t, newDoltHarness(t), script)
	}

	for _, script := range BrokenAutoIncrementTests {
		t.Run(script.Name, func(t *testing.T) {
			t.Skip()
			enginetest.TestScript(t, newDoltHarness(t), script)
		})
	}
}

func TestDoltAutoIncrementPrepared(t *testing.T) {
	for _, script := range DoltAutoIncrementTests {
		// doing commits on different branches is antagonistic to engine reuse, use a new engine on each script
		enginetest.TestScriptPrepared(t, newDoltHarness(t), script)
	}

	for _, script := range BrokenAutoIncrementTests {
		t.Run(script.Name, func(t *testing.T) {
			t.Skip()
			enginetest.TestScriptPrepared(t, newDoltHarness(t), script)
		})
	}
}

func TestDoltConflictsTableNameTable(t *testing.T) {
	for _, script := range DoltConflictTableNameTableTests {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, script := range Dolt1ConflictTableNameTableTests {
			enginetest.TestScript(t, newDoltHarness(t), script)
		}
	}
}

// tests new format behavior for keyless merges that create CVs and conflicts
func TestKeylessDoltMergeCVsAndConflicts(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	for _, script := range KeylessMergeCVsAndConflictsScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

// eventually this will be part of TestDoltMerge
func TestDoltMergeArtifacts(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	for _, script := range MergeArtifactsScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

// these tests are temporary while there is a difference between the old format
// and new format merge behaviors.
func TestOldFormatMergeConflictsAndCVs(t *testing.T) {
	if types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	for _, script := range OldFormatMergeConflictsAndCVsScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltReset(t *testing.T) {
	for _, script := range DoltReset {
		// dolt versioning conflicts with reset harness -- use new harness every time
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltGC(t *testing.T) {
	t.SkipNow()
	for _, script := range DoltGC {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltBranch(t *testing.T) {
	for _, script := range DoltBranchScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltTag(t *testing.T) {
	for _, script := range DoltTagTestScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltRemote(t *testing.T) {
	for _, script := range DoltRemoteTestScripts {
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

// TestSingleTransactionScript is a convenience method for debugging a single transaction test. Unskip and set to the
// desired test.
func TestSingleTransactionScript(t *testing.T) {
	t.Skip()

	script := queries.TransactionTest{
		Name: "allow commit conflicts on, conflict on dolt_merge",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SELECT DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ set autocommit = off, dolt_allow_commit_conflicts = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into test values (1, 1)",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_checkout('-b', 'new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client a */ call dolt_commit('-am', 'commit on main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ insert into test values (1, 2)",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 2}},
			},
			{ // no error because of our session settings
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{ // TODO: it should be possible to do this without specifying a literal in the subselect, but it's not working
				Query: "/* client b */ update test t set val = (select their_val from dolt_conflicts_test where our_pk = 1) where pk = 1",
				Expected: []sql.Row{{gmstypes.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from test order by 1",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{
				Query:    "/* client b */ select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	}

	enginetest.TestTransactionScript(t, newDoltHarness(t), script)
}

func TestBrokenSystemTableQueries(t *testing.T) {
	t.Skip()

	enginetest.RunQueryTests(t, newDoltHarness(t), BrokenSystemTableQueries)
}

func TestHistorySystemTable(t *testing.T) {
	harness := newDoltHarness(t).WithParallelism(2)
	harness.Setup(setup.MydbData)
	for _, test := range HistorySystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestHistorySystemTablePrepared(t *testing.T) {
	harness := newDoltHarness(t).WithParallelism(2)
	harness.Setup(setup.MydbData)
	for _, test := range HistorySystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestBrokenHistorySystemTablePrepared(t *testing.T) {
	t.Skip()
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range BrokenHistorySystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestUnscopedDiffSystemTable(t *testing.T) {
	for _, test := range UnscopedDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, newDoltHarness(t), test)
		})
	}
}

func TestUnscopedDiffSystemTablePrepared(t *testing.T) {
	for _, test := range UnscopedDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, newDoltHarness(t), test)
		})
	}
}

func TestDiffTableFunction(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestDiffTableFunctionPrepared(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestDiffSummaryTableFunction(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffSummaryTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestDiffSummaryTableFunctionPrepared(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffSummaryTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestLogTableFunction(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range LogTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestLogTableFunctionPrepared(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range LogTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestCommitDiffSystemTable(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range CommitDiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestCommitDiffSystemTablePrepared(t *testing.T) {
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range CommitDiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestDiffSystemTable(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, test := range Dolt1DiffSystemTableScripts {
			enginetest.TestScript(t, newDoltHarness(t), test)
		}
	}
}

func TestDiffSystemTablePrepared(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, test := range Dolt1DiffSystemTableScripts {
			enginetest.TestScriptPrepared(t, newDoltHarness(t), test)
		}
	}
}

func mustNewEngine(t *testing.T, h enginetest.Harness) *gms.Engine {
	e, err := h.NewEngine(t)
	if err != nil {
		require.NoError(t, err)
	}
	return e
}

func TestSystemTableIndexes(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, stt := range SystemTableIndexTests {
		harness := newDoltHarness(t).WithParallelism(2)
		harness.SkipSetupCommit()
		e := mustNewEngine(t, harness)
		defer e.Close()

		ctx := enginetest.NewContext(harness)
		for _, q := range stt.setup {
			enginetest.RunQuery(t, e, harness, q)
		}

		for _, tt := range stt.queries {
			t.Run(fmt.Sprintf("%s: %s", stt.name, tt.query), func(t *testing.T) {
				if tt.skip {
					t.Skip()
				}

				ctx = ctx.WithQuery(tt.query)
				if tt.exp != nil {
					enginetest.TestQueryWithContext(t, ctx, e, harness, tt.query, tt.exp, nil, nil)
				}
			})
		}
	}
}

func TestSystemTableIndexesPrepared(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, stt := range SystemTableIndexTests {
		harness := newDoltHarness(t).WithParallelism(2)
		harness.SkipSetupCommit()
		e := mustNewEngine(t, harness)
		defer e.Close()

		ctx := enginetest.NewContext(harness)
		for _, q := range stt.setup {
			enginetest.RunQuery(t, e, harness, q)
		}

		for _, tt := range stt.queries {
			t.Run(fmt.Sprintf("%s: %s", stt.name, tt.query), func(t *testing.T) {
				if tt.skip {
					t.Skip()
				}

				ctx = ctx.WithQuery(tt.query)
				if tt.exp != nil {
					enginetest.TestPreparedQueryWithContext(t, ctx, e, harness, tt.query, tt.exp, nil)
				}
			})
		}
	}
}

func TestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, newDoltHarness(t))
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, newDoltHarness(t))
}

func TestAddAutoIncrementColumn(t *testing.T) {
	enginetest.TestAddAutoIncrementColumn(t, newDoltHarness(t))
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, newDoltHarness(t))
}

func TestPersist(t *testing.T) {
	harness := newDoltHarness(t)
	dEnv := dtestutils.CreateTestEnv()
	localConf, ok := dEnv.Config.GetConfig(env.LocalConfig)
	require.True(t, ok)
	globals := config.NewPrefixConfig(localConf, env.SqlServerGlobalsPrefix)
	newPersistableSession := func(ctx *sql.Context) sql.PersistableSession {
		session := ctx.Session.(*dsess.DoltSession).WithGlobals(globals)
		err := session.RemoveAllPersistedGlobals()
		require.NoError(t, err)
		return session
	}

	enginetest.TestPersist(t, harness, newPersistableSession)
}

func TestTypesOverWire(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestTypesOverWire(t, harness, newSessionBuilder(harness))
}

func TestDoltCommit(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltCommitTests {
		enginetest.TestScript(t, harness, script)
	}
}

func TestDoltCommitPrepared(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltCommitTests {
		enginetest.TestScriptPrepared(t, harness, script)
	}
}

func TestQueriesPrepared(t *testing.T) {
	enginetest.TestQueriesPrepared(t, newDoltHarness(t))
}

func TestPreparedStaticIndexQuery(t *testing.T) {
	enginetest.TestPreparedStaticIndexQuery(t, newDoltHarness(t))
}

func TestStatistics(t *testing.T) {
	enginetest.TestStatistics(t, newDoltHarness(t))
}

func TestSpatialQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)

	enginetest.TestSpatialQueriesPrepared(t, newDoltHarness(t))
}

func TestPreparedStatistics(t *testing.T) {
	enginetest.TestStatisticsPrepared(t, newDoltHarness(t))
}

func TestVersionedQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestVersionedQueriesPrepared(t, newDoltHarness(t))
}

func TestInfoSchemaPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestInfoSchemaPrepared(t, newDoltHarness(t))
}

func TestUpdateQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestUpdateQueriesPrepared(t, newDoltHarness(t))
}

func TestInsertQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestInsertQueriesPrepared(t, newDoltHarness(t))
}

func TestReplaceQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestReplaceQueriesPrepared(t, newDoltHarness(t))
}

func TestDeleteQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestDeleteQueriesPrepared(t, newDoltHarness(t))
}

func TestScriptsPrepared(t *testing.T) {
	var skipped []string
	if types.IsFormat_DOLT(types.Format_Default) {
		skipped = append(skipped, newFormatSkippedScripts...)
	}
	skipPreparedTests(t)
	enginetest.TestScriptsPrepared(t, newDoltHarness(t).WithSkippedQueries(skipped))
}

func TestInsertScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestInsertScriptsPrepared(t, newDoltHarness(t))
}

func TestComplexIndexQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestComplexIndexQueriesPrepared(t, newDoltHarness(t))
}

func TestJsonScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestJsonScriptsPrepared(t, newDoltHarness(t))
}

func TestCreateCheckConstraintsScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestCreateCheckConstraintsScriptsPrepared(t, newDoltHarness(t))
}

func TestInsertIgnoreScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestInsertIgnoreScriptsPrepared(t, newDoltHarness(t))
}

func TestInsertErrorScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestInsertErrorScriptsPrepared(t, newDoltHarness(t))
}

func TestViewsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestViewsPrepared(t, newDoltHarness(t))
}

func TestVersionedViewsPrepared(t *testing.T) {
	t.Skip("not supported for prepareds")
	skipPreparedTests(t)
	enginetest.TestVersionedViewsPrepared(t, newDoltHarness(t))
}

func TestShowTableStatusPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestShowTableStatusPrepared(t, newDoltHarness(t))
}

func TestPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestPrepared(t, newDoltHarness(t))
}

func TestPreparedInsert(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestPreparedInsert(t, newDoltHarness(t))
}

func TestPreparedStatements(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestPreparedStatements(t, newDoltHarness(t))
}

func TestCharsetCollationEngine(t *testing.T) {
	skipOldFormat(t)
	enginetest.TestCharsetCollationEngine(t, newDoltHarness(t))
}

func TestCharsetCollationWire(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	enginetest.TestCharsetCollationWire(t, harness, newSessionBuilder(harness))
}

func TestDatabaseCollationWire(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	enginetest.TestDatabaseCollationWire(t, harness, newSessionBuilder(harness))
}

func TestAddDropPrimaryKeys(t *testing.T) {
	t.Run("adding and dropping primary keys does not result in duplicate NOT NULL constraints", func(t *testing.T) {
		harness := newDoltHarness(t)
		addPkScript := queries.ScriptTest{
			Name: "add primary keys",
			SetUpScript: []string{
				"create table test (id int not null, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
				"ALTER TABLE test DROP PRIMARY KEY",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
				"ALTER TABLE test DROP PRIMARY KEY",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
				"ALTER TABLE test DROP PRIMARY KEY",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "show create table test",
					Expected: []sql.Row{
						{"test", "CREATE TABLE `test` (\n" +
							"  `id` int NOT NULL,\n" +
							"  `c1` int,\n" +
							"  PRIMARY KEY (`id`),\n" +
							"  KEY `c1_idx` (`c1`)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
					},
				},
			},
		}

		enginetest.TestScript(t, harness, addPkScript)

		// make sure there is only one NOT NULL constraint after all those mutations
		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.session))
		ws, err := harness.session.WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, "test")
		require.NoError(t, err)
		require.True(t, ok)

		sch, err := table.GetSchema(ctx)
		for _, col := range sch.GetAllCols().GetColumns() {
			count := 0
			for _, cc := range col.Constraints {
				if cc.GetConstraintType() == schema.NotNullConstraintType {
					count++
				}
			}
			require.Less(t, count, 2)
		}
	})

	t.Run("Add primary key to table with index", func(t *testing.T) {
		harness := newDoltHarness(t)
		script := queries.ScriptTest{
			Name: "add primary keys to table with index",
			SetUpScript: []string{
				"create table test (id int not null, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test ADD constraint test_check CHECK (c1 > 0)",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "show create table test",
					Expected: []sql.Row{
						{"test", "CREATE TABLE `test` (\n" +
							"  `id` int NOT NULL,\n" +
							"  `c1` int,\n" +
							"  PRIMARY KEY (`id`),\n" +
							"  KEY `c1_idx` (`c1`),\n" +
							"  CONSTRAINT `test_check` CHECK ((`c1` > 0))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
					},
				},
				{
					Query: "select * from test order by id",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
		}
		enginetest.TestScript(t, harness, script)

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.session))
		ws, err := harness.session.WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, "test")
		require.NoError(t, err)
		require.True(t, ok)

		// Assert the new index map is not empty
		newRows, err := table.GetIndexRowData(ctx, "c1_idx")
		require.NoError(t, err)
		empty, err := newRows.Empty()
		require.NoError(t, err)
		assert.False(t, empty)
		count, err := newRows.Count()
		require.NoError(t, err)
		assert.Equal(t, count, uint64(2))
	})

	t.Run("Add primary key when one more cells contain NULL", func(t *testing.T) {
		harness := newDoltHarness(t)
		script := queries.ScriptTest{
			Name: "Add primary key when one more cells contain NULL",
			SetUpScript: []string{
				"create table test (id int not null, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test ADD PRIMARY KEY (c1)",
				"ALTER TABLE test ADD COLUMN (c2 INT NULL)",
				"ALTER TABLE test DROP PRIMARY KEY",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE test ADD PRIMARY KEY (id, c1, c2)",
					ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
				},
			},
		}
		enginetest.TestScript(t, harness, script)
	})

	t.Run("Drop primary key from table with index", func(t *testing.T) {
		harness := newDoltHarness(t)
		script := queries.ScriptTest{
			Name: "Drop primary key from table with index",
			SetUpScript: []string{
				"create table test (id int not null primary key, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test DROP PRIMARY KEY",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "show create table test",
					Expected: []sql.Row{
						{"test", "CREATE TABLE `test` (\n" +
							"  `id` int NOT NULL,\n" +
							"  `c1` int,\n" +
							"  KEY `c1_idx` (`c1`)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
					},
				},
				{
					Query: "select * from test order by id",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
		}

		enginetest.TestScript(t, harness, script)

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.session))
		ws, err := harness.session.WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, "test")
		require.NoError(t, err)
		require.True(t, ok)

		// Assert the index map is not empty
		newIdx, err := table.GetIndexRowData(ctx, "c1_idx")
		assert.NoError(t, err)
		empty, err := newIdx.Empty()
		require.NoError(t, err)
		assert.False(t, empty)
		count, err := newIdx.Count()
		require.NoError(t, err)
		assert.Equal(t, count, uint64(2))
	})
}

func TestDoltVerifyConstraints(t *testing.T) {
	for _, script := range DoltVerifyConstraintsTestScripts {
		harness := newDoltHarness(t)
		enginetest.TestScript(t, harness, script)
	}
}

func TestDoltStorageFormat(t *testing.T) {
	var expectedFormatString string
	if types.IsFormat_DOLT(types.Format_Default) {
		expectedFormatString = "NEW ( __DOLT__ )"
	} else {
		expectedFormatString = fmt.Sprintf("OLD ( %s )", types.Format_Default.VersionString())
	}
	script := queries.ScriptTest{
		Name: "dolt storage format function works",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select dolt_storage_format()",
				Expected: []sql.Row{{expectedFormatString}},
			},
		},
	}
	enginetest.TestScript(t, newDoltHarness(t), script)
}

func TestDoltStorageFormatPrepared(t *testing.T) {
	var expectedFormatString string
	if types.IsFormat_DOLT(types.Format_Default) {
		expectedFormatString = "NEW ( __DOLT__ )"
	} else {
		expectedFormatString = fmt.Sprintf("OLD ( %s )", types.Format_Default.VersionString())
	}
	enginetest.TestPreparedQuery(t, newDoltHarness(t), "SELECT dolt_storage_format()", []sql.Row{{expectedFormatString}}, nil)
}

func TestThreeWayMergeWithSchemaChangeScripts(t *testing.T) {
	skipOldFormat(t)
	for _, script := range ThreeWayMergeWithSchemaChangeTestScripts {
		enginetest.TestScript(t, newDoltHarness(t), convertMergeScriptTest(script))
	}
}

func TestThreeWayMergeWithSchemaChangeScriptsPrepared(t *testing.T) {
	skipOldFormat(t)
	for _, script := range ThreeWayMergeWithSchemaChangeTestScripts {
		enginetest.TestScriptPrepared(t, newDoltHarness(t), convertMergeScriptTest(script))
	}
}

var newFormatSkippedScripts = []string{
	// Different query plans
	"Partial indexes are used and return the expected result",
	"Multiple indexes on the same columns in a different order",
}

func skipOldFormat(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
}

func skipPreparedTests(t *testing.T) {
	if skipPrepared {
		t.Skip("skip prepared")
	}
}

func newSessionBuilder(harness *DoltHarness) server.SessionBuilder {
	return func(ctx context.Context, conn *mysql.Conn, host string) (sql.Session, error) {
		return harness.session, nil
	}
}
