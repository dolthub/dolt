// Copyright 2024 Dolthub, Inc.
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
	"io"
	"os"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/doltcmd"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const skipPreparedFlag = "DOLT_SKIP_PREPARED_ENGINETESTS"

var skipPrepared bool

func init() {
	sqle.MinRowsPerPartition = 8
	sqle.MaxRowsPerPartition = 1024

	if v := os.Getenv(skipPreparedFlag); v != "" {
		skipPrepared = true
	}
}

func RunSchemaOverridesTest(t *testing.T, harness DoltEnginetestHarness) {
	tcc := &testCommitClock{}
	cleanup := installTestCommitClock(tcc)
	defer cleanup()

	for _, script := range SchemaOverrideTests {
		sql.RunWithNowFunc(tcc.Now, func() error {
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)

			engine, err := harness.NewEngine(t)
			if err != nil {
				panic(err)
			}

			enginetest.TestScriptWithEngine(t, engine, harness, script)
			return nil
		})
	}
}

type testCommitClock struct {
	unixNano int64
}

func (tcc *testCommitClock) Now() time.Time {
	now := time.Unix(0, tcc.unixNano)
	tcc.unixNano += int64(time.Hour)
	return now
}

func installTestCommitClock(tcc *testCommitClock) func() {
	oldNowFunc := datas.CommitterDate
	oldCommitLoc := datas.CommitLoc
	datas.CommitterDate = tcc.Now
	datas.CommitLoc = time.UTC
	return func() {
		datas.CommitterDate = oldNowFunc
		datas.CommitLoc = oldCommitLoc
	}
}

func RunAutoIncrementTrackerLockModeTest(t *testing.T, harness DoltEnginetestHarness) {
	for _, lockMode := range []int64{0, 1, 2} {
		t.Run(fmt.Sprintf("lock mode %d", lockMode), func(t *testing.T) {
			testAutoIncrementTrackerWithLockMode(t, harness, lockMode)
		})
	}
}

// testAutoIncrementTrackerWithLockMode tests that interleaved inserts don't cause deadlocks, regardless of the value of innodb_autoinc_lock_mode.
// In a real use case, these interleaved operations would be happening in different sessions on different threads.
// In order to make the test behave predictably, we manually interleave the two iterators.
func testAutoIncrementTrackerWithLockMode(t *testing.T, harness DoltEnginetestHarness, lockMode int64) {
	err := sql.SystemVariables.AssignValues(map[string]interface{}{"innodb_autoinc_lock_mode": lockMode})
	require.NoError(t, err)

	setupScripts := []setup.SetupScript{[]string{
		"CREATE TABLE test1 (pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,c0 int,index t1_c_index (c0));",
		"CREATE TABLE test2 (pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,c0 int,index t2_c_index (c0));",
		"CREATE TABLE timestamps (pk int NOT NULL PRIMARY KEY AUTO_INCREMENT, t int);",
		"CREATE TRIGGER t1 AFTER INSERT ON test1 FOR EACH ROW INSERT INTO timestamps VALUES (0, 1);",
		"CREATE TRIGGER t2 AFTER INSERT ON test2 FOR EACH ROW INSERT INTO timestamps VALUES (0, 2);",
		"CREATE VIEW bin AS SELECT 0 AS v UNION ALL SELECT 1;",
		"CREATE VIEW sequence5bit AS SELECT b1.v + 2*b2.v + 4*b3.v + 8*b4.v + 16*b5.v AS v from bin b1, bin b2, bin b3, bin b4, bin b5;",
	}}

	harness = harness.NewHarness(t)
	defer harness.Close()
	harness.Setup(setup.MydbData, setupScripts)
	e := mustNewEngine(t, harness)

	defer e.Close()
	ctx := enginetest.NewContext(harness)

	// Confirm that the system variable was correctly set.
	_, iter, _, err := e.Query(ctx, "select @@innodb_autoinc_lock_mode")
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	assert.Equal(t, rows, []sql.Row{{lockMode}})

	// Ordinarily QueryEngine.query manages transactions.
	// Since we can't use that for this test, we manually start a new transaction.
	ts := ctx.Session.(sql.TransactionSession)
	tx, err := ts.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)

	getTriggerIter := func(query string) sql.RowIter {
		root, err := e.AnalyzeQuery(ctx, query)
		require.NoError(t, err)

		var triggerNode *plan.TriggerExecutor
		transform.Node(root, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			if triggerNode != nil {
				return n, transform.SameTree, nil
			}
			if t, ok := n.(*plan.TriggerExecutor); ok {
				triggerNode = t
			}
			return n, transform.NewTree, nil
		})
		iter, err := e.EngineAnalyzer().ExecBuilder.Build(ctx, triggerNode, nil)
		require.NoError(t, err)
		return iter
	}

	iter1 := getTriggerIter("INSERT INTO test1 (c0) select v from sequence5bit;")
	iter2 := getTriggerIter("INSERT INTO test2 (c0) select v from sequence5bit;")

	// Alternate the iterators until they're exhausted.
	var err1 error
	var err2 error
	for err1 != io.EOF || err2 != io.EOF {
		if err1 != io.EOF {
			var row1 sql.Row
			require.NoError(t, err1)
			row1, err1 = iter1.Next(ctx)
			_ = row1
		}
		if err2 != io.EOF {
			require.NoError(t, err2)
			_, err2 = iter2.Next(ctx)
		}
	}
	err = iter1.Close(ctx)
	require.NoError(t, err)
	err = iter2.Close(ctx)
	require.NoError(t, err)

	dsess.DSessFromSess(ctx.Session).CommitTransaction(ctx, ctx.GetTransaction())

	// Verify that the inserts are seen by the engine.
	{
		_, iter, _, err := e.Query(ctx, "select count(*) from timestamps")
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
		assert.Equal(t, rows, []sql.Row{{int64(64)}})
	}

	// Verify that the insert operations are actually interleaved by inspecting the order that values were added to `timestamps`
	{
		_, iter, _, err := e.Query(ctx, "select (select min(pk) from timestamps where t = 1) < (select max(pk) from timestamps where t = 2)")
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
		assert.Equal(t, rows, []sql.Row{{true}})
	}

	{
		_, iter, _, err := e.Query(ctx, "select (select min(pk) from timestamps where t = 2) < (select max(pk) from timestamps where t = 1)")
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
		assert.Equal(t, rows, []sql.Row{{true}})
	}
}

func RunVersionedQueriesTest(t *testing.T, h DoltEnginetestHarness) {
	h.Setup(setup.MydbData, []setup.SetupScript{VersionedQuerySetup, VersionedQueryViews})

	e, err := h.NewEngine(t)
	require.NoError(t, err)

	for _, tt := range queries.VersionedQueries {
		enginetest.TestQueryWithEngine(t, h, e, tt)
	}

	for _, tt := range queries.VersionedScripts {
		enginetest.TestScriptWithEngine(t, e, h, tt)
	}
}

func RunQueryTestPlans(t *testing.T, harness DoltEnginetestHarness) {
	harness = harness.NewHarness(t)
	defer harness.Close()
	enginetest.TestQueryPlans(t, harness, queries.PlanTests)
}

func RunDoltDiffQueryPlansTest(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	harness.Setup(setup.SimpleSetup...)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	for _, tt := range append(DoltDiffPlanTests, DoltCommitPlanTests...) {
		enginetest.TestQueryPlanWithName(t, tt.Query, harness, e, tt.Query, tt.ExpectedPlan, sql.DescribeOptions{})
	}
}

func RunBranchPlanTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range BranchPlanTests {
		t.Run(script.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()

			e := mustNewEngine(t, harness)
			defer e.Close()

			for _, statement := range script.SetUpScript {
				ctx := enginetest.NewContext(harness).WithQuery(statement)
				enginetest.RunQueryWithContext(t, e, harness, ctx, statement)
			}
			for _, tt := range script.Queries {
				t.Run(tt.Query, func(t *testing.T) {
					TestIndexedAccess(t, e, harness, tt.Query, tt.Index)
				})
			}
		})
	}
}

func RunInfoSchemaTests(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	enginetest.TestInfoSchema(t, h)

	for _, script := range DoltInfoSchemaScripts {
		func() {
			h = h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunInsertIntoErrorsTest(t *testing.T, h DoltEnginetestHarness) {
	h = h.WithSkippedQueries([]string{
		"create table bad (vb varbinary(65535))",
		"insert into bad values (repeat('0', 65536))",
	})
	defer h.Close()
	enginetest.TestInsertIntoErrors(t, h)
}

func RunGeneratedColumnTests(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	enginetest.TestGeneratedColumns(t,
		// virtual indexes are failing for certain lookups on this test
		harness.WithSkippedQueries([]string{"create table t (pk int primary key, col1 int as (pk + 1));"}))

	for _, script := range GeneratedColumnMergeTestScripts {
		func() {
			h := harness.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunBranchDdlTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DdlBranchTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunBranchDdlTestPrepared(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DdlBranchTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunIndexPrefixTest(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	enginetest.TestIndexPrefix(t, harness)
	for _, script := range DoltIndexPrefixScripts {
		enginetest.TestScript(t, harness, script)
	}
}

func RunBigBlobsTest(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h.Setup(setup.MydbData, setup.BlobData)
	for _, tt := range BigBlobWriteQueries {
		enginetest.RunWriteQueryTest(t, h, tt)
	}
}

func RunAdaptiveBigBlobsTest(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h.Setup(setup.MydbData, BigAdaptiveBlobQueriesSetup)
	enginetest.RunQueryTests(t, h, BigAdaptiveBlobQueries)
	for _, tt := range BigAdaptiveBlobWriteQueries {
		enginetest.RunWriteQueryTest(t, h, tt)
	}
}

func RunAdaptiveBigTextTest(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h.Setup(setup.MydbData, BigAdaptiveTextQueriesSetup)
	enginetest.RunQueryTests(t, h, BigAdaptiveTextQueries)
	for _, tt := range BigAdaptiveTextWriteQueries {
		enginetest.RunWriteQueryTest(t, h, tt)
	}
}

func RunDropEngineTest(t *testing.T, h DoltEnginetestHarness) {
	func() {
		h := h.NewHarness(t)
		defer h.Close()
		enginetest.TestScript(t, h, queries.ScriptTest{
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
	}()

	t.Skip("Dolt doesn't yet support dropping the primary database, which these tests do")
	h = h.NewHarness(t)
	defer h.Close()
	enginetest.TestDropDatabase(t, h)
}

func RunForeignKeyBranchesTest(t *testing.T, h DoltEnginetestHarness) {
	setupPrefix := []string{
		"call dolt_branch('b1')",
		"use mydb/b1",
	}
	assertionsPrefix := []queries.ScriptTestAssertion{
		{
			Query:            "use mydb/b1",
			SkipResultsCheck: true,
		},
	}
	for _, script := range queries.ForeignKeyTests {
		// New harness for every script because we create branches
		h := h.NewHarness(t)
		h.Setup(setup.MydbData, setup.Parent_childData)
		modifiedScript := script
		modifiedScript.SetUpScript = append(setupPrefix, modifiedScript.SetUpScript...)
		modifiedScript.Assertions = append(assertionsPrefix, modifiedScript.Assertions...)
		enginetest.TestScript(t, h, modifiedScript)
	}

	for _, script := range ForeignKeyBranchTests {
		// New harness for every script because we create branches
		h := h.NewHarness(t)
		h.Setup(setup.MydbData, setup.Parent_childData)
		enginetest.TestScript(t, h, script)
	}
}

func RunForeignKeyBranchesPreparedTest(t *testing.T, h DoltEnginetestHarness) {
	setupPrefix := []string{
		"call dolt_branch('b1')",
		"use mydb/b1",
	}
	assertionsPrefix := []queries.ScriptTestAssertion{
		{
			Query:            "use mydb/b1",
			SkipResultsCheck: true,
		},
	}
	for _, script := range queries.ForeignKeyTests {
		// New harness for every script because we create branches
		h := h.NewHarness(t)
		h.Setup(setup.MydbData, setup.Parent_childData)
		modifiedScript := script
		modifiedScript.SetUpScript = append(setupPrefix, modifiedScript.SetUpScript...)
		modifiedScript.Assertions = append(assertionsPrefix, modifiedScript.Assertions...)
		enginetest.TestScriptPrepared(t, h, modifiedScript)
	}

	for _, script := range ForeignKeyBranchTests {
		// New harness for every script because we create branches
		h := h.NewHarness(t)
		h.Setup(setup.MydbData, setup.Parent_childData)
		enginetest.TestScriptPrepared(t, h, script)
	}
}

func RunBranchViewsTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range ViewBranchTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunBranchViewsPreparedTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range ViewBranchTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunVersionedViewsTest(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h.Setup(setup.MydbData, []setup.SetupScript{VersionedQuerySetup, VersionedQueryViews})

	e, err := h.NewEngine(t)
	require.NoError(t, err)

	for _, testCase := range queries.VersionedViewTests {
		t.Run(testCase.Query, func(t *testing.T) {
			ctx := enginetest.NewContext(h)
			enginetest.TestQueryWithContext(t, ctx, e, h, testCase.Query, testCase.Expected, testCase.ExpectedColumns, nil, nil)
		})
	}
}

func RunVariableTest(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	enginetest.TestVariables(t, h)
	for _, script := range DoltSystemVariables {
		enginetest.TestScript(t, h, script)
	}
}

func RunStoredProceduresTest(t *testing.T, h DoltEnginetestHarness) {
	tests := make([]queries.ScriptTest, 0, len(queries.ProcedureLogicTests))
	for _, test := range queries.ProcedureLogicTests {
		// TODO: this passes locally but SOMETIMES fails tests on GitHub, no clue why
		if test.Name != "ITERATE and LEAVE loops" {
			tests = append(tests, test)
		}
	}
	queries.ProcedureLogicTests = tests

	defer h.Close()
	enginetest.TestStoredProcedures(t, h)
}

func RunDoltStoredProceduresTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltProcedureTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltStoredProceduresPreparedTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltProcedureTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunCallAsOfTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltCallAsOf {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunLargeJsonObjectsTest(t *testing.T, harness DoltEnginetestHarness) {
	SkipByDefaultInCI(t)
	defer harness.Close()
	for _, script := range LargeJsonObjectScriptTests {
		enginetest.TestScript(t, harness, script)
	}
}

func RunTransactionTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range queries.TransactionTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
	for _, script := range DoltTransactionTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
	for _, script := range DoltStoredProcedureTransactionTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
	for _, script := range DoltConflictHandlingTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
	for _, script := range DoltConstraintViolationTransactionTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
}

func RunBranchTransactionTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range BranchIsolationTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
}

func RunMultiDbTransactionsTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range MultiDbTransactionTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}

	for _, script := range MultiDbSavepointTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestTransactionScript(t, h, script)
		}()
	}
}

func RunMultiDbTransactionsPreparedTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range MultiDbTransactionTests {
		// func() {
		h := h.NewHarness(t)
		defer h.Close()
		enginetest.TestScriptPrepared(t, h, script)
		// }()
	}
}

func RunDoltScriptsTest(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range DoltScripts {
		// go func() {
		harness := harness.NewHarness(t)

		enginetest.TestScript(t, harness, script)
		harness.Close()
		// }()
	}
}

func RunDoltTempTableScripts(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range DoltTempTableScripts {
		harness := harness.NewHarness(t)
		enginetest.TestScript(t, harness, script)
		harness.Close()
	}
}

func RunDoltRevisionDbScriptsTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltRevisionDbScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}

	// Testing a commit-qualified database revision spec requires
	// a little extra work to get the generated commit hash
	h = h.NewHarness(t)
	defer h.Close()
	e, err := h.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()
	ctx := h.NewContext()
	ctx.SetCurrentDatabase("mydb")

	setupScripts := []setup.SetupScript{
		{"create table t01 (pk int primary key, c1 int)"},
		{"call dolt_add('.');"},
		{"call dolt_commit('-am', 'creating table t01 on main');"},
		{"insert into t01 values (1, 1), (2, 2);"},
		{"call dolt_commit('-am', 'adding rows to table t01 on main');"},
		{"insert into t01 values (3, 3);"},
		{"call dolt_commit('-am', 'adding another row to table t01 on main');"},
	}
	_, err = enginetest.RunSetupScripts(ctx, h.Engine(), setupScripts, true)
	require.NoError(t, err)

	_, iter, _, err := h.Engine().Query(ctx, "select hashof('HEAD~2');")
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
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
				Expected: []sql.Row{{"mydb"}, {"mydb/" + commithash}, {"information_schema"}, {"mysql"}},
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
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
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

	enginetest.TestScript(t, h, scriptTest)
}

func RunDoltRevisionDbScriptsPreparedTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltRevisionDbScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunDoltDdlScripts(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
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

	// TODO: these scripts should be general enough to go in GMS
	for _, script := range AddDropPrimaryKeysScripts {
		e, err := harness.NewEngine(t)
		require.NoError(t, err)
		enginetest.TestScriptWithEngine(t, e, harness, script)
	}
}

func RunShowCreateTableTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range ShowCreateTableScriptTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunCreateDatabaseTest(t *testing.T, h *DoltHarness) {
	enginetest.TestCreateDatabase(t, h)
	h.Close()

	for _, script := range DoltCreateDatabaseScripts {
		h := h.NewHarness(t)
		enginetest.TestScript(t, h, script)
		h.Close()
	}
}

func RunShowCreateTablePreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range ShowCreateTableScriptTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunDoltMergeTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range MergeScripts {
		// harness can't reset effectively when there are new commits / branches created, so use a new harness for
		// each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			h.Setup(setup.MydbData)
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltMergePreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range MergeScripts {
		// harness can't reset effectively when there are new commits / branches created, so use a new harness for
		// each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunDoltRebaseTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltRebaseScriptTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			h.SkipSetupCommit()
			enginetest.TestScript(t, h, script)
		}()
	}

	testMultiSessionScriptTests(t, DoltRebaseMultiSessionScriptTests)
}

func RunDoltRebasePreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltRebaseScriptTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			h.SkipSetupCommit()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunDoltRevertTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range RevertScripts {
		// harness can't reset effectively. Use a new harness for each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltRevertPreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range RevertScripts {
		// harness can't reset effectively. Use a new harness for each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltAutoIncrementTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltAutoIncrementTests {
		// doing commits on different branches is antagonistic to engine reuse, use a new engine on each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltAutoIncrementPreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltAutoIncrementTests {
		// doing commits on different branches is antagonistic to engine reuse, use a new engine on each script
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltConflictsTableNameTableTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltConflictTableNameTableTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, script := range Dolt1ConflictTableNameTableTests {
			func() {
				h := h.NewHarness(t)
				defer h.Close()
				enginetest.TestScript(t, h, script)
			}()
		}
	}
}

func RunKeylessDoltMergeCVsAndConflictsTests(t *testing.T, h DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	for _, script := range KeylessMergeCVsAndConflictsScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltMergeArtifacts(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range MergeArtifactsScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
	for _, script := range SchemaConflictScripts {
		h := h.NewHarness(t)
		enginetest.TestScript(t, h, script)
		h.Close()
	}
}

func RunDoltResetTest(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltResetTestScripts {
		// dolt versioning conflicts with reset harness -- use new harness every time
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltCheckoutTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltCheckoutScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}

	h = h.NewHarness(t)
	defer h.Close()
	engine, err := h.NewEngine(t)
	require.NoError(t, err)
	readOnlyEngine, err := h.NewReadOnlyEngine(engine.EngineAnalyzer().Catalog.DbProvider)
	require.NoError(t, err)

	for _, script := range DoltCheckoutReadOnlyScripts {
		enginetest.TestScriptWithEngine(t, readOnlyEngine, h, script)
	}
}

func RunDoltCheckoutPreparedTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltCheckoutScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}

	h = h.NewHarness(t)
	defer h.Close()
	engine, err := h.NewEngine(t)
	require.NoError(t, err)
	readOnlyEngine, err := h.NewReadOnlyEngine(engine.EngineAnalyzer().Catalog.DbProvider)
	require.NoError(t, err)

	for _, script := range DoltCheckoutReadOnlyScripts {
		enginetest.TestScriptWithEnginePrepared(t, readOnlyEngine, h, script)
	}
}

func RunDoltBranchTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltBranchScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltTagTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltTagTestScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltRemoteTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltRemoteTestScripts {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltUndropTests(t *testing.T, h DoltEnginetestHarness) {
	h.UseLocalFileSystem()
	defer h.Close()
	for _, script := range DoltUndropTestScripts {
		enginetest.TestScript(t, h, script)
	}
}

func RunHistorySystemTableTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range HistorySystemTableScriptTests {
		harness = harness.NewHarness(t)
		harness.Setup(setup.MydbData)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunHistorySystemTableTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range HistorySystemTableScriptTests {
		harness = harness.NewHarness(t)
		harness.Setup(setup.MydbData)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunUnscopedDiffSystemTableTests(t *testing.T, h DoltEnginetestHarness) {
	for _, test := range UnscopedDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, test)
		})
	}
}

func RunUnscopedDiffSystemTableTestsPrepared(t *testing.T, h DoltEnginetestHarness) {
	for _, test := range UnscopedDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, test)
		})
	}
}

func RunColumnDiffSystemTableTests(t *testing.T, h DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("correct behavior of dolt_column_diff only guaranteed on new format")
	}
	for _, test := range ColumnDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, h.NewHarness(t), test)
		})
	}
}

func RunColumnDiffSystemTableTestsPrepared(t *testing.T, h DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("correct behavior of dolt_column_diff only guaranteed on new format")
	}
	for _, test := range ColumnDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, h.NewHarness(t), test)
		})
	}
}

func RunStatBranchTests(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	for _, test := range StatBranchTests {
		t.Run(test.Name, func(t *testing.T) {
			// reset engine so provider statistics are clean
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)
			harness = harness.WithConfigureStats(true)
			e := mustNewEngine(t, harness)
			defer e.Close()
			enginetest.TestScriptWithEngine(t, e, harness, test)
		})
	}
}

func mustNewEngine(t *testing.T, h enginetest.Harness) enginetest.QueryEngine {
	e, err := h.NewEngine(t)
	if err != nil {
		require.NoError(t, err)
	}
	return e
}

func RunDiffTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunDiffTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunDiffStatTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffStatTableFunctionScriptTests {
		harness = harness.NewHarness(t)
		harness.Setup(setup.MydbData)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunDiffStatTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffStatTableFunctionScriptTests {
		harness = harness.NewHarness(t)
		harness.Setup(setup.MydbData)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunDiffSummaryTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffSummaryTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunDiffSummaryTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DiffSummaryTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunDoltPatchTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range PatchTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunDoltPatchTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range PatchTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunLogTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range LogTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			harness.SkipSetupCommit()
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunLogTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range LogTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			harness.SkipSetupCommit()
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunCommitDiffSystemTableTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range CommitDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunCommitDiffSystemTableTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range CommitDiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunDoltDiffSystemTableTests(t *testing.T, h DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, test := range DiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			h = h.NewHarness(t)
			defer h.Close()
			h.Setup(setup.MydbData)
			enginetest.TestScript(t, h, test)
		})
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, test := range Dolt1DiffSystemTableScripts {
			func() {
				h = h.NewHarness(t)
				defer h.Close()
				h.Setup(setup.MydbData)
				enginetest.TestScript(t, h, test)
			}()
		}
	}
}

func RunDoltDiffSystemTableTestsPrepared(t *testing.T, h DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, test := range DiffSystemTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			h = h.NewHarness(t)
			defer h.Close()
			h.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, h, test)
		})
	}

	if types.IsFormat_DOLT(types.Format_Default) {
		for _, test := range Dolt1DiffSystemTableScripts {
			func() {
				h = h.NewHarness(t)
				defer h.Close()
				h.Setup(setup.MydbData)
				enginetest.TestScriptPrepared(t, h, test)
			}()
		}
	}
}

func RunSchemaDiffTableFunctionTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range SchemaDiffTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunSchemaDiffTableFunctionTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range SchemaDiffTableFunctionScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunDoltDatabaseCollationDiffsTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range DoltDatabaseCollationScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunQueryDiffTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range QueryDiffTableScriptTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunSystemTableIndexesTests(t *testing.T, harness DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, stt := range SystemTableIndexTests {
		harness = harness.NewHarness(t).WithParallelism(1)
		defer harness.Close()
		harness.SkipSetupCommit()
		e := mustNewEngine(t, harness)
		defer e.Close()
		e.EngineAnalyzer().Coster = memo.NewMergeBiasedCoster()

		ctx := enginetest.NewContext(harness)
		for _, q := range stt.setup {
			enginetest.RunQueryWithContext(t, e, harness, ctx, q)
		}

		costers := []string{"inner", "lookup", "hash", "merge"}
		for i, c := range costers {
			t.Run(c, func(t *testing.T) {
				e.EngineAnalyzer().Coster = biasedCosters[i]
				for _, tt := range stt.queries {
					if tt.query == "select count(*) from dolt_blame_xy" && c == "inner" {
						// todo we either need join hints to work inside the blame view
						// and force the window relation to be primary, or we need the
						// blame view's timestamp columns to be specific enough to not
						// overlap during testing.
						t.Skip("the blame table is unstable as secondary table in join with exchange node")
					}
					t.Run(fmt.Sprintf("%s(%s): %s", stt.name, c, tt.query), func(t *testing.T) {
						if tt.skip {
							t.Skip()
						}

						ctx = ctx.WithQuery(tt.query)
						if tt.exp != nil {
							enginetest.TestQueryWithContext(t, ctx, e, harness, tt.query, tt.exp, nil, nil, nil)
						}
					})
				}
			})
		}
	}
}

var biasedCosters = []memo.Coster{
	memo.NewInnerBiasedCoster(),
	memo.NewLookupBiasedCoster(),
	memo.NewHashBiasedCoster(),
	memo.NewMergeBiasedCoster(),
}

func RunSystemTableIndexesTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	for _, stt := range SystemTableIndexTests {
		harness = harness.NewHarness(t).WithParallelism(2)
		defer harness.Close()
		harness.SkipSetupCommit()
		e := mustNewEngine(t, harness)
		defer e.Close()

		ctx := enginetest.NewContext(harness)
		for _, q := range stt.setup {
			enginetest.RunQueryWithContext(t, e, harness, ctx, q)
		}

		for _, tt := range stt.queries {
			t.Run(fmt.Sprintf("%s: %s", stt.name, tt.query), func(t *testing.T) {
				if tt.skip {
					t.Skip()
				}

				ctx = ctx.WithQuery(tt.query)
				if tt.exp != nil {
					enginetest.TestPreparedQueryWithContext(t, ctx, e, harness, tt.query, tt.exp, nil, nil, false)
				}
			})
		}
	}
}

func RunSystemTableFunctionIndexesTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range SystemTableFunctionIndexTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)
			enginetest.TestScript(t, harness, test)
		})
	}
}

func RunSystemTableFunctionIndexesTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, test := range SystemTableFunctionIndexTests {
		t.Run(test.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			harness.Setup(setup.MydbData)
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func RunAddAutoIncrementColumnTests(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(t, h, script)
	}
}

func RunDoltCherryPickTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range DoltCherryPickTests {
		harness = harness.NewHarness(t)
		enginetest.TestScript(t, harness, script)
		harness.Close()
	}
}

func RunDoltCherryPickTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range DoltCherryPickTests {
		harness = harness.NewHarness(t)
		enginetest.TestScriptPrepared(t, harness, script)
		harness.Close()
	}
}

func RunDoltCommitTests(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	for _, script := range DoltCommitTests {
		enginetest.TestScript(t, harness, script)
	}
}

func RunDoltCommitTestsPrepared(t *testing.T, harness DoltEnginetestHarness) {
	defer harness.Close()
	for _, script := range DoltCommitTests {
		enginetest.TestScriptPrepared(t, harness, script)
	}
}

func RunStatsHistogramTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltHistogramTests {
		func() {
			h = h.NewHarness(t).WithConfigureStats(true)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunStatsStorageTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltHistogramTests {
		func() {
			h = h.NewHarness(t).WithConfigureStats(true)
			e := mustNewEngine(t, h)
			if enginetest.IsServerEngine(e) {
				return
			}
			defer e.Close()
			defer h.Close()
			enginetest.TestScriptWithEngine(t, e, h, script)
		}()
	}
}

// these are sensitive to cardinality estimates,
// particularly the join-filter tests that trade-off
// smallest table first vs smallest join first
func RunJoinStatsTests(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h = h.WithConfigureStats(true)
	enginetest.TestJoinStats(t, h)
}

func RunPreparedStatisticsTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltHistogramTests {
		func() {
			h := h.NewHarness(t).WithConfigureStats(true)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunVersionedQueriesPreparedTests(t *testing.T, h DoltEnginetestHarness) {
	defer h.Close()
	h.Setup(setup.MydbData, []setup.SetupScript{VersionedQuerySetup, VersionedQueryViews})

	e, err := h.NewEngine(t)
	require.NoError(t, err)

	for _, tt := range queries.VersionedQueries {
		enginetest.TestPreparedQueryWithEngine(t, h, e, tt)
	}

	for _, tt := range queries.VersionedScripts {
		enginetest.TestScriptWithEnginePrepared(t, e, h, tt)
	}
}

func RunAddDropPrimaryKeysTests(t *testing.T, harness DoltEnginetestHarness) {
	t.Run("adding and dropping primary keys does not result in duplicate NOT NULL constraints", func(t *testing.T) {
		harness = harness.NewHarness(t)
		defer harness.Close()
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
		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.Session()))
		ws, err := harness.Session().WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, doltdb.TableName{Name: "test"})
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
		harness := harness.NewHarness(t)
		defer harness.Close()
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

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.Session()))
		ws, err := harness.Session().WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, doltdb.TableName{Name: "test"})
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
		harness := harness.NewHarness(t)
		defer harness.Close()
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
		harness := harness.NewHarness(t)
		defer harness.Close()
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

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.Session()))
		ws, err := harness.Session().WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, doltdb.TableName{Name: "test"})
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

func RunDoltVerifyConstraintsTests(t *testing.T, harness DoltEnginetestHarness) {
	for _, script := range DoltVerifyConstraintsTestScripts {
		func() {
			harness = harness.NewHarness(t)
			defer harness.Close()
			enginetest.TestScript(t, harness, script)
		}()
	}
}

func RunDoltStorageFormatTests(t *testing.T, h DoltEnginetestHarness) {
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
	defer h.Close()
	enginetest.TestScript(t, h, script)
}

func RunThreeWayMergeWithSchemaChangeScripts(t *testing.T, h DoltEnginetestHarness) {
	skipOldFormat(t)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsBasicCases, "basic cases", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsForDataConflicts, "data conflicts", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsCollations, "collation changes", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsConstraints, "constraint changes", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsSchemaConflicts, "schema conflicts", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsGeneratedColumns, "generated columns", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsForJsonConflicts, "json merge", false)

	// Run non-symmetric schema merge tests in just one direction
	t.Run("type changes", func(t *testing.T) {
		for _, script := range SchemaChangeTestsTypeChanges {
			// run in a func() so we can cleanly defer closing the harness
			func() {
				h := h.NewHarness(t)
				defer h.Close()
				enginetest.TestScript(t, h, convertMergeScriptTest(script, false))
			}()
		}
	})
}

func RunThreeWayMergeWithSchemaChangeScriptsPrepared(t *testing.T, h DoltEnginetestHarness) {
	skipOldFormat(t)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsBasicCases, "basic cases", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsForDataConflicts, "data conflicts", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsCollations, "collation changes", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsConstraints, "constraint changes", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsSchemaConflicts, "schema conflicts", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsGeneratedColumns, "generated columns", false)
	runMergeScriptTestsInBothDirections(t, SchemaChangeTestsForJsonConflicts, "json merge", false)

	// Run non-symmetric schema merge tests in just one direction
	t.Run("type changes", func(t *testing.T) {
		for _, script := range SchemaChangeTestsTypeChanges {
			// run in a func() so we can cleanly defer closing the harness
			func() {
				h := h.NewHarness(t)
				defer h.Close()
				enginetest.TestScriptPrepared(t, h, convertMergeScriptTest(script, false))
			}()
		}
	})
}

// runMergeScriptTestsInBothDirections creates a new test run, named |name|, and runs the specified merge |tests|
// in both directions (right to left merge, and left to right merge). If
// |runAsPrepared| is true then the test scripts will be run using the prepared
// statement test code.
func runMergeScriptTestsInBothDirections(t *testing.T, tests []MergeScriptTest, name string, runAsPrepared bool) {
	t.Run(name, func(t *testing.T) {
		t.Run("right to left merges", func(t *testing.T) {
			for _, script := range tests {
				// run in a func() so we can cleanly defer closing the harness
				func() {
					h := newDoltHarness(t)
					defer h.Close()
					if runAsPrepared {
						enginetest.TestScriptPrepared(t, h, convertMergeScriptTest(script, false))
					} else {
						enginetest.TestScript(t, h, convertMergeScriptTest(script, false))
					}
				}()
			}
		})
		t.Run("left to right merges", func(t *testing.T) {
			for _, script := range tests {
				func() {
					h := newDoltHarness(t)
					defer h.Close()
					if runAsPrepared {
						enginetest.TestScriptPrepared(t, h, convertMergeScriptTest(script, true))
					} else {
						enginetest.TestScript(t, h, convertMergeScriptTest(script, true))
					}
				}()
			}
		})
	})
}

func SkipByDefaultInCI(t *testing.T) {
	if os.Getenv("CI") != "" && os.Getenv("DOLT_TEST_RUN_NON_RACE_TESTS") == "" {
		t.Skip()
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
		newCtx := harness.NewSession()
		return newCtx.Session, nil
	}
}

func RunDoltReflogTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltReflogTestScripts {
		func() {
			h = h.NewHarness(t)
			defer h.Close()
			h.UseLocalFileSystem()
			h.SkipSetupCommit()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltReflogTestsPrepared(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltReflogTestScripts {
		func() {
			h = h.NewHarness(t)
			defer h.Close()
			h.UseLocalFileSystem()
			h.SkipSetupCommit()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}

func RunDoltWorkspaceTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltWorkspaceScriptTests {
		func() {
			h = h.NewHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func RunDoltHelpSystemTableTests(t *testing.T, harness DoltEnginetestHarness) {
	dtables.DoltCommand = doltcmd.DoltCommand

	for _, script := range DoltHelpScripts {
		t.Run(script.Name, func(t *testing.T) {
			harness = harness.NewHarness(t)
			defer harness.Close()
			enginetest.TestScript(t, harness, script)
		})
	}
}
