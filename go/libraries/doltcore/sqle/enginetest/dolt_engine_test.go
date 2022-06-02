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
	"os"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
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

	var test queries.QueryTest
	test = queries.QueryTest{
		Query: `select i from mytable where i = 1`,
		Expected: []sql.Row{
			{1},
		},
	}

	harness := newDoltHarness(t)
	engine := enginetest.NewEngine(t, harness)
	enginetest.CreateIndexes(t, harness, engine)
	//engine.Analyzer.Debug = true
	//engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, test.Query, test.Expected, test.ExpectedColumns, nil)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	//t.Skip()

	var scripts = []queries.ScriptTest{
		{
			Name: "alter modify column type, make primary key spatial",
			SetUpScript: []string{
				"create table point_tbl (p int primary key)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "alter table point_tbl modify column p point primary key",
					ExpectedErr: schema.ErrUsingSpatialKey,
				},
			},
		},
	}

	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range scripts {
		enginetest.TestScript(t, harness, test)
	}
}

func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()

	var test queries.QueryTest
	test = queries.QueryTest{
		Query: `SELECT ST_SRID(g, 0) from geometry_table order by i`,
		Expected: []sql.Row{
			{sql.Point{X: 1, Y: 2}},
			{sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{sql.Point{X: 1, Y: 2}},
			{sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
		},
	}

	harness := newDoltHarness(t)
	//engine := enginetest.NewEngine(t, harness)
	//enginetest.CreateIndexes(t, harness, engine)
	engine := enginetest.NewSpatialEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, test.Query, test.Expected, nil, nil)
}

func TestVersionedQueries(t *testing.T) {
	enginetest.TestVersionedQueries(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	skipNewFormat(t)

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
	enginetest.TestQueryPlans(t, newDoltHarness(t).WithParallelism(1).WithSkippedQueries(skipped))
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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		for i := len(queries.InsertScripts) - 1; i >= 0; i-- {
			//TODO: on duplicate key broken for foreign keys in new format
			if queries.InsertScripts[i].Name == "Insert on duplicate key" {
				queries.InsertScripts = append(queries.InsertScripts[:i], queries.InsertScripts[i+1:]...)
			}
		}
	}
	enginetest.TestInsertInto(t, newDoltHarness(t))
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, newDoltHarness(t))
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
	var skipped []string
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// skip update for join
		patternToSkip := "join"
		skipped = make([]string, 0)
		for _, q := range queries.UpdateTests {
			if strings.Contains(strings.ToLower(q.WriteQuery), patternToSkip) {
				skipped = append(skipped, q.WriteQuery)
			}
		}
	}

	enginetest.TestUpdate(t, newDoltHarness(t).WithSkippedQueries(skipped))
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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		skipped = append(skipped,
			// Different error output for primary key error
			"failed statements data validation for INSERT, UPDATE",
			// missing FK violation
			"failed statements data validation for DELETE, REPLACE",
			// wrong results
			"Indexed Join On Keyless Table",
			// spurious fk violation
			"Nested Subquery projections (NTC)",
			// Different query plans
			"Partial indexes are used and return the expected result",
			"Multiple indexes on the same columns in a different order",
			// panic
			"Ensure proper DECIMAL support (found by fuzzer)",
		)
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

			for _, statement := range script.SetUpScript {
				if sh, ok := interface{}(harness).(enginetest.SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				enginetest.RunQueryWithContext(t, engine, ctx, statement)
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
						enginetest.AssertErrWithCtx(t, engine, ctx, assertion.Query, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, ctx, assertion.Query, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.TestQueryWithContext(t, ctx, engine, assertion.Query, assertion.Expected, nil, nil)
					})
				}
			}
		})
	}
}

func TestJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, newDoltHarness(t))
}

func TestUserPrivileges(t *testing.T) {
	enginetest.TestUserPrivileges(t, newDoltHarness(t))
}

func TestUserAuthentication(t *testing.T) {
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
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
		//TODO: fix REPLACE always returning a successful deletion
		if test.Name != "Parameters resolve inside of REPLACE" {
			tests = append(tests, test)
		}
	}
	queries.ProcedureLogicTests = tests

	enginetest.TestStoredProcedures(t, newDoltHarness(t))
}

func TestTransactions(t *testing.T) {
	skipNewFormat(t)
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

func TestDoltMerge(t *testing.T) {
	skipNewFormat(t)
	for _, script := range MergeScripts {
		// dolt versioning conflicts with reset harness -- use new harness every time
		enginetest.TestScript(t, newDoltHarness(t), script)
	}
}

func TestDoltReset(t *testing.T) {
	for _, script := range DoltReset {
		// dolt versioning conflicts with reset harness -- use new harness every time
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'commit on new-branch')",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ call dolt_merge('main')",
				Expected: []sql.Row{{0}},
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
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				Query:    "/* client b */ delete from dolt_conflicts_test",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
	skipNewFormat(t)
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range HistorySystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
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

func TestDiffTableFunction(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffTableFunctionScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestCommitDiffSystemTable(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range CommitDiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestDiffSystemTable(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	for _, test := range DiffSystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScript(t, harness, test)
		})
	}
}

func TestTestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, newDoltHarness(t))
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, newDoltHarness(t))
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
		session := ctx.Session.(*dsess.DoltSession).Session.NewDoltSession(globals)
		err := session.RemoveAllPersistedGlobals()
		require.NoError(t, err)
		return session
	}

	enginetest.TestPersist(t, harness, newPersistableSession)
}

func TestKeylessUniqueIndex(t *testing.T) {
	harness := newDoltHarness(t)
	enginetest.TestKeylessUniqueIndex(t, harness)
}

func TestQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestQueriesPrepared(t, newDoltHarness(t))
}

func TestPreparedStaticIndexQuery(t *testing.T) {
	enginetest.TestPreparedStaticIndexQuery(t, newDoltHarness(t))
}

func TestSpatialQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)

	enginetest.TestSpatialQueriesPrepared(t, newDoltHarness(t))
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
	var skipped []string
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// skip select join for update
		skipped = make([]string, 0)
		for _, q := range queries.UpdateTests {
			if strings.Contains(strings.ToLower(q.WriteQuery), "join") {
				skipped = append(skipped, q.WriteQuery)
			}
		}
	}

	enginetest.TestUpdateQueriesPrepared(t, newDoltHarness(t).WithSkippedQueries(skipped))
}

func TestInsertQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	var skipped []string
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// skip keyless
		skipped = make([]string, 0)
		for _, q := range queries.UpdateTests {
			if strings.Contains(strings.ToLower(q.WriteQuery), "keyless") {
				skipped = append(skipped, q.WriteQuery)
			}
		}
	}

	enginetest.TestInsertQueriesPrepared(t, newDoltHarness(t).WithSkippedQueries(skipped))
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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		skipped = append(skipped,
			// Different error output for primary key error
			"failed statements data validation for INSERT, UPDATE",
			// missing FK violation
			"failed statements data validation for DELETE, REPLACE",
			// wrong results
			"Indexed Join On Keyless Table",
			// spurious fk violation
			"Nested Subquery projections (NTC)",
			// Different query plans
			"Partial indexes are used and return the expected result",
			"Multiple indexes on the same columns in a different order",
			// panic
			"Ensure proper DECIMAL support (found by fuzzer)",
		)
		for _, s := range queries.SpatialScriptTests {
			skipped = append(skipped, s.Name)
		}
	}

	skipPreparedTests(t)
	enginetest.TestScriptsPrepared(t, newDoltHarness(t).WithSkippedQueries(skipped))
}

func TestInsertScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	if types.IsFormat_DOLT_1(types.Format_Default) {
		for i := len(queries.InsertScripts) - 1; i >= 0; i-- {
			//TODO: on duplicate key broken for foreign keys in new format
			if queries.InsertScripts[i].Name == "Insert on duplicate key" {
				queries.InsertScripts = append(queries.InsertScripts[:i], queries.InsertScripts[i+1:]...)
			}
		}
	}
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
	//TODO: on duplicate key broken for foreign keys in new format
	skipNewFormat(t)
	skipPreparedTests(t)
	enginetest.TestPreparedInsert(t, newDoltHarness(t))
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
		assert.False(t, newRows.Empty())
		assert.Equal(t, newRows.Count(), uint64(2))
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
		assert.False(t, newIdx.Empty())
		assert.Equal(t, newIdx.Count(), uint64(2))
	})
}

func skipNewFormat(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}
}

func skipPreparedTests(t *testing.T) {
	if skipPrepared {
		t.Skip("skip prepared")
	}
}
