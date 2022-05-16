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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/pkg/profile"
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
var skipPreparedFlag = "DOLT_SKIP_PREPARED_ENGINETESTS"

func init() {
	sqle.MinRowsPerPartition = 8
	sqle.MaxRowsPerPartition = 1024

	if v := os.Getenv(skipPreparedFlag); v != "" {
		skipPrepared = true
	}
}

func TestQueries(t *testing.T) {
	defer profile.Start().Stop()
	enginetest.TestQueries(t, newDoltHarness(t))
}

func TestSingleQuery(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
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

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, test.ExpectedColumns)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var scripts = []enginetest.ScriptTest{
		{
			Name: "Multialter DDL with ADD/DROP Primary Key",
			SetUpScript: []string{
				"CREATE TABLE t(pk int primary key, v1 int)",
			},
			Assertions: []enginetest.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE t ADD COLUMN (v2 int), drop primary key, add primary key (v2)",
					Expected: []sql.Row{{sql.NewOkResult(0)}},
				},
				{
					Query: "DESCRIBE t",
					Expected: []sql.Row{
						{"pk", "int", "NO", "", "", ""},
						{"v1", "int", "YES", "", "", ""},
						{"v2", "int", "NO", "PRI", "", ""},
					},
				},
				{
					Query:       "ALTER TABLE t ADD COLUMN (v3 int), drop primary key, add primary key (notacolumn)",
					ExpectedErr: sql.ErrKeyColumnDoesNotExist,
				},
				{
					Query: "DESCRIBE t",
					Expected: []sql.Row{
						{"pk", "int", "NO", "", "", ""},
						{"v1", "int", "YES", "", "", ""},
						{"v2", "int", "NO", "PRI", "", ""},
					},
				},
			},
		},
	}

	harness := newDoltHarness(t)
	for _, test := range scripts {
		myDb := harness.NewDatabase("mydb")
		databases := []sql.Database{myDb}
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		//engine.Analyzer.Debug = true
		//engine.Analyzer.Verbose = true
		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		Query: `SELECT ST_SRID(g, 0) from geometry_table order by i`,
		Expected: []sql.Row{
			{sql.Point{X: 1, Y: 2}},
			{sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{sql.Point{X: 1, Y: 2}},
			{sql.Linestring{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{sql.Polygon{Lines: []sql.Linestring{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
		},
	}

	harness := newDoltHarness(t)
	//engine := enginetest.NewEngine(t, harness)
	//enginetest.CreateIndexes(t, harness, engine)
	engine := enginetest.NewSpatialEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, nil)
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
	skipNewFormat(t)
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
		for i := len(enginetest.InsertScripts) - 1; i >= 0; i-- {
			//TODO: test uses keyless foreign key logic which is not yet fully implemented
			if enginetest.InsertScripts[i].Name == "Insert on duplicate key" {
				enginetest.InsertScripts = append(enginetest.InsertScripts[:i], enginetest.InsertScripts[i+1:]...)
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
	skipNewFormat(t)
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
		// skip update ffor join
		patternToSkip := "join"
		skipped = make([]string, 0)
		for _, q := range enginetest.UpdateTests {
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

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, newDoltHarness(t))
}

func TestScripts(t *testing.T) {
	skipNewFormat(t)

	skipped := []string{
		"create index r_c0 on r (c0);",
		// These rely on keyless tables which orders its rows by hash rather than contents, meaning changing types causes different ordering
		"SELECT group_concat(`attribute`) FROM t where o_id=2",
		"SELECT group_concat(o_id) FROM t WHERE `attribute`='color'",

		// TODO(aaron): go-mysql-server GroupBy with grouping
		// expressions currently has a bug where it does not insert
		// necessary Sort nodes.  These queries used to work by
		// accident based on the return order from the storage layer,
		// but they no longer do.
		"SELECT pk, SUM(DISTINCT v1), MAX(v1) FROM mytable GROUP BY pk ORDER BY pk",
		"SELECT pk, MIN(DISTINCT v1), MAX(DISTINCT v1) FROM mytable GROUP BY pk ORDER BY pk",

		// no support for naming unique constraints yet, engine dependent
		"show create table t2",
	}
	enginetest.TestScripts(t, newDoltHarness(t).WithSkippedQueries(skipped))
}

// TestDoltUserPrivileges tests Dolt-specific code that needs to handle user privilege checking
func TestDoltUserPrivileges(t *testing.T) {
	skipNewFormat(t)

	harness := newDoltHarness(t)
	for _, script := range DoltUserPrivTests {
		t.Run(script.Name, func(t *testing.T) {
			myDb := harness.NewDatabase("mydb")
			databases := []sql.Database{myDb}
			engine := enginetest.NewEngineWithDbs(t, harness, databases)
			defer engine.Close()

			ctx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.Analyzer.Catalog.GrantTables.AddRootAccount()

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
	skipNewFormat(t)
	enginetest.TestCreateTable(t, newDoltHarness(t))
}

func TestPkOrdinalsDDL(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestPkOrdinalsDDL(t, newDoltHarness(t))
}

func TestPkOrdinalsDML(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestPkOrdinalsDML(t, newDoltHarness(t))
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newDoltHarness(t))
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, newDoltHarness(t))
}

func TestRenameColumn(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestRenameColumn(t, newDoltHarness(t))
}

func TestAddColumn(t *testing.T) {
	enginetest.TestAddColumn(t, newDoltHarness(t))
}

func TestModifyColumn(t *testing.T) {
	enginetest.TestModifyColumn(t, newDoltHarness(t))
}

func TestDropColumn(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestDropColumn(t, newDoltHarness(t))
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, newDoltHarness(t))
}

func TestDropDatabase(t *testing.T) {
	t.Skip("Dolt doesn't yet support dropping the primary database, which these tests do")
	enginetest.TestDropDatabase(t, newDoltHarness(t))
}

func TestCreateForeignKeys(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestCreateForeignKeys(t, newDoltHarness(t))
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, newDoltHarness(t))
}

func TestForeignKeys(t *testing.T) {
	skipNewFormat(t)
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

func TestExplode(t *testing.T) {
	t.Skipf("Unsupported types")
	enginetest.TestExplode(t, newDoltHarness(t))
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, newDoltHarness(t))
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, newDoltHarness(t))
}

func TestVersionedViews(t *testing.T) {
	skipNewFormat(t)
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
	skipNewFormat(t)
	enginetest.TestColumnDefaults(t, newDoltHarness(t))
}

func TestAlterTable(t *testing.T) {
	skipNewFormat(t)
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
	skipNewFormat(t)
	enginetest.TestTriggers(t, newDoltHarness(t))
}

func TestRollbackTriggers(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestRollbackTriggers(t, newDoltHarness(t))
}

func TestStoredProcedures(t *testing.T) {
	tests := make([]enginetest.ScriptTest, 0, len(enginetest.ProcedureLogicTests))
	for _, test := range enginetest.ProcedureLogicTests {
		//TODO: fix REPLACE always returning a successful deletion
		if test.Name != "Parameters resolve inside of REPLACE" {
			tests = append(tests, test)
		}
	}
	enginetest.ProcedureLogicTests = tests

	enginetest.TestStoredProcedures(t, newDoltHarness(t))
}

func TestTransactions(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestTransactionScripts(t, newDoltHarness(t))

	for _, script := range DoltTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}

	for _, script := range DoltSqlFuncTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}

	for _, script := range DoltConflictHandlingTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	skipNewFormat(t)
	enginetest.TestConcurrentTransactions(t, newDoltHarness(t))
}

func TestDoltScripts(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		//TODO: add prolly path for index verification
		t.Skip("new format using old noms path, need to update")
	}
	harness := newDoltHarness(t)
	for _, script := range DoltScripts {
		enginetest.TestScript(t, harness, script)
	}
}

func TestDescribeTableAsOf(t *testing.T) {
	// This test relies on altering schema in order to describe the table at different revisions
	// and see changes. Until the new storage format supports altering schema, we need to skip them.
	// Once the new storage format supports altering schema, we can move these ScriptTests back into
	// the DoltScripts var so they get picked up by the TestDoltScripts method and remove this method.
	skipNewFormat(t)

	enginetest.TestScript(t, newDoltHarness(t), DescribeTableAsOfScriptTest)
}

func TestShowCreateTableAsOf(t *testing.T) {
	// This test relies on altering schema in order to show the create table statement at different revisions
	// and see changes. Until the new storage format supports altering schema, we need to skip them.
	// Once the new storage format supports altering schema, we can move these ScriptTests back into
	// the DoltScripts var so they get picked up by the TestDoltScripts method and remove this method.
	skipNewFormat(t)

	enginetest.TestScript(t, newDoltHarness(t), ShowCreateTableAsOfScriptTest)
}

func TestDoltMerge(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	for _, script := range DoltMerge {
		enginetest.TestScript(t, harness, script)
	}
}

func TestDoltReset(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	for _, script := range DoltReset {
		enginetest.TestScript(t, harness, script)
	}
}

// TestSingleTransactionScript is a convenience method for debugging a single transaction test. Unskip and set to the
// desired test.
func TestSingleTransactionScript(t *testing.T) {
	t.Skip()

	script := enginetest.TransactionTest{
		Name: "allow commit conflicts on, conflict on dolt_merge",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"INSERT INTO test VALUES (0, 0)",
			"SELECT DOLT_COMMIT('-a', '-m', 'initial table');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
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
	for _, test := range HistorySystemTableScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestUnscopedDiffSystemTable(t *testing.T) {
	harness := newDoltHarness(t)
	for _, test := range UnscopedDiffSystemTableScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestDiffTableFunction(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)

	for _, test := range DiffTableFunctionScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestCommitDiffSystemTable(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	for _, test := range CommitDiffSystemTableScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestDiffSystemTable(t *testing.T) {
	skipNewFormat(t)
	harness := newDoltHarness(t)
	for _, test := range DiffSystemTableScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestTestReadOnlyDatabases(t *testing.T) {
	enginetest.TestReadOnlyDatabases(t, newDoltHarness(t))
}

func TestAddDropPks(t *testing.T) {
	skipNewFormat(t)
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
	skipNewFormat(t)
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
	skipNewFormat(t)
	skipPreparedTests(t)

	enginetest.TestSpatialQueriesPrepared(t, newDoltHarness(t))
}

func TestVersionedQueriesPrepared(t *testing.T) {
	skipNewFormat(t)
	skipPreparedTests(t)
	enginetest.TestVersionedQueriesPrepared(t, newDoltHarness(t))
}

func TestInfoSchemaPrepared(t *testing.T) {
	skipNewFormat(t)
	skipPreparedTests(t)
	enginetest.TestInfoSchemaPrepared(t, newDoltHarness(t))
}

func TestUpdateQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	var skipped []string
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// skip select join for update
		skipped = make([]string, 0)
		for _, q := range enginetest.UpdateTests {
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
		for _, q := range enginetest.UpdateTests {
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
	skipNewFormat(t)
	skipPreparedTests(t)
	enginetest.TestScriptsPrepared(t, newDoltHarness(t))
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

func TestExplodePrepared(t *testing.T) {
	t.Skip("feature not supported")
	skipPreparedTests(t)
	enginetest.TestExplodePrepared(t, newDoltHarness(t))
}

func TestViewsPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestViewsPrepared(t, newDoltHarness(t))
}

func TestVersionedViewsPrepared(t *testing.T) {
	t.Skip("unsupported for prepareds")
	skipPreparedTests(t)
	enginetest.TestVersionedViewsPrepared(t, newDoltHarness(t))
}

func TestShowTableStatusPrepared(t *testing.T) {
	skipPreparedTests(t)
	enginetest.TestShowTableStatusPrepared(t, newDoltHarness(t))
}

func TestPrepared(t *testing.T) {
	skipNewFormat(t)
	skipPreparedTests(t)
	enginetest.TestPrepared(t, newDoltHarness(t))
}

func TestPreparedInsert(t *testing.T) {
	skipPreparedTests(t)
	if types.IsFormat_DOLT_1(types.Format_Default) {
		//TODO: test uses keyless foreign key logic which is not yet fully implemented
		t.Skip("test uses keyless foreign key logic which is not yet fully implemented")
	}
	enginetest.TestPreparedInsert(t, newDoltHarness(t))
}

func TestAddDropPrimaryKeys(t *testing.T) {
	skipNewFormat(t)
	t.Run("adding and dropping primary keys does not result in duplicate NOT NULL constraints", func(t *testing.T) {
		harness := newDoltHarness(t)
		addPkScript := enginetest.ScriptTest{
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
			Assertions: []enginetest.ScriptTestAssertion{
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
		script := enginetest.ScriptTest{
			Name: "add primary keys to table with index",
			SetUpScript: []string{
				"create table test (id int not null, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test ADD constraint test_check CHECK (c1 > 0)",
				"ALTER TABLE test ADD PRIMARY KEY(id)",
			},
			Assertions: []enginetest.ScriptTestAssertion{
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
			},
		}
		enginetest.TestScript(t, harness, script)

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.session))
		ws, err := harness.session.WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, "test")
		require.NoError(t, err)
		require.True(t, ok)

		require.NoError(t, err)

		// Assert the new index map is not empty
		newMap, err := table.GetNomsRowData(ctx)
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))
	})

	t.Run("Add primary key when one more cells contain NULL", func(t *testing.T) {
		harness := newDoltHarness(t)
		script := enginetest.ScriptTest{
			Name: "Add primary key when one more cells contain NULL",
			SetUpScript: []string{
				"create table test (id int not null, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test ADD PRIMARY KEY (c1)",
				"ALTER TABLE test ADD COLUMN (c2 INT NULL)",
				"ALTER TABLE test DROP PRIMARY KEY",
			},
			Assertions: []enginetest.ScriptTestAssertion{
				{
					Query:          "ALTER TABLE test ADD PRIMARY KEY (id, c1, c2)",
					ExpectedErrStr: "primary key cannot have NULL values",
				},
			},
		}
		enginetest.TestScript(t, harness, script)
	})

	t.Run("Drop primary key from table with index", func(t *testing.T) {
		harness := newDoltHarness(t)
		script := enginetest.ScriptTest{
			Name: "Drop primary key from table with index",
			SetUpScript: []string{
				"create table test (id int not null primary key, c1 int);",
				"create index c1_idx on test(c1)",
				"insert into test values (1,1),(2,2)",
				"ALTER TABLE test DROP PRIMARY KEY",
			},
			Assertions: []enginetest.ScriptTestAssertion{
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
			},
		}
		enginetest.TestScript(t, harness, script)

		ctx := sql.NewContext(context.Background(), sql.WithSession(harness.session))
		ws, err := harness.session.WorkingSet(ctx, "mydb")
		require.NoError(t, err)

		table, ok, err := ws.WorkingRoot().GetTable(ctx, "test")
		require.NoError(t, err)
		require.True(t, ok)

		require.NoError(t, err)

		// Assert the index map is not empty
		newMap, err := table.GetNomsIndexRowData(ctx, "c1_idx")
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))
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
