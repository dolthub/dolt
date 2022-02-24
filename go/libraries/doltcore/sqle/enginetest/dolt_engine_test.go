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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

func init() {
	sqle.MinRowsPerPartition = 8
	sqle.MaxRowsPerPartition = 1024
}

func TestQueries(t *testing.T) {
	enginetest.TestQueries(t, newDoltHarness(t))
}

func TestSingleQuery(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		Query:    `SELECT * from mytable`,
		Expected: []sql.Row{},
	}

	harness := newDoltHarness(t)
	engine := enginetest.NewEngine(t, harness)
	enginetest.CreateIndexes(t, harness, engine)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, test.ExpectedColumns, test.Bindings)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var scripts = []enginetest.ScriptTest{
		{
			Name: "insert into sparse auto_increment table",
			SetUpScript: []string{
				"create table auto (pk int primary key auto_increment)",
				"insert into auto values (10), (20), (30)",
				"insert into auto values (NULL)",
				"insert into auto values (40)",
				"insert into auto values (0)",
			},
			Assertions: []enginetest.ScriptTestAssertion{
				{
					Query: "select * from auto order by 1",
					Expected: []sql.Row{
						{10}, {20}, {30}, {31}, {40}, {41},
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
		engine.Analyzer.Debug = true
		engine.Analyzer.Verbose = true
		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestVersionedQueries(t *testing.T) {
	enginetest.TestVersionedQueries(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// todo(andy): unskip after secondary index support
		t.Skip()
	}

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
	enginetest.TestInsertInto(t, newDoltHarness(t))
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, newDoltHarness(t))
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, newDoltHarness(t))
}

func TestReplaceInto(t *testing.T) {
	t.Skipf("Skipping, replace returns the wrong number of rows in some cases")
	enginetest.TestReplaceInto(t, newDoltHarness(t))
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, newDoltHarness(t))
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newDoltHarness(t))
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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		// todo(andy): unskip
		t.Skip()
	}

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

func TestPkOrdinals(t *testing.T) {
	enginetest.TestPkOrdinals(t, newDoltHarness(t))
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
	t.Skip("Dolt doesn't yet support dropping the primary database, which these tests do")
	enginetest.TestDropDatabase(t, newDoltHarness(t))
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, newDoltHarness(t))
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, newDoltHarness(t))
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

func TestJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, newDoltHarness(t))
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newDoltHarness(t))
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
	enginetest.TestTransactionScripts(t, newDoltHarness(t))
	for _, script := range DoltTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}

	for _, script := range DoltSqlFuncTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t), script)
	}
}

func TestDoltScripts(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltScripts {
		enginetest.TestScript(t, harness, script)
	}
}

func TestDoltMerge(t *testing.T) {
	harness := newDoltHarness(t)
	for _, script := range DoltMerge {
		enginetest.TestScript(t, harness, script)
	}
}

func TestScopedDoltHistorySystemTables(t *testing.T) {
	harness := newDoltHarness(t)
	for _, test := range ScopedDoltHistoryScriptTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

// TestSingleTransactionScript is a convenience method for debugging a single transaction test. Unskip and set to the
// desired test.
func TestSingleTransactionScript(t *testing.T) {
	t.Skip()

	script := enginetest.TransactionTest{
		Name: "rollback",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	}

	enginetest.TestTransactionScript(t, newDoltHarness(t), script)
}

func TestSystemTableQueries(t *testing.T) {
	enginetest.RunQueryTests(t, newDoltHarness(t), BrokenSystemTableQueries)
}

func TestUnscopedDoltDiffSystemTable(t *testing.T) {
	harness := newDoltHarness(t)
	for _, test := range UnscopedDiffTableTests {
		databases := harness.NewDatabases("mydb")
		engine := enginetest.NewEngineWithDbs(t, harness, databases)
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptWithEngine(t, engine, harness, test)
		})
	}
}

func TestDoltDiffSystemTable(t *testing.T) {
	harness := newDoltHarness(t)
	for _, test := range DiffTableTests {
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
