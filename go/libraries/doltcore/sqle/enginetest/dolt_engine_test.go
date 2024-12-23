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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

// SkipPreparedsCount is used by the "ci-check-repo CI workflow
// as a reminder to consider prepareds when adding a new
// enginetest suite.
const SkipPreparedsCount = 83

func TestQueries(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestQueries(t, h)
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
		enginetest.RunQueryWithContext(t, engine, harness, nil, q)
	}

	// engine.EngineAnalyzer().Debug = true
	// engine.EngineAnalyzer().Verbose = true

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

func TestSchemaOverrides(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSchemaOverridesTest(t, harness)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var scripts = []queries.ScriptTest{}

	for _, script := range scripts {
		harness := newDoltHarness(t)
		harness.Setup(setup.MydbData)

		engine, err := harness.NewEngine(t)
		if err != nil {
			panic(err)
		}
		// engine.EngineAnalyzer().Debug = true
		// engine.EngineAnalyzer().Verbose = true

		enginetest.TestScriptWithEngine(t, engine, harness, script)
	}
}

func newUpdateResult(matched, updated int) gmstypes.OkResult {
	return gmstypes.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{Matched: matched, Updated: updated},
	}
}

func TestAutoIncrementTrackerLockMode(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunAutoIncrementTrackerLockModeTest(t, harness)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleMergeScript(t *testing.T) {
	t.Skip()
	var scripts = []MergeScriptTest{
		{
			Name: "adding generated column to one side, non-generated column to other side",
			AncSetUpScript: []string{
				"create table t (pk int primary key);",
				"insert into t values (1), (2);",
			},
			RightSetUpScript: []string{
				"alter table t add column col2 varchar(100);",
				"insert into t (pk, col2) values (3, '3hello'), (4, '4hello');",
				"alter table t add index (col2);",
			},
			LeftSetUpScript: []string{
				"alter table t add column col1 int default (pk + 100);",
				"insert into t (pk) values (5), (6);",
				"alter table t add index (col1);",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "call dolt_merge('right');",
					Expected: []sql.Row{{doltCommit, 0, 0}},
				},
				{
					Query: "select pk, col1, col2 from t;",
					Expected: []sql.Row{
						{1, 101, nil},
						{2, 102, nil},
						{3, 103, "3hello"},
						{4, 104, "4hello"},
						{5, 105, nil},
						{6, 106, nil},
					},
				},
			},
		},
		// {
		// 	Name: "adding generated columns to both sides",
		// 	AncSetUpScript: []string{
		// 		"create table t (pk int primary key);",
		// 		"insert into t values (1), (2);",
		// 	},
		// 	RightSetUpScript: []string{
		// 		"alter table t add column col2 varchar(100) as (concat(pk, 'hello'));",
		// 		"insert into t (pk) values (3), (4);",
		// 		"alter table t add index (col2);",
		// 	},
		// 	LeftSetUpScript: []string{
		// 		"alter table t add column col1 int as (pk + 100) stored;",
		// 		"insert into t (pk) values (5), (6);",
		// 		"alter table t add index (col1);",
		// 	},
		// 	Assertions: []queries.ScriptTestAssertion{
		// 		{
		// 			Query:    "call dolt_merge('right');",
		// 			Expected: []sql.Row{{doltCommit, 0, 0}},
		// 		},
		// 		{
		// 			Query: "select pk, col1, col2 from t;",
		// 			Expected: []sql.Row{
		// 				{1, 101, "1hello"},
		// 				{2, 102, "2hello"},
		// 				{3, 103, "3hello"},
		// 				{4, 104, "4hello"},
		// 				{5, 105, "5hello"},
		// 				{6, 106, "6hello"},
		// 			},
		// 		},
		// 	},
		// },
		// {
		// 	Name: "adding a column with a literal default value",
		// 	AncSetUpScript: []string{
		// 		"CREATE table t (pk int primary key);",
		// 		"INSERT into t values (1);",
		// 	},
		// 	RightSetUpScript: []string{
		// 		"alter table t add column c1 varchar(100) default ('hello');",
		// 		"insert into t values (2, 'hi');",
		// 		"alter table t add index idx1 (c1, pk);",
		// 	},
		// 	LeftSetUpScript: []string{
		// 		"insert into t values (3);",
		// 	},
		// 	Assertions: []queries.ScriptTestAssertion{
		// 		{
		// 			Query:    "call dolt_merge('right');",
		// 			Expected: []sql.Row{{doltCommit, 0, 0}},
		// 		},
		// 		{
		// 			Query:    "select * from t;",
		// 			Expected: []sql.Row{{1, "hello"}, {2, "hi"}, {3, "hello"}},
		// 		},
		// 	},
		// },
		// {
		// 	Name: "check constraint violation - right side violates new check constraint",
		// 	AncSetUpScript: []string{
		// 		"set autocommit = 0;",
		// 		"CREATE table t (pk int primary key, col00 int, col01 int, col1 varchar(100) default ('hello'));",
		// 		"INSERT into t values (1, 0, 0, 'hi');",
		// 		"alter table t add index idx1 (col1);",
		// 	},
		// 	RightSetUpScript: []string{
		// 		"insert into t values (2, 0, 0, DEFAULT);",
		// 	},
		// 	LeftSetUpScript: []string{
		// 		"alter table t drop column col00;",
		// 		"alter table t drop column col01;",
		// 		"alter table t add constraint CHECK (col1 != concat('he', 'llo'))",
		// 	},
		// 	Assertions: []queries.ScriptTestAssertion{
		// 		{
		// 			Query:    "call dolt_merge('right');",
		// 			Expected: []sql.Row{{"", 0, 1}},
		// 		},
		// 		{
		// 			Query:    "select * from dolt_constraint_violations;",
		// 			Expected: []sql.Row{{"t", uint64(1)}},
		// 		},
		// 		{
		// 			Query:    `select violation_type, pk, col1, violation_info like "\%NOT((col1 = concat('he','llo')))\%" from dolt_constraint_violations_t;`,
		// 			Expected: []sql.Row{{uint64(3), 2, "hello", true}},
		// 		},
		// 	},
		// },
	}
	for _, test := range scripts {
		// t.Run("merge right into left", func(t *testing.T) {
		// 	enginetest.TestScript(t, newDoltHarness(t), convertMergeScriptTest(test, false))
		// })
		t.Run("merge left into right", func(t *testing.T) {
			enginetest.TestScript(t, newDoltHarness(t), convertMergeScriptTest(test, true))
		})
	}
}

func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()

	harness := newDoltHarness(t)
	// engine := enginetest.NewEngine(t, harness)
	// enginetest.CreateIndexes(t, harness, engine)
	// engine := enginetest.NewSpatialEngine(t, harness)
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
		enginetest.RunQueryWithContext(t, engine, harness, nil, q)
	}

	// engine.Analyzer.Debug = true
	// engine.Analyzer.Verbose = true

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

	var script = queries.ScriptTest{
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
				Query: "select * from dolt_history_xy where commit_hash = (select dolt_log.commit_hash from dolt_log limit 1 offset 1) order by 1",
				Expected: []sql.Row{
					sql.Row{0, 1, "itt2nrlkbl7jis4gt9aov2l32ctt08th", "billy bob", time.Date(1970, time.January, 1, 19, 0, 0, 0, time.Local)},
					sql.Row{2, 3, "itt2nrlkbl7jis4gt9aov2l32ctt08th", "billy bob", time.Date(1970, time.January, 1, 19, 0, 0, 0, time.Local)},
				},
			},
			{
				Query: "select count(*) from dolt_history_xy where commit_hash = (select dolt_log.commit_hash from dolt_log limit 1 offset 1)",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select count(*) from dolt_history_xy where commit_hash = 'itt2nrlkbl7jis4gt9aov2l32ctt08th'",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	}

	tcc := &testCommitClock{}
	cleanup := installTestCommitClock(tcc)
	defer cleanup()

	sql.RunWithNowFunc(tcc.Now, func() error {
		harness := newDoltHarness(t)
		enginetest.TestScriptPrepared(t, harness, script)
		return nil
	})
}

func TestVersionedQueries(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	defer h.Close()

	RunVersionedQueriesTest(t, h)
}

func TestAnsiQuotesSqlMode(t *testing.T) {
	enginetest.TestAnsiQuotesSqlMode(t, newDoltHarness(t))
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	enginetest.TestAnsiQuotesSqlModePrepared(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunQueryTestPlans(t, harness)
}

func TestIntegrationQueryPlans(t *testing.T) {
	harness := newDoltEnginetestHarness(t).WithConfigureStats(true)
	defer harness.Close()
	enginetest.TestIntegrationPlans(t, harness)
}

func TestDoltDiffQueryPlans(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("only new format support system table indexing")
	}

	harness := newDoltEnginetestHarness(t).WithParallelism(2) // want Exchange nodes
	RunDoltDiffQueryPlansTest(t, harness)
}

func TestBranchPlans(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunBranchPlanTests(t, harness)
}

func TestQueryErrors(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestQueryErrors(t, h)
}

func TestInfoSchema(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunInfoSchemaTests(t, h)
}

func TestColumnAliases(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestColumnAliases(t, h)
}

func TestOrderByGroupBy(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestOrderByGroupBy(t, h)
}

func TestAmbiguousColumnResolution(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestAmbiguousColumnResolution(t, h)
}

func TestInsertInto(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInsertInto(t, h)
}

func TestInsertIgnoreInto(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInsertIgnoreInto(t, h)
}

// TODO: merge this into the above test when we remove old format
func TestInsertDuplicateKeyKeyless(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	enginetest.TestInsertDuplicateKeyKeyless(t, newDoltHarness(t))
}

// TODO: merge this into the above test when we remove old format
func TestInsertDuplicateKeyKeylessPrepared(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	enginetest.TestInsertDuplicateKeyKeylessPrepared(t, newDoltHarness(t))
}

// TODO: merge this into the above test when we remove old format
func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, h)
}

// TODO: merge this into the above test when we remove old format
func TestIgnoreIntoWithDuplicateUniqueKeyKeylessPrepared(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeylessPrepared(t, newDoltHarness(t))
}

func TestInsertIntoErrors(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunInsertIntoErrorsTest(t, h)
}

func TestGeneratedColumns(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunGeneratedColumnTests(t, harness)
}

func TestGeneratedColumnPlans(t *testing.T) {
	enginetest.TestGeneratedColumnPlans(t, newDoltHarness(t))
}

func TestSpatialQueries(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestSpatialQueries(t, h)
}

func TestReplaceInto(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestReplaceInto(t, h)
}

func TestReplaceIntoErrors(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestReplaceIntoErrors(t, h)
}

func TestUpdate(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestUpdate(t, h)
}

func TestUpdateIgnore(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestUpdateIgnore(t, h)
}

func TestUpdateErrors(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestUpdateErrors(t, h)
}

func TestDeleteFrom(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDelete(t, h)
}

func TestDeleteFromErrors(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDeleteErrors(t, h)
}

func TestSpatialDelete(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestSpatialDelete(t, h)
}

func TestSpatialScripts(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestSpatialScripts(t, h)
}

func TestSpatialScriptsPrepared(t *testing.T) {
	enginetest.TestSpatialScriptsPrepared(t, newDoltHarness(t))
}

func TestSpatialIndexScripts(t *testing.T) {
	skipOldFormat(t)
	enginetest.TestSpatialIndexScripts(t, newDoltHarness(t))
}

func TestSpatialIndexScriptsPrepared(t *testing.T) {
	skipOldFormat(t)
	enginetest.TestSpatialIndexScriptsPrepared(t, newDoltHarness(t))
}

func TestSpatialIndexPlans(t *testing.T) {
	skipOldFormat(t)
	enginetest.TestSpatialIndexPlans(t, newDoltHarness(t))
}

func TestTruncate(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestTruncate(t, h)
}

func TestConvert(t *testing.T) {
	if types.IsFormat_LD(types.Format_Default) {
		t.Skip("noms format has outdated type enforcement")
	}
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestConvertPrepared(t, h)
}

func TestConvertPrepared(t *testing.T) {
	if types.IsFormat_LD(types.Format_Default) {
		t.Skip("noms format has outdated type enforcement")
	}
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestConvertPrepared(t, h)
}

func TestScripts(t *testing.T) {
	var skipped []string
	if types.IsFormat_DOLT(types.Format_Default) {
		skipped = append(skipped, newFormatSkippedScripts...)
	}
	h := newDoltHarness(t).WithSkippedQueries(skipped)
	defer h.Close()
	enginetest.TestScripts(t, h)
}

func TestNumericErrorScripts(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNumericErrorScripts(t, h)
}

// TestDoltUserPrivileges tests Dolt-specific code that needs to handle user privilege checking
func TestDoltUserPrivileges(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
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

			engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

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
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.TestQueryWithContext(t, ctx, engine, harness, assertion.Query, assertion.Expected, nil, nil, nil)
					})
				}
			}
		})
	}
}

func TestJoinOps(t *testing.T) {
	if types.IsFormat_LD(types.Format_Default) {
		t.Skip("DOLT_LD keyless indexes are not sorted")
	}

	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJoinOps(t, h, enginetest.DefaultJoinOpTests)
}

func TestJoinPlanning(t *testing.T) {
	if types.IsFormat_LD(types.Format_Default) {
		t.Skip("DOLT_LD keyless indexes are not sorted")
	}
	h := newDoltEnginetestHarness(t).WithConfigureStats(true)
	defer h.Close()
	enginetest.TestJoinPlanning(t, h)
}

func TestJoinQueries(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJoinQueries(t, h)
}

func TestJoinQueriesPrepared(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJoinQueriesPrepared(t, h)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJSONTableQueries(t, h)
}

// TestJSONTableQueriesPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueriesPrepared(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJSONTableQueriesPrepared(t, h)
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJSONTableScripts(t, h)
}

// TestJSONTableScriptsPrepared runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScriptsPrepared(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestJSONTableScriptsPrepared(t, h)
}

func TestUserPrivileges(t *testing.T) {
	h := newDoltHarness(t)
	h.setupTestProcedures = true
	h.configureStats = true
	defer h.Close()
	enginetest.TestUserPrivileges(t, h)
}

func TestUserAuthentication(t *testing.T) {
	t.Skip("Unexpected panic, need to fix")
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestUserAuthentication(t, h)
}

func TestComplexIndexQueries(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestComplexIndexQueries(t, h)
}

func TestCreateTable(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestCreateTable(t, h)
}

func TestRowLimit(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestRowLimit(t, h)
}

func TestBranchDdl(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunBranchDdlTest(t, h)
}

func TestBranchDdlPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunBranchDdlTestPrepared(t, h)
}

func TestPkOrdinalsDDL(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPkOrdinalsDDL(t, h)
}

func TestPkOrdinalsDML(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPkOrdinalsDML(t, h)
}

func TestDropTable(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDropTable(t, h)
}

func TestRenameTable(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestRenameTable(t, h)
}

func TestRenameColumn(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestRenameColumn(t, h)
}

func TestAddColumn(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestAddColumn(t, h)
}

func TestModifyColumn(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestModifyColumn(t, h)
}

func TestDropColumn(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDropColumn(t, h)
}

func TestCreateDatabase(t *testing.T) {
	h := newDoltHarness(t)
	RunCreateDatabaseTest(t, h)
}

func TestBlobs(t *testing.T) {
	skipOldFormat(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestBlobs(t, h)
}

func TestIndexes(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
	enginetest.TestIndexes(t, harness)
}

func TestIndexPrefix(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	RunIndexPrefixTest(t, harness)
}

func TestBigBlobs(t *testing.T) {
	skipOldFormat(t)

	h := newDoltHarness(t)
	RunBigBlobsTest(t, h)
}

func TestDropDatabase(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDropEngineTest(t, h)
}

func TestCreateForeignKeys(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestCreateForeignKeys(t, h)
}

func TestDropForeignKeys(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDropForeignKeys(t, h)
}

func TestForeignKeys(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestForeignKeys(t, h)
}

func TestForeignKeyBranches(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunForeignKeyBranchesTest(t, h)
}

func TestForeignKeyBranchesPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunForeignKeyBranchesPreparedTest(t, h)
}

func TestFulltextIndexes(t *testing.T) {
	if !types.IsFormat_DOLT(types.Format_Default) {
		t.Skip("FULLTEXT is not supported on the old format")
	}
	if runtime.GOOS == "windows" && os.Getenv("CI") != "" {
		t.Skip("For some reason, this is flaky only on Windows CI.")
	}
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestFulltextIndexes(t, h)
}

func TestCreateCheckConstraints(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestCreateCheckConstraints(t, h)
}

func TestChecksOnInsert(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestChecksOnInsert(t, h)
}

func TestChecksOnUpdate(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestChecksOnUpdate(t, h)
}

func TestDisallowedCheckConstraints(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDisallowedCheckConstraints(t, h)
}

func TestDropCheckConstraints(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDropCheckConstraints(t, h)
}

func TestReadOnly(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestReadOnly(t, h, false /* testStoredProcedures */)
}

func TestViews(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestViews(t, h)
}

func TestBranchViews(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunBranchViewsTest(t, h)
}

func TestBranchViewsPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunBranchViewsPreparedTest(t, h)
}

func TestVersionedViews(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunVersionedViewsTest(t, h)
}

func TestWindowFunctions(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestWindowFunctions(t, h)
}

func TestWindowRowFrames(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestWindowRowFrames(t, h)
}

func TestWindowRangeFrames(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestWindowRangeFrames(t, h)
}

func TestNamedWindows(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNamedWindows(t, h)
}

func TestNaturalJoin(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNaturalJoin(t, h)
}

func TestNaturalJoinEqual(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNaturalJoinEqual(t, h)
}

func TestNaturalJoinDisjoint(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNaturalJoinEqual(t, h)
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInnerNestedInNaturalJoins(t, h)
}

func TestColumnDefaults(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestColumnDefaults(t, h)
}

func TestOnUpdateExprScripts(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestOnUpdateExprScripts(t, h)
}

func TestAlterTable(t *testing.T) {
	// This is a newly added test in GMS that dolt doesn't support yet
	h := newDoltHarness(t).WithSkippedQueries([]string{"ALTER TABLE t42 ADD COLUMN s varchar(20), drop check check1"})
	defer h.Close()
	enginetest.TestAlterTable(t, h)
}

func TestVariables(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunVariableTest(t, h)
}

func TestVariableErrors(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestVariableErrors(t, h)
}

func TestLoadDataPrepared(t *testing.T) {
	t.Skip("feature not supported")
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestLoadDataPrepared(t, h)
}

func TestLoadData(t *testing.T) {
	t.Skip()
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestLoadData(t, h)
}

func TestLoadDataErrors(t *testing.T) {
	t.Skip()
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestLoadDataErrors(t, h)
}

func TestSelectIntoFile(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestSelectIntoFile(t, h)
}

func TestJsonScripts(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	skippedTests := []string{
		"round-trip into table", // The current Dolt JSON format does not preserve decimals and unsigneds in JSON.
	}
	// TODO: fix this, use a skipping harness
	enginetest.TestJsonScripts(t, h, skippedTests)
}

func TestTriggers(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestTriggers(t, h)
}

func TestRollbackTriggers(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestRollbackTriggers(t, h)
}

func TestStoredProcedures(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunStoredProceduresTest(t, h)
}

func TestDoltStoredProcedures(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltStoredProceduresTest(t, h)
}

func TestDoltStoredProceduresPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltStoredProceduresPreparedTest(t, h)
}

func TestEvents(t *testing.T) {
	doltHarness := newDoltHarness(t)
	defer doltHarness.Close()
	enginetest.TestEvents(t, doltHarness)
}

func TestCallAsOf(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunCallAsOfTest(t, h)
}

func TestLargeJsonObjects(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunLargeJsonObjectsTest(t, harness)
}

func TestTransactions(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunTransactionTests(t, h)
}

func TestBranchTransactions(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunBranchTransactionTest(t, h)
}

func TestMultiDbTransactions(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunMultiDbTransactionsTest(t, h)
}

func TestMultiDbTransactionsPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunMultiDbTransactionsPreparedTest(t, h)
}

func TestConcurrentTransactions(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestConcurrentTransactions(t, h)
}

func TestDoltScripts(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltScriptsTest(t, harness)
}

func TestDoltTempTableScripts(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltTempTableScripts(t, harness)
}

func TestDoltRevisionDbScripts(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRevisionDbScriptsTest(t, h)
}

func TestDoltRevisionDbScriptsPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRevisionDbScriptsPreparedTest(t, h)
}

func TestDoltDdlScripts(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltDdlScripts(t, harness)
}

func TestBrokenDdlScripts(t *testing.T) {
	for _, script := range BrokenDDLScripts {
		t.Skip(script.Name)
	}
}

func TestDescribeTableAsOf(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestScript(t, h, DescribeTableAsOfScriptTest)
}

func TestShowCreateTable(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunShowCreateTableTests(t, h)
}

func TestShowCreateTablePrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunShowCreateTablePreparedTests(t, h)
}

func TestViewsWithAsOf(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestScript(t, h, ViewsWithAsOfScriptTest)
}

func TestViewsWithAsOfPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestScriptPrepared(t, h, ViewsWithAsOfScriptTest)
}

func TestDoltMerge(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltMergeTests(t, h)
}

func TestDoltMergePrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltMergePreparedTests(t, h)
}

func TestDoltRebase(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRebaseTests(t, h)
}

func TestDoltRebasePrepared(t *testing.T) {
	h := newDoltHarness(t)
	RunDoltRebasePreparedTests(t, h)
}

func TestDoltRevert(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRevertTests(t, h)
}

func TestDoltRevertPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRevertPreparedTests(t, h)
}

func TestDoltAutoIncrement(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltAutoIncrementTests(t, h)
}

func TestDoltAutoIncrementPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltAutoIncrementPreparedTests(t, h)
}

func TestDoltConflictsTableNameTable(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltConflictsTableNameTableTests(t, h)
}

// tests new format behavior for keyless merges that create CVs and conflicts
func TestKeylessDoltMergeCVsAndConflicts(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunKeylessDoltMergeCVsAndConflictsTests(t, h)
}

// eventually this will be part of TestDoltMerge
func TestDoltMergeArtifacts(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltMergeArtifacts(t, h)
}

// these tests are temporary while there is a difference between the old format
// and new format merge behaviors.
func TestOldFormatMergeConflictsAndCVs(t *testing.T) {
	if types.IsFormat_DOLT(types.Format_Default) {
		t.Skip()
	}
	for _, script := range OldFormatMergeConflictsAndCVsScripts {
		func() {
			h := newDoltHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func TestDoltReset(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltResetTest(t, h)
}

func TestDoltGC(t *testing.T) {
	t.SkipNow()
	for _, script := range DoltGC {
		func() {
			h := newDoltHarness(t)
			defer h.Close()
			enginetest.TestScript(t, h, script)
		}()
	}
}

func TestDoltCheckout(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltCheckoutTests(t, h)
}

func TestDoltCheckoutPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltCheckoutPreparedTests(t, h)
}

func TestDoltBranch(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltBranchTests(t, h)
}

func TestDoltTag(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltTagTests(t, h)
}

func TestDoltRemote(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltRemoteTests(t, h)
}

func TestDoltUndrop(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltUndropTests(t, h)
}

// TestSingleTransactionScript is a convenience method for debugging a single transaction test. Unskip and set to the
// desired test.
func TestSingleTransactionScript(t *testing.T) {
	t.Skip()

	tcc := &testCommitClock{}
	cleanup := installTestCommitClock(tcc)
	defer cleanup()

	sql.RunWithNowFunc(tcc.Now, func() error {
		script := queries.TransactionTest{
			Name: "Insert error with auto commit off",
			SetUpScript: []string{
				"create table t1 (pk int primary key, val int)",
				"insert into t1 values (0,0)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:            "/* client a */ set autocommit = off",
					SkipResultsCheck: true,
				},
				{
					Query:            "/* client b */ set autocommit = off",
					SkipResultsCheck: true,
				},
				{
					Query:    "/* client a */ insert into t1 values (1, 1)",
					Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
				},
				{
					Query:       "/* client a */ insert into t1 values (1, 2)",
					ExpectedErr: sql.ErrPrimaryKeyViolation,
				},
				{
					Query:    "/* client a */ insert into t1 values (2, 2)",
					Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
				},
				{
					Query:    "/* client a */ select * from t1 order by pk",
					Expected: []sql.Row{{0, 0}, {1, 1}, {2, 2}},
				},
				{
					Query:    "/* client b */ select * from t1 order by pk",
					Expected: []sql.Row{{0, 0}},
				},
				{
					Query:            "/* client a */ commit",
					SkipResultsCheck: true,
				},
				{
					Query:            "/* client b */ start transaction",
					SkipResultsCheck: true,
				},
				{
					Query:    "/* client b */ select * from t1 order by pk",
					Expected: []sql.Row{{0, 0}, {1, 1}, {2, 2}},
				},
				{
					Query:    "/* client a */ select * from t1 order by pk",
					Expected: []sql.Row{{0, 0}, {1, 1}, {2, 2}},
				},
			},
		}

		h := newDoltHarness(t)
		defer h.Close()
		enginetest.TestTransactionScript(t, h, script)

		return nil
	})
}

func TestBrokenSystemTableQueries(t *testing.T) {
	t.Skip()

	h := newDoltHarness(t)
	defer h.Close()
	enginetest.RunQueryTests(t, h, BrokenSystemTableQueries)
}

func TestHistorySystemTable(t *testing.T) {
	harness := newDoltEnginetestHarness(t).WithParallelism(2)
	RunHistorySystemTableTests(t, harness)
}

func TestHistorySystemTablePrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t).WithParallelism(2)
	RunHistorySystemTableTestsPrepared(t, harness)
}

func TestBrokenHistorySystemTablePrepared(t *testing.T) {
	t.Skip()
	harness := newDoltHarness(t)
	defer harness.Close()
	harness.Setup(setup.MydbData)
	for _, test := range BrokenHistorySystemTableScriptTests {
		harness.engine = nil
		t.Run(test.Name, func(t *testing.T) {
			enginetest.TestScriptPrepared(t, harness, test)
		})
	}
}

func TestUnscopedDiffSystemTable(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunUnscopedDiffSystemTableTests(t, h)
}

func TestUnscopedDiffSystemTablePrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunUnscopedDiffSystemTableTestsPrepared(t, h)
}

func TestColumnDiffSystemTable(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunColumnDiffSystemTableTests(t, h)
}

func TestColumnDiffSystemTablePrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunColumnDiffSystemTableTestsPrepared(t, h)
}

func TestStatBranchTests(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunStatBranchTests(t, harness)
}

func TestStatsFunctions(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunStatsFunctionsTest(t, harness)
}

func TestDiffTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffTableFunctionTests(t, harness)
}

func TestDiffTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffTableFunctionTestsPrepared(t, harness)
}

func TestDiffStatTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffStatTableFunctionTests(t, harness)
}

func TestDiffStatTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffStatTableFunctionTestsPrepared(t, harness)
}

func TestDiffSummaryTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffSummaryTableFunctionTests(t, harness)
}

func TestDiffSummaryTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDiffSummaryTableFunctionTestsPrepared(t, harness)
}

func TestPatchTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltPatchTableFunctionTests(t, harness)
}

func TestPatchTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltPatchTableFunctionTestsPrepared(t, harness)
}

func TestLogTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunLogTableFunctionTests(t, harness)
}

func TestLogTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunLogTableFunctionTestsPrepared(t, harness)
}

func TestDoltReflog(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltReflogTests(t, h)
}

func TestDoltReflogPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltReflogTestsPrepared(t, h)
}

func TestCommitDiffSystemTable(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunCommitDiffSystemTableTests(t, harness)
}

func TestCommitDiffSystemTablePrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunCommitDiffSystemTableTestsPrepared(t, harness)
}

func TestDiffSystemTable(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltDiffSystemTableTests(t, h)
}

func TestDiffSystemTablePrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltDiffSystemTableTestsPrepared(t, h)
}

func TestSchemaDiffTableFunction(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSchemaDiffTableFunctionTests(t, harness)
}

func TestSchemaDiffTableFunctionPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSchemaDiffTableFunctionTestsPrepared(t, harness)
}

func TestDoltDatabaseCollationDiffs(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltDatabaseCollationDiffsTests(t, harness)
}

func TestQueryDiff(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunQueryDiffTests(t, harness)
}

func TestSystemTableIndexes(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSystemTableIndexesTests(t, harness)
}

func TestSystemTableIndexesPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSystemTableIndexesTestsPrepared(t, harness)
}

func TestSystemTableFunctionIndexes(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSystemTableFunctionIndexesTests(t, harness)
}

func TestSystemTableFunctionIndexesPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunSystemTableFunctionIndexesTestsPrepared(t, harness)
}

func TestReadOnlyDatabases(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestReadOnlyDatabases(t, h)
}

func TestAddDropPks(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestAddDropPks(t, h)
}

func TestAddAutoIncrementColumn(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunAddAutoIncrementColumnTests(t, h)
}

func TestNullRanges(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestNullRanges(t, h)
}

func TestPersist(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()
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
	defer harness.Close()
	enginetest.TestTypesOverWire(t, harness, newSessionBuilder(harness))
}

func TestDoltCherryPick(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltCherryPickTests(t, harness)
}

func TestDoltCherryPickPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltCherryPickTestsPrepared(t, harness)
}

func TestDoltCommit(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltCommitTests(t, harness)
}

func TestDoltCommitPrepared(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltCommitTestsPrepared(t, harness)
}

func TestQueriesPrepared(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestQueriesPrepared(t, h)
}

func TestStatsHistograms(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunStatsHistogramTests(t, h)
}

// TestStatsIO force a provider reload in-between setup and assertions that
// forces a round trip of the statistics table before inspecting values.
func TestStatsStorage(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunStatsStorageTests(t, h)
}

func TestStatsIOWithoutReload(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunStatsIOTestsWithoutReload(t, h)
}

func TestJoinStats(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunJoinStatsTests(t, h)
}

func TestStatisticIndexes(t *testing.T) {
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestStatisticIndexFilters(t, h)
}

func TestSpatialQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)

	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestSpatialQueriesPrepared(t, h)
}

func TestPreparedStatistics(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunPreparedStatisticsTests(t, h)
}

func TestVersionedQueriesPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunVersionedQueriesPreparedTests(t, h)
}

func TestInfoSchemaPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInfoSchemaPrepared(t, h)
}

func TestUpdateQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestUpdateQueriesPrepared(t, h)
}

func TestInsertQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInsertQueriesPrepared(t, h)
}

func TestReplaceQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestReplaceQueriesPrepared(t, h)
}

func TestDeleteQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestDeleteQueriesPrepared(t, h)
}

func TestScriptsPrepared(t *testing.T) {
	var skipped []string
	if types.IsFormat_DOLT(types.Format_Default) {
		skipped = append(skipped, newFormatSkippedScripts...)
	}
	skipPreparedTests(t)
	h := newDoltHarness(t).WithSkippedQueries(skipped)
	defer h.Close()
	enginetest.TestScriptsPrepared(t, h)
}

func TestInsertScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInsertScriptsPrepared(t, h)
}

func TestComplexIndexQueriesPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestComplexIndexQueriesPrepared(t, h)
}

func TestJsonScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	skippedTests := []string{
		"round-trip into table", // The current Dolt JSON format does not preserve decimals and unsigneds in JSON.
	}
	enginetest.TestJsonScriptsPrepared(t, h, skippedTests)
}

func TestCreateCheckConstraintsScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestCreateCheckConstraintsScriptsPrepared(t, h)
}

func TestInsertIgnoreScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestInsertIgnoreScriptsPrepared(t, h)
}

func TestInsertErrorScriptsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltEnginetestHarness(t)
	defer h.Close()
	h = h.WithSkippedQueries([]string{
		"create table bad (vb varbinary(65535))",
		"insert into bad values (repeat('0', 65536))",
	})
	enginetest.TestInsertErrorScriptsPrepared(t, h)
}

func TestViewsPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestViewsPrepared(t, h)
}

func TestVersionedViewsPrepared(t *testing.T) {
	t.Skip("not supported for prepareds")
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestVersionedViewsPrepared(t, h)
}

func TestShowTableStatusPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestShowTableStatusPrepared(t, h)
}

func TestPrepared(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPrepared(t, h)
}

func TestDoltPreparedScripts(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	DoltPreparedScripts(t, h)
}

func TestPreparedInsert(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPreparedInsert(t, h)
}

func TestPreparedStatements(t *testing.T) {
	skipPreparedTests(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPreparedStatements(t, h)
}

func TestCharsetCollationEngine(t *testing.T) {
	skipOldFormat(t)
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestCharsetCollationEngine(t, h)
}

func TestCharsetCollationWire(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	defer harness.Close()
	enginetest.TestCharsetCollationWire(t, harness, newSessionBuilder(harness))
}

func TestDatabaseCollationWire(t *testing.T) {
	skipOldFormat(t)
	harness := newDoltHarness(t)
	defer harness.Close()
	enginetest.TestDatabaseCollationWire(t, harness, newSessionBuilder(harness))
}

func TestAddDropPrimaryKeys(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunAddDropPrimaryKeysTests(t, harness)
}

func TestDoltVerifyConstraints(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltVerifyConstraintsTests(t, harness)
}

func TestDoltStorageFormat(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	RunDoltStorageFormatTests(t, h)
}

func TestDoltStorageFormatPrepared(t *testing.T) {
	var expectedFormatString string
	if types.IsFormat_DOLT(types.Format_Default) {
		expectedFormatString = "NEW ( __DOLT__ )"
	} else {
		expectedFormatString = fmt.Sprintf("OLD ( %s )", types.Format_Default.VersionString())
	}
	h := newDoltHarness(t)
	defer h.Close()
	enginetest.TestPreparedQuery(t, h, "SELECT dolt_storage_format()", []sql.Row{{expectedFormatString}}, nil)
}

func TestThreeWayMergeWithSchemaChangeScripts(t *testing.T) {
	h := newDoltEnginetestHarness(t)

	RunThreeWayMergeWithSchemaChangeScripts(t, h)
}

func TestThreeWayMergeWithSchemaChangeScriptsPrepared(t *testing.T) {
	h := newDoltEnginetestHarness(t)

	RunThreeWayMergeWithSchemaChangeScriptsPrepared(t, h)
}

// If CREATE DATABASE has an error within the DatabaseProvider, it should not
// leave behind intermediate filesystem state.
func TestCreateDatabaseErrorCleansUp(t *testing.T) {
	dh := newDoltHarness(t)
	require.NotNil(t, dh)
	e, err := dh.NewEngine(t)
	require.NoError(t, err)
	require.NotNil(t, e)

	doltDatabaseProvider := dh.provider.(*sqle.DoltDatabaseProvider)
	doltDatabaseProvider.InitDatabaseHooks = append(doltDatabaseProvider.InitDatabaseHooks,
		func(_ *sql.Context, _ *sqle.DoltDatabaseProvider, name string, _ *env.DoltEnv, _ dsess.SqlDatabase) error {
			if name == "cannot_create" {
				return fmt.Errorf("there was an error initializing this database. abort!")
			}
			return nil
		})

	err = dh.provider.CreateDatabase(enginetest.NewContext(dh), "can_create")
	require.NoError(t, err)

	err = dh.provider.CreateDatabase(enginetest.NewContext(dh), "cannot_create")
	require.Error(t, err)

	fs := dh.multiRepoEnv.FileSystem()
	exists, _ := fs.Exists("cannot_create")
	require.False(t, exists)
	exists, isDir := fs.Exists("can_create")
	require.True(t, exists)
	require.True(t, isDir)
}

// TestStatsAutoRefreshConcurrency tests some common concurrent patterns that stats
// refresh is subject to -- namely reading/writing the stats objects in (1) DML statements
// (2) auto refresh threads, and (3) manual ANALYZE statements.
// todo: the dolt_stat functions should be concurrency tested
func TestStatsAutoRefreshConcurrency(t *testing.T) {
	// create engine
	harness := newDoltHarness(t)
	harness.Setup(setup.MydbData)
	harness.configureStats = true
	engine := mustNewEngine(t, harness)
	defer engine.Close()

	enginetest.RunQueryWithContext(t, engine, harness, nil, `create table xy (x int primary key, y int, z int, key (z), key (y,z), key (y,z,x))`)
	enginetest.RunQueryWithContext(t, engine, harness, nil, `create table uv (u int primary key, v int, w int, key (w), key (w,u), key (u,w,v))`)

	sqlDb, _ := harness.provider.BaseDatabase(harness.NewContext(), "mydb")

	// Setting an interval of 0 and a threshold of 0 will result
	// in the stats being updated after every operation
	intervalSec := time.Duration(0)
	thresholdf64 := 0.
	bThreads := sql.NewBackgroundThreads()
	branches := []string{"main"}
	statsProv := engine.EngineAnalyzer().Catalog.StatsProvider.(*statspro.Provider)

	// it is important to use new sessions for this test, to avoid working root conflicts
	readCtx := enginetest.NewSession(harness)
	writeCtx := enginetest.NewSession(harness)
	refreshCtx := enginetest.NewSession(harness)
	newCtx := func(context.Context) (*sql.Context, error) {
		return refreshCtx, nil
	}

	err := statsProv.InitAutoRefreshWithParams(newCtx, sqlDb.Name(), bThreads, intervalSec, thresholdf64, branches)
	require.NoError(t, err)

	execQ := func(ctx *sql.Context, q string, id int, tag string) {
		_, iter, _, err := engine.Query(ctx, q)
		require.NoError(t, err)
		_, err = sql.RowIterToRows(ctx, iter)
		// fmt.Printf("%s %d\n", tag, id)
		require.NoError(t, err)
	}

	iters := 50
	{
		// 3 threads to test auto-refresh/DML concurrency safety
		// - auto refresh (read + write)
		// - write (write only)
		// - read (read only)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			for i := 0; i < iters; i++ {
				q := "select count(*) from xy a join xy b on a.x = b.x"
				execQ(readCtx, q, i, "read")
				q = "select count(*) from uv a join uv b on a.u = b.u"
				execQ(readCtx, q, i, "read")
			}
			wg.Done()
		}()

		go func() {
			for i := 0; i < iters; i++ {
				q := fmt.Sprintf("insert into xy values (%d,%d,%d)", i, i, i)
				execQ(writeCtx, q, i, "write")
				q = fmt.Sprintf("insert into uv values (%d,%d,%d)", i, i, i)
				execQ(writeCtx, q, i, "write")
			}
			wg.Done()
		}()

		wg.Wait()
	}

	{
		// 3 threads to test auto-refresh/manual ANALYZE concurrency
		// - auto refresh (read + write)
		// - add (read + write)
		// - drop (write only)

		wg := sync.WaitGroup{}
		wg.Add(2)

		analyzeAddCtx := enginetest.NewSession(harness)
		analyzeDropCtx := enginetest.NewSession(harness)

		// hammer the provider with concurrent stat updates
		go func() {
			for i := 0; i < iters; i++ {
				execQ(analyzeAddCtx, "analyze table xy,uv", i, "analyze create")
			}
			wg.Done()
		}()

		go func() {
			for i := 0; i < iters; i++ {
				execQ(analyzeDropCtx, "analyze table xy drop histogram on (y,z)", i, "analyze drop yz")
				execQ(analyzeDropCtx, "analyze table uv drop histogram on (w,u)", i, "analyze drop wu")
			}
			wg.Done()
		}()

		wg.Wait()
	}
}

func TestDoltWorkspace(t *testing.T) {
	harness := newDoltEnginetestHarness(t)
	RunDoltWorkspaceTests(t, harness)
}
