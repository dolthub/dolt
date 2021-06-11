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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func init() {
	sqle.MinRowsPerPartition = 2
}

func TestQueries(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestQueries(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestQueries(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestSingleQuery(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		Query: `SELECT 
					myTable.i, 
					(SELECT 
						dolt_commit_diff_mytable.diff_type 
					FROM 
						dolt_commit_diff_mytable
					WHERE (
						dolt_commit_diff_mytable.from_commit = 'abc' AND 
						dolt_commit_diff_mytable.to_commit = 'abc' AND
						dolt_commit_diff_mytable.to_i = myTable.i  -- extra filter clause
					)) AS diff_type 
				FROM myTable`,
		Expected: []sql.Row{},
	}

	harness := newDoltHarness(t)
	engine := enginetest.NewEngine(t, harness)
	//engine.Analyzer.Debug = true
	//engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, test.ExpectedColumns, test.Bindings)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()

	var scripts = []enginetest.ScriptTest{
		{
			// All DECLARE statements are only allowed under BEGIN/END blocks
			Name: "Top-level DECLARE statements",
			Assertions: []enginetest.ScriptTestAssertion{
				{
					Query:    "select 1+1",
					Expected: []sql.Row{{2}},
				},
			},
		},
		{
			Name: "last_insert_id() behavior",
			SetUpScript: []string{
				"create table a (x int primary key, y int)",
			},
			Assertions: []enginetest.ScriptTestAssertion{},
		},
	}

	harness := newDoltHarness(t)
	for _, test := range scripts {
		engine := enginetest.NewEngine(t, harness)
		engine.Analyzer.Debug = true
		engine.Analyzer.Verbose = true

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestVersionedQueries(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestVersionedQueries(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestVersionedQueries(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
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
	enginetest.TestQueryPlans(t, newDoltHarness(t).WithParallelism(1).WithSkippedQueries(skipped))
}

func TestQueryErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestQueryErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestQueryErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestInfoSchema(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestInfoSchema(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestInfoSchema(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestColumnAliases(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestColumnAliases(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestColumnAliases(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestOrderByGroupBy(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestOrderByGroupBy(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestOrderByGroupBy(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestAmbiguousColumnResolution(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestAmbiguousColumnResolution(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestAmbiguousColumnResolution(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestInsertInto(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestInsertInto(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestInsertInto(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestInsertIgnoreInto(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestInsertIgnoreInto(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestInsertIgnoreInto(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestInsertIntoErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestInsertIntoErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestInsertIntoErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestReplaceInto(t *testing.T) {
	t.Skipf("Skipping, replace returns the wrong number of rows in some cases")
	enginetest.TestReplaceInto(t, newDoltHarness(t))
}

func TestReplaceIntoErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestReplaceIntoErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestReplaceIntoErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestUpdate(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestUpdate(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestUpdate(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestUpdateErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestUpdateErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestUpdateErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDeleteFrom(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDelete(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDelete(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDeleteFromErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDeleteErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDeleteErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestTruncate(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestTruncate(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestTruncate(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestScripts(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestScripts(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestScripts(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestCreateTable(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestCreateTable(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestCreateTable(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDropTable(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDropTable(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDropTable(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestRenameTable(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestRenameTable(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestRenameTable(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestRenameColumn(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestRenameColumn(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestRenameColumn(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestAddColumn(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestAddColumn(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestAddColumn(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestModifyColumn(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestModifyColumn(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestModifyColumn(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDropColumn(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDropColumn(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDropColumn(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestCreateForeignKeys(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestCreateForeignKeys(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestCreateForeignKeys(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDropForeignKeys(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDropForeignKeys(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDropForeignKeys(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestCreateCheckConstraints(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestCreateCheckConstraints(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestCreateCheckConstraints(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestChecksOnInsert(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestChecksOnInsert(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestChecksOnInsert(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestChecksOnUpdate(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestChecksOnUpdate(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestChecksOnUpdate(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDisallowedCheckConstraints(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDisallowedCheckConstraints(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDisallowedCheckConstraints(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestDropCheckConstraints(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestDropCheckConstraints(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestDropCheckConstraints(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestExplode(t *testing.T) {
	t.Skipf("Unsupported types")
	enginetest.TestExplode(t, newDoltHarness(t))
}

func TestReadOnly(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestReadOnly(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestReadOnly(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestViews(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestViews(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestViews(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestVersionedViews(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestVersionedViews(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestVersionedViews(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestNaturalJoin(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestNaturalJoin(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestNaturalJoin(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestNaturalJoinEqual(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestNaturalJoinEqual(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestNaturalJoinEqual(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestNaturalJoinDisjoint(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestNaturalJoinEqual(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestNaturalJoinEqual(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestInnerNestedInNaturalJoins(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestInnerNestedInNaturalJoins(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestColumnDefaults(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestColumnDefaults(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestColumnDefaults(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestVariables(t *testing.T) {
	// Can't run these tests more than once because they set and make assertions about global vars, which obviously
	// persist outside sessions.
	enginetest.TestVariables(t, newDoltHarness(t))
}

func TestVariableErrors(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestVariableErrors(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestVariableErrors(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestJsonScripts(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestJsonScripts(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestJsonScripts(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestTriggers(t *testing.T) {
	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestTriggers(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestTriggers(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
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

	t.Run("no transactions", func(t *testing.T) {
		enginetest.TestStoredProcedures(t, newDoltHarness(t))
	})
	t.Run("with transactions", func(t *testing.T) {
		enginetest.TestStoredProcedures(t, newDoltHarness(t).withTransactionsEnabled(true))
	})
}

func TestTransactions(t *testing.T) {
	enginetest.TestTransactionScripts(t, newDoltHarness(t).withTransactionsEnabled(true))
	for _, script := range DoltTransactionTests {
		enginetest.TestTransactionScript(t, newDoltHarness(t).withTransactionsEnabled(true), script)
	}
}
