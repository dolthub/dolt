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
			Name: "Two column index",
			SetUpScript: []string{
				`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX (v1, v2));`,
				`INSERT INTO test VALUES (0,0,48),(1,0,52),(2,2,4),(3,2,10),(4,3,35),(5,5,36),(6,5,60),(7,6,1),(8,6,51),
(9,6,60),(10,6,73),(11,9,44),(12,9,97),(13,13,44),(14,14,53),(15,14,57),(16,14,98),(17,16,19),(18,16,53),(19,16,95),
(20,18,31),(21,19,48),(22,19,75),(23,19,97),(24,24,60),(25,25,14),(26,25,31),(27,27,9),(28,27,24),(29,28,24),(30,28,83),
(31,31,14),(32,33,39),(33,34,22),(34,34,91),(35,35,89),(36,38,20),(37,38,66),(38,39,55),(39,39,86),(40,40,97),(41,42,0),
(42,42,82),(43,43,63),(44,44,48),(45,44,67),(46,45,22),(47,45,31),(48,45,63),(49,45,86),(50,46,46),(51,47,5),(52,48,22),
(53,49,0),(54,50,0),(55,50,14),(56,51,35),(57,54,38),(58,56,0),(59,56,60),(60,57,29),(61,57,49),(62,58,12),(63,58,32),
(64,59,29),(65,59,45),(66,59,54),(67,60,66),(68,61,3),(69,61,34),(70,63,19),(71,63,69),(72,65,80),(73,65,97),(74,67,95),
(75,68,11),(76,69,34),(77,72,52),(78,74,81),(79,76,39),(80,78,0),(81,78,90),(82,79,36),(83,80,61),(84,80,88),(85,81,4),
(86,82,16),(87,83,30),(88,83,74),(89,84,9),(90,84,45),(91,86,56),(92,86,88),(93,87,51),(94,89,3),(95,93,19),(96,93,21),
(97,93,96),(98,98,0),(99,98,51),(100,98,61);`,
			},
			Assertions: []enginetest.ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE (((v1<20 AND v2<=46) OR (v1<>4 AND v2=26)) OR (v1>36 AND v2<>13));",
					Expected: []sql.Row{
						{58, 56, 0}, {61, 57, 49}, {72, 65, 80}, {85, 81, 4}, {3, 2, 10}, {49, 45, 86}, {5, 5, 36}, {50, 46, 46}, {62, 58, 12}, {92, 86, 88}, {47, 45, 31}, {54, 50, 0}, {55, 50, 14}, {87, 83, 30}, {91, 86, 56}, {66, 59, 54}, {76, 69, 34}, {79, 76, 39}, {46, 45, 22}, {57, 54, 38}, {68, 61, 3}, {93, 87, 51}, {4, 3, 35}, {7, 6, 1}, {45, 44, 67}, {52, 48, 22}, {2, 2, 4}, {53, 49, 0}, {69, 61, 34}, {73, 65, 97}, {90, 84, 45}, {82, 79, 36}, {11, 9, 44}, {20, 18, 31}, {41, 42, 0}, {43, 43, 63}, {65, 59, 45}, {100, 98, 61}, {95, 93, 19}, {13, 13, 44}, {56, 51, 35}, {59, 56, 60}, {67, 60, 66}, {77, 72, 52}, {89, 84, 9}, {63, 58, 32}, {83, 80, 61}, {39, 39, 86}, {17, 16, 19}, {38, 39, 55}, {40, 40, 97}, {74, 67, 95}, {78, 74, 81}, {81, 78, 90}, {88, 83, 74}, {37, 38, 66}, {48, 45, 63}, {51, 47, 5}, {64, 59, 29}, {80, 78, 0}, {86, 82, 16}, {96, 93, 21}, {98, 98, 0}, {75, 68, 11}, {84, 80, 88}, {99, 98, 51}, {44, 44, 48}, {60, 57, 29}, {70, 63, 19}, {71, 63, 69}, {36, 38, 20}, {42, 42, 82}, {94, 89, 3}, {97, 93, 96},
					},
				},
				{
					Query: "SELECT * FROM test WHERE (((v1<=52 AND v2<40) AND (v1<30) OR (v1<=75 AND v2 BETWEEN 54 AND 54)) OR (v1<>31 AND v2<>56));",
					Expected: []sql.Row{
						{19, 16, 95}, {58, 56, 0}, {61, 57, 49}, {72, 65, 80}, {85, 81, 4}, {3, 2, 10}, {49, 45, 86}, {5, 5, 36}, {9, 6, 60}, {50, 46, 46}, {62, 58, 12}, {92, 86, 88}, {15, 14, 57}, {47, 45, 31}, {54, 50, 0}, {55, 50, 14}, {87, 83, 30}, {16, 14, 98}, {66, 59, 54}, {76, 69, 34}, {79, 76, 39}, {21, 19, 48}, {46, 45, 22}, {57, 54, 38}, {68, 61, 3}, {93, 87, 51}, {4, 3, 35}, {7, 6, 1}, {45, 44, 67}, {52, 48, 22}, {2, 2, 4}, {12, 9, 97}, {30, 28, 83}, {53, 49, 0}, {69, 61, 34}, {73, 65, 97}, {90, 84, 45}, {82, 79, 36}, {0, 0, 48}, {10, 6, 73}, {11, 9, 44}, {20, 18, 31}, {41, 42, 0}, {43, 43, 63}, {65, 59, 45}, {100, 98, 61}, {95, 93, 19}, {1, 0, 52}, {13, 13, 44}, {56, 51, 35}, {59, 56, 60}, {67, 60, 66}, {77, 72, 52}, {89, 84, 9}, {24, 24, 60}, {33, 34, 22}, {35, 35, 89}, {63, 58, 32}, {83, 80, 61}, {39, 39, 86}, {8, 6, 51}, {14, 14, 53}, {17, 16, 19}, {23, 19, 97}, {26, 25, 31}, {29, 28, 24}, {38, 39, 55}, {40, 40, 97}, {74, 67, 95}, {78, 74, 81}, {81, 78, 90}, {88, 83, 74}, {28, 27, 24}, {37, 38, 66}, {48, 45, 63}, {51, 47, 5}, {64, 59, 29}, {80, 78, 0}, {86, 82, 16}, {96, 93, 21}, {98, 98, 0}, {25, 25, 14}, {27, 27, 9}, {32, 33, 39}, {75, 68, 11}, {84, 80, 88}, {99, 98, 51}, {6, 5, 60}, {22, 19, 75}, {44, 44, 48}, {60, 57, 29}, {70, 63, 19}, {71, 63, 69}, {18, 16, 53}, {34, 34, 91}, {36, 38, 20}, {42, 42, 82}, {94, 89, 3}, {97, 93, 96},
					},
				},

				{
					Query: "SELECT * FROM test WHERE ((v1>42 AND v2<=13) OR (v1=7));",
					Expected: []sql.Row{
						{58, 56, 0}, {85, 81, 4}, {62, 58, 12}, {54, 50, 0}, {68, 61, 3}, {53, 49, 0}, {89, 84, 9}, {51, 47, 5}, {80, 78, 0}, {98, 98, 0}, {75, 68, 11}, {94, 89, 3},
					},
				},
				{
					Query: "SELECT * FROM test WHERE (((((v1<71 AND v2<7) OR (v1<=21 AND v2<=48)) OR (v1=44 AND v2 BETWEEN 21 AND 83)) OR (v1<=72 AND v2<>27)) OR (v1=35 AND v2 BETWEEN 78 AND 89));",
					Expected: []sql.Row{
						{19, 16, 95}, {58, 56, 0}, {61, 57, 49}, {72, 65, 80}, {3, 2, 10}, {49, 45, 86}, {5, 5, 36}, {9, 6, 60}, {50, 46, 46}, {62, 58, 12}, {15, 14, 57}, {47, 45, 31}, {54, 50, 0}, {55, 50, 14}, {16, 14, 98}, {66, 59, 54}, {76, 69, 34}, {21, 19, 48}, {46, 45, 22}, {57, 54, 38}, {68, 61, 3}, {4, 3, 35}, {7, 6, 1}, {45, 44, 67}, {52, 48, 22}, {2, 2, 4}, {12, 9, 97}, {30, 28, 83}, {53, 49, 0}, {69, 61, 34}, {73, 65, 97}, {0, 0, 48}, {10, 6, 73}, {11, 9, 44}, {20, 18, 31}, {41, 42, 0}, {43, 43, 63}, {65, 59, 45}, {1, 0, 52}, {13, 13, 44}, {56, 51, 35}, {59, 56, 60}, {67, 60, 66}, {77, 72, 52}, {24, 24, 60}, {33, 34, 22}, {35, 35, 89}, {63, 58, 32}, {39, 39, 86}, {8, 6, 51}, {14, 14, 53}, {17, 16, 19}, {23, 19, 97}, {26, 25, 31}, {29, 28, 24}, {38, 39, 55}, {40, 40, 97}, {74, 67, 95}, {28, 27, 24}, {37, 38, 66}, {48, 45, 63}, {51, 47, 5}, {64, 59, 29}, {25, 25, 14}, {27, 27, 9}, {32, 33, 39}, {75, 68, 11}, {6, 5, 60}, {22, 19, 75}, {31, 31, 14}, {44, 44, 48}, {60, 57, 29}, {70, 63, 19}, {71, 63, 69}, {18, 16, 53}, {34, 34, 91}, {36, 38, 20}, {42, 42, 82},
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

// TestDoltUserPrivileges tests Dolt-specific code that needs to handle user privilege checking
func TestDoltUserPrivileges(t *testing.T) {
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
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}
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

func TestBrokenSystemTableQueries(t *testing.T) {
	t.Skip()

	enginetest.RunQueryTests(t, newDoltHarness(t), BrokenSystemTableQueries)
}

func TestHistorySystemTable(t *testing.T) {
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
