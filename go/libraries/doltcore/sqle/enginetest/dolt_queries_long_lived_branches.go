// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

// Starting state for all test in this file. We create three empty tables A,B,C which have each have a single in pk column
// Three branches will be created: brA, brB, brC.
// brA will insert 1,2,3 into A.       It will be one commit ahead of main.
// brB will insert 10,20,30 into B.    It will be one commit ahead of main.
// brC will insert 100,200,300 into C. It will be one commit ahead of brB
var createTablesAndBranches = []string{
	"CREATE TABLE A (pk INT PRIMARY KEY);",
	"CREATE TABLE B (pk INT PRIMARY KEY);",
	"CREATE TABLE C (pk INT PRIMARY KEY);",
	"CALL DOLT_COMMIT('-Am', 'empty tables');",
	"CALL DOLT_CHECKOUT('-b', 'brA', 'main');",
	"INSERT INTO A VALUES (1),(2),(3);",
	"CALL DOLT_COMMIT('-am', 'insert into A while on brA');",
	"CALL DOLT_CHECKOUT('-b', 'brB', 'main');",
	"INSERT INTO B VALUES (10),(20),(30);",
	"CALL DOLT_COMMIT('-am', 'insert into B while on brB');",
	"CALL DOLT_CHECKOUT('-b', 'brC', 'brB');", // Branch from brB, not main.
	"INSERT INTO C VALUES (100),(200),(300);",
	"CALL DOLT_COMMIT('-am', 'insert into C while on brC');",
}

var DoltLongLivedBranchTests = []queries.ScriptTest{
	{
		// * (HEAD -> A) Merge branch 'B' into A
		// |\
		// | *       (B) Merge branch 'C' into B
		// * |\          Merge branch 'B' into A
		// |\| *     (C) More change on C of C
		// | * |         Merge branch 'C' into B
		// | |\|
		// | | *         Changes on the C branch of A Table. robomerge "IGNORE"
		// | * |         more commit on B
		// | | *         Changes on C
		// | |/
		// | *           changes on B
		// */            changes on A
		// *      (main) add the empty tables
		// *             Initialize data repository
		Name: "Test revert robomerge ignore",
		SetUpScript: append(createTablesAndBranches,
			[]string{
				"CALL DOLT_CHECKOUT('brC');",
				"INSERT INTO A VALUES (42),(53),(64);",
				"CALL DOLT_COMMIT('-am', 'insert into A while on brC. Revert Me');",
				"SET @revert_me = (SELECT DOLT_HASHOF('HEAD'));",
				"CALL DOLT_CHECKOUT('brB');",
				"INSERT INTO B VALUES (40),(50),(60);",
				"CALL DOLT_COMMIT('-am', 'insert into B while on brB');",
				"CALL DOLT_MERGE('brC');",
				"CALL DOLT_REVERT(@revert_me);", // Revert that changes on table A. They should not show up on subsequent merges.
				"CALL DOLT_RESET('HEAD~1');",
				"CALL DOLT_COMMIT('-a', '--amend');", // Flatten the revert into the merge commit.
				"SET @B_MERGE_1 = (SELECT DOLT_HASHOF('HEAD'));",
				"CALL DOLT_CHECKOUT('brA');",
				"CALL DOLT_MERGE('brB');",
				"SET @A_MERGE_1 = (SELECT DOLT_HASHOF('HEAD'));",
				"CALL DOLT_CHECKOUT('brC');",
				"INSERT INTO C VALUES (400),(500),(600);",
				"CALL DOLT_COMMIT('-am', 'insert into C while on branch brC');",
				"CALL DOLT_CHECKOUT('brB');",
				"CALL DOLT_MERGE('brC');",
				"CALL DOLT_CHECKOUT('brA');",
				"CALL DOLT_MERGE('brB');",
				"SET @A_MERGE_2 = (SELECT DOLT_HASHOF('HEAD'));",
			}...,
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM A AS OF 'brB';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM A AS OF @B_MERGE_1;",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT * FROM A AS OF @A_MERGE_1;",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
			{
				Query: "SELECT * FROM A AS OF @A_MERGE_2;",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
			{
				Query: "SELECT * FROM A AS OF 'brC';",
				Expected: []sql.Row{
					{42}, {53}, {64},
				},
			},
		},
	},
}
