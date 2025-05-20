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
	"fmt"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Starting state for all test in this file. We create three empty tables A,B,C which have each have a pk column and a value column (both ints),
// Three branches will be created: brA, brB, brC.
// brA will insert 1,2,3 into A.       It will be one commit ahead of main.
// brB will insert 10,20,30 into B.    It will be one commit ahead of main.
// brC will insert 100,200,300 into C. It will be one commit ahead of brB
var createTablesAndBranches = []string{
	"CREATE TABLE A (pk INT PRIMARY KEY,val INT);",
	"CREATE TABLE B (pk INT PRIMARY KEY,val INT);",
	"CREATE TABLE C (pk INT PRIMARY KEY,val INT);",
	"CALL DOLT_COMMIT('-Am', 'empty tables');",
	"CALL DOLT_CHECKOUT('-b', 'brA', 'main');",
	"INSERT INTO A VALUES (1,1),(2,2),(3,3);",
	"CALL DOLT_COMMIT('-am', 'insert into A while on brA');",
	"CALL DOLT_CHECKOUT('-b', 'brB', 'main');",
	"INSERT INTO B VALUES (10,10),(20,20),(30,30);",
	"CALL DOLT_COMMIT('-am', 'insert into B while on brB');",
	"CALL DOLT_CHECKOUT('-b', 'brC', 'brB');", // Branch from brB, not main.
	"INSERT INTO C VALUES (100,100),(200,200),(300,300);",
	"CALL DOLT_COMMIT('-am', 'insert into C while on brC');",
}

// brBChangesTableA is a set of changes that will
// 1) checkout brB
// 2) insert 3 values into A
// 3) record commit as @BR_B_CHANGES_A
var brBChangesTableA = []string{
	"CALL DOLT_CHECKOUT('brB');",
	"INSERT INTO A VALUES (42,42),(53,53),(64,64);",
	"CALL DOLT_COMMIT('-am', 'insert into A while on brB');",
	captureHEADAs("BR_B_CHANGES_A"),
}

/*
	 mergeBrAToBrB is a set of changes that will:
	 1) checkout brB, merge brA into it.
	 2) checkout brC, merge brB into it.
	  A  B  C
		      *
		     /|
		    / |
		   *  |
		  /|  |
		 / |  |
		*  *  *
		A' B' C'
*/
var mergeBrAToBrBAndBrC = []string{
	"CALL DOLT_CHECKOUT('brB');",
	"CALL DOLT_MERGE('--no-ff', 'brA');",
	"CALL DOLT_CHECKOUT('brC');",
	"CALL DOLT_MERGE('--no-ff', 'brB');",
}

/*
	 mergeBrCToBrBAndBrA is a set of changes that will:
	 1) checkout brB, merge brC into it.
	 2) checkout brA, merge brB into it.
	  A  B  C
		*
		|\
		| \
		|  *
		|  |\
		|  | \
		*  *  *
		A' B' C'
*/
var mergeBrCToBrBAndBrA = []string{
	"CALL DOLT_CHECKOUT('brB');",
	"CALL DOLT_MERGE('--no-ff', 'brC');",
	"CALL DOLT_CHECKOUT('brA');",
	"CALL DOLT_MERGE('--no-ff', 'brB');",
}

/*
 * brAextedTableASchema is a set of changes that will:
 * 1) checkout brA
 * 2) alter the schema of A Table to add a new_col column (int, default 4321)
 * 3) Branch B and C will be unchanged.
 *
 * A o----o A
 *
 * B o      B
 *
 * C o      C
 */
var brAextendTableASchema = []string{
	"CALL DOLT_CHECKOUT('brA');",
	"ALTER TABLE A ADD COLUMN new_col INT DEFAULT 4321;",
	"CALL DOLT_COMMIT('-am', 'extend table A schema');",
}

/*
 * brAextedTableASchema is a set of changes that will:
 * 1) checkout brA
 * 2) alter the schema of A Table to add a new_col column (int, default 1221)
 * 3) Branch A and C will be unchanged
 *
 * A o      A
 *
 * B o----o B
 *
 * C o      C
 */
var brBextendTableASchema = []string{
	"CALL DOLT_CHECKOUT('brB');",
	"ALTER TABLE A ADD COLUMN new_col INT DEFAULT 1221;",
	"CALL DOLT_COMMIT('-am', 'extend table A schema on brB');",
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
		// | | |
		// | | *         Changes on C
		// | |/
		// | *           changes on B
		// */            changes on A
		// *      (main) add the empty tables
		// *             Initialize data repository
		Name: "Test revert robomerge ignore",
		SetUpScript: chain(
			createTablesAndBranches,
			"CALL DOLT_CHECKOUT('brC');",
			"INSERT INTO A VALUES (42,24),(53,35),(64,46);",
			"CALL DOLT_COMMIT('-am', 'insert into A while on brC. Revert Me');",
			captureHEADAs("revert_me"),
			"CALL DOLT_CHECKOUT('brB');",
			"INSERT INTO B VALUES (40,4),(50,5),(60,6);",
			"CALL DOLT_COMMIT('-am', 'insert into B while on brB');",
			"CALL DOLT_MERGE('brC');",
			"CALL DOLT_REVERT(@revert_me);", // Revert that changes on table A. They should not show up on subsequent merges.
			"CALL DOLT_RESET('HEAD~1');",
			"CALL DOLT_COMMIT('-a', '--amend');", // Flatten the revert into the merge commit.
			captureHEADAs("B_MERGE_1"),
			"CALL DOLT_CHECKOUT('brA');",
			"CALL DOLT_MERGE('brB');",
			captureHEADAs("A_MERGE_1"),
			"CALL DOLT_CHECKOUT('brC');",
			"INSERT INTO C VALUES (400,400),(500,500),(600,600);",
			"CALL DOLT_COMMIT('-am', 'insert into C while on branch brC');",
			"CALL DOLT_CHECKOUT('brB');",
			"CALL DOLT_MERGE('brC');",
			"CALL DOLT_CHECKOUT('brA');",
			"CALL DOLT_MERGE('brB');",
			captureHEADAs("A_MERGE_2"),
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
				Query: "SELECT pk FROM A AS OF @A_MERGE_1;",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
			{
				Query: "SELECT pk FROM A AS OF @A_MERGE_2;",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
			{
				Query: "SELECT pk FROM A AS OF 'brC';",
				Expected: []sql.Row{
					{42}, {53}, {64},
				},
			},
		},
	},
	{
		Name: "cross merges",
		SetUpScript: chain(
			createTablesAndBranches,
			brBChangesTableA,
			"CALL DOLT_CHECKOUT('brA');",
			captureHEADAs("A_initial"),
			"INSERT INTO A VALUES (1111,1111);",
			"CALL DOLT_COMMIT('-am', 'insert into A while on brA');",
			"CALL DOLT_MERGE('--no-ff', @BR_B_CHANGES_A);",
			"CALL DOLT_REVERT(@BR_B_CHANGES_A);",
			captureHEADAs("reverted_br_b_changes_a"),
			"CALL DOLT_CHECKOUT('brB');",
			"INSERT INTO B VALUES (2222,2222);",
			"CALL DOLT_COMMIT('-am', 'insert into B while on brB');",
			"CALL DOLT_MERGE('--no-ff', @A_initial);",
			captureHEADAs("B_MERGE_ME"),
			"CALL DOLT_MERGE('--no-ff', @reverted_br_b_changes_a);",
			"CALL DOLT_CHECKOUT('brA');",
			"CALL DOLT_MERGE('--no-ff', @B_MERGE_ME);",
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM A AS OF 'brA';",
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3}, {1111, 1111},
				},
			},
			{
				// Verify that values inserted into A while on brB don't come back after the cross merge.
				Query: "SELECT * FROM A AS OF 'brB';",
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3}, {1111, 1111},
				},
			},
		},
	},
	{
		Name: "cross merge using predefined merge variables",
		SetUpScript: chain(
			createTablesAndBranches,
			"CALL DOLT_CHECKOUT('brA');",
			"INSERT INTO A VALUES (1111,1111);",
			"CALL DOLT_COMMIT('-am', 'insert into A while on brA');",
			mergeBrAToBrBAndBrC,
			"CALL DOLT_CHECKOUT('brB');",
			"INSERT INTO A VALUES (3333,3333);",
			"DELETE FROM A WHERE pk = 1111;",
			"CALL DOLT_COMMIT('-am', 'insert additional into A while on brB');",
			mergeBrCToBrBAndBrA,
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM A AS OF 'brA';",
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3}, {1111, 1111}, {3333, 3333},
				},
			},
			{
				Query: "SELECT * FROM A AS OF 'brB';",
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3}, {1111, 1111},
				},
			},
			{
				Query: "SELECT * FROM A AS OF 'brC';",
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3}, {1111, 1111}, {3333, 3333},
				},
			},
		},
	},
	{
		Name: "re-resolve with multiple branches and conflicts",
		SetUpScript: chain(
			createTablesAndBranches,
			"CALL DOLT_CHECKOUT('brA');",
			"INSERT INTO A VALUES (4, 4);",
			"CALL DOLT_COMMIT('-Am', 'Insert (4,4) on brA');",
			"CALL DOLT_CHECKOUT('brB');",
			"INSERT INTO A VALUES (5, 5);",
			"CALL DOLT_COMMIT('-Am', 'Insert (5,5) on brB');",
			"CALL DOLT_CHECKOUT('brC');",
			"INSERT INTO A VALUES (6, 6);",
			"CALL DOLT_COMMIT('-Am', 'Insert (6,6) on brC');",
			"CALL DOLT_CHECKOUT('brA');",
			"UPDATE A SET val = 10 WHERE pk = 1;",
			"CALL DOLT_COMMIT('-Am', 'Update (1,10) on brA');",
			"CALL DOLT_CHECKOUT('brB');",
			"UPDATE A SET val = 20 WHERE pk = 1;",
			"CALL DOLT_COMMIT('-Am', 'Update (1,20) on brB');",
			"CALL DOLT_CHECKOUT('brC');",
			"CALL DOLT_MERGE('brB');",
			"CALL DOLT_CHECKOUT('brB');",
			"SET @@autocommit = 0;",
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('brA');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_pk, our_val, their_val FROM dolt_conflicts_A;",
				Expected: []sql.Row{{1, 20, 10}},
			},
			{
				Query:    "UPDATE dolt_conflicts_A SET our_val = 30;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 1, Warnings: 0}}}},
			},
			{
				Query:    "DELETE FROM dolt_conflicts_A;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-a', '-m', 'Resolve conflict by setting value to 30');",
				SkipResultsCheck: true,
			},
			{
				Query:            "CALL DOLT_CHECKOUT('brC');",
				SkipResultsCheck: true,
			},
			{
				Query:            "CALL DOLT_MERGE('brB');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM A ORDER BY pk;",
				Expected: []sql.Row{
					{1, 30},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
				},
			},
		},
	},
	{
		Name: "schema changes which should conflict",
		SetUpScript: chain(
			createTablesAndBranches,
			brAextendTableASchema,
			"CALL DOLT_CHECKOUT('brB');",
			"ALTER TABLE A ADD COLUMN new_col INT DEFAULT 1221;",
			"CALL DOLT_COMMIT('-am', 'extend table A schema on brB');",
			"CALL DOLT_CHECKOUT('brA');",
			"SET @@autocommit = 0;",
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('brB');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
		},
	},
}

// captureHEADAs returns a SQL statement that captures the hash of HEAD into a variable with the given name.
func captureHEADAs(name string) string {
	return fmt.Sprintf("SET @%s = (SELECT DOLT_HASHOF('HEAD'));", name)
}

// chain flattens any mix of string and []string into a single []string.
func chain(parts ...interface{}) []string {
	var out []string
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			out = append(out, v)
		case []string:
			out = append(out, v...)
		default:
			panic(fmt.Sprintf("chain: unsupported type %T", p))
		}
	}
	return out
}
