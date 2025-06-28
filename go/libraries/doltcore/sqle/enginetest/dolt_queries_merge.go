// Copyright 2022 Dolthub, Inc.
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
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
)

type MergeScriptTest struct {
	// Name of the script test
	Name string
	// The sql statements to generate the ancestor commit
	AncSetUpScript []string
	// The sql statements to generate the right commit
	RightSetUpScript []string
	// The sql statements to generate the left commit
	LeftSetUpScript []string
	// The set of assertions to make after setup, in order
	Assertions []queries.ScriptTestAssertion
	// For tests that make a single assertion, Query can be set for the single assertion
	Query string
	// For tests that make a single assertion, Expected can be set for the single assertion
	Expected []sql.Row
	// For tests that make a single assertion, ExpectedErr can be set for the expected error
	ExpectedErr *errors.Kind
	// SkipPrepared is true when we skip a test for prepared statements only
	SkipPrepared bool
}

type doltCommitValidator struct{}

var _ enginetest.CustomValueValidator = &doltCommitValidator{}
var hashRegex = regexp.MustCompile(`^[0-9a-v]{32}$`)

func (dcv *doltCommitValidator) Validate(val interface{}) (bool, error) {
	hash, ok := val.(string)
	if !ok {
		return false, nil
	}
	return hashRegex.MatchString(hash), nil
}

var doltCommit = &doltCommitValidator{}

var MergeScripts = []queries.ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/7275
		Name: "keyless table merge with constraint violations",
		SetUpScript: []string{
			"CREATE TABLE aTable (aColumn INT NULL, bColumn INT NULL, UNIQUE INDEX aColumn_UNIQUE (aColumn ASC) VISIBLE, UNIQUE INDEX bColumn_UNIQUE (bColumn ASC) VISIBLE);",
			"CALL dolt_commit('-Am', 'add tables');",
			"CALL dolt_checkout('-b', 'side');",
			"INSERT INTO aTable VALUES (1,2);",
			"CALL dolt_commit('-am', 'add side data');",

			"CALL dolt_checkout('main');",
			"INSERT INTO aTable VALUES (1,3);",
			"CALL dolt_commit('-am', 'add main data');",
			"CALL dolt_checkout('side');",
			"SET @@dolt_force_transaction_commit=1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM aTable;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * FROM aTable;",
				Expected: []sql.Row{{1, 2}, {1, 3}},
			},
			{
				Query:    "SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{{"aTable", uint64(2)}},
			},
			{
				Query: "SELECT from_root_ish, violation_type, hex(dolt_row_hash), aColumn, bColumn, CAST(violation_info as CHAR) FROM dolt_constraint_violations_aTable;",
				Expected: []sql.Row{
					{doltCommit, "unique index", "5A1ED8633E1842FCA8EE529E4F1C5944", 1, 2, `{"Name": "aColumn_UNIQUE", "Columns": ["aColumn"]}`},
					{doltCommit, "unique index", "A922BFBF4E5489501A3808BC5CD702C0", 1, 3, `{"Name": "aColumn_UNIQUE", "Columns": ["aColumn"]}`},
				},
			},
			{
				// Fix the data
				Query:    "UPDATE aTable SET aColumn = 2 WHERE bColumn = 2;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				// clear out the violations
				Query:    "DELETE FROM dolt_constraint_violations_aTable;",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				// Commit the merge after resolving the constraint violations
				Query:    "call dolt_commit('-am', 'merging in main and resolving unique constraint violations');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// Merging again is a no-op
				Query:    "call dolt_merge('main');",
				Expected: []sql.Row{{"", 0, 0, "cannot fast forward from a to b. a is ahead of b already"}},
			},
		},
	},

	{
		// When there is a constraint violation for duplicate copies of a row in a keyless table, each row
		// will violate constraint in exactly the same way. Currently, the dolt_constraint_violations_<table>
		// system table will only contain one row for each unique violation. In other words, there may be N
		// duplicate rows in the keyless table that violate the constraint, but only one row is shown in the
		// constraint system table representing them all.
		// TODO: We could add a new column to the PK for the constraints table to represent a unique ID/count
		//       for the duplicate rows, and then we could support a 1:1 mapping of rows in the keyless table
		//       to rows in the constraint violation system table.
		Name: "keyless table merge with constraint violation on duplicate rows",
		SetUpScript: []string{
			"CREATE TABLE parent (pk INT primary key);",
			"insert into parent values (1), (2);",
			"CREATE TABLE aTable (aColumn INT NULL, bColumn INT NULL);",
			"INSERT INTO aTable VALUES (1, 1);",
			"CALL dolt_commit('-Am', 'add tables');",

			"CALL dolt_checkout('-b', 'side');",
			"INSERT INTO aTable VALUES (2, -1), (2, -1);",
			"CALL dolt_commit('-am', 'add side data');",

			"CALL dolt_checkout('main');",
			"ALTER TABLE aTable add foreign key (bColumn) references parent(pk);",
			"CALL dolt_commit('-am', 'add main data');",
			"CALL dolt_checkout('side');",
			"SET @@dolt_force_transaction_commit=1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM aTable ORDER BY aColumn;",
				Expected: []sql.Row{{1, 1}, {2, -1}, {2, -1}},
			},
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * FROM aTable ORDER BY aColumn;",
				Expected: []sql.Row{{1, 1}, {2, -1}, {2, -1}},
			},
			{
				Query:    "SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{{"aTable", uint64(1)}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"aTable", false, "constraint violation"},
				},
			},
			{
				Query: "SELECT from_root_ish, violation_type, hex(dolt_row_hash), aColumn, bColumn, CAST(violation_info as CHAR) FROM dolt_constraint_violations_aTable;",
				Expected: []sql.Row{
					{doltCommit, "foreign key", "13F8480978D0556FA9AE6DF5745A7ACA", 2, -1, `{"Index": "bColumn", "Table": "aTable", "Columns": ["bColumn"], "OnDelete": "RESTRICT", "OnUpdate": "RESTRICT", "ForeignKey": "atable_ibfk_1", "ReferencedIndex": "", "ReferencedTable": "parent", "ReferencedColumns": ["pk"]}`},
				},
			},
			{
				// Fix the data
				Query:    "UPDATE aTable SET bColumn = 2 WHERE bColumn = -1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: uint64(2), Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				// clear out the violations
				Query:    "DELETE FROM dolt_constraint_violations_aTable;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				// Commit the merge after resolving the constraint violations
				Query:    "call dolt_commit('-am', 'merging in main and resolving unique constraint violations');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// Merging again is a no-op
				Query:    "call dolt_merge('main');",
				Expected: []sql.Row{{"", 0, 0, "cannot fast forward from a to b. a is ahead of b already"}},
			},
		},
	},
	{
		// Unique checks should not include the content of deleted rows in checks. Tests two updates: one triggers
		// going from a smaller key to a higher key, and one going from a higher key to a smaller key (in order to test
		// delete/insert events in either order). https://github.com/dolthub/dolt/issues/6319
		Name: "unique constraint checks do not consider deleted rows",
		SetUpScript: []string{
			"set @@autocommit=0;",
			"create table tableA (pk varchar(255) primary key, col1 varchar(255),UNIQUE KEY unique1 (col1))",
			"insert into tableA values ('B', '1'), ('C', 2), ('Y', '100')",
			"call dolt_commit('-Am', 'creating table');",
			"call dolt_branch('feature');",
			"update tableA set pk = 'A' where pk='B';",
			"update tableA set pk = 'Z' where pk='Y';",
			"call dolt_commit('-am', 'update two rows');",
			"call dolt_checkout('feature');",
			"update tableA set col1='C' where pk='C';",
			"call dolt_commit('-am', 'added row on branch feature');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_constraint_violations_tableA;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tableA;",
				Expected: []sql.Row{{"A", "1"}, {"C", "C"}, {"Z", "100"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('main', 'feature-branch', 'test')",
				Expected: []sql.Row{},
			},
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'new-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'new-branch'"}},
			},
			{
				Query:    "INSERT INTO test VALUES (4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff correctly works with autocommit off, no checkout",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_BRANCH('feature-branch')",
			"use `mydb/feature-branch`",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"use mydb/main;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from test order by 1",
				Expected: []sql.Row{
					{1}, {2}, {3}, {1000},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE fails on non-branch revision",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_BRANCH('feature-branch')",
			"use `mydb/feature-branch`",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"use `mydb/main~`",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				ExpectedErrStr: "this operation is not supported while in a detached head state",
			},
			{
				Query:          "SELECT * FROM dolt_preview_merge_conflicts('main', 'feature-branch', 'test')",
				ExpectedErrStr: "this operation is not supported while in a detached head state",
			},
			{
				Query:          "CALL DOLT_MERGE('feature-branch')",
				ExpectedErrStr: "this operation is not supported while in a detached head state",
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE no-ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '--no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{5}}, // includes the merge commit created by no-ff and setup commits
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a no-ff"}}, // includes the merge commit created by no-ff
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'other-branch'"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE no-ff correctly works with autocommit off, no checkout",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"CALL DOLT_BRANCH('feature-branch')",
			"USE `mydb/feature-branch`",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff', '--date', '2022-08-06T12:00:01');",
			"use `mydb/main`",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '--no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{5}}, // includes the merge commit created by no-ff and setup commits
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a no-ff"}}, // includes the merge commit created by no-ff
			},
			{
				Query: "select * from test order by 1",
				Expected: []sql.Row{
					{1}, {2}, {3}, {1000},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE without conflicts correctly works with autocommit off with commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"CALL DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--commit')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "select message from dolt_log where date > '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a merge"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE without conflicts correctly works with autocommit off and no commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"CALL DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--no-commit')",
				Expected: []sql.Row{{"", 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", ""}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", true, "modified"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				// careful to filter out the initial commit, which will be later than the ones above
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other')",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main')",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflicts can be correctly resolved when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'update a value', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_pk, base_val, our_pk, our_val, our_diff_type, their_pk, their_val, their_diff_type FROM dolt_preview_merge_conflicts('main', 'feature-branch', 'test')",
				Expected: []sql.Row{{0, 0, 0, 1001, "modified", 0, 1000, "modified"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", "test"}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", false, "modified"}, {"test", false, "conflict"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"update a value"}},
			},
			{
				Query:    "SELECT * FROM dolt_conflicts",
				Expected: []sql.Row{{"test", uint64(1)}},
			},
			{
				Query:    "SELECT base_pk, base_val, our_pk, our_val, our_diff_type, their_pk, their_val, their_diff_type FROM dolt_conflicts_test",
				Expected: []sql.Row{{0, 0, 0, 1001, "modified", 0, 1000, "modified"}},
			},
			{
				Query:    "DELETE FROM dolt_conflicts_test",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", false, "modified"}},
			},
			{
				Query:    "SELECT * from test ORDER BY pk",
				Expected: []sql.Row{{0, 1001}, {1, 1}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", ""}},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}}, // merge wasn't committed yet, so still shows conflict between branches
			},
			{
				Query:    "CALL DOLT_COMMIT('-m', 'merged');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// TODO: These tests are skipped, because we have temporarily disabled dolt_conflicts_resolve
		//       when there are schema conflicts, since schema conflicts prevent table data from being
		//       merged, and resolving the schema changes, but not completing the data merge will likely
		//       give customers unexpected results.
		//       https://github.com/dolthub/dolt/issues/6616
		Name: "CALL DOLT_MERGE with schema conflicts can be correctly resolved using dolt_conflicts_resolve when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"ALTER TABLE test MODIFY val bigint;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"CALL DOLT_CHECKOUT('main');",
			"ALTER TABLE test MODIFY val smallint;",
			"CALL DOLT_COMMIT('-a', '-m', 'update val col', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Skip:     true,
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Skip:     true,
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", "test"}},
			},
			{
				Skip:     true,
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", false, "schema conflict"}},
			},
			{
				Skip:     true,
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Skip:     true,
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"update val col"}},
			},
			{
				Skip:     true,
				Query:    "SELECT COUNT(*) FROM dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Skip:     true,
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--ours', 'test');",
				Expected: []sql.Row{{0}},
			},
			{
				Skip:     true,
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", ""}},
			},
			{
				Skip:     true,
				Query:    "SELECT COUNT(*) FROM dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
			{
				Skip:     true,
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", true, "merged"}},
			},
			{
				Skip:             true,
				Query:            "CALL DOLT_COMMIT('-m', 'merged');",
				SkipResultsCheck: true,
			},
			{
				Skip:     true,
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Skip:     true,
				Query:    "SHOW CREATE TABLE test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` int NOT NULL,\n  `val` smallint,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "merge conflicts prevent new branch creation",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'update a value', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", "test"}},
			},
			{
				Query:    "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{{"test", false, "modified"}, {"test", false, "conflict"}},
			},
			{
				// errors because creating a new branch implicitly commits the current transaction
				Query:          "CALL DOLT_CHECKOUT('-b', 'other-branch')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsCommit.Error(),
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff & squash correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "SELECT count(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff & squash with a checkout in between",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other')",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main')",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'new-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'new-branch'"}},
			},
			{
				Query:    "INSERT INTO test VALUES (4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff no checkout",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL dolt_branch('feature-branch')",
			"use `mydb/feature-branch`",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"use mydb/main;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'new-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'new-branch'"}},
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"new-branch"}},
			},
			{
				Query:    "INSERT INTO test VALUES (4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {4}, {1000}},
			},
			{
				Query:            "use `mydb/main`",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE no-ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{5}}, // includes the merge commit created by no-ff and setup commits
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a no-ff"}}, // includes the merge commit created by no-ff
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'other-branch'"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with no conflicts works",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"CALL DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:02');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature-branch', '--no-commit', '--commit')",
				ExpectedErrStr: "cannot define both 'commit' and 'no-commit' flags at the same time",
			},
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{6}}, // includes the merge commit and a new commit created by successful merge
			},
			{
				Query:    "select message from dolt_log where date > '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a merge"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with no conflicts works, no checkout",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"CALL dolt_branch('feature-branch')",
			"use `mydb/feature-branch`",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:01');",
			"use mydb/main",
			"INSERT INTO test VALUES (5),(6),(7);",
			"CALL DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:02');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature-branch', '--no-commit', '--commit')",
				ExpectedErrStr: "cannot define both 'commit' and 'no-commit' flags at the same time",
			},
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{6}}, // includes the merge commit and a new commit created by successful merge
			},
			{
				Query:    "select message from dolt_log where date > '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a merge"}},
			},
			{
				Query: "select * from test order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3}, {5}, {6}, {7}, {1000},
				},
			},
			{
				Query:            "use `mydb/feature-branch`",
				SkipResultsCheck: true,
			},
			{
				Query: "select * from test order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3}, {1000},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with no conflicts works with no-commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:01');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"CALL DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:02');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--no-commit')",
				Expected: []sql.Row{{"", 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'other-branch'"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE when current or ahead results in a no-op",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-A', '-m', 'commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('HEAD', 'HEAD~1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('HEAD', 'HEAD~1', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('HEAD~1')",
				Expected: []sql.Row{{"", 0, 0, "cannot fast forward from a to b. a is ahead of b already"}},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('HEAD', 'HEAD')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('HEAD', 'HEAD', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('HEAD')",
				Expected: []sql.Row{{"", 0, 0, "Everything up-to-date"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflict is queryable and committable with dolt_allow_commit_conflicts on",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'update a value');",
			"set dolt_allow_commit_conflicts = on",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_preview_merge_conflicts('main', 'feature-branch', 'test')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * FROM dolt_conflicts",
				Expected: []sql.Row{{"test", uint64(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_conflicts_test",
				Expected: []sql.Row{{1}},
			},
			{
				// Test case-insensitive table name
				Query:    "SELECT count(*) FROM dolt_conflicts_TeST",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL DOLT_MERGE('--abort')",
				Expected: []sql.Row{{"", 0, 0, "merge aborted"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{0, 1001}},
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SET dolt_allow_commit_conflicts = 0",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:          "CALL DOLT_MERGE('feature-branch')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsAutoCommit.Error(),
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test", // transaction has been rolled back, 0 results
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflicts can be aborted when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'update a value');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", false, "modified"}, {"test", false, "conflict"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL DOLT_MERGE('--abort')",
				Expected: []sql.Row{{"", 0, 0, "merge aborted"}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT * FROM test ORDER BY pk",
				Expected: []sql.Row{{0, 1001}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0, "Switched to branch 'other-branch'"}},
			},
		},
	},
	{
		Name: "DOLT_MERGE(--abort) clears staged",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key);",
			"INSERT INTO test VALUES (0),(1),(2);",
			"set autocommit = off;",
			"CREATE TABLE one_pk (pk1 BIGINT NOT NULL, c1 BIGINT, c2 BIGINT, PRIMARY KEY (pk1));",
			"CALL DOLT_ADD('.');",
			"call dolt_commit('-a', '-m', 'add tables');",
			"call dolt_checkout('-b', 'feature-branch');",
			"call dolt_checkout('main');",
			"INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);",
			"call dolt_commit('-a', '-m', 'changed main');",
			"call dolt_checkout('feature-branch');",
			"INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);",
			"call dolt_commit('-a', '-m', 'changed feature branch');",
			"call dolt_checkout('main');",
			"call dolt_merge('feature-branch');",
			"call dolt_merge('--abort');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_status;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE complains when a merge overrides local changes",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"CALL DOLT_COMMIT('-a', '-m', 'Step 1');",
			"CALL DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature-branch')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('main', 'feature-branch', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:          "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				ExpectedErrStr: "error: local changes would be stomped by merge:\n\ttest\n Please commit your changes before you merge.",
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
		},
	},
	{
		Name: "Drop and add primary key on two branches converges to same schema",
		SetUpScript: []string{
			"create table t1 (i int);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 't1 table')",
			"call dolt_checkout('-b', 'b1')",
			"alter table t1 add primary key(i)",
			"alter table t1 drop primary key",
			"alter table t1 add primary key(i)",
			"alter table t1 drop primary key",
			"alter table t1 add primary key(i)",
			"call dolt_commit('-am', 'b1 primary key changes')",
			"call dolt_checkout('main')",
			"alter table t1 add primary key(i)",
			"call dolt_commit('-am', 'main primary key change')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'b1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('main', 'b1', 't1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('b1')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Constraint violations are persisted",
		SetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CREATE table other (pk int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_BRANCH('branch2');",
			"DELETE FROM parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete parent 1');",
			"CALL DOLT_CHECKOUT('branch1');",
			"INSERT INTO CHILD VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child of parent 1');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('branch1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 1, 1}},
			},
			{
				// Test case-insensitive table name
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_CHILD;",
				Expected: []sql.Row{{"foreign key", 1, 1}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"child", false, "constraint violation"},
				},
			},
		},
	},
	{
		// from constraint-violations.bats
		Name: "ancestor contains fk, main parent remove with backup, other child add, restrict",
		SetUpScript: []string{
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);",
			"INSERT INTO child VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'MC1');",
			"CALL DOLT_BRANCH('other');",
			"DELETE from parent WHERE pk = 20;",
			"CALL DOLT_COMMIT('-am', 'MC2');",

			"CALL DOLT_CHECKOUT('other');",
			"INSERT INTO child VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'OC1');",
			"CALL DOLT_CHECKOUT('main');",
			"set DOLT_FORCE_TRANSACTION_COMMIT = on;",
			"CALL DOLT_MERGE('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_parent",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_child",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_status;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{10, 1}, {30, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	// unique indexes
	{
		Name: "unique keys, insert violation",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL dolt_add('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (2, 1), (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (1, 1), (4, 4);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 3}, {4, 4}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 1, 1}, {"unique index", 2, 1}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "right", "refs/heads/main", "t"}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"t", false, "constraint violation"},
				},
			},
		},
	},
	{
		Name: "unique keys, update violation from left",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t values (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 3 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, 3}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 2, 3}, {"unique index", 3, 3}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"t", false, "constraint violation"},
				},
			},
		},
	},
	{
		Name: "unique keys, update violation from right",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 3 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, 3}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 2, 3}, {"unique index", 3, 3}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"t", false, "constraint violation"},
				},
			},
		},
	},
	{
		Name: "cell-wise merges can result in a unique key violation",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int, col2 int, UNIQUE col1_col2_u (col1, col2));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO T VALUES (1, 1, 1), (2, NULL, NULL);",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col2 = 1 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 1 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1, 1}, {2, 1, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1, col2 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 1, 1, 1}, {"unique index", 2, 1, 1}},
			},
			{
				Query: "select * from dolt_status;",
				Expected: []sql.Row{
					{"t", false, "constraint violation"},
				},
			},
		},
	},
	// Behavior between new and old format diverges in the case where right adds
	// a unique key constraint and resolves existing violations.
	// In the old format, because the violations exist on the left the merge is aborted.
	// In the new format, the merge can be completed successfully without error.
	// See MergeArtifactScripts and OldFormatMergeConflictsAndCVsScripts
	{
		Name: "left adds a unique key constraint and resolves existing violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'left adds a unique index');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "insert two tables with the same name and different schema",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int, extracol int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				ExpectedErrStr: "table with same name 't' added in 2 commits can't be merged",
			},
			{
				Query:          "CALL DOLT_MERGE('other');",
				ExpectedErrStr: "table with same name 't' added in 2 commits can't be merged",
			},
		},
	},
	{
		Name: "insert two tables with the same name and schema that don't conflict",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "insert two tables with the same name and schema that conflict",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, -1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 1, 1, -1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "merge with new triggers defined",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			// create table and trigger1 (main & other)
			"CREATE TABLE x(a BIGINT PRIMARY KEY)",
			"CREATE TRIGGER trigger1 BEFORE INSERT ON x FOR EACH ROW SET new.a = new.a + 1",
			"CALL dolt_add('-A')",
			"CALL dolt_commit('-m', 'added table with trigger')",
			"CALL dolt_branch('-c', 'main', 'other')",
			// create trigger2 on main
			"CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10",
			"CALL dolt_commit('-am', 'created trigger2 on main')",
			// create trigger3 & trigger4 on other
			"CALL dolt_checkout('other')",
			"CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100",
			"CREATE TRIGGER trigger4 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 1000",
			"CALL dolt_commit('-am', 'created triggers 3 & 4 on other');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select count(*) from dolt_schemas where type = 'trigger';",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "dolt_merge() works with no auto increment overlap",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t (c0) VALUES (1), (2);",
			"CALL dolt_commit('-a', '-m', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t (c0) VALUES (3), (4);",
			"CALL dolt_commit('-a', '-m', 'cm2');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "INSERT INTO t(c0) VALUES (5),(6),(7);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 5}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
				},
			},
		},
	},
	{
		Name: "dolt_merge() (3way) works with no auto increment overlap",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t (c0) VALUES (1);",
			"CALL dolt_commit('-a', '-m', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t (pk,c0) VALUES (3,3), (4,4);",
			"CALL dolt_commit('-a', '-m', 'cm2');",
			"CALL dolt_checkout('main');",
			"INSERT INTO t (c0) VALUES (5);",
			"CALL dolt_commit('-a', '-m', 'cm3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "INSERT INTO t(c0) VALUES (6),(7),(8);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 6}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
					{8, 8},
				},
			},
		},
	},
	{
		Name: "dolt_merge() with a gap in an auto increment key",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"INSERT INTO t (c0) VALUES (1), (2);",
			"CALL dolt_add('-A');",
			"CALL dolt_commit('-am', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t VALUES (4,4), (5,5);",
			"CALL dolt_commit('-am', 'cm2');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "INSERT INTO t VALUES (3,3),(NULL,6);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
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
		Name: "dolt_merge() (3way) with a gap in an auto increment key",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"INSERT INTO t (c0) VALUES (1);",
			"CALL dolt_add('-A');",
			"CALL dolt_commit('-am', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t VALUES (4,4), (5,5);",
			"CALL dolt_commit('-am', 'cm2');",
			"CALL dolt_checkout('main');",
			"INSERT INTO t (c0) VALUES (6);",
			"CALL dolt_commit('-am', 'cm3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'test')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "INSERT INTO t VALUES (3,3),(NULL,7);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
				},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. No conflicts expected.",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1), (2);",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_CHECKOUT('-b', 'right');",
			"update t set col1 = 1 where pk = 1;",
			"update t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'right cm');",
			"CALL DOLT_CHECKOUT('main');",
			"DELETE from t where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left cm');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'right')",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{2, nil, nil}},
			},
		},
	},
	{
		Name: "dropping constraint from one branch drops from both",
		SetUpScript: []string{
			"create table t (i int)",
			"alter table t add constraint c check (i > 0)",
			"call dolt_commit('-Am', 'initial commit')",

			"call dolt_checkout('-b', 'other')",
			"insert into t values (1)",
			"call dolt_commit('-Am', 'changes to other')",

			"call dolt_checkout('main')",
			"alter table t drop constraint c",
			"call dolt_commit('-Am', 'changes to main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "dropping constraint from one branch drops from both, no checkout",
		SetUpScript: []string{
			"create table t (i int)",
			"alter table t add constraint c check (i > 0)",
			"call dolt_commit('-Am', 'initial commit')",

			"call dolt_branch('other')",
			"use mydb/other",
			"insert into t values (1)",
			"alter table t drop constraint c",
			"call dolt_commit('-Am', 'changes to other')",

			"use mydb/main",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "insert into t values (-1)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "insert into t values (-1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "merge constraint with valid data on different branches",
		SetUpScript: []string{
			"create table t (i int)",
			"call dolt_commit('-Am', 'initial commit')",

			"call dolt_checkout('-b', 'other')",
			"insert into t values (1)",
			"call dolt_commit('-Am', 'changes to other')",

			"call dolt_checkout('main')",
			"alter table t add check (i < 10)",
			"call dolt_commit('-Am', 'changes to main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "resolving a deleted and modified row handles constraint checks",
		SetUpScript: []string{
			"create table test(a int primary key, b int, c int );",
			"alter table test add check (b < 4);",
			"insert into test values (1, 2, 3);",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'create test table');",

			"call dolt_checkout('-b', 'other');",
			"alter table test drop column c;",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'drop column');",

			"call dolt_checkout('main');",
			"delete from test;",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'remove row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select * from test",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "resolving a modified/modified row still checks nullness constraint",
		SetUpScript: []string{
			"create table test(a int primary key, b int, c int);",
			"insert into test values (1, 2, 3);",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'create test table');",

			"call dolt_checkout('-b', 'other');",
			"alter table test modify c int not null;",
			"update test set b = NULL;",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'drop column');",

			"call dolt_checkout('main');",
			"alter table test modify b int not null",
			"update test set c = NULL;",
			"call dolt_add('test');",
			"call dolt_commit('-m', 'remove row');",
			"set dolt_force_transaction_commit = on;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Skip:     true, // TODO: constraint violations
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{{"test", uint64(1), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select a, b, c from dolt_constraint_violations_test;",
				Expected: []sql.Row{{1, nil, nil}},
			},
		},
	},
	{
		Name: "Pk convergent updates to sec diff congruent",
		SetUpScript: []string{
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0), (1,1,1)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"update xyz set z = 2 where z = 1",
			"call dolt_commit('-am', 'update z=2')",

			"call dolt_checkout('main')",
			"update xyz set y = 2 where z = 1",
			"call dolt_commit('-am', 'update y=2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select y from xyz where y >= 0",
				Expected: []sql.Row{{0}, {2}},
			},
			{
				Query:    "select z from xyz where y >= 0",
				Expected: []sql.Row{{0}, {2}},
			},
		},
	},
	{
		Name: "Pk convergent left and right adds",
		SetUpScript: []string{
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"insert into xyz values (1,1,1)",
			"call dolt_commit('-am', 'adds')",

			"call dolt_checkout('main')",
			"insert into xyz values (1,1,1), (2,2,2)",
			"call dolt_commit('-am', 'adds')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select y from xyz where y >= 0",
				Expected: []sql.Row{{0}, {1}, {2}},
			},
			{
				Query:    "select z from xyz where y >= 0",
				Expected: []sql.Row{{0}, {1}, {2}},
			},
		},
	},
	{
		Name: "Pk adds+convergent adds to sec congruent",
		SetUpScript: []string{
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0), (1,1,1)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"insert into xyz values (3,3,3)",
			"update xyz set z = 5 where z = 1",
			"call dolt_commit('-am', 'right adds + edit')",

			"call dolt_checkout('main')",
			"insert into xyz values (4,4,4)",
			"update xyz set y = 2 where z = 1",
			"call dolt_commit('-am', 'left adds + update')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select y from xyz where y >= 0 order by 1",
				Expected: []sql.Row{{0}, {2}, {3}, {4}},
			},
			{
				Query:    "select z from xyz where y >= 0 order by 1",
				Expected: []sql.Row{{0}, {3}, {4}, {5}},
			},
		},
	},
	{
		Name: "Left deletes",
		SetUpScript: []string{
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0), (1,1,1),(2,2,2),(3,3,3)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"delete from xyz where y = 3",
			"call dolt_commit('-am', 'right deletes')",

			"call dolt_checkout('main')",
			"delete from xyz where y = 1",
			"call dolt_commit('-am', 'left deletes')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select y from xyz where y >= 0 order by 1",
				Expected: []sql.Row{{0}, {2}},
			},
			{
				Query:    "select z from xyz where y >= 0 order by 1",
				Expected: []sql.Row{{0}, {2}},
			},
		},
	},
	{
		Name: "delete conflict",
		SetUpScript: []string{
			"set @@dolt_allow_commit_conflicts = 1",
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0), (1,1,1)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"delete from xyz where y = 1",
			"call dolt_commit('-am', 'right delete')",

			"call dolt_checkout('main')",
			"update xyz set y = 2 where y = 1",
			"call dolt_commit('-am', 'left update')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature')",
				Expected: []sql.Row{{"xyz", uint64(1), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select our_y, our_diff_type, their_y, their_diff_type from dolt_conflicts_xyz",
				Expected: []sql.Row{{2, "modified", nil, "removed"}},
			},
		},
	},
	{
		Name: "divergent edit conflict",
		SetUpScript: []string{
			"set @@dolt_allow_commit_conflicts = 1",
			"create table xyz (x int primary key, y int, z int, key y_idx(y), key z_idx(z))",
			"insert into xyz values (0,0,0), (1,1,1)",
			"call dolt_commit('-Am', 'make table')",

			"call dolt_checkout('-b', 'feature')",
			"update xyz set y = 3 where y = 1",
			"call dolt_commit('-am', 'right delete')",

			"call dolt_checkout('main')",
			"update xyz set y = 2 where y = 1",
			"call dolt_commit('-am', 'left update')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'feature')",
				Expected: []sql.Row{{"xyz", uint64(1), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select our_y, our_diff_type, their_y, their_diff_type from dolt_conflicts_xyz",
				Expected: []sql.Row{{2, "modified", 3, "modified"}},
			},
		},
	},
	{
		Name: "Merge errors if the primary key types have changed (even if the new type has the same NomsKind)",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 bigint, pk2 bigint, PRIMARY KEY (pk1, pk2));",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"ALTER TABLE t MODIFY COLUMN pk2 tinyint",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'right commit');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'right')",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys",
			},
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys",
			},
		},
	},
	{
		Name:        "`Delete from table` should keep artifacts - conflicts",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, 1, 100},
					{nil, nil, nil, nil, 2, 200},
				},
			},
		},
	},
	{
		Name:        "`Truncate table` should keep artifacts - conflicts",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "truncate t;",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, 1, 100},
					{nil, nil, nil, nil, 2, 200},
				},
			},
		},
	},
	{
		Name:        "`Delete from table` should keep artifacts - violations",
		SetUpScript: createViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		Name:        "`Truncate table` should keep artifacts - violations",
		SetUpScript: createViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "truncate t;",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		Name: "parent index is longer than child index",
		SetUpScript: []string{
			"create table parent (i int primary key, x int, y int, z int, index (y, x, z));",
			"create table child (y int, x int, primary key(y, x), foreign key (y, x) references parent(y, x));",
			"insert into parent values (100,1,1,1), (200,2,1,2), (300,1,null,1);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_BRANCH('other');",

			"DELETE from parent WHERE x = 2;",
			"CALL DOLT_COMMIT('-am', 'main');",

			"CALL DOLT_CHECKOUT('other');",
			"INSERT INTO child VALUES (1, 2);",
			"CALL DOLT_COMMIT('-am', 'other');",

			"CALL DOLT_CHECKOUT('main');",
			"set DOLT_FORCE_TRANSACTION_COMMIT = on;",
			"CALL DOLT_MERGE('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{
					{"child", uint64(1)},
				},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_parent",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT y, x from dolt_constraint_violations_child",
				Expected: []sql.Row{
					{1, 2},
				},
			},
		},
	},
	{
		Name: "parallel column updates (repro issue #4547)",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"create table t (rowId int not null, col1 varchar(255), col2 varchar(255), keyCol varchar(60), dataA varchar(255), dataB varchar(255), PRIMARY KEY (rowId), UNIQUE KEY uniqKey (col1, col2, keyCol));",
			"insert into t (rowId, col1, col2, keyCol, dataA, dataB) values (1, '1', '2', 'key-a', 'test1', 'test2')",
			"CALL DOLT_COMMIT('-Am', 'new table');",

			"CALL DOLT_CHECKOUT('-b', 'other');",
			"update t set dataA = 'other'",
			"CALL DOLT_COMMIT('-am', 'update data other');",

			"CALL DOLT_CHECKOUT('main');",
			"update t set dataB = 'main'",
			"CALL DOLT_COMMIT('-am', 'update on main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_t",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT * from t",
				Expected: []sql.Row{
					{1, "1", "2", "key-a", "other", "main"},
				},
			},
		},
	},
	{
		Name: "try to merge a nullable field into a non-null column",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(3,3);",
			"call dolt_commit('-Am', 'new table with NULL value');",
			"call dolt_checkout('-b', 'other')",
			"insert into test values (2,NULL);",
			"call dolt_commit('-am', 'inserted null value')",
			"call dolt_checkout('main');",
			"alter table test modify c0 int not null;",
			"insert into test values (4,4)",
			"call dolt_commit('-am', 'modified column c0 to not null');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('other')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations",
				Expected: []sql.Row{{"test", uint(1)}},
			},
			{
				Query: "select violation_type, pk, violation_info from dolt_constraint_violations_test",
				Expected: []sql.Row{
					{"not null", 2, merge.NullViolationMeta{Columns: []string{"c0"}}},
				},
			},
		},
	},
	{
		Name: "merge fulltext with renamed table",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), FULLTEXT idx (v1));",
			"INSERT INTO test VALUES (1, 'abc');",
			"CALL dolt_commit('-Am', 'Initial commit')",
			"call dolt_branch('other')",
			"DROP INDEX idx ON test;",
			"INSERT INTO test VALUES (2, 'def');",
			"RENAME TABLE test TO test_temp;",
			"ALTER TABLE test_temp ADD FULLTEXT INDEX idx (v1);",
			"RENAME TABLE test_temp TO test;",
			"call dolt_commit('-Am', 'Renamed pseudo-index tables')",
			"call dolt_checkout('other')",
			"INSERT INTO test VALUES (3, 'ghi');",
			"call dolt_commit('-Am', 'Insertion commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Skip:     true, // TODO: conflict: table with same name deleted and modified
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('other')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "SELECT v1 FROM test WHERE MATCH(v1) AGAINST ('abc def ghi');",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "merge when schemas are equal, but column tags are different",
		SetUpScript: []string{
			// Create a branch where t doesn't exist yet
			"call dolt_branch('branch1');",
			// Create t on main, but change column types so that the tag won't match branch1
			"CREATE TABLE t (pk INT PRIMARY KEY, col1 int);",
			"call dolt_commit('-Am', 'creating table t on main');",
			"ALTER TABLE t modify column col1 varchar(255);",
			"call dolt_commit('-am', 'modifying table t on main');",
			"INSERT INTO t values (1, 'one'), (2, 'two');",
			"call dolt_commit('-am', 'inserting two rows into t on main');",

			// Create t on branch1, without an intermediate type change, so that the tag doesn't match main
			"call dolt_checkout('branch1');",
			"CREATE TABLE t (pk INT PRIMARY KEY, col1 varchar(255));",
			"call dolt_commit('-Am', 'creating table t on branch1');",
			"INSERT INTO t values (3, 'three');",
			"call dolt_commit('-am', 'inserting one row into t on branch1');",
			"SET @PreMergeBranch1Commit = dolt_hashof('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('branch1', 'main')",
				Expected: []sql.Row{},
			},
			{
				// We can merge from main -> branch1, even though the column tags are not identical
				Query:    "call dolt_merge('main')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}, {3, "three"}},
			},
			{
				// Reset branch1 to the pre-merge commit, so we can test merging branch1 -> main
				Query:    "CALL dolt_reset('--hard', @PreMergeBranch1Commit);",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL dolt_checkout('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				// We can merge from branch1 -> main, even though the column tags are not identical
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}, {3, "three"}},
			},
		},
	},
	{
		// Ensure that column defaults are normalized to the same thing, so they merge with no issue
		Name: "merge with float column default",
		SetUpScript: []string{
			"create table t (f float);",
			"call dolt_commit('-Am', 'setup');",
			"call dolt_branch('other');",
			"alter table t modify column f float default '1.00';",
			"call dolt_commit('-Am', 'change default on main');",
			"call dolt_checkout('other');",
			"alter table t modify column f float default '1.000000000';",
			"call dolt_commit('-Am', 'change default on other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		// Ensure that column defaults are normalized to the same thing, so they merge with no issue
		Name: "merge with float 1.23 column default",
		SetUpScript: []string{
			"create table t (f float);",
			"call dolt_commit('-Am', 'setup');",
			"call dolt_branch('other');",
			"alter table t modify column f float default '1.23000';",
			"call dolt_commit('-Am', 'change default on main');",
			"call dolt_checkout('other');",
			"alter table t modify column f float default '1.23000000000';",
			"call dolt_commit('-Am', 'change default on other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		// Ensure that column defaults are normalized to the same thing, so they merge with no issue
		Name: "merge with decimal 1.23 column default",
		SetUpScript: []string{
			"create table t (d decimal(20, 10));",
			"call dolt_commit('-Am', 'setup');",
			"call dolt_branch('other');",
			"alter table t modify column d decimal(20, 10) default '1.23000';",
			"call dolt_commit('-Am', 'change default on main');",
			"call dolt_checkout('other');",
			"alter table t modify column d decimal(20, 10) default '1.23000000000';",
			"call dolt_commit('-Am', 'change default on other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('main')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
	{
		// Ensure that column defaults are normalized to the same thing, so they merge with no issue
		Name: "merge with different types",
		SetUpScript: []string{
			"create table t (f float);",
			"call dolt_commit('-Am', 'setup');",
			"call dolt_branch('other');",
			"alter table t modify column f float default 1.23;",
			"call dolt_commit('-Am', 'change default on main');",
			"call dolt_checkout('other');",
			"alter table t modify column f float default '1.23';",
			"call dolt_commit('-Am', 'change default on other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('other', 'main')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('main')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
		},
	},
}

var KeylessMergeCVsAndConflictsScripts = []queries.ScriptTest{
	{
		Name: "Keyless merge with unique indexes documents violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table t (col1 int, col2 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (2, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, col1, col2 from dolt_constraint_violations_t ORDER BY col1 ASC;",
				Expected: []sql.Row{{"unique index", 1, 1}, {"unique index", 2, 1}},
			},
			{
				Query:    "SELECT * from t ORDER BY col1 ASC;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		Name: "Keyless merge with foreign keys documents violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table parent (pk int PRIMARY KEY);",
			"CREATE table child (parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent (pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO child VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE from parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Keyless merge documents conflicts",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (col1 int, col2 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_col1, base_col2, our_col1, our_col2, their_col1, their_col2 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 1, 1, 1}},
			},
		},
	},
	{
		// this won't automatically become a PK because col2 is nullable
		Name: "unique key violation for keyless table",
		SetUpScript: []string{
			"create table t (col1 int not null, col2 int, col3 int);",
			"alter table t add unique index (col1, col2);",
			"call dolt_commit('-Am', 'init');",

			"call dolt_checkout('-b', 'right');",
			"insert into t values (1, null, null);",
			"insert into t values (3, 3, null);",
			"call dolt_commit('-Am', 'right cm');",

			"call dolt_checkout('main');",
			"insert into t values (2, null, null);",
			"insert into t values (3, 3, 1);",
			"call dolt_commit('-Am', 'left cm');",

			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select col1, col2, col3 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{3, 3, nil}, {3, 3, 1}},
			},
		},
	},
}

var DoltConflictTableNameTableTests = []queries.ScriptTest{
	{
		Name: "conflict diff types",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1);",
			"INSERT INTO t VALUES (2, 2);",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'create table with row');",

			"CALL DOLT_CHECKOUT('-b', 'other');",
			"UPDATE t set col1 = 3 where pk = 1;",
			"UPDATE t set col1 = 0 where pk = 2;",
			"DELETE FROM t where pk = 3;",
			"INSERT INTO t VALUES (4, -4);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t set col1 = 2 where pk = 1;",
			"DELETE FROM t where pk = 2;",
			"UPDATE t set col1 = 0 where pk = 3;",
			"INSERT INTO t VALUES (4, 4);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{{"t", uint64(4), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, our_diff_type, their_pk, their_col1, their_diff_type" +
					" from dolt_conflicts_t ORDER BY COALESCE(base_pk, our_pk, their_pk) ASC;",
				Expected: []sql.Row{
					{1, 1, 1, 2, "modified", 1, 3, "modified"},
					{2, 2, nil, nil, "removed", 2, 0, "modified"},
					{3, 3, 3, 0, "modified", nil, nil, "removed"},
					{nil, nil, 4, 4, "added", 4, -4, "added"},
				},
			},
		},
	},
	{
		Name: "keyless cardinality columns",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1), (2), (3), (4), (6);",
			"CALL DOLT_COMMIT('-am', 'init');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (1);",
			"DELETE FROM t where col1 = 2;",
			"INSERT INTO t VALUES (3);",
			"INSERT INTO t VALUES (4), (4);",
			"INSERT INTO t VALUES (5);",
			"DELETE from t where col1 = 6;",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE FROM t WHERE col1 = 1;",
			"INSERT INTO t VALUES (2);",
			"INSERT INTO t VALUES (3);",
			"INSERT INTO t VALUES (4);",
			"INSERT INTO t VALUES (5);",
			"DELETE from t where col1 = 6;",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'right')",
				Expected: []sql.Row{{"t", uint64(6), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "SELECT base_col1, our_col1, their_col1, our_diff_type, their_diff_type, base_cardinality, our_cardinality, their_cardinality from dolt_conflicts_t ORDER BY COALESCE(base_col1, our_col1, their_col1) ASC;",
				Expected: []sql.Row{
					{1, nil, 1, "removed", "modified", uint64(1), uint64(0), uint64(2)},
					{2, 2, nil, "modified", "removed", uint64(1), uint64(2), uint64(0)},
					{3, 3, 3, "modified", "modified", uint64(1), uint64(2), uint64(2)},
					{4, 4, 4, "modified", "modified", uint64(1), uint64(2), uint64(3)},
					{nil, 5, 5, "added", "added", uint64(0), uint64(1), uint64(1)},
					{6, nil, nil, "removed", "removed", uint64(1), uint64(0), uint64(0)},
				},
			},
		},
	},
}

var createConflictsSetupScript = []string{
	"create table t (pk int primary key, col1 int);",
	"call dolt_commit('-Am', 'create table');",
	"call dolt_checkout('-b', 'other');",

	"insert into t values (1, 100);",
	"insert into t values (2, 200);",
	"call dolt_commit('-Am', 'other commit');",

	"call dolt_checkout('main');",
	"insert into t values (1, -100);",
	"insert into t values (2, -200);",
	"call dolt_commit('-Am', 'main commit');",

	"set dolt_allow_commit_conflicts = on;",
	"call dolt_merge('other');",
}

var createViolationsSetupScript = []string{
	"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
	"CALL DOLT_COMMIT('-Am', 'create table');",

	"CALL DOLT_CHECKOUT('-b', 'other');",
	"INSERT INTO t VALUES (2, 1), (3, 3);",
	"CALL DOLT_COMMIT('-am', 'other insert');",

	"CALL DOLT_CHECKOUT('main');",
	"INSERT INTO t values (1, 1), (4, 4);",
	"CALL DOLT_COMMIT('-am', 'main insert');",

	"SET dolt_force_transaction_commit = on;",
	"call dolt_merge('other');",
}

var Dolt1ConflictTableNameTableTests = []queries.ScriptTest{
	{
		Name:        "Provides a dolt_conflicts_id",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "set @hash1 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1);",
			},
			{
				Query: "set @hash2 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 2);",
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
				},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t where dolt_conflict_id = @hash2;",
				Expected: []sql.Row{
					{nil, nil, 2, -200, 2, 200},
				},
			},
			// Make sure that we can update using it
			{
				Query:    "update dolt_conflicts_t SET our_col1 = their_col1 where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			// And delete
			{
				Query:    "delete from dolt_conflicts_t where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 2, -200, 2, 200},
				},
			},
		},
	},
	{
		Name: "dolt_conflicts_id is unique across merges",
		SetUpScript: append(createConflictsSetupScript, []string{
			"CALL DOLT_COMMIT('-afm', 'commit conflicts');",

			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"UPDATE t SET col1 = 9999 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'commit on other2');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 8888 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'commit on main');",

			"CALL DOLT_MERGE('other2');",

			"set @hash1 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1 and their_col1 = 100);",
			"set @hash2 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1 and their_col1 = 9999);",
			"set @hash3 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 2);",
		}...),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select @hash1 != @hash2 AND @hash2 != @hash3;",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		Name:        "Updates on our columns get applied to the source table - smoke",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col1 = 1000 where our_pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 1000, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 1000},
					{2, -200},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col1 = their_col1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 100, 1, 100},
					{nil, nil, 2, 200, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 100},
					{2, 200},
				},
			},
		},
	},
	{
		Name: "Updates on our columns get applied to the source table - compound / inverted pks",
		SetUpScript: []string{
			"create table t (pk2 int, pk1 int, col1 int, primary key (pk1, pk2));",
			"call dolt_commit('-Am', 'create table');",

			"call dolt_checkout('-b', 'other');",
			"insert into t values (1, 1, 100), (2, 1, 200);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"insert into t values (1, 1, -100), (2, 1, -200);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk1, base_pk2, base_col1, our_pk1, our_pk2, our_col1, their_pk1, their_pk2, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 1, -100, 1, 1, 100},
					{nil, nil, nil, 1, 2, -200, 1, 2, 200},
				},
			},
			{
				Query:    "Update dolt_conflicts_t set our_col1 = 1000;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk1, base_pk2, base_col1, our_pk1, our_pk2, our_col1, their_pk1, their_pk2, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 1, 1000, 1, 1, 100},
					{nil, nil, nil, 1, 2, 1000, 1, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 1, 1000},
					{2, 1, 1000},
				},
			},
		},
	},
	{
		Name: "Updates on our columns get applied to the source table - keyless",
		SetUpScript: []string{
			"create table t (name varchar(100), price int);",
			"call dolt_commit('-Am', 'create table');",

			"call dolt_checkout('-b', 'other');",
			"insert into t values ('apple', 1);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"insert into t values ('apple', 1), ('apple', 1);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(2), "apple", 1, uint64(1)},
				},
			},
			// Arguably this behavior is weird. If you ran this same query
			// against the original table, it would update two rows. Since this
			// was run against the conflicts table, only one row is updated.
			{
				Query: "update dolt_conflicts_t set our_name = 'orange' where our_name = 'apple'",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Updated: 1, Matched: 1}}},
				},
			},
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(1), "apple", 1, uint64(1)},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{"apple", 1}, {"orange", 1}},
			},
			// Updating cardinality should be no-op.
			{
				Query: "update dolt_conflicts_t set our_cardinality = 10, their_cardinality = 10, base_cardinality = 10;",
			},
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(1), "apple", 1, uint64(1)},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{"apple", 1}, {"orange", 1}},
			},
		},
	},
	{
		Name: "Updating our cols when the row is missing inserts the row",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, null);",
			"insert into t values (2, null);",
			"insert into t values (3, null);",
			"call dolt_commit('-Am', 'create table');",
			"call dolt_checkout('-b', 'other');",

			"update t set col1 = 100 where pk = 1;",
			"delete from t where pk = 2;",
			"update t set col1 = 300 where pk = 3;",
			"insert into t values (4, 400);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"update t set col1 = -100 where pk = 1;",
			"update t set col1 = -200 where pk = 2;",
			"delete from t where pk = 3;",
			"insert into t values (4, -400);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, nil, 1, -100, 1, 100},
					{2, nil, 2, -200, nil, nil},
					{3, nil, nil, nil, 3, 300},
					{nil, nil, 4, -400, 4, 400},
				},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, nil, nil, nil, 1, 100},
					{2, nil, nil, nil, nil, nil},
					{3, nil, nil, nil, 3, 300},
					{nil, nil, nil, nil, 4, 400},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			// The new rows PKs must be fully specified
			{
				Query:          "update dolt_conflicts_t set our_col1 = their_col1;",
				ExpectedErrStr: "column name 'our_pk' is non-nullable but attempted to set a value of null",
			},
			// Take theirs
			{
				Query:    "update dolt_conflicts_t set our_pk = their_pk, our_col1 = their_col1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 4, Updated: 3}}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 100}, {3, 300}, {4, 400}},
			},
		},
	},
	{
		Name:        "Updating our cols after schema change",
		SetUpScript: append(createConflictsSetupScript, "ALTER TABLE t add column col2 int FIRST;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show create table dolt_conflicts_t;",
				Expected: []sql.Row{{"dolt_conflicts_t", "CREATE TABLE `dolt_conflicts_t` (\n  `from_root_ish` varchar(1023),\n  `base_pk` int,\n  `base_col1` int,\n  `our_pk` int NOT NULL,\n  `our_col2` int,\n  `our_col1` int,\n  `our_diff_type` varchar(1023),\n  `their_pk` int,\n  `their_col1` int,\n  `their_diff_type` varchar(1023),\n  `dolt_conflict_id` varchar(1023)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, our_col2, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, nil, 1, 100},
					{nil, nil, 2, -200, nil, 2, 200},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col2 = their_col1",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select pk, col1, col2 from t;",
				Expected: []sql.Row{
					{1, -100, 100},
					{2, -200, 200},
				},
			},
		},
	},
	{
		Name:        "Updates on their or base columns do nothing",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -100}, {2, -200}},
			},
			{
				Query:    "update dolt_conflicts_t set base_col1 = 9999, their_col1 = 9999;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -100}, {2, -200}},
			},
		},
	},
}

// MergeArtifactsScripts tests new format merge behavior where
// existing violations and conflicts are merged together.
var MergeArtifactsScripts = []queries.ScriptTest{
	{
		Name: "conflicts on different branches can be merged",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on",
			"CALL DOLT_CHECKOUT('-b', 'conflicts1');",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",
			"CALL DOLT_BRANCH('conflicts2');",

			// branches conflicts1 and conflicts2 both have a table t with no rows

			// create a conflict for pk 1 in conflicts1
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert pk 1');",
			"CALL DOLT_BRANCH('other');",
			"UPDATE t set col1 = 100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE T set col1 = -100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('conflicts1');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on conflicts1');",

			// create a conflict for pk 2 in conflicts2
			"CALL DOLT_CHECKOUT('conflicts2');",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'insert pk 2');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE t set col1 = 100 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other2');",
			"UPDATE T set col1 = -100 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('conflicts2');",
			"CALL DOLT_MERGE('other2');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on conflicts2');",

			"CALL DOLT_CHECKOUT('conflicts1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{1, 1, 1, 100, 1, -100}},
			},
			{
				Query:    "SELECT pk, col1 from t;",
				Expected: []sql.Row{{1, 100}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('conflicts2');",
				Expected: []sql.Row{{0, "Switched to branch 'conflicts2'"}},
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{2, 2, 2, 100, 2, -100}},
			},
			{
				Query:    "SELECT pk, col1 from t;",
				Expected: []sql.Row{{2, 100}},
			},
			{
				Query:    "CALL DOLT_MERGE('conflicts1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, 1, 1, 100, 1, -100},
					{2, 2, 2, 100, 2, -100},
				},
			},
			{
				Query: "SELECT pk, col1 from t;",
				Expected: []sql.Row{
					{1, 100},
					{2, 100},
				},
			},
			{
				Query: "UPDATE t SET col1 = 300;",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 2,
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, 1, 1, 300, 1, -100},
					{2, 2, 2, 300, 2, -100},
				},
			},
		},
	},
	{
		Name: "conflicts of different schemas can't coexist",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert pk 1');",

			"CALL DOLT_BRANCH('other');",
			"UPDATE t set col1 = 100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other');",

			"UPDATE T set col1 = -100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('main');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on main');",
			"ALTER TABLE t ADD COLUMN col2 int;",
			"CALL DOLT_COMMIT('-afm', 'alter schema');",
			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"UPDATE t set col2 = -1000 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'update pk 1 to -1000');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t set col2 = 1000 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'update pk 1 to 1000');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:          "CALL DOLT_MERGE('other2');",
				ExpectedErrStr: "the existing conflicts are of a different schema than the conflicts generated by this merge. Please resolve them and try again",
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{1, 1, 1, 100, 1, -100}},
			},
			{
				Query:    "SELECT pk, col1, col2 from t;",
				Expected: []sql.Row{{1, 100, 1000}},
			},
		},
	},
	{
		Name: "violations with an older commit hash are overwritten if the value is the same",
		SetUpScript: []string{
			"set dolt_force_transaction_commit = on;",

			"CALL DOLT_CHECKOUT('-b', 'viol1');",
			"CREATE TABLE parent (pk int PRIMARY KEY);",
			"CREATE TABLE child (pk int PRIMARY KEY, fk int, FOREIGN KEY (fk) REFERENCES parent (pk));",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup table');",
			"CALL DOLT_BRANCH('viol2');",
			"CALL DOLT_BRANCH('other3');",
			"INSERT INTO parent VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'viol1 setup');",

			"CALL DOLT_CHECKOUT('-b', 'other');",
			"INSERT INTO child VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child of 1');",

			"CALL DOLT_CHECKOUT('viol1');",
			"DELETE FROM parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete 1');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit violations 1');",

			"CALL DOLT_CHECKOUT('viol2');",
			"INSERT INTO parent values (2);",
			"CALL DOLT_COMMIT('-am', 'viol2 setup');",

			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"INSERT into child values (2, 2);",
			"CALL DOLT_COMMIT('-am', 'insert child of 2');",

			"CALL DOLT_CHECKOUT('viol2');",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'delete 2');",
			"CALL DOLT_MERGE('other2');",
			"CALL DOLT_COMMIT('-afm', 'commit violations 2');",

			"CALL DOLT_CHECKOUT('other3');",
			"INSERT INTO PARENT VALUES (3);",
			"CALL DOLT_COMMIT('-am', 'edit needed to trigger three-way merge');",

			"CALL DOLT_CHECKOUT('viol1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 1, 1}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('viol2');",
				Expected: []sql.Row{{0, "Switched to branch 'viol2'"}},
			},
			{
				Query:    "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 2, 2}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('viol1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			// the commit hashes for the above two violations change in this merge
			{
				Query:    "SELECT violation_type, fk, pk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{"foreign key", 1, 1}, {"foreign key", 2, 2}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'commit active merge');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "UPDATE child set fk = 4;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 0, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'update children to new value');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_MERGE('other3');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{
					{"foreign key", 1, 1},
					{"foreign key", 1, 4},
					{"foreign key", 2, 2},
					{"foreign key", 2, 4}},
			},
		},
	},
	{
		Name: "merging unique key violations in left and right",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table t');",
			"CALL DOLT_BRANCH('right');",
			"CALL DOLT_BRANCH('left2');",

			"CALL DOLT_CHECKOUT('-b', 'right2');",
			"INSERT INTO T VALUES (4, 1);",
			"CALL DOLT_COMMIT('-am', 'right2 insert');",

			"CALL DOLT_CHECKOUT('right');",
			"INSERT INTO T VALUES (3, 1);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('left2');",
			"INSERT INTO T VALUES (2, 1);",
			"CALL DOLT_COMMIT('-am', 'left2 insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO T VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Skip:     true, // TODO: constraint violations
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'left2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('left2');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 1, 1}, {"unique index", 2, 1}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'commit unique key viol');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('right');",
				Expected: []sql.Row{{0, "Switched to branch 'right'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('right2');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{3, 1}, {4, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"unique index", 3, 1}, {"unique index", 4, 1}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'commit unique key viol');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}, {4, 1}},
			},
			{
				Query: "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{"unique index", 1, 1},
					{"unique index", 2, 1},
					{"unique index", 3, 1},
					{"unique index", 4, 1}},
			},
		},
	},
	{
		Name: "right adds a unique key constraint and resolves existing violations.",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'right adds a unique index');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "unique key violation should be thrown even if a PK column is used in the unique index",
		SetUpScript: []string{
			"create table t (col1 int not null, col2 int not null, col3 int, primary key (col1, col2));",
			"alter table t add unique (col2, col3);",
			"call dolt_commit('-Am', 'init');",

			"call dolt_checkout('-b', 'right');",
			"insert into t values (1, 2, 3);",
			"call dolt_commit('-Am', 'right');",

			"call dolt_checkout('main');",
			"insert into t values (2, 2, 3);",
			"call dolt_commit('-Am', 'left');",

			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select col1, col2, col3 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 2, 3}, {2, 2, 3}},
			},
		},
	},
	{
		Name: "unique key violation should be thrown even if a PK column is used in the unique index 2",
		SetUpScript: []string{
			"create table wxyz (w int, x int, y int, z int, primary key (x, w));",
			"alter table wxyz add unique (z, x);",
			"call dolt_commit('-Am', 'init');",

			"call dolt_checkout('-b', 'right');",
			"insert into wxyz values (1, 2, 3, 4);",
			"call dolt_commit('-Am', 'right');",

			"call dolt_checkout('main');",
			"insert into wxyz values (5, 2, 6, 4);",
			"call dolt_commit('-Am', 'left');",

			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select w, x, y, z from dolt_constraint_violations_wxyz;",
				Expected: []sql.Row{{1, 2, 3, 4}, {5, 2, 6, 4}},
			},
		},
	},
	{
		Name: "unique key violations should not be thrown for keys with null values",
		SetUpScript: []string{
			"create table t (col1 int not null, col2 int not null, col3 int, primary key (col1, col2));",
			"alter table t add unique (col2, col3);",
			"call dolt_commit('-Am', 'init');",

			"call dolt_checkout('-b', 'right');",
			"insert into t values (1, 2, null);",
			"call dolt_commit('-Am', 'right');",

			"call dolt_checkout('main');",
			"insert into t values (2, 2, null);",
			"call dolt_commit('-Am', 'left');",

			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "select count(*) from dolt_constraint_violations;",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 2, nil}, {2, 2, nil}},
			},
		},
	},
	{
		Name: "regression test for bad column ordering in schema",
		SetUpScript: []string{
			"CREATE TABLE t (col1 enum ('A', 'B'), col2 varchar(100), primary key (col2));",
			"ALTER TABLE t add unique index (col1);",
			"call DOLT_COMMIT('-Am', 'initial');",

			"call DOLT_CHECKOUT('-b', 'right');",
			"insert into t values ('A', 'first');",
			"call DOLT_COMMIT('-Am', 'right');",

			"call DOLT_CHECKOUT('main');",
			"insert into t values ('A', 'second');",
			"call DOLT_COMMIT('-Am', 'left');",

			"set dolt_force_transaction_commit = 1;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select col1, col2 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"A", "first"}, {"A", "second"}},
			},
		},
	},
	{
		Name: "Multiple foreign key violations for a given row not supported",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			`
			CREATE TABLE parent(
			  pk int PRIMARY KEY, 
			  col1 int, 
			  col2 int, 
			  INDEX par_col1_idx (col1), 
			  INDEX par_col2_idx (col2)
			);`,
			`
			CREATE TABLE child(
			  pk int PRIMARY KEY,
			  col1 int,
			  col2 int,
			  FOREIGN KEY (col1) REFERENCES parent(col1),
			  FOREIGN KEY (col2) REFERENCES parent(col2)
			);`,
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO CHILD VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE from parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete parent');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "error storing constraint violation for primary key (( 1 )): another violation already exists\nnew violation: {\"Columns\":[\"col1\"],\"ForeignKey\":\"child_ibfk_1\",\"Index\":\"col1\",\"OnDelete\":\"RESTRICT\",\"OnUpdate\":\"RESTRICT\",\"ReferencedColumns\":[\"col1\"],\"ReferencedIndex\":\"par_col1_idx\",\"ReferencedTable\":\"parent\",\"Table\":\"child\"} old violation: ({\"Columns\":[\"col2\"],\"ForeignKey\":\"child_ibfk_2\",\"Index\":\"col2\",\"OnDelete\":\"RESTRICT\",\"OnUpdate\":\"RESTRICT\",\"ReferencedColumns\":[\"col2\"],\"ReferencedIndex\":\"par_col2_idx\",\"ReferencedTable\":\"parent\",\"Table\":\"child\"})",
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Multiple unique key violations for a given row not supported",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table t (pk int PRIMARY KEY, col1 int UNIQUE, col2 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT into t VALUES (2, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "error storing constraint violation for primary key (( 1 )): another violation already exists\nnew violation: {\"Columns\":[\"col1\"],\"Name\":\"col1\"} old violation: ({\"Columns\":[\"col2\"],\"Name\":\"col2\"})",
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1, 1}},
			},
		},
	},
	{
		Name: "Multiple unique key violations part 1 (repro issue #5719)",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (id int NOT NULL, col1 varchar(255), col2 varchar(255), col3 varchar(255), PRIMARY KEY (id), UNIQUE KEY uniq_idx (col1,col2,col3));",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t (id, col1, col2, col3) VALUES (1, 'val1', 'val1', 'val1'), (4, 'val1', 'val1', 'val2')",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t (id, col1, col2, col3) VALUES (2, 'val1', 'val1', 'val1'), (3, 'val1', 'val1', 'val2');",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(4)}},
			},
			{
				Query: "select id, col1, col2, col3 from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{1, "val1", "val1", "val1"},
					{2, "val1", "val1", "val1"},
					{3, "val1", "val1", "val2"},
					{4, "val1", "val1", "val2"},
				},
			},
		},
	},
	{
		Name: "Multiple unique key violations part 2 (repro issue #5719)",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (id int NOT NULL, col1 varchar(255), col2 varchar(255), col3 varchar(255), PRIMARY KEY (id), UNIQUE KEY uniq_idx (col1,col2,col3));",
			"INSERT INTO t (id, col1, col2, col3) VALUES (1, 'val1', 'val1', 'val1'), (2, 'val1', 'val2', 'val1')",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'new table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col3 = 'val2'",
			"CALL DOLT_COMMIT('-am', 'right update');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t (id, col1, col2, col3) VALUES (3, 'val1', 'val1', 'val2');",
			"CALL DOLT_COMMIT('-am', 'main insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(2)}},
			},
			{
				Query: "select id, col1, col2, col3 from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{1, "val1", "val1", "val2"},
					{3, "val1", "val1", "val2"},
				},
			},
		},
	},
	{
		Name: "Multiple unique key violations part 3 (repro issue #5719)",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (id int NOT NULL, col1 varchar(255), col2 varchar(255), col3 varchar(255), PRIMARY KEY (id), UNIQUE KEY uniq_idx (col1,col2,col3));",
			"INSERT INTO t (id, col1, col2, col3) VALUES (1, 'val1', 'val1', 'val1'), (4, 'val1', 'val2', 'val1')",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'new table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col3 = 'val2'",
			"CALL DOLT_COMMIT('-am', 'right update');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t (id, col1, col2, col3) VALUES (3, 'val1', 'val1', 'val2');",
			"CALL DOLT_COMMIT('-am', 'main insert');",
			"CALL DOLT_CHECKOUT('right');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "select * from dolt_constraint_violations;",
				Expected: []sql.Row{{"t", uint64(2)}},
			},
			{
				Query: "select id, col1, col2, col3 from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{1, "val1", "val1", "val2"},
					{3, "val1", "val1", "val2"},
				},
			},
		},
	},
}

var SchemaConflictScripts = []queries.ScriptTest{
	{
		Name: "schema conflicts return an error when autocommit is enabled",
		SetUpScript: []string{
			"set @@autocommit=1;",
			"create table t (pk int primary key, c0 varchar(20))",
			"call dolt_commit('-Am', 'added table t')",
			"call dolt_checkout('-b', 'other')",
			"alter table t modify column c0 int",
			"call dolt_commit('-am', 'altered t on branch other')",
			"call dolt_checkout('main')",
			"alter table t modify column c0 datetime(6)",
			"call dolt_commit('-am', 'altered t on branch main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "call dolt_merge('other')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsAutoCommit.Error(),
			},
			{
				Query:    "select * from dolt_schema_conflicts",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "divergent type change causes schema conflict",
		SetUpScript: []string{
			"set @@autocommit=0;",
			"create table t (pk int primary key, c0 varchar(20))",
			"call dolt_commit('-Am', 'added table t')",
			"call dolt_checkout('-b', 'other')",
			"alter table t modify column c0 int",
			"call dolt_commit('-am', 'altered t on branch other')",
			"call dolt_checkout('main')",
			"alter table t modify column c0 datetime(6)",
			"call dolt_commit('-am', 'altered t on branch main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "call dolt_merge('other')",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query: "select * from dolt_schema_conflicts",
				Expected: []sql.Row{{
					"t",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c0` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c0` datetime(6),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c0` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					"different column definitions for our column c0 and their column c0",
				}},
			},
			{
				Query: "select * from dolt_status",
				Expected: []sql.Row{
					{"t", false, "schema conflict"},
				},
			},
		},
	},
}

// OldFormatMergeConflictsAndCVsScripts tests old format merge behavior
// where violations are appended and merges are aborted if there are existing
// violations and/or conflicts.
var OldFormatMergeConflictsAndCVsScripts = []queries.ScriptTest{
	{
		Name: "merging branches into a constraint violated head. Any new violations are appended",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CREATE table other (pk int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_BRANCH('branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// we need dolt_force_transaction_commit because we want to
				// transaction commit constraint violations that occur as a
				// result of a merge.
				Query:    "set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "DELETE FROM parent where pk = 1;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-am', 'delete parent 1');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch1');",
				Expected: []sql.Row{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "INSERT INTO CHILD VALUES (1, 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-am', 'insert child of parent 1');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}},
			},
			{
				Query:    "COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:          "CALL DOLT_COMMIT('-am', 'commit constraint violations');",
				ExpectedErrStr: "error: the table(s) child have constraint violations",
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'commit constraint violations');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_BRANCH('branch3');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "DELETE FROM parent where pk = 2;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'remove parent 2');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0, "Switched to branch 'branch2'"}},
			},
			{
				Query:    "INSERT INTO OTHER VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-am', 'non-fk insert');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch2');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}},
			},
			{
				Query:    "COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:          "CALL DOLT_COMMIT('-am', 'commit non-conflicting merge');",
				ExpectedErrStr: "error: the table(s) child have constraint violations",
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'commit non-conflicting merge');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch3');",
				Expected: []sql.Row{{0, "Switched to branch 'branch3'"}},
			},
			{
				Query:    "INSERT INTO CHILD VALUES (2, 2);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'add child of parent 2');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch3');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}, {uint16(1), 2, 2}},
			},
		},
	},
	{
		Name: "conflicting merge aborts when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'create table with data');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE parent SET col1 = 2 where pk = 1;",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 2 and remove pk = 2');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE parent SET col1 = 3 where pk = 1;",
			"INSERT into child VALUEs (1, 2);",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 3 and adding child of pk 2');",
			"CALL DOLT_CHECKOUT('other2')",
			"UPDATE parent SET col1 = 4 where pk = 1",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 4');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET dolt_force_transaction_commit = 1",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
			// commit so we can merge again
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'committing merge conflicts');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:          "CALL DOLT_MERGE('other2');",
				ExpectedErrStr: "existing unresolved conflicts would be overridden by new conflicts produced by merge. Please resolve them and try again",
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
		},
	},
	{
		Name: "non-conflicting / non-violating merge succeeds when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'create table with data');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE parent SET col1 = 2 where pk = 1;",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 2 and remove pk = 2');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE parent SET col1 = 3 where pk = 1;",
			"INSERT into child VALUES (1, 2);",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 3 and adding child of pk 2');",
			"CALL DOLT_CHECKOUT('other2')",
			"INSERT INTO parent values (3, 1);",
			"CALL DOLT_COMMIT('-am', 'insert parent with pk 3');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET dolt_force_transaction_commit = 1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
			// commit so we can merge again
			{
				Query:    "CALL DOLT_COMMIT('-afm', 'committing merge conflicts');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "CALL DOLT_MERGE('other2');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}, {3, 1}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
		},
	},
	// Unique key violations
	{
		Name: "unique key violations that already exist in the left abort the merge with an error",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'table');",
			"CALL DOLT_BRANCH('right');",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'data');",

			"CALL DOLT_CHECKOUT('right');",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'unqiue constraint');",

			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "duplicate unique key given: [1]",
			},
			{
				Query:    "SELECT * from t",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `col1` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	// not a base case, but helpful to understand...
	{
		Name: "right adds a unique key constraint and fixes existing violations. On merge, because left still has the violation, merge is aborted.",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.');",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'right adds a unique index');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "duplicate unique key given: [1]",
			},
		},
	},
}

var GeneratedColumnMergeTestScripts = []queries.ScriptTest{
	{
		Name: "merge a generated stored column",
		SetUpScript: []string{
			"create table t1 (id bigint primary key, v1 bigint, v2 bigint, v3 bigint as (v1 + v2) stored, index (v3))",
			"insert into t1 (id, v1, v2) values (1, 1, 1), (2, 2, 2)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_checkout('-b', 'branch1')",
			"insert into t1 (id, v1, v2) values (3, 3, 3)",
			"call dolt_commit('-Am', 'branch1 commit')",
			"call dolt_checkout('main')",
			"call dolt_checkout('-b', 'branch2')",
			"insert into t1 (id, v1, v2) values (4, 4, 4)",
			"call dolt_commit('-Am', 'branch2 commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 1, 1, 2},
					{2, 2, 2, 4},
					{3, 3, 3, 6},
				},
			},
			{
				Query:    "select id from t1 where v3 = 6",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "call dolt_merge('branch2')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 1, 1, 2},
					{2, 2, 2, 4},
					{3, 3, 3, 6},
					{4, 4, 4, 8},
				},
			},
			{
				Query:    "select id from t1 where v3 = 8",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "merge a generated column with non-conflicting changes on both sides",
		SetUpScript: []string{
			"create table t1 (id bigint primary key, v1 bigint, v2 bigint, v3 bigint as (v1 + v2) stored)",
			"insert into t1 (id, v1, v2) values (1, 1, 1), (2, 2, 2)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('branch1')",
			"call dolt_branch('branch2')",
			"call dolt_checkout('branch1')",
			"update t1 set v1 = 4 where id = 1",
			"call dolt_commit('-Am', 'branch1 commit')",
			"call dolt_checkout('branch2')",
			"update t1 set v2 = 5 where id = 1",
			"call dolt_commit('-Am', 'branch2 commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 4, 1, 5},
					{2, 2, 2, 4},
				},
			},
			{
				Query:    "select id from t1 where v3 = 5",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('branch2')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 4, 5, 9},
					{2, 2, 2, 4},
				},
			},
			{
				Query:    "select id from t1 where v3 = 9",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "merge a generated column created on another branch",
		SetUpScript: []string{
			"create table t1 (id bigint primary key, v1 bigint, v2 bigint)",
			"insert into t1 (id, v1, v2) values (1, 1, 1), (2, 2, 2)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('branch1')",
			"insert into t1 (id, v1, v2) values (3, 3, 3)",
			"call dolt_commit('-Am', 'main commit')",
			"call dolt_checkout('branch1')",
			"alter table t1 add column v3 bigint as (v1 + v2) stored",
			"alter table t1 add key idx_v3 (v3)",
			"insert into t1 (id, v1, v2) values (4, 4, 4)",
			"call dolt_commit('-Am', 'branch1 commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 1, 1, 2},
					{2, 2, 2, 4},
					{3, 3, 3, 6},
					{4, 4, 4, 8},
				},
			},
			{
				Query:    "select id from t1 where v3 = 6",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select id from t1 where v3 = 8",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "merge a virtual column",
		SetUpScript: []string{
			"create table t1 (id bigint primary key, v1 bigint, v2 bigint, v3 bigint as (v1 + v2), index (v3))",
			"insert into t1 (id, v1, v2) values (1, 2, 3), (4, 5, 6)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_checkout('-b', 'branch1')",
			"insert into t1 (id, v1, v2) values (7, 8, 9)",
			"call dolt_commit('-Am', 'branch1 commit')",
			"call dolt_checkout('main')",
			"call dolt_checkout('-b', 'branch2')",
			"insert into t1 (id, v1, v2) values (10, 11, 12)",
			"call dolt_commit('-Am', 'branch2 commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 2, 3, 5},
					{4, 5, 6, 11},
					{7, 8, 9, 17},
				},
			},
			{
				Query:    "select id from t1 where v3 = 17",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "call dolt_merge('branch2')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 2, 3, 5},
					{4, 5, 6, 11},
					{7, 8, 9, 17},
					{10, 11, 12, 23},
				},
			},
			{
				Query:    "select id from t1 where v3 = 23",
				Expected: []sql.Row{{10}},
			},
		},
	},
	{
		Name: "merge a virtual column created on another branch",
		SetUpScript: []string{
			"create table t1 (id bigint primary key, v1 bigint, v2 bigint)",
			"insert into t1 (id, v1, v2) values (1, 2, 3), (4, 5, 6)",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('branch1')",
			"insert into t1 (id, v1, v2) values (7, 8, 9)",
			"call dolt_commit('-Am', 'main commit')",
			"call dolt_checkout('branch1')",
			"alter table t1 add column v3 bigint as (v1 + v2)",
			"alter table t1 add key idx_v3 (v3)",
			"insert into t1 (id, v1, v2) values (10, 11, 12)",
			"call dolt_commit('-Am', 'branch1 commit')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('branch1')",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "select * from t1 order by id",
				Expected: []sql.Row{
					{1, 2, 3, 5},
					{4, 5, 6, 11},
					{7, 8, 9, 17},
					{10, 11, 12, 23},
				},
			},
			{
				Query:    "select id from t1 where v3 = 17",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "select id from t1 where v3 = 23",
				Expected: []sql.Row{{10}},
			},
		},
	},
}

var PreviewMergeConflictsFunctionScripts = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_branch('branch2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			// dolt_preview_merge_conflicts_summary
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1', 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary(null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary('main', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary(123, 'branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts_summary('fake-branch', 'main');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts_summary('main', 'fake-branch');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts_summary('main...branch1', 'branch2');",
				ExpectedErrStr: "string is not a valid branch or hash",
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary('main', concat('branch', '1'));",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts_summary(hashof('main'), 'branch1');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			// dolt_preview_merge_conflicts
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', 'branch1');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't', 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', 123, 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts(123, 'branch1', 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('fake-branch', 'main', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('main', 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('main...branch1', 'branch2', 't');",
				ExpectedErrStr: "string is not a valid branch or hash",
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', concat('branch', '1'), 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts(hashof('main'), 'branch1', 't');",
				ExpectedErr: dtablefunctions.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 'nope');",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "basic case with single table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1='one!' where pk=1",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'update row 1 on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1='one?' where pk=1",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'update row 1 on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_pk, base_c1, base_c2, our_pk, our_c1, our_c2, our_diff_type, their_pk, their_c1, their_c2, their_diff_type from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{{1, "one", "two", 1, "one?", "two", "modified", 1, "one!", "two", "modified"}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_pk, base_c1, base_c2, our_pk, our_c1, our_c2, our_diff_type, their_pk, their_c1, their_c2, their_diff_type from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{{1, "one", "two", 1, "one?", "two", "modified", 1, "one!", "two", "modified"}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary(@Commit1, @Commit2)", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts(@Commit1, @Commit2, 't')", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch2', 'main')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_pk, base_c1, base_c2, our_pk, our_c1, our_c2, our_diff_type, their_pk, their_c1, their_c2, their_diff_type from dolt_preview_merge_conflicts('branch2', 'main', 't')",
				Expected: []sql.Row{{1, "one", "two", 1, "one!", "two", "modified", 1, "one?", "two", "modified"}},
			},
		},
	},
	{
		Name: "basic case with keyless table",
		SetUpScript: []string{
			"create table t (pk int, c1 varchar(20), c2 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1='one!' where pk=1",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'update row 1 on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1='one?' where pk=1",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'update row 1 on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary(@Commit1, @Commit2)", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts(@Commit1, @Commit2, 't')", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch2', 'main')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch2', 'main', 't')",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "basic case with multiple tables, data conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"create table t2 (pk int primary key, c1 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"insert into t2 values(100, 'hundred');",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1='one!' where pk=1",
			"update t2 set c1='hundred!' where pk=100",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'update row 1 on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1='one?' where pk=1",
			"update t2 set c1='hundred?' where pk=100",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'update row 1 on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",

			"create table keyless (id int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('main', 'branch2', 't2')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't2')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary(@Commit1, @Commit2)", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts(@Commit1, @Commit2, 't')", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch2', 'main')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch2', 'main', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch2', 'main', 't2')",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "basic case with multiple tables, schema conflict",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"create table t2 (pk int primary key, c1 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"insert into t2 values(100, 'hundred');",
			"call dolt_add('.')",
			"set @Commit1 = '';",
			"call dolt_commit_hash_out(@Commit1, '-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1='one!' where pk=1",
			"alter table t2 alter column c1 set default 'default';",
			"set @Commit2 = '';",
			"call dolt_commit_hash_out(@Commit2, '-am', 'update row 1 on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1='one?' where pk=1",
			"alter table t2 alter column c1 set default 'default2';",
			"set @Commit3 = '';",
			"call dolt_commit_hash_out(@Commit3, '-am', 'update row 1 on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",

			"create table keyless (id int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", nil, uint64(1)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "SELECT count(*) from dolt_preview_merge_conflicts('main', 'branch2', 't2')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", nil, uint64(1)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't2')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary(@Commit1, @Commit2)", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts(@Commit1, @Commit2, 't')", // not branches
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch2', 'main')",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}, {"t2", nil, uint64(1)}},
			},
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch2', 'main', 't')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "SELECT count(*) from dolt_preview_merge_conflicts('branch2', 'main', 't2')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
		},
	},
	{
		Name: "schema-only conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20) default 'orig', c2 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"alter table t alter column c1 set default 'default1';",
			"call dolt_commit('-am', 'change default on branch2');",

			"call dolt_checkout('branch1')",
			"alter table t alter column c1 set default 'default2';",
			"call dolt_commit('-am', 'change default on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
		},
	},
	{
		Name: "mixed schema and data conflicts in same table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20) default 'orig', c2 varchar(20));",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1='one!' where pk=1",
			"alter table t alter column c1 set default 'default1';",
			"call dolt_commit('-am', 'data and schema changes on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1='one?' where pk=1",
			"alter table t alter column c1 set default 'default2';",
			"call dolt_commit('-am', 'data and schema changes on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:          "SELECT * from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				ExpectedErrStr: "schema conflicts found: 1",
			},
		},
	},
	{
		Name: "column type conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20));",
			"insert into t values (1, 'one');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial commit');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"alter table t modify column c1 varchar(50);",
			"call dolt_commit('-am', 'change column to varchar(50) on branch2');",

			"call dolt_checkout('branch1')",
			"alter table t modify column c1 text;",
			"call dolt_commit('-am', 'change column to text on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(1)}},
			},
		},
	},
	{
		Name: "foreign key constraint conflicts",
		SetUpScript: []string{
			"create table parent (pk int primary key, name varchar(20));",
			"create table child (pk int primary key, parent_pk int, data varchar(20));",
			"insert into parent values (1, 'parent1'), (2, 'parent2');",
			"insert into child values (1, 1, 'child1'), (2, 2, 'child2');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial tables');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"alter table child add constraint fk1 foreign key (parent_pk) references parent(pk) on delete cascade;",
			"call dolt_commit('-am', 'add fk with cascade on branch2');",

			"call dolt_checkout('branch1')",
			"alter table child add constraint fk1 foreign key (parent_pk) references parent(pk) on delete restrict;",
			"call dolt_commit('-am', 'add fk with restrict on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "check constraint conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, score int);",
			"insert into t values (1, 85), (2, 92);",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial table');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"alter table t add constraint chk1 check (score >= 0);",
			"call dolt_commit('-am', 'add check >= 0 on branch2');",

			"call dolt_checkout('branch1')",
			"alter table t add constraint chk1 check (score > 0);",
			"call dolt_commit('-am', 'add check > 0 on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(2)}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(2)}},
			},
		},
	},
	{
		Name: "index conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, name varchar(20), email varchar(50));",
			"insert into t values (1, 'alice', 'alice@email.com'), (2, 'bob', 'bob@email.com');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial table');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"create index idx_name on t(name);",
			"call dolt_commit('-am', 'add name index on branch2');",

			"call dolt_checkout('branch1')",
			"create unique index idx_name on t(name);",
			"call dolt_commit('-am', 'add unique name index on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(2)}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", nil, uint64(2)}},
			},
		},
	},
	{
		Name: "many data conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20), c3 varchar(20));",
			"insert into t values (1, 'a1', 'b1', 'c1'), (2, 'a2', 'b2', 'c2'), (3, 'a3', 'b3', 'c3'), (4, 'a4', 'b4', 'c4'), (5, 'a5', 'b5', 'c5');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial data');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"update t set c1=concat(c1, '_branch2') where pk in (1,2,3);",
			"update t set c2=concat(c2, '_branch2') where pk in (2,4);",
			"call dolt_commit('-am', 'modify multiple rows on branch2');",

			"call dolt_checkout('branch1')",
			"update t set c1=concat(c1, '_branch1') where pk in (1,2,3);",
			"update t set c2=concat(c2, '_branch1') where pk in (2,4);",
			"update t set c3=concat(c3, '_branch1') where pk = 5;",
			"call dolt_commit('-am', 'modify multiple rows on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{{"t", uint64(4), uint64(0)}}, // 4 rows have conflicts (1,2,3,4)
			},
			{
				Query:    "SELECT COUNT(*) from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{{"t", uint64(4), uint64(0)}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "additional conflicts testing with multiple columns",
		SetUpScript: []string{
			"create table test_table (pk int primary key, col1 varchar(20), col2 int);",
			"insert into test_table values (1, 'original', 100), (2, 'second', 200);",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial commit');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",

			"update test_table set col1 = 'branch2_val', col2 = 300 where pk = 1;",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'modify on branch2');",

			"call dolt_checkout('branch1')",
			"update test_table set col1 = 'branch1_val', col2 = 400 where pk = 1;",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'modify on branch1');",

			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT count(*) from dolt_preview_merge_conflicts('branch1', 'branch2', 'test_table')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT base_pk, our_col1, their_col1 from dolt_preview_merge_conflicts('branch1', 'branch2', 'test_table')",
				Expected: []sql.Row{{1, "branch1_val", "branch2_val"}},
			},
		},
	},
	{
		Name: "empty result when no conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, value varchar(20));",
			"insert into t values (1, 'original');",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'initial setup');",

			"call dolt_branch('branch1')",
			"call dolt_checkout('-b', 'branch2')",
			"insert into t values (2, 'branch2_new');",
			"call dolt_commit('-am', 'add row on branch2');",

			"call dolt_checkout('branch1')",
			"insert into t values (3, 'branch1_new');",
			"call dolt_commit('-am', 'add row on branch1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch1')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('main', 'branch2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts_summary('branch1', 'branch2')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch1', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('main', 'branch2', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_preview_merge_conflicts('branch1', 'branch2', 't')",
				Expected: []sql.Row{},
			},
		},
	},
}

// convertMergeScriptTest converts a MergeScriptTest into a standard ScriptTest. If flipSides is true, then the
// left and right setup is swapped (i.e. left setup is done on right branch and right setup is done on main branch).
// This enables us to test merges in both directions, since the merge code is asymmetric and some code paths currently
// only run on the left side of the merge.
func convertMergeScriptTest(mst MergeScriptTest, flipSides bool) queries.ScriptTest {
	// Ancestor setup
	setupScript := mst.AncSetUpScript
	setupScript = append(setupScript, "CALL DOLT_COMMIT('-Am', 'ancestor commit');")
	setupScript = append(setupScript, "CALL DOLT_BRANCH('right');")

	// Right-side setup
	if flipSides {
		setupScript = append(setupScript, "CALL DOLT_CHECKOUT('main');")
	} else {
		setupScript = append(setupScript, "CALL DOLT_CHECKOUT('right');")
	}
	setupScript = append(setupScript, mst.RightSetUpScript...)
	setupScript = append(setupScript, "CALL DOLT_COMMIT('-Am', 'right commit');")

	// Left-side setup
	if flipSides {
		setupScript = append(setupScript, "CALL DOLT_CHECKOUT('right');")
	} else {
		setupScript = append(setupScript, "CALL DOLT_CHECKOUT('main');")
	}
	setupScript = append(setupScript, mst.LeftSetUpScript...)
	setupScript = append(setupScript, "CALL DOLT_COMMIT('-Am', 'left commit');")

	// Always run the tests with the main branch checked out
	if flipSides {
		setupScript = append(setupScript, "CALL DOLT_CHECKOUT('main');")
	}

	// Any assertions referencing our_ or their_ need to be flipped
	assertions := make([]queries.ScriptTestAssertion, len(mst.Assertions))
	copy(assertions, mst.Assertions)
	if flipSides {
		for i, assertion := range assertions {
			assertions[i].Query = flipStatement(assertion.Query)
		}
	}

	return queries.ScriptTest{
		Name:         mst.Name,
		SetUpScript:  setupScript,
		Assertions:   assertions,
		Query:        mst.Query,
		Expected:     mst.Expected,
		ExpectedErr:  mst.ExpectedErr,
		SkipPrepared: mst.SkipPrepared,
	}
}

// flipStatement replaces "our_" with "their_" and vice versa in the given query |s| so that the
// query can be re-used to test a merge in the opposite direction.
func flipStatement(s string) string {
	newS := strings.ReplaceAll(s, "our_", "temp_")
	newS = strings.ReplaceAll(newS, "their_", "our_")
	newS = strings.ReplaceAll(newS, "temp_", "their_")
	return newS
}
