// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var RevertScripts = []queries.ScriptTest{
	{
		SkipPrepared: true, // https://github.com/dolthub/dolt/issues/6300
		Name:         "dolt_revert() reverts HEAD",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'answer of the universe: 42');",
			"call dolt_revert('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from test as of 'HEAD' where pk = 2;",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~1' where pk = 2;",
				Expected: []sql.Row{{2, 42}},
			},
		},
	},
	{
		SkipPrepared: true, // https://github.com/dolthub/dolt/issues/6300
		Name:         "dolt_revert() reverts HEAD~1",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'answer of the universe');",
			"update test set c0 = 23 where pk = 3;",
			"call dolt_commit('-am', 'answer of the universe');",
			"call dolt_revert('HEAD~1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from test as of 'HEAD' where pk = 2;",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~2' where pk = 2;",
				Expected: []sql.Row{{2, 42}},
			},
			{
				Query:    "select * from test as of 'HEAD' where pk = 3;",
				Expected: []sql.Row{{3, 23}},
			},
		},
	},
	{
		Name: "dolt_revert() multi-commit: one commit per revert",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"insert into test values (4, 4);",
			"call dolt_commit('-am', 'insert row 4');",
			"insert into test values (5, 5);",
			"call dolt_commit('-am', 'insert row 5');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{5}},
			},
			{
				// Reverting two commits should produce two separate commits.
				Query:    "call dolt_revert('HEAD', 'HEAD~1');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "select message from dolt_log limit 2;",
				Expected: []sql.Row{{`Revert "insert row 5"`}, {`Revert "insert row 4"`}},
			},
			{
				// Both rows should be gone.
				Query:    "select * from test order by pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "dolt_revert() detects conflicts and returns conflict counts",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'first change');",
			"update test set c0 = 23 where pk = 2;",
			"call dolt_commit('-am', 'second change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Reverting HEAD~1 creates a conflict: both ours (HEAD) and theirs
				// (HEAD~2, parent of the reverted commit) modified pk=2.
				Query:    "call dolt_revert('HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
			{
				// Working set is left in conflicted state; no new commit was created.
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{"second change"}},
			},
		},
	},
	{
		// If a revert is aborted during a conflict, the set should be reset back to
		// before the revert, i.e. any commits created by revert should NOT be present.
		Name: "dolt_revert() --abort restores pre-revert state",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int);",
			"insert into test values (1,1),(2,2);",
			"call dolt_commit('-Am', 'seed table');",
			"insert into test values (3,3);",
			"call dolt_commit('-am', 'insert 3,3');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'first change');",
			"update test set c0 = 23 where pk = 2;",
			"call dolt_commit('-am', 'second change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_revert('HEAD~2', 'HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
			{
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{"Revert \"insert 3,3\""}},
			},
			{
				Query:    "call dolt_revert('--abort');",
				Expected: []sql.Row{{"", 0, 0, 0}},
			},
			{
				// Conflicts cleared.
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				// Data restored to pre-revert state (second change still present).
				Query:    "select c0 from test where pk = 2;",
				Expected: []sql.Row{{23}},
			},
			{
				// When a revert of multiple commits is aborted, NONE of the
				// successfully reverted commits should be reachable.
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{"second change"}},
			},
		},
	},
	{
		Name: "dolt_revert() --continue: successful conflict resolution workflow",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'first change');",
			"update test set c0 = 23 where pk = 2;",
			"call dolt_commit('-am', 'second change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_revert('HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
			{
				// --continue with conflicts still present returns counts, no commit.
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// Resolve conflict by accepting ours and staging.
				Query:            "delete from dolt_conflicts_test;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update test set c0 = 23 where pk = 2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('test');",
				SkipResultsCheck: true,
			},
			{
				// Now --continue should succeed and create the revert commit.
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				// Commit message should reflect the reverted commit.
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{`Revert "first change"`}},
			},
			{
				// Revert commit should have only one parent (not a merge commit).
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
				Expected: []sql.Row{{1}},
			},
			{
				// Conflicts cleared.
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_revert() --continue: ignored table in working set",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into dolt_ignore values ('ignored_*', 1);",
			"create table ignored_1 (id int primary key);",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'first change');",
			"update test set c0 = 23 where pk = 2;",
			"call dolt_commit('-am', 'second change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_revert('HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// Resolve conflict and stage.
				Query:            "delete from dolt_conflicts_test;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update test set c0 = 23 where pk = 2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('test');",
				SkipResultsCheck: true,
			},
			{
				// Create another ignored table in the working set
				Query:            "create table ignored_2 (id int primary key);",
				SkipResultsCheck: true,
			},
			{
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{`Revert "first change"`}},
			},
		},
	},
	{
		Name: "dolt_revert() --continue: multiple table conflicts",
		SetUpScript: []string{
			"create table t1 (pk int primary key, v varchar(100));",
			"create table t2 (pk int primary key, v varchar(100));",
			"insert into t1 values (1, 'original_t1');",
			"insert into t2 values (1, 'original_t2');",
			"call dolt_commit('-Am', 'seed tables');",
			// Commit to revert: change both tables.
			"update t1 set v = 'changed_t1' where pk = 1;",
			"update t2 set v = 'changed_t2' where pk = 1;",
			"call dolt_commit('-am', 'change both tables');",
			// Subsequent commit that modifies the same rows, creating conflicts when reverting above.
			"update t1 set v = 'newer_t1' where pk = 1;",
			"update t2 set v = 'newer_t2' where pk = 1;",
			"call dolt_commit('-am', 'newer changes');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_revert('HEAD~1');",
				Expected: []sql.Row{{"", 2, 0, 0}},
			},
			{
				Query:    "select `table` from dolt_conflicts order by `table`;",
				Expected: []sql.Row{{"t1"}, {"t2"}},
			},
			{
				// Resolve t1, but not t2 yet.
				Query:            "delete from dolt_conflicts_t1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update t1 set v = 'resolved_t1' where pk = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t1');",
				SkipResultsCheck: true,
			},
			{
				// t2 conflict still present.
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// Resolve t2.
				Query:            "delete from dolt_conflicts_t2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update t2 set v = 'resolved_t2' where pk = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t2');",
				SkipResultsCheck: true,
			},
			{
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select * from t1;",
				Expected: []sql.Row{{1, "resolved_t1"}},
			},
			{
				Query:    "select * from t2;",
				Expected: []sql.Row{{1, "resolved_t2"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_revert() --continue not in a revert state",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1);",
			"call dolt_commit('-Am', 'seed table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_revert('--continue');",
				ExpectedErrStr: "error: There is no revert in progress",
			},
		},
	},
	{
		Name: "dolt_revert() --abort not in a revert state",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1);",
			"call dolt_commit('-Am', 'seed table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_revert('--abort');",
				ExpectedErrStr: "error: There is no revert in progress",
			},
		},
	},
	{
		Name: "dolt_revert() --continue and --abort are mutually exclusive",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'first change');",
			"update test set c0 = 23 where pk = 2;",
			"call dolt_commit('-am', 'second change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_revert('HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:          "call dolt_revert('--continue', '--abort');",
				ExpectedErrStr: "error: --continue and --abort are mutually exclusive",
			},
			{
				Query:          "call dolt_revert('--abort', '--continue');",
				ExpectedErrStr: "error: --continue and --abort are mutually exclusive",
			},
		},
	},
	{
		Name: "dolt_revert() multi-commit: stops at first conflict",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int);",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			// commit A - clean revert target
			"insert into test values (4, 4);",
			"call dolt_commit('-am', 'insert row 4');",
			// commit B - will conflict when reverted
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'change row 2');",
			// commit C - modifies same row as B's revert, causing conflict
			"update test set c0 = 99 where pk = 2;",
			"call dolt_commit('-am', 'change row 2 again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Reverting HEAD~2 (insert row 4) first, then HEAD~1 (change row 2).
				// HEAD~2 reverts cleanly (commit created). HEAD~1 conflicts.
				Query:    "call dolt_revert('HEAD~2', 'HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// The first revert (insert row 4) committed cleanly.
				Query:    "select pk from test order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				// The second revert (change row 2) left conflicts.
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
		},
	},
	{
		Name: "dolt_revert() fails with untracked tables",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3)",
			"call dolt_commit('-Am', 'seed table')",
			"update test set c0 = 42 where pk = 2",
			"call dolt_commit('-am', 'answer of the universe: 42')",
			"create table dont_track (pk int primary key)",
			"insert into dont_track values (1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_revert('HEAD')",
				ExpectedErrStr: "error: Your local changes would be overwritten by revert.\nhint: Please commit your changes before you revert.",
			},
		},
	},
	{
		SkipPrepared: true, // https://github.com/dolthub/dolt/issues/6300
		Name:         "dolt_revert() respects dolt_ignore",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3)",
			"insert into dolt_ignore values ('dont_*', 1)",
			"call dolt_commit('-Am', 'seed table')",
			"update test set c0 = 42 where pk = 2",
			"call dolt_commit('-am', 'answer of the universe: 42')",
			"create table dont_track (id int primary key)",
			"insert into dont_track values (1)",
			"call dolt_revert('HEAD')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from test as of 'HEAD' where pk = 2;",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~1' where pk = 2;",
				Expected: []sql.Row{{2, 42}},
			},
			{
				Query:          "select * from dont_track as of 'HEAD'",
				ExpectedErrStr: "table not found: dont_track",
			},
			{
				Query:    "select * from dolt_status_ignored",
				Expected: []sql.Row{{"dont_track", byte(0), "new table", true}},
			},
		},
	},
	{
		Name: "dolt_revert() detects constraint violations and returns violation counts",
		SetUpScript: []string{
			"create table test2 (pk int primary key, c0 int)",
			"insert into test2 values (1,1),(2,NULL),(3,3);",
			"call dolt_commit('-Am', 'new table with NULL value');",
			"delete from test2 where pk = 2;",
			"call dolt_commit('-am', 'deleted row with NULL value');",
			"alter table test2 modify c0 int not null",
			"call dolt_commit('-am', 'modified column c0 to not null');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_revert('head~1');",
				Expected: []sql.Row{{"", 0, 0, 1}},
			},
			{
				Query:    "select `table` from dolt_constraint_violations;",
				Expected: []sql.Row{{"test2"}},
			},
			{
				Query:    "select violation_type, pk, c0 from dolt_constraint_violations_test2;",
				Expected: []sql.Row{{"not null", 2, nil}},
			},
		},
	},
	{
		Name: "dolt_revert() commit has current timestamp",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int)",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			"update test set c0 = 42 where pk = 2;",
			"call dolt_commit('-am', 'answer of the universe: 42');",
			"call dolt_revert('HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// The revert commit must use the current system time, not the zero time (0001-01-01).
				Query:    "select year(date) > 2000 from dolt_log limit 1;",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		// Revert three commits where the first reverts cleanly and the last two each
		// produce conflicts. --continue must be called twice: once after resolving the
		// second commit's conflict and again after resolving the third commit's conflict.
		Name: "dolt_revert() --continue called multiple times for multi-commit series",
		SetUpScript: []string{
			"create table test (pk int primary key, c0 int);",
			"insert into test values (1,1),(2,2),(3,3);",
			"call dolt_commit('-Am', 'seed table');",
			// Commit A: change pk=1. Will revert cleanly.
			"update test set c0 = 10 where pk = 1;",
			"call dolt_commit('-am', 'change pk1');",
			// Commit B: change pk=2. Will conflict when reverted because of the clobber commit.
			"update test set c0 = 20 where pk = 2;",
			"call dolt_commit('-am', 'change pk2');",
			// Commit C: change pk=3. Will conflict when reverted because of the clobber commit.
			"update test set c0 = 30 where pk = 3;",
			"call dolt_commit('-am', 'change pk3');",
			// Clobber commit: overwrite pk=2 and pk=3 so reverting B and C will conflict.
			"update test set c0 = 21 where pk = 2;",
			"update test set c0 = 31 where pk = 3;",
			"call dolt_commit('-am', 'clobber pk2 and pk3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				// Revert A (HEAD~3), B (HEAD~2), C (HEAD~1).
				// A reverts cleanly (commit created). B conflicts → stop.
				Query:    "call dolt_revert('HEAD~3', 'HEAD~2', 'HEAD~1');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// A's revert committed: pk=1 restored to 1.
				Query:    "select c0 from test where pk = 1;",
				Expected: []sql.Row{{1}},
			},
			{
				// B's revert left a conflict on the test table.
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
			{
				// Resolve B's conflict: keep clobber value for pk=2.
				Query:            "delete from dolt_conflicts_test;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update test set c0 = 21 where pk = 2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('test');",
				SkipResultsCheck: true,
			},
			{
				// First --continue: commits B's revert, then automatically tries C.
				// C conflicts → stop and return conflict counts.
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				// B's revert is now committed.
				Query:    "select message from dolt_log limit 2;",
				Expected: []sql.Row{{`Revert "change pk2"`}, {`Revert "change pk1"`}},
			},
			{
				// C's revert left a conflict on the test table.
				Query:    "select `table` from dolt_conflicts;",
				Expected: []sql.Row{{"test"}},
			},
			{
				// Resolve C's conflict: keep clobber value for pk=3.
				Query:            "delete from dolt_conflicts_test;",
				SkipResultsCheck: true,
			},
			{
				Query:            "update test set c0 = 31 where pk = 3;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('test');",
				SkipResultsCheck: true,
			},
			{
				// Second --continue: commits C's revert, series complete.
				Query:    "call dolt_revert('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				// All three revert commits present in log (most recent first).
				Query:    "select message from dolt_log limit 3;",
				Expected: []sql.Row{{`Revert "change pk3"`}, {`Revert "change pk2"`}, {`Revert "change pk1"`}},
			},
			{
				// No conflicts remaining.
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				// Final data: pk=1 restored, pk=2 and pk=3 kept at resolved clobber values.
				Query:    "select pk, c0 from test order by pk;",
				Expected: []sql.Row{{1, 1}, {2, 21}, {3, 31}},
			},
		},
	},
	{
		Name: "dolt_revert() automatically resolves some conflicts",
		SetUpScript: []string{
			"create table tableA (id int primary key, col1 varchar(255));",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'new table');",
			"insert into tableA values (1, 'test')",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'new row');",
			"call dolt_branch('new_row');",
			"ALTER TABLE tableA MODIFY col1 TEXT",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'change col');",
			"call dolt_revert('new_row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from tableA",
				Expected: []sql.Row{},
			},
		},
	},
}
