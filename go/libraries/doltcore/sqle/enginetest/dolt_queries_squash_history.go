// Copyright 2026 Dolthub, Inc.
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

var DoltSquashHistoryScriptTests = []queries.ScriptTest{
	{
		Name: "dolt_squash_history: default collapses history to init <- HEAD",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'c3');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"set @user = (select committer from dolt_log where message = 'c1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "call dolt_squash_history('--message', 'squashed');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"squashed"}, {"Initialize data repository"}},
			},
			{
				// The initial commit must be preserved unchanged as the merge base.
				Query:    "select count(*) from dolt_log where commit_hash = @init;",
				Expected: []sql.Row{{1}},
			},
			{
				// Author is the current session user, matching earlier commits.
				Query:    "select count(*) from dolt_log where message = 'squashed' and committer = @user;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select count(*) from dolt_status;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "dolt_squash_history: explicit --first collapses a suffix",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'c3');",
			"insert into t values (4);",
			"call dolt_commit('-am', 'c4');",
			"set @c3 = (select commit_hash from dolt_log where message = 'c3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_squash_history('--message', 'sq', '--first', @c3);",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// c3 and c4 collapse onto c2; init, c1, c2, sq remain.
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"sq"}, {"c2"}, {"c1"}, {"Initialize data repository"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
		},
	},
	{
		Name: "dolt_squash_history: --first accepts a relative ref (HEAD~N)",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'c3');",
			"insert into t values (4);",
			"call dolt_commit('-am', 'c4');",
			"insert into t values (5);",
			"call dolt_commit('-am', 'c5');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// HEAD~4 resolves to c1, collapsing c1..c5 onto the initial commit.
				Query:    "call dolt_squash_history('--message', 'sq', '--first', 'HEAD~4');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"sq"}, {"Initialize data repository"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}, {5}},
			},
		},
	},
	{
		Name: "dolt_squash_history: --first accepts a branch name",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'c3');",
			"set @c1 = (select commit_hash from dolt_log where message = 'c1');",
			"call dolt_branch('base', @c1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// The 'base' branch points at c1, so it collapses c1..c3 onto the initial commit.
				Query:    "call dolt_squash_history('--message', 'sq', '--first', 'base');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"sq"}, {"Initialize data repository"}},
			},
			{
				// The other branch is untouched and still resolves to its commit.
				Query:    "select count(*) from dolt_log as of 'base';",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "dolt_squash_history: --first accepts a tag",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'c3');",
			"set @c1 = (select commit_hash from dolt_log where message = 'c1');",
			"call dolt_tag('v1', @c1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// The 'v1' tag points at c1, so it collapses c1..c3 onto the initial commit.
				Query:    "call dolt_squash_history('--message', 'sq', '--first', 'v1');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"sq"}, {"Initialize data repository"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "dolt_squash_history: --first that is a merge commit keeps both parents",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'A');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"call dolt_branch('b', @init);",
			"call dolt_checkout('b');",
			"create table t (pk int primary key);",
			"insert into t values (2);",
			"call dolt_commit('-Am', 'C');",
			"call dolt_checkout('main');",
			"call dolt_merge('b', '--no-commit');",
			"call dolt_commit('-Am', 'M');",
			"set @merge = dolt_hashof('HEAD');",
			// Commits above the merge, so squashing from the merge actually collapses a range.
			"insert into t values (3);",
			"call dolt_commit('-am', 'N');",
			"insert into t values (4);",
			"call dolt_commit('-am', 'O');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_squash_history('--message', 'collapsed', '--first', @merge);",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// The collapsed commit keeps exactly the merge commit's two parents.
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
				Expected: []sql.Row{{2}},
			},
			{
				// The merge and everything above it (M, N, O) collapse into a single commit
				// on top of A and C, leaving 4 reachable commits: collapsed, A, C, init.
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select message from dolt_log limit 1;",
				Expected: []sql.Row{{"collapsed"}},
			},
			{
				Query:    "select count(*) from dolt_log where message in ('M', 'N', 'O');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select count(*) from dolt_log where message in ('A', 'C');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
		},
	},
	{
		Name: "dolt_squash_history: merge inside the collapsed range succeeds",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'A');",
			"set @a = dolt_hashof('HEAD');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"call dolt_branch('b', @init);",
			"call dolt_checkout('b');",
			"create table t (pk int primary key);",
			"insert into t values (2);",
			"call dolt_commit('-Am', 'C');",
			"call dolt_checkout('main');",
			"call dolt_merge('b', '--no-commit');",
			"call dolt_commit('-Am', 'M');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'N');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// --first = A reparents onto init; the merge commit M is inside the collapsed range.
				Query:    "call dolt_squash_history('--message', 'flattened', '--first', @a);",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"flattened"}, {"Initialize data repository"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "dolt_squash_history: error when --message is omitted",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'c1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history();",
				ExpectedErrStr: "must provide a commit message with --message",
			},
		},
	},
	{
		Name: "dolt_squash_history: error when --first is the initial commit",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'c1');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history('--message', 'x', '--first', @init);",
				ExpectedErrStr: "--first cannot be the initial commit",
			},
		},
	},
	{
		Name: "dolt_squash_history: error with uncommitted changes",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (2);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "cannot squash history with uncommitted changes",
			},
		},
	},
	{
		Name: "dolt_squash_history: aborts when a merge is in progress",
		SetUpScript: []string{
			"create table t (pk int primary key, v int);",
			"insert into t values (1, 1);",
			"call dolt_commit('-Am', 'c1');",
			"call dolt_branch('other');",
			"update t set v = 2 where pk = 1;",
			"call dolt_commit('-am', 'main change');",
			"call dolt_checkout('other');",
			"update t set v = 3 where pk = 1;",
			"call dolt_commit('-am', 'other change');",
			"call dolt_checkout('main');",
			"set @@autocommit = 0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('other');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "cannot squash history while a merge is in progress; abort the merge first",
			},
		},
	},
	{
		// Cherry-pick and revert use the merge state, so MergeActive covers them too.
		Name: "dolt_squash_history: aborts when a cherry-pick is in progress",
		SetUpScript: []string{
			"set @@autocommit = 0;",
			"create table t (pk int primary key, v int);",
			"insert into t values (1, 1);",
			"call dolt_commit('-Am', 'c1');",
			"call dolt_checkout('-b', 'feature');",
			"update t set v = 2 where pk = 1;",
			"call dolt_commit('-am', 'feature change');",
			"call dolt_checkout('main');",
			"update t set v = 3 where pk = 1;",
			"call dolt_commit('-am', 'main change');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call dolt_cherry_pick(dolt_hashof('feature'));",
				SkipResultsCheck: true,
			},
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "cannot squash history while a merge is in progress; abort the merge first",
			},
		},
	},
	{
		Name: "dolt_squash_history: aborts when a revert is in progress",
		SetUpScript: []string{
			"set @@autocommit = 0;",
			"create table t (pk int primary key, v int);",
			"insert into t values (1, 1);",
			"call dolt_commit('-Am', 'c1');",
			"update t set v = 2 where pk = 1;",
			"call dolt_commit('-am', 'c2');",
			"set @c2 = dolt_hashof('HEAD');",
			"update t set v = 99 where pk = 1;",
			"call dolt_commit('-am', 'c3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call dolt_revert(@c2);",
				SkipResultsCheck: true,
			},
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "cannot squash history while a merge is in progress; abort the merge first",
			},
		},
	},
	{
		// A rebase that has started but not continued leaves a CLEAN working set, so the
		// clean-working-set check cannot catch it -- RebaseActive is required here.
		Name: "dolt_squash_history: aborts when a rebase is in progress",
		SetUpScript: []string{
			"create table t (pk int primary key, v int);",
			"insert into t values (1, 1);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (2, 2);",
			"call dolt_commit('-am', 'c2');",
			"insert into t values (3, 3);",
			"call dolt_commit('-am', 'c3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call dolt_rebase('-i', 'HEAD~2');",
				SkipResultsCheck: true,
			},
			{
				// The working set is clean at this point, so this abort comes from RebaseActive.
				Query:    "select count(*) from dolt_status;",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "cannot squash history while a rebase is in progress; abort the rebase first",
			},
		},
	},
	{
		Name: "dolt_squash_history: error when --first is not an ancestor of HEAD",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'c1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'c2');",
			"call dolt_checkout('-b', 'side');",
			"insert into t values (50);",
			"call dolt_commit('-am', 'sidecommit');",
			"set @side = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'main3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history('--message', 'x', '--first', @side);",
				ExpectedErrStr: "--first must be an ancestor of HEAD",
			},
		},
	},
	{
		Name: "dolt_squash_history: error when the initial commit has multiple children and no --first",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'A');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"call dolt_branch('b', @init);",
			"call dolt_checkout('b');",
			"create table t (pk int primary key);",
			"insert into t values (2);",
			"call dolt_commit('-Am', 'C');",
			"call dolt_checkout('main');",
			"call dolt_merge('b', '--no-commit');",
			"call dolt_commit('-Am', 'M');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "the initial commit has multiple children; specify --first with the commit to squash from",
			},
		},
	},
	{
		// The initial commit has two children (A on main, C on 'other'), but only A is
		// reachable from HEAD, so the default first commit is unambiguous and squash succeeds.
		Name: "dolt_squash_history: default resolves when only one child of the initial commit is reachable",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"insert into t values (1);",
			"call dolt_commit('-Am', 'A');",
			"set @init = (select commit_hash from dolt_log where message = 'Initialize data repository');",
			"call dolt_branch('other', @init);",
			"call dolt_checkout('other');",
			"create table t (pk int primary key);",
			"insert into t values (99);",
			"call dolt_commit('-Am', 'C');",
			"call dolt_checkout('main');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'B');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'D');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_squash_history('--message', 'sq');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// A, B, D collapse onto the initial commit.
				Query:    "select message from dolt_log;",
				Expected: []sql.Row{{"sq"}, {"Initialize data repository"}},
			},
			{
				// The unrelated 'other' branch (the second child of init) is untouched.
				Query:    "select message from dolt_log as of 'other';",
				Expected: []sql.Row{{"C"}, {"Initialize data repository"}},
			},
			{
				Query:    "select * from t order by pk;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name:        "dolt_squash_history: error when the branch has only the initial commit",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_squash_history('--message', 'x');",
				ExpectedErrStr: "nothing to squash: the branch contains only the initial commit",
			},
		},
	},
}
