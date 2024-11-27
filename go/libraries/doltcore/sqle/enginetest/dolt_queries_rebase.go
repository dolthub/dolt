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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
)

var DoltRebaseScriptTests = []queries.ScriptTest{
	{
		Name:        "dolt_rebase errors: basic errors",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rebase('--abort');",
				ExpectedErrStr: "no rebase in progress",
			}, {
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "no rebase in progress",
			}, {
				Query:          "call dolt_rebase('main');",
				ExpectedErrStr: "non-interactive rebases not currently supported",
			}, {
				Query:          "call dolt_rebase('-i');",
				ExpectedErrStr: "not enough args",
			}, {
				Query:          "call dolt_rebase('-i', 'main1', 'main2');",
				ExpectedErrStr: "rebase takes at most one positional argument.",
			}, {
				Query:          "call dolt_rebase('--abrot');",
				ExpectedErrStr: "error: unknown option `abrot'",
			}, {
				Query:          "call dolt_rebase('-i', 'doesnotexist');",
				ExpectedErrStr: "branch not found: doesnotexist",
			},
		},
	},
	{
		Name: "dolt_rebase errors: working set not clean",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"insert into t values (0);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "call dolt_rebase('-i', 'main');",
				ExpectedErr: dprocedures.ErrRebaseUncommittedChanges,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:       "call dolt_rebase('-i', 'main');",
				ExpectedErr: dprocedures.ErrRebaseUncommittedChanges,
			},
		},
	},
	{
		SkipPrepared: true,
		Name:         "dolt_rebase errors: no database selected",
		SetUpScript: []string{
			"create database temp;",
			"use temp;",
			"drop database temp;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select database();",
				Expected: []sql.UntypedSqlRow{{nil}},
			},
			{
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: "no database selected",
			},
		},
	},
	{
		Name: "dolt_rebase errors: active merge, cherry-pick, or rebase",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",
			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (0, 'nada');",
			"call dolt_commit('-am', 'inserting row 0');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Merging main creates a conflict, so we're in an active
				// merge until we resolve.
				Query:    "call dolt_merge('main');",
				Expected: []sql.UntypedSqlRow{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: "unable to start rebase while a merge is in progress – abort the current merge before proceeding",
			},
		},
	},
	{
		Name: "dolt_rebase errors: rebase working branch already exists",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",
			"call dolt_branch('dolt_rebase_branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: "fatal: A branch named 'dolt_rebase_branch1' already exists.",
			},
		},
	},
	{
		Name: "dolt_rebase errors: invalid rebase plans",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:          "update dolt_rebase set rebase_order=1.0 where rebase_order=2.0;",
				ExpectedErrStr: "duplicate primary key given: [1]",
			},
			{
				Query: "update dolt_rebase set action='squash';",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{
					RowsAffected: 2,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  2,
						Updated:  2,
						Warnings: 0,
					},
				}}},
			},
			{
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: rebase.ErrInvalidRebasePlanSquashFixupWithoutPick.Error(),
			},
			{
				Query: "update dolt_rebase set action='drop' where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
			{
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: rebase.ErrInvalidRebasePlanSquashFixupWithoutPick.Error(),
			},
			{
				Query: "update dolt_rebase set action='pick', commit_hash='doesnotexist' where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
			{
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "invalid commit hash: doesnotexist",
			},
			{
				Query: "update dolt_rebase set commit_hash='0123456789abcdef0123456789abcdef' where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
			{
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "unable to resolve commit hash 0123456789abcdef0123456789abcdef: target commit not found",
			},
		},
	},
	{
		Name: "dolt_rebase errors: unresolved conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "delete from dolt_rebase where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.NewOkResult(1)}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"2", "pick", doltCommit, "updating row 1 on branch1"},
				},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				// Trying to --continue a rebase when there are conflicts results in an error.
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUnresolvedConflicts,
			},
		},
	},
	{
		Name: "dolt_rebase errors: uncommitted changes before first --continue",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "insert into t values (100, 'hundo');",
				Expected: []sql.UntypedSqlRow{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUncommittedChanges,
			},
		},
	},
	{
		Name: "dolt_rebase errors: data conflicts when @@autocommit is enabled",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",
			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "update dolt_rebase set rebase_order=3 where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"2", "pick", doltCommit, "updating row 1 on branch1"},
					{"3.00", "pick", doltCommit, "inserting row 1 on branch1"},
				},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflictsCantBeResolved,
			},
		},
	},
	{
		Name: "dolt_rebase: rebased commit becomes empty; --empty not specified",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on branch1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10 on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 10 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: rebased commit becomes empty; --empty=keep",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on branch1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10 on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', '--empty', 'keep', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 10 on branch1"},
					{"inserting row 0 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: rebased commit becomes empty; --empty=drop",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0 on branch1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10 on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', '--empty', 'drop', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 10 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: no commits to rebase",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10');",
			"insert into t values (100);",
			"call dolt_commit('-am', 'inserting row 100');",
			"insert into t values (1000);",
			"call dolt_commit('-am', 'inserting row 1000');",
			"insert into t values (10000);",
			"call dolt_commit('-am', 'inserting row 10000');",
			"insert into t values (100000);",
			"call dolt_commit('-am', 'inserting row 100000');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:          "call dolt_rebase('-i', 'HEAD');",
				ExpectedErrStr: "didn't identify any commits!",
			},
			{
				// if the rebase doesn't start, then we should remain on the original branch
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				// and the rebase working branch shouldn't be present
				Query:    "select * from dolt_branches where name='dolt_rebase_branch1';",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "dolt_rebase: abort properly cleans up",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10');",
			"insert into t values (100);",
			"call dolt_commit('-am', 'inserting row 100');",
			"insert into t values (1000);",
			"call dolt_commit('-am', 'inserting row 1000');",
			"insert into t values (10000);",
			"call dolt_commit('-am', 'inserting row 10000');",
			"insert into t values (100000);",
			"call dolt_commit('-am', 'inserting row 100000');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "call dolt_rebase('--abort');",
				Expected: []sql.UntypedSqlRow{{0, "Interactive rebase aborted"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:    "select name from dolt_branches",
				Expected: []sql.UntypedSqlRow{{"main"}, {"branch1"}},
			},
		},
	},
	{
		Name: "dolt_rebase: rebase plan using every action",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (10);",
			"call dolt_commit('-am', 'inserting row 10');",
			"insert into t values (100);",
			"call dolt_commit('-am', 'inserting row 100');",
			"insert into t values (1000);",
			"call dolt_commit('-am', 'inserting row 1000');",
			"insert into t values (10000);",
			"call dolt_commit('-am', 'inserting row 10000');",
			"insert into t values (100000);",
			"call dolt_commit('-am', 'inserting row 100000');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1"},
					{"2", "pick", doltCommit, "inserting row 10"},
					{"3", "pick", doltCommit, "inserting row 100"},
					{"4", "pick", doltCommit, "inserting row 1000"},
					{"5", "pick", doltCommit, "inserting row 10000"},
					{"6", "pick", doltCommit, "inserting row 100000"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=6.1 where rebase_order=6;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='squash' where rebase_order in (2, 3);",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(2), Info: plan.UpdateInfo{
					Matched: 2,
					Updated: 2,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='drop' where rebase_order = 4;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='reword', commit_message='reworded!' where rebase_order = 5;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='fixup' where rebase_order = 6.10;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1"},
					{"2", "squash", doltCommit, "inserting row 10"},
					{"3", "squash", doltCommit, "inserting row 100"},
					{"4", "drop", doltCommit, "inserting row 1000"},
					{"5", "reword", doltCommit, "reworded!"},
					{"6.10", "fixup", doltCommit, "inserting row 100000"},
				},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				// When rebase completes, rebase status should be cleared
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "no rebase in progress",
			},
			{
				// The dolt_rebase table is gone after rebasing completes
				Query:          "select * from dolt_rebase;",
				ExpectedErrStr: "table not found: dolt_rebase",
			},
			{
				// The working branch for the rebase is deleted after rebasing completes
				Query:    "select name from dolt_branches",
				Expected: []sql.UntypedSqlRow{{"main"}, {"branch1"}},
			},
			{
				// Assert that the commit history is now composed of different commits
				Query: "select message from dolt_log order by date desc;",
				Expected: []sql.UntypedSqlRow{
					{"reworded!"},
					{"inserting row 1\n\ninserting row 10\n\ninserting row 100"},
					{"inserting row 0"},
					{"creating table t"},
					{"Initialize data repository"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0}, {1}, {10}, {100}, {10000}, {100000}},
			},
		},
	},
	{
		Name: "dolt_rebase: negative rebase order",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",
			"update t set c1='ein' where pk=1;",
			"call dolt_commit('-am', 'updating row 1, again, on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "update dolt_rebase set rebase_order=rebase_order-12.34;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(3), Info: plan.UpdateInfo{
					Matched: 3,
					Updated: 3,
				}}}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"-11.34", "pick", doltCommit, "inserting row 1 on branch1"},
					{"-10.34", "pick", doltCommit, "updating row 1 on branch1"},
					{"-9.34", "pick", doltCommit, "updating row 1, again, on branch1"},
				},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "ein"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"updating row 1, again, on branch1"},
					{"updating row 1 on branch1"},
					{"inserting row 1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: data conflicts with pick",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",
			"update t set c1='ein' where pk=1;",
			"call dolt_commit('-am', 'updating row 1, again, on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1 on branch1"},
					{"2", "pick", doltCommit, "updating row 1 on branch1"},
					{"3", "pick", doltCommit, "updating row 1, again, on branch1"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=3.5 where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				// We remain on the rebase working branch while resolving conflicts
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				// The base of the cherry-picked commit has (1, "one"), but ours doesn't have that record (nil, nil)
				// since we reordered the insert. The cherry-picked commit is trying to modify the row to (1, "uno").
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{1, "one", nil, nil, "removed", 1, "uno", "modified"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Resolve conflicts, by accepting theirs, which inserts (1, "uno") into t
				// When we continue the rebase, a Dolt commit will be created for these changes
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// Our new commit shows up as the first commit on top of the latest commit from the upstream branch
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Table t includes the change from the tip of main (0, "zero"), as well as the conflict we just resolved
				Query:    "SELECT * FROM t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "uno"}},
			},
			{
				// If we don't stage the table, then rebase will give us an error about having unstaged changes
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUnstagedChanges,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "ein"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"updating row 1, again, on branch1"},
					{"updating row 1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Now we're resolving a conflict from reordering the insert of (1, "one"). This was originally
				// an insert, so the base has (nil, nil), ours is (1, "ein").
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{nil, nil, 1, "ein", "added", 1, "one", "added"}},
			},
			{
				// Accept the new values from the cherry-picked commit (1, "ein").
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// We can commit manually, or we can continue the rebase and let it commit for us
				Query:    "CALL DOLT_COMMIT('-am', 'OVERRIDDEN COMMIT MESSAGE');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "one"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"OVERRIDDEN COMMIT MESSAGE"},
					{"updating row 1, again, on branch1"},
					{"updating row 1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: data conflicts with squash",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'inserting row 1 on branch1');",
			"update t set c1='uno' where pk=1;",
			"call dolt_commit('-am', 'updating row 1 on branch1');",
			"update t set c1='ein' where pk=1;",
			"call dolt_commit('-am', 'updating row 1, again, on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1 on branch1"},
					{"2", "pick", doltCommit, "updating row 1 on branch1"},
					{"3", "pick", doltCommit, "updating row 1, again, on branch1"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=3.5 where rebase_order=2;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='squash' where rebase_order=3;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				// We remain on the rebase working branch while resolving conflicts
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				// The base of the cherry-picked commit has (1, "uno"), but ours has (1, "one"), so this is a data
				// conflict. The cherry-picked commit is trying to modify the row to (1, "ein").
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{1, "uno", 1, "one", "modified", 1, "ein", "modified"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Resolve conflicts, by accepting theirs, which inserts (1, "ein") into t
				// When we continue the rebase, a Dolt commit will be created for these changes
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				// Table t includes the change from the tip of main (0, "zero"), as well as the conflict we just resolved
				Query:    "SELECT * FROM t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "ein"}},
			},
			{
				// If we don't stage the table, then rebase will give us an error about having unstaged changes
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUnstagedChanges,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "ein"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 1 on branch1\n\nupdating row 1, again, on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Now we're resolving a conflict from updating (1, "one") to (1, "uno"). Our side currently has
				// (1, "ein"), so this is marked as a conflict.
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{1, "one", 1, "ein", "modified", 1, "uno", "modified"}},
			},
			{
				// Accept the new values from the cherry-picked commit (1, "uno").
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "CALL DOLT_COMMIT('-am', 'OVERRIDDEN COMMIT MESSAGE');",
				Expected: []sql.UntypedSqlRow{{doltCommit}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {1, "uno"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"OVERRIDDEN COMMIT MESSAGE"},
					{"inserting row 1 on branch1\n\nupdating row 1, again, on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: data conflicts with fixup",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"insert into t values (-1, 'negative');",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"update t set c1='-1' where pk=-1;",
			"call dolt_commit('-am', 'updating row -1 on branch1');",
			"delete from t where c1 = '-1';",
			"call dolt_commit('-am', 'deleting -1 on branch1');",
			"insert into t values (999, 'nines');",
			"call dolt_commit('-am', 'inserting row 999 on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "updating row -1 on branch1"},
					{"2", "pick", doltCommit, "deleting -1 on branch1"},
					{"3", "pick", doltCommit, "inserting row 999 on branch1"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=3.5, action='fixup' where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"2", "pick", doltCommit, "deleting -1 on branch1"},
					{"3", "pick", doltCommit, "inserting row 999 on branch1"},
					{"3.50", "fixup", doltCommit, "updating row -1 on branch1"},
				},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				// We remain on the rebase working branch while resolving conflicts
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				// The base of the cherry-picked commit has (-1, "-1"), but ours has (-1, "negative"), so this is a
				// data conflict. The cherry-picked commit is trying to delete the row, but can't find an exact match.
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{-1, "-1", -1, "negative", "modified", nil, nil, "removed"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Resolve conflicts, by accepting theirs, which inserts (1, "ein") into t
				// When we continue the rebase, a Dolt commit will be created for these changes
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}},
			},
			{
				// If we don't stage the table, then rebase will give us an error about having unstaged changes
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUnstagedChanges,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {999, "nines"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 999 on branch1"},
					{"deleting -1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Now we're resolving a conflict where row -1 is updated, but it has already been deleted
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{-1, "negative", nil, nil, "removed", -1, "-1", "modified"}},
			},
			{
				// Accept the new values from the cherry-picked commit (1, "uno").
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{-1, "-1"}, {0, "zero"}, {999, "nines"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 999 on branch1"},
					{"deleting -1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: data conflicts with reword",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(100));",
			"insert into t values (-1, 'negative');",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",
			"insert into t values (0, 'zero');",
			"call dolt_commit('-am', 'inserting row 0 on main');",

			"call dolt_checkout('branch1');",
			"update t set c1='-1' where pk=-1;",
			"call dolt_commit('-am', 'updating row -1 on branch1');",
			"delete from t where c1 = '-1';",
			"call dolt_commit('-am', 'deleting -1 on branch1');",
			"insert into t values (999, 'nines');",
			"call dolt_commit('-am', 'inserting row 999 on branch1');",

			"set @@autocommit=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "updating row -1 on branch1"},
					{"2", "pick", doltCommit, "deleting -1 on branch1"},
					{"3", "pick", doltCommit, "inserting row 999 on branch1"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=3.5, action='reword', commit_message='reworded message!' where rebase_order=1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"2", "pick", doltCommit, "deleting -1 on branch1"},
					{"3", "pick", doltCommit, "inserting row 999 on branch1"},
					{"3.50", "reword", doltCommit, "reworded message!"},
				},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				// We remain on the rebase working branch while resolving conflicts
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.UntypedSqlRow{{"t", uint64(1)}},
			},
			{
				// The base of the cherry-picked commit has (-1, "-1"), but ours has (-1, "negative"), so this is a
				// data conflict. The cherry-picked commit is trying to delete the row, but can't find an exact match.
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{-1, "-1", -1, "negative", "modified", nil, nil, "removed"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Resolve conflicts, by accepting theirs, which inserts (1, "ein") into t
				// When we continue the rebase, a Dolt commit will be created for these changes
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}},
			},
			{
				// If we don't stage the table, then rebase will give us an error about having unstaged changes
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseUnstagedChanges,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseDataConflict,
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0, "zero"}, {999, "nines"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 999 on branch1"},
					{"deleting -1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				// Now we're resolving a conflict where row -1 is updated, but it has already been deleted
				Query:    "select base_pk, base_c1, our_pk, our_c1, our_diff_type, their_pk, their_c1, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.UntypedSqlRow{{-1, "negative", nil, nil, "removed", -1, "-1", "modified"}},
			},
			{
				// Accept the new values from the cherry-picked commit (-1, "-1")
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{-1, "-1"}, {0, "zero"}, {999, "nines"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"reworded message!"},
					{"inserting row 999 on branch1"},
					{"deleting -1 on branch1"},
					{"inserting row 0 on main"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_rebase: schema conflicts",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"alter table t add column c1 varchar(100) NOT NULL;",
			"call dolt_commit('-am', 'adding column c1');",
			"alter table t modify column c1 varchar(100) comment 'foo';",
			"call dolt_commit('-am', 'altering column c1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1"},
					{"2", "pick", doltCommit, "adding column c1"},
					{"3", "pick", doltCommit, "altering column c1"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=3.1 where rebase_order=2;",
				Expected: []sql.UntypedSqlRow{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				// Encountering a conflict during a rebase returns an error and aborts the rebase
				Query:       "call dolt_rebase('--continue');",
				ExpectedErr: dprocedures.ErrRebaseSchemaConflict,
			},
			{
				// The rebase state has been cleared after hitting a conflict
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "no rebase in progress",
			},
			{
				// We're back to the original branch
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				// The schema conflicts table should be empty, since the rebase was aborted
				Query:    "select * from dolt_schema_conflicts;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		// Tests that the rebase plan can be changed in non-standard ways, such as adding new commits to the plan
		// and completely removing commits from the plan. These changes are also valid with Git.
		Name: "dolt_rebase: non-standard plan changes",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",
			"call dolt_branch('branch2');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch2');",
			"insert into t values (999);",
			"call dolt_commit('-am', 'inserting row 999');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'inserting row 2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'inserting row 3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1"},
					{"2", "pick", doltCommit, "inserting row 2"},
					{"3", "pick", doltCommit, "inserting row 3"},
				},
			},
			{
				Query:    "delete from dolt_rebase where rebase_order > 1;",
				Expected: []sql.UntypedSqlRow{{gmstypes.NewOkResult(2)}},
			},
			{
				// NOTE: This uses "pick", not reword, so we expect the commit message from the commit to be
				//       used, and not the custom commit message inserted into the table.
				Query:    "insert into dolt_rebase values (2.12, 'pick', hashof('branch2'), 'inserting row 0');",
				Expected: []sql.UntypedSqlRow{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 999"},
					{"inserting row 1"},
					{"inserting row 0"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{0}, {1}, {999}},
			},
		},
	},
	{
		// Merge commits are skipped during a rebase
		Name: "dolt_rebase: merge commits",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_branch('branch1');",

			"insert into t values (0);",
			"call dolt_commit('-am', 'inserting row 0');",

			"call dolt_checkout('branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'inserting row 2');",
			"call dolt_merge('main');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'inserting row 3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 3"},
					{"Merge branch 'main' into branch1"},
					{"inserting row 2"},
					{"inserting row 0"},
					{"inserting row 1"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
			{
				Query: "call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order;",
				Expected: []sql.UntypedSqlRow{
					{"1", "pick", doltCommit, "inserting row 1"},
					{"2", "pick", doltCommit, "inserting row 2"},
					{"3", "pick", doltCommit, "inserting row 3"},
				},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.UntypedSqlRow{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.UntypedSqlRow{
					{"inserting row 3"},
					{"inserting row 2"},
					{"inserting row 1"},
					{"inserting row 0"},
					{"creating table t"},
					{"Initialize data repository"},
				},
			},
		},
	},
}

var DoltRebaseMultiSessionScriptTests = []queries.ScriptTest{
	{
		// When the branch HEAD is changed while a rebase is in progress, the rebase should fail
		Name: "dolt_rebase errors: branch HEAD changed during rebase",
		SetUpScript: []string{
			"create table t (pk int primary key);",
			"call dolt_commit('-Am', 'creating table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1);",
			"call dolt_commit('-am', 'inserting row 1');",
			"insert into t values (2);",
			"call dolt_commit('-am', 'inserting row 2');",
			"insert into t values (3);",
			"call dolt_commit('-am', 'inserting row 3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:    "/* client b */ call dolt_checkout('branch1');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "/* client b */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query: "/* client a */ call dolt_rebase('-i', 'main');",
				Expected: []sql.UntypedSqlRow{{0, "interactive rebase started on branch dolt_rebase_branch1; " +
					"adjust the rebase plan in the dolt_rebase table, then " +
					"continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "/* client b */ insert into t values (1000);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:            "/* client b */ call dolt_commit('-am', 'inserting row 1000');",
				SkipResultsCheck: true,
			},
			{
				Query:          "/* client a */ call dolt_rebase('--continue');",
				ExpectedErrStr: "Error 1105 (HY000): rebase aborted due to changes in branch branch1",
			},
		},
	},
}
