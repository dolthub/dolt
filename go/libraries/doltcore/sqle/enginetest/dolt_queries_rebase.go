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
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: dprocedures.ErrRebaseUncommittedChanges.Error(),
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: dprocedures.ErrRebaseUncommittedChanges.Error(),
			},
		},
	},
	{
		Name: "dolt_rebase errors: no database selected",
		SetUpScript: []string{
			"create database temp;",
			"use temp;",
			"drop database temp;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select database();",
				Expected: []sql.Row{{nil}},
			},
			{
				// TODO: This test isn't working because AssertErr is called and currently always
				//       creates a new Context, which is always initialized with mydb as the current
				//       database. If we changed evaluation.go:126 to use AssertErrWithCtx instead and
				//       reused the existing Context instance, then we could probably make this work.
				Skip:           true,
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: "no database selected",
			},
		},
	},
	/*
	   TODO: Error cases:
	        - merge commits - merge commits should be fine, just skipped
	        - conflicts – e.g. reordering commits in a way that causes a conflict
	*/
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
				Expected: []sql.Row{{"", 0, 1}},
			},
			{
				Query:          "call dolt_rebase('-i', 'main');",
				ExpectedErrStr: "unable to start rebase while a merge is in progress – abort the current merge before proceeding",
			},
		},
	},
	{
		Name: "dolt_rebase errors: invalid rebase plans",
		SetUpScript: []string{
			// TODO: consider sharing setupscripts for rebase test setups?
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
			// TODO: Test that deleting a row from the rebase plan is equivalent to marking it as a drop
			// TODO: Test that new commit hashes can be added
			{
				Query:    "call dolt_rebase('-i', 'main');",
				Expected: []sql.Row{{0, "interactive rebase started"}},
			},
			{
				Query: "update dolt_rebase set action='squash';",
				Expected: []sql.Row{{gmstypes.OkResult{
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
				Expected: []sql.Row{{gmstypes.OkResult{
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
				Expected: []sql.Row{{gmstypes.OkResult{
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
				Expected: []sql.Row{{gmstypes.OkResult{
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

	/*
			TODO: Other cases?
			    - CLI: dolt status should show that we're in a rebase
		        - TEST: If the `dolt_rebase_<branchname>` branch already exists... what do we do? Warn the user that a rebase may be in progress by another user. Have them manually delete the rebase branch if they really want to (or some force option?)
		        - TEST: If another session updates the branch being rebased while a rebase operation is in progress, then the rebase should fail
	*/
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
				Query:    "call dolt_rebase('-i', 'main');",
				Expected: []sql.Row{{0, "interactive rebase started"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"dolt_rebase_branch1"}},
			},
			{
				Query:    "call dolt_rebase('--abort');",
				Expected: []sql.Row{{0, "interactive rebase aborted"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:    "select name from dolt_branches",
				Expected: []sql.Row{{"main"}, {"branch1"}},
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
				Query:    "call dolt_rebase('-i', 'main');",
				Expected: []sql.Row{{0, "interactive rebase started"}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='squash' where rebase_order in (2, 3);",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(2), Info: plan.UpdateInfo{
					Matched: 2,
					Updated: 2,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='drop' where rebase_order = 4;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='reword', commit_message='reworded!' where rebase_order = 5;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='fixup' where rebase_order = 6.10;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.Row{{0, "interactive rebase completed"}},
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
				Expected: []sql.Row{{"main"}, {"branch1"}},
			},
			{
				// Assert that the commit history is now composed of different commits
				Query: "select message from dolt_log order by date desc;",
				Expected: []sql.Row{
					{"reworded!"},
					{"inserting row 1\n\ninserting row 10\n\ninserting row 100"},
					{"inserting row 0"},
					{"creating table t"},
					{"Initialize data repository"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{0}, {1}, {10}, {100}, {10000}, {100000}},
			},
		},
	},
}
