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
)

var DoltRebaseScriptTests = []queries.ScriptTest{
	{
		/*
		   TODO: Error cases:
		        - already in a rebase or a merge/cherry-pick/etc
		        - working set not clean
		        - wrong number of args
		        - invalid args
		        - no database selected
		*/
		Name:        "dolt_rebase: error cases",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rebase('--abort');",
				ExpectedErrStr: "no rebase in progress",
			}, {
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "no rebase in progress",
			},
		},
	},
	/*
		TODO: Other cases
		  - test the skip rebase action
	*/
	{
		Name: "dolt_rebase: basic case",
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
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_rebase('main');",
				// TODO: Add status message: "rebase started"
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_rebase order by rebase_order ASC;",
				Expected: []sql.Row{
					{uint(1), uint(1), doltCommit, "inserting row 1"},
					{uint(2), uint(1), doltCommit, "inserting row 10"},
					{uint(3), uint(1), doltCommit, "inserting row 100"},
				},
			},
			{
				Query: "update dolt_rebase set rebase_order=4 where rebase_order=3;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(1), Info: plan.UpdateInfo{
					Matched: 1,
					Updated: 1,
				}}}},
			},
			{
				Query: "update dolt_rebase set action='squash' where rebase_order > 1;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: uint64(2), Info: plan.UpdateInfo{
					Matched: 2,
					Updated: 2,
				}}}},
			},
			{
				Query: "call dolt_rebase('--continue');",
				// TODO: Return a human readable status (e.g. rebase completed successfully)
				Expected: []sql.Row{{0}},
			},
			{
				// When rebase completes, rebase status should be cleared and the dolt_rebase table should be removed
				Query:          "call dolt_rebase('--continue');",
				ExpectedErrStr: "no rebase in progress",
			},
			{
				// The dolt_rebase table is gone after rebasing completes
				Query:          "select * from dolt_rebase;",
				ExpectedErrStr: "table not found: dolt_rebase",
			},
			{
				// Assert that the commit history is now composed of different commits
				Query: "select message from dolt_log order by date desc;",
				Expected: []sql.Row{
					{"inserting row 100"},
					{"inserting row 0"},
					{"creating table t"},
					{"Initialize data repository"}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{0}, {1}, {10}, {100}},
			},
		},
	},
}
