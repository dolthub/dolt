// Copyright 2024 Dolthub, Inc.
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

var DoltHelpScripts = []queries.ScriptTest{
	{
		Name:        "dolt_help arguments are valid json",
		SetUpScript: []string{},
		Query:       "select * from dolt_help where json_valid(arguments)=false;",
		Expected:    []sql.Row{},
	},
	{
		Name: "dolt_help synopsis remains the same after multiple queries",
		SetUpScript: []string{
			"set @InitialSynopsis=(select synopsis from dolt_help where name='dolt_branch')",
		},
		Query:    "select * from dolt_help where name='dolt_branch' and synopsis!=@InitialSynopsis",
		Expected: []sql.Row{},
	},
	{
		Name:        "dolt_help names are correct",
		SetUpScript: []string{},
		Query:       "select name from dolt_help",
		Expected: []sql.Row{
			{"dolt_add"},
			{"dolt_reset"},
			{"dolt_clean"},
			{"dolt_commit"},
			{"dolt_branch"},
			{"dolt_checkout"},
			{"dolt_merge"},
			{"dolt_conflicts_resolve"},
			{"dolt_cherry_pick"},
			{"dolt_revert"},
			{"dolt_clone"},
			{"dolt_fetch"},
			{"dolt_pull"},
			{"dolt_push"},
			{"dolt_remote"},
			{"dolt_backup"},
			{"dolt_tag"},
			{"dolt_gc"},
			{"dolt_rebase"},
		},
	},
	{
		Name:        "dolt_help types are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select type from dolt_help where name='dolt_rebase'",
				Expected: []sql.Row{
					{"procedure"},
				},
			},
			{
				Query: "select type from dolt_help where name='dolt_gc'",
				Expected: []sql.Row{
					{"procedure"},
				},
			},
			{
				Query: "select type from dolt_help where name='dolt_tag'",
				Expected: []sql.Row{
					{"procedure"},
				},
			},
		},
	},
	{
		Name:        "dolt_help synopses are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(*) from dolt_help where name='dolt_conflicts_resolve' and synopsis like '%dolt conflicts resolve%table%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_merge' and synopsis like '%dolt merge%branch%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_branch' and synopsis like '%dolt branch%--list%'",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name:        "dolt_help short descriptions are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select short_description from dolt_help where name='dolt_clean'",
				Expected: []sql.Row{
					{"Deletes untracked working tables"},
				},
			},
			{
				Query: "select short_description from dolt_help where name='dolt_checkout'",
				Expected: []sql.Row{
					{"Switch branches or restore working tree tables"},
				},
			},
			{
				Query: "select short_description from dolt_help where name='dolt_fetch'",
				Expected: []sql.Row{
					{"Download objects and refs from another repository"},
				},
			},
		},
	},

	{
		Name:        "dolt_help long descriptions are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(*) from dolt_help where name='dolt_add' and long_description like '%updates the list of tables%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_cherry_pick' and long_description like '%Applies the changes from an existing commit%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_revert' and long_description like '%Removes the changes made in a commit%'",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},

	{
		Name:        "dolt_help arguments are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(*) from dolt_help where name='dolt_commit' and arguments like '%--amend%Amend previous commit%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_commit' and arguments like '%--skip-empty%Only create a commit if there are staged changes%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_rebase' and arguments like '%--continue%Continue an interactive rebase%'",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select count(*) from dolt_help where name='dolt_revert' and arguments like '%--author%Specify an explicit author%'",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
}
