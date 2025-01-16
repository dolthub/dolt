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
	"encoding/json"

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
				Query: "select synopsis from dolt_help where name='dolt_conflicts_resolve'",
				Expected: []sql.Row{
					{"dolt conflicts resolve --ours|--theirs <table>..."},
				},
			},
			{
				Query: "select synopsis from dolt_help where name='dolt_merge'",
				Expected: []sql.Row{
					{`dolt merge [--squash] <branch>
dolt merge --no-ff [-m message] <branch>
dolt merge --abort`,
					},
				},
			},
			{
				Query: "select synopsis from dolt_help where name='dolt_branch'",
				Expected: []sql.Row{
					{`dolt branch [--list] [-v] [-a] [-r]
dolt branch [-f] <branchname> [<start-point>]
dolt branch -m [-f] [<oldbranch>] <newbranch>
dolt branch -c [-f] [<oldbranch>] <newbranch>
dolt branch -d [-f] [-r] <branchname>...`},
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
				Query: "select long_description from dolt_help where name='dolt_add'",
				Expected: []sql.Row{
					{`
This command updates the list of tables using the current content found in the working root, to prepare the content staged for the next commit. It adds the current content of existing tables as a whole or remove tables that do not exist in the working root anymore.

This command can be performed multiple times before a commit. It only adds the content of the specified table(s) at the time the add command is run; if you want subsequent changes included in the next commit, then you must run dolt add again to add the new content to the index.

The dolt status command can be used to obtain a summary of which tables have changes that are staged for the next commit.`},
				},
			},
			{
				Query: "select long_description from dolt_help where name='dolt_cherry_pick'",
				Expected: []sql.Row{
					{`
Applies the changes from an existing commit and creates a new commit from the current HEAD. This requires your working tree to be clean (no modifications from the HEAD commit).

Cherry-picking merge commits or commits with table drops/renames is not currently supported. 

If any data conflicts, schema conflicts, or constraint violations are detected during cherry-picking, you can use Dolt's conflict resolution features to resolve them. For more information on resolving conflicts, see: https://docs.dolthub.com/concepts/dolt/git/conflicts.
`},
				},
			},
			{
				Query: "select long_description from dolt_help where name='dolt_revert'",
				Expected: []sql.Row{
					{`Removes the changes made in a commit (or series of commits) from the working set, and then automatically commits the result. This is done by way of a three-way merge. Given a specific commit (e.g. <b>HEAD~1</b>), this is similar to applying the patch from <b>HEAD~1..HEAD~2</b>, giving us a patch of what to remove to effectively remove the influence of the specified commit. If multiple commits are specified, then this process is repeated for each commit in the order specified. This requires a clean working set.

Any conflicts or constraint violations caused by the merge cause the command to fail.`},
				},
			},
		},
	},

	{
		Name:        "dolt_help arguments are correct",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select arguments from dolt_help where name='dolt_commit'",
				Expected: []sql.Row{
					{string(jsonMarshalOrPanic(map[string]string{
						"--amend":       "Amend previous commit",
						"-A, --ALL":     "Adds all tables and databases (including new tables) in the working set to the staged set.",
						"-a, --all":     "Adds all existing, changed tables (but not new tables) in the working set to the staged set.",
						"-f, --force":   "Ignores any foreign key warnings and proceeds with the commit.",
						"--skip-empty":  "Only create a commit if there are staged changes. If no changes are staged, the call to commit is a no-op. Cannot be used with --allow-empty.",
						"--allow-empty": "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety. Cannot be used with --skip-empty.", "--date=<date>": "Specify the date used in the commit. If not specified the current system time is used.", "--author=<author>": "Specify an explicit author using the standard A U Thor <author@example.com> format.", "-m <msg>, --message=<msg>": "Use the given <msg> as the commit message.", "-S <key-id>, --gpg-sign=<key-id>": "Sign the commit using GPG. If no key-id is provided the key-id is taken from 'user.signingkey' the in the configuration",
					}))},
				},
			},
			{
				Query: "select arguments from dolt_help where name='dolt_rebase'",
				Expected: []sql.Row{
					{string(jsonMarshalOrPanic(map[string]string{
						"--abort":           "Abort an interactive rebase and return the working set to the pre-rebase state",
						"--continue":        "Continue an interactive rebase after adjusting the rebase plan",
						"--empty=<empty>":   "How to handle commits that are not empty to start, but which become empty after rebasing. Valid values are: drop (default) or keep",
						"-i, --interactive": "Start an interactive rebase",
					}))},
				},
			},
			{
				Query: "select arguments from dolt_help where name='dolt_revert'",
				Expected: []sql.Row{
					{string(jsonMarshalOrPanic(map[string]string{
						"--author=<author>": "Specify an explicit author using the standard A U Thor <author@example.com> format.",
						"<revision>":        "The commit revisions. If multiple revisions are given, they're applied in the order given.",
					}))},
				},
			},
		},
	},
}

func jsonMarshalOrPanic(v any) []byte {
	argsJson, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return argsJson
}
