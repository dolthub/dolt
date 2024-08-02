// Copyright 2021 Dolthub, Inc.
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

	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

var DoltProcedureTests = []queries.ScriptTest{
	{
		Name: "dolt_commit in a loop",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure commit_many()
begin
  declare i int default 1;
	commits: loop
		insert into t(b) values (i);
		call dolt_commit('-am', concat('inserted row ', cast (i as char)));
		if i >= 10 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call commit_many();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {10, 10}},
			},
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{13}}, // init, setup for test harness, initial commit in setup script, 10 commits in procedure
			},
			{
				Query:    "select * from t as of `HEAD~5`",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
		},
	},
	{
		Name: "dolt_branch in a loop",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		insert into t(b) values (i);
		call dolt_branch(concat('branch', i));
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in a loop, insert after branch",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		call dolt_branch(concat('branch', i));
		insert into t(b) values (i);
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in conditional chain",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		if i = 1 then
			insert into t(b) values (i);
			call dolt_branch('branch1');
		elseif i = 2 then
			call dolt_branch('branch2');
			insert into t(b) values (i);
		elseif i = 3 then
			insert into t(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t(b) values (i);
		end if;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in case statement",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		case i
	  when 1 then
			insert into t(b) values (i);
			call dolt_branch('branch1');
		when 2 then
			call dolt_branch('branch2');
			insert into t(b) values (i);
		when 3 then
			insert into t(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t(b) values (i);
		end case;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in trigger",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"create table t2(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create trigger branch_trigger after insert on t for each row
begin
  declare i int default 1;
	commits: loop
		case i
	  when 1 then
			insert into t2(b) values (i);
			call dolt_branch('branch1');
		when 2 then
			call dolt_branch('branch2');
			insert into t2(b) values (i);
		when 3 then
			insert into t2(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t2(b) values (i);
		end case;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into t values (1, 1);",
				Expected: []sql.Row{
					{gmstypes.OkResult{RowsAffected: 1, InsertID: 4}},
				},
			},
			{
				Query: "select name from dolt_branches order by 1",
				Expected: []sql.Row{
					{"branch1"},
					{"branch2"},
					{"branch3"},
					{"branch4"},
					{"main"},
				},
			},
			{
				// For some reason, calling stored procedures disables inserts
				Skip:  true,
				Query: "select * from t2 order by 1",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
				},
			},
		},
	},
	{
		Name: "checkout new branch, insert, and commit in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure edit_on_branch()
begin
	call dolt_checkout('-b', 'branch1');
	insert into t(b) values (100);
	call dolt_commit('-am', 'new row');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call edit_on_branch();",
				Skip:             true,
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Skip:     true,
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Skip:     true,
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Skip:     true,
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Skip:     true,
				Expected: []sql.Row{{1, 100}},
			},
		},
	},
	{
		Name: "checkout existing branch and commit in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			"call dolt_branch('branch1');",
			`create procedure edit_on_branch()
begin
	call dolt_checkout('branch1');
	insert into t(b) values (100);
	call dolt_commit('-am', 'new row');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call edit_on_branch();",
				Skip:             true,
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Skip:     true,
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Skip:     true,
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Skip:     true,
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Skip:     true,
				Expected: []sql.Row{{1, 100}},
			},
		},
	},
	{
		Name: "merge in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			"call dolt_branch('branch1');",
			"insert into t(a, b) values (1, 100);",
			"call dolt_commit('-am', 'new row');",
			"call dolt_checkout('branch1');",
			"insert into t(a, b) values (2, 200);",
			"call dolt_commit('-am', 'new row on branch1');",
			"call dolt_checkout('main');",
			`create procedure merge_branch(branchName varchar(255))
begin
	call dolt_checkout(branchName);
	call dolt_merge('--no-ff', 'main');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call merge_branch('branch1');",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 100}},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
		},
	},
}
