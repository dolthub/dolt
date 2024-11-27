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
				Expected: []sql.UntypedSqlRow{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~1' where pk = 2;",
				Expected: []sql.UntypedSqlRow{{2, 42}},
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
				Expected: []sql.UntypedSqlRow{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~2' where pk = 2;",
				Expected: []sql.UntypedSqlRow{{2, 42}},
			},
			{
				Query:    "select * from test as of 'HEAD' where pk = 3;",
				Expected: []sql.UntypedSqlRow{{3, 23}},
			},
		},
	},
	{
		Name: "dolt_revert() detects conflicts",
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
				Query:          "call dolt_revert('HEAD~1');",
				ExpectedErrStr: "revert currently does not handle conflicts",
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
				ExpectedErrStr: "You must commit any changes before using revert",
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
				Expected: []sql.UntypedSqlRow{{2, 2}},
			},
			{
				Query:    "select * from test as of 'HEAD~1' where pk = 2;",
				Expected: []sql.UntypedSqlRow{{2, 42}},
			},
			{
				Query:          "select * from dont_track as of 'HEAD'",
				ExpectedErrStr: "table not found: dont_track",
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.UntypedSqlRow{{"dont_track", false, "new table"}},
			},
		},
	},
	{
		Name: "dolt_revert() detects not null violation (issue #4527)",
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
				Query:          "call dolt_revert('head~1');",
				ExpectedErrStr: "revert currently does not handle constraint violations",
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
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
}
