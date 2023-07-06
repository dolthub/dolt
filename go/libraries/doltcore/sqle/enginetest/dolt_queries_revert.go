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
)

var RevertScripts = []queries.ScriptTest{
	{
		Name: "dolt_revert() reverts HEAD",
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
		Name: "dolt_revert() reverts HEAD~1",
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
}
