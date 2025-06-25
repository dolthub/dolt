// Copyright 2025 Dolthub, Inc.
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

var DoltRmTests = []queries.ScriptTest{
	{
		Name: "dolt Rm without tables",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rm();",
				ExpectedErrStr: "Nothing specified, nothing removed. Which tables should I remove?",
			},
		},
	},
	{
		Name: "simple dolt Rm",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"CALL DOLT_COMMIT('-A', '-m', 'created table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_rm('test');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "select * from test;",
				ExpectedErrStr: "table not found: test",
			},
		},
	},
	{
		Name: "dolt Rm staged table",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"CALL DOLT_ADD('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rm('test');",
				ExpectedErrStr: "error: the table(s) test have changes saved in the index.",
			},
		},
	},
	{
		Name: "dolt Rm unstaged table",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_rm('test');",
				ExpectedErrStr: "error: the table(s) test do not exist",
			},
		},
	},
	{
		Name: "dolt Rm with cached option",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"INSERT INTO test VALUES (1)",
			"CALL DOLT_COMMIT('-A', '-m', 'created table')",
			"call dolt_rm('test', '--cached');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from test;",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "dolt Rm staged table with cached option",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"CALL DOLT_ADD('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_RM('test', '--cached');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"test", false, "new table"},
				},
			},
		},
	},
	{
		Name: "dolt Rm staged and unstaged with cached option",
		SetUpScript: []string{
			"CREATE TABLE committed (i int)",
			"CALL DOLT_COMMIT('-A', '-m', 'created table')",
			"CREATE TABLE staged (i int)",
			"CALL DOLT_ADD('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_rm('committed','staged','--cached')",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT * FROM dolt_status",
				Expected: []sql.Row{
					{"committed", true, "deleted"},
					{"staged", false, "new table"},
					{"committed", false, "new table"},
				},
			},
		},
	},
	/*TODO:
	ADD TESTS RELATED TO STUFF WHEN YOU SHOULDN'T BE ABLE TO REMOVE
	FOR EXAMPLE, FOREIGN KEY CONSTRAINTS.
	*/
}
