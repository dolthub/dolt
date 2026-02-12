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

func init() {
	DoltProcedureTests = append(DoltProcedureTests, DoltCleanProcedureScripts...)
}

// DoltCleanProcedureScripts are script tests for the dolt_clean procedure.
var DoltCleanProcedureScripts = []queries.ScriptTest{
	{
		Name: "dolt_clean does not drop tables matching dolt_ignore",
		SetUpScript: []string{
			"CREATE TABLE ignored_foo (id int primary key);",
			"INSERT INTO ignored_foo VALUES (1);",
			"INSERT INTO dolt_ignore VALUES ('ignored_*', true);",
			"CALL dolt_add('dolt_ignore');",
			"CALL dolt_commit('-m', 'add dolt_ignore');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM ignored_foo;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL dolt_clean();",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * FROM ignored_foo;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{{"ignored_foo"}},
			},
		},
	},
	{
		Name: "dolt_clean -x drops tables matching dolt_ignore",
		SetUpScript: []string{
			"CREATE TABLE ignored_bar (id int primary key);",
			"INSERT INTO ignored_bar VALUES (1);",
			"INSERT INTO dolt_ignore VALUES ('ignored_*', true);",
			"CALL dolt_add('dolt_ignore');",
			"CALL dolt_commit('-m', 'add dolt_ignore');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM ignored_bar;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL dolt_clean('-x');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "SELECT * FROM ignored_bar;",
				ExpectedErrStr: "table not found: ignored_bar",
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{},
			},
		},
	},
}
