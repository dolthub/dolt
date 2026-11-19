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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var DoltCreateDatabaseScripts = []queries.ScriptTest{
	{
		Name: "create database simple",
		SetUpScript: []string{
			"CREATE DATABASE if not exists mydb", // TODO: this is an artifact of how we run the tests
			"CREATE DATABASE test",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW DATABASES",
				Expected: []sql.Row{
					{"information_schema"},
					{"mydb"},
					{"mysql"},
					{"test"},
				},
			},
			{
				Query:            "USE test",
				SkipResultsCheck: true,
			},
			{
				Query:            "CREATE TABLE foo (bar INT)",
				SkipResultsCheck: true,
			},
			{
				Query:            "USE mydb",
				SkipResultsCheck: true,
			},
			{
				Query:            "INSERT INTO test.foo VALUES (1)",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM test.foo",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "create database with non standard collation, create branch",
		SetUpScript: []string{
			"CREATE DATABASE test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use test",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_branch('b1')",
				SkipResultsCheck: true,
			},
			{
				Query: "show create database test",
				Expected: []sql.Row{
					{"test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */"},
				},
			},
		},
	},
	{
		Name: "create database in a transaction",
		SetUpScript: []string{
			"START TRANSACTION",
			"CREATE DATABASE test",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "USE test",
				SkipResultsCheck: true,
			},
			{
				Query:            "CREATE TABLE foo (bar INT)",
				SkipResultsCheck: true,
			},
			{
				Query:            "USE mydb",
				SkipResultsCheck: true,
			},
			{
				Query:            "INSERT INTO test.foo VALUES (1)",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM test.foo",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "create database and schema forbid revision delimiters in names",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CREATE DATABASE `mydb@branch`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb@branch",
			},
			{
				Query:          "CREATE DATABASE `mydb/too`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb/too",
			},
			{
				Query:          "CREATE SCHEMA `mydb@branch`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb@branch",
			},
			{
				Query:          "CREATE SCHEMA `mydb/too`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb/too",
			},
			{
				Query:          "CREATE DATABASE IF NOT EXISTS `mydb@branch`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb@branch",
			},
			{
				Query:          "CREATE SCHEMA IF NOT EXISTS `mydb/too`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb/too",
			},
			{
				Query:    "START TRANSACTION",
				Expected: []sql.Row{},
			},
			{
				Query:          "CREATE DATABASE `mydb@txbad`",
				ExpectedErr:    sql.ErrWrongDBName,
				ExpectedErrStr: "mydb@txbad",
			},
			{
				Query:    "CREATE DATABASE tx_ok",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SHOW DATABASES LIKE 'tx_ok'",
				Expected: []sql.Row{{"tx_ok"}},
			},
		},
	},
}
