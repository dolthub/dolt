package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
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
}
