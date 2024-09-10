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
		Name: "create database in a transaction",
		SetUpScript: []string{
			"CREATE DATABASE if not exists mydb", // TODO: this is an artifact of how we run the tests
			"START TRANSACTION",
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
}
