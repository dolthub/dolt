package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltCreateDatabaseScripts = []queries.ScriptTest{
	{
		Name: "create database",
		SetUpScript: []string{
			"CREATE DATABASE test",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW DATABASES",
				Expected: []sql.Row{
					{"information_schema"},
					{"mysql"},
					{"test"},
				},
			},
		},
	},
}
