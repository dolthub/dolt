package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
)

var BrokenSystemTableQueries = []enginetest.QueryTest{
	{
		Query: `SELECT 
					myTable.i, 
					(SELECT 
						U0.diff_type 
					FROM 
						dolt_commit_diff_mytable U0 
					WHERE (
						U0.from_commit = 'abc' AND 
						U0.to_commit = 'abc'
					)) AS diff_type 
				FROM myTable`,
		Expected: []sql.Row{},
	},
	{
		// extra filter clause breaks filter pushdown
		// `dolt_commit_diff_*` relies on filter pushdown to function
		Query: `SELECT 
					myTable.i, 
					(SELECT 
						dolt_commit_diff_mytable.diff_type 
					FROM 
						dolt_commit_diff_mytable
					WHERE (
						dolt_commit_diff_mytable.from_commit = 'abc' AND 
						dolt_commit_diff_mytable.to_commit = 'abc' AND
						dolt_commit_diff_mytable.to_i = myTable.i  -- extra filter clause
					)) AS diff_type 
				FROM myTable`,
		Expected: []sql.Row{},
	},
}
