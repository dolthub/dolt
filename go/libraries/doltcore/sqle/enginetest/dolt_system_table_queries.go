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
)

var BrokenSystemTableQueries = []queries.QueryTest{
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

var BackupsSystemTableQueries = queries.ScriptTest{
	Name: "dolt_backups table",
	SetUpScript: []string{
		`call dolt_backup("add", "backup3", "file:///tmp/backup3");`,
		`call dolt_backup("add", "backup1", "file:///tmp/backup1");`,
		`call dolt_backup("add", "backup2", "aws://[ddb_table:ddb_s3_bucket]/db1");`,
	},
	Assertions: []queries.ScriptTestAssertion{
		{
			// Query for just the names because on Windows the Drive letter is inserted into the file path
			Query: "select name from dolt_backups;",
			Expected: []sql.Row{
				{"backup1"},
				{"backup2"},
				{"backup3"},
			},
		},
		{
			Query:    "select url from dolt_backups where name = 'backup2';",
			Expected: []sql.Row{{"aws://[ddb_table:ddb_s3_bucket]/db1"}},
		},
		{
			Query:          "delete from dolt_backups where name = 'backup1';",
			ExpectedErrStr: "table doesn't support DELETE FROM",
		},
		{
			Query:          "update dolt_backups set name = 'backup1' where name = 'backup2';",
			ExpectedErrStr: "table doesn't support UPDATE",
		},
		{
			Query:          "insert into dolt_backups values ('backup4', 'file:///tmp/broken');", // nolint: gas
			ExpectedErrStr: "table doesn't support INSERT INTO",
		},
		{
			Query:    "call dolt_backup('add', 'backup4', 'aws://[ddb_table_4:ddb_s3_bucket_4]/db1');",
			Expected: []sql.Row{{0}},
		},
		{
			Query:    "call dolt_backup('remove', 'backup1');",
			Expected: []sql.Row{{0}},
		},
		{
			Query: "select * from dolt_backups where url like 'aws://%'",
			Expected: []sql.Row{
				{"backup2", "aws://[ddb_table:ddb_s3_bucket]/db1"},
				{"backup4", "aws://[ddb_table_4:ddb_s3_bucket_4]/db1"},
			},
		},
	},
}
