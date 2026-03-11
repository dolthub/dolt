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

var DoltStatusTableScripts = []queries.ScriptTest{
	{
		Name: "dolt_status detached head is read-only clean",
		SetUpScript: []string{
			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"CALL DOLT_TAG('tag1');",
			"SET @head_hash = (SELECT HASHOF('main') LIMIT 1);",
			"SET @status_by_hash = CONCAT('SELECT * FROM `mydb/', @head_hash, '`.dolt_status;');",
			"PREPARE status_by_hash FROM @status_by_hash;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM `mydb/tag1`.dolt_status;",
				Expected: []sql.Row{},
			},
			{
				Query:    "EXECUTE status_by_hash;",
				Expected: []sql.Row{},
			},
			{
				Query:       "SELECT * FROM `information_schema`.dolt_status;",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/8283
		Name: "dolt_status tests",
		SetUpScript: []string{
			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"SET @commit1 = HASHOF('HEAD');",
			"CALL DOLT_TAG('tag1');",
			"CALL DOLT_CHECKOUT('-b', 'branch1');",
			"CREATE TABLE abc (pk int);",
			"CALL DOLT_ADD('abc');",
			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int primary key, v varchar(100));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_status;",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM `mydb/main`.dolt_status;",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'tag1';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF @commit1;",
				Expected: []sql.Row{},
			},
			{
				// HEAD is a special revision spec
				Query:    "SELECT * FROM dolt_status AS OF 'head';",
				Expected: []sql.Row{{"t", byte(0), "new table"}},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'HEAD~1';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM dolt_status AS OF 'branch1';",
				Expected: []sql.Row{{"abc", byte(1), "new table"}},
			},
			{
				Query:    "SELECT * FROM `mydb/branch1`.dolt_status;",
				Expected: []sql.Row{{"abc", byte(1), "new table"}},
			},
		},
	},
}
