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
		Expected: []sql.UntypedSqlRow{},
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
		Expected: []sql.UntypedSqlRow{},
	},
}
