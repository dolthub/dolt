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

var DoltProcedureTests = []queries.ScriptTest{
	{
		Name: "dolt_commit in a loop",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure commit_many()
begin
  declare i int default 1;
	commits: loop
		insert into t(b) values (i);
		call dolt_commit('-am', concat('inserted row ', cast (i as char)));
		if i >= 10 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call commit_many();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {10, 10}},
			},
			{
				Query: "select count(*) from dolt_log;",
				Expected: []sql.Row{{13}}, // init, setup for test harness, initial commit in setup script, 10 commits in procedure
			},
			{
				Query: "select * from t as of `HEAD~5`",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
		},
	},
}