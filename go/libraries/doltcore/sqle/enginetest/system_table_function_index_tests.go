// Copyright 2023 Dolthub, Inc.
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

var SystemTableFunctionIndexTests = []queries.ScriptTest{
	{
		Name: "test DOLT_PATCH() indexes",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values(1, 'one', 'two');",
			"set @Commit0 = HashOf('HEAD');",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'commit one');",
			"set @Commit1 = HashOf('HEAD');",
			"create table diff_type_name(name varchar(20), t varchar(20));",
			"insert into diff_type_name values ('s', 'schema'), ('d', 'data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT table_name, diff_type, statement from dolt_patch(@Commit0, @Commit1, 't') WHERE diff_type = 'schema';",
				Expected: []sql.UntypedSqlRow{
					{"t", "schema", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT table_name, diff_type, statement from dolt_patch(@Commit0, @Commit1, 't') WHERE diff_type = 'data';",
				Expected: []sql.UntypedSqlRow{
					{"t", "data", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(diff_type_name,dolt_patch) LOOKUP_JOIN(diff_type_name,dolt_patch) */ name, t, table_name, statement from diff_type_name join dolt_patch(@Commit0, @Commit1, 't') on diff_type_name.t = diff_type;",
				Expected: []sql.UntypedSqlRow{
					{"s", "schema", "t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"d", "data", "t", "INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
			{
				Query: "SELECT ( SELECT statement FROM (SELECT * FROM dolt_patch(@Commit0, @Commit1, 't') where diff_type = diff_type_name.t) as rhs) from diff_type_name;",
				Expected: []sql.UntypedSqlRow{
					{"CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `c1` varchar(20),\n  `c2` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"},
					{"INSERT INTO `t` (`pk`,`c1`,`c2`) VALUES (1,'one','two');"},
				},
				ExpectedIndexes: []string{"diff_type"},
			},
		},
	},
}
