// Copyright 2022 Dolthub, Inc.
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

var ForeignKeyBranchTests = []queries.ScriptTest{
	{
		Name:         "create fk on branch",
		SetUpScript:  []string {
			"call dolt_branch('b1')",
			"use mydb/b1",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "use mydb/b1",
				SkipResultsCheck: false,
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `v1` int,\n" +
					"  `v2` int,\n" +
					"  PRIMARY KEY (`id`),\n" +
					"  KEY `v1` (`v1`),\n" +
					"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:            "use mydb/main",
				SkipResultsCheck: false,
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `v1` int,\n" +
						"  `v2` int,\n" +
						"  PRIMARY KEY (`id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into child values (1, 1, 1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:            "insert into `mydb/b1`.child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name:         "create fk with branch checkout",
		SetUpScript:  []string {
			"call dolt_branch('b1')",
			"call dolt_checkout('b1')",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            			"call dolt_checkout('b1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `v1` int,\n" +
						"  `v2` int,\n" +
						"  PRIMARY KEY (`id`),\n" +
						"  KEY `v1` (`v1`),\n" +
						"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into child values (1, 1, 1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:            			"call dolt_checkout('main')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `v1` int,\n" +
						"  `v2` int,\n" +
						"  PRIMARY KEY (`id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into child values (1, 1, 1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
	{
		Name:         "create fk on branch not being used",
		SetUpScript:  []string {
			"call dolt_branch('b1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            			"ALTER TABLE `mydb/b1`.child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
				Skip: true, // Incorrectly flagged as a cross-DB foreign key relation
			},
			{
				Query:    "SHOW CREATE TABLE `mydb/b1`.child;",
				Skip: true,
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `v1` int,\n" +
						"  `v2` int,\n" +
						"  PRIMARY KEY (`id`),\n" +
						"  KEY `v1` (`v1`),\n" +
						"  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into `mydb/b1`.child values (1, 1, 1)",
				Skip: true,
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Skip: true,
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `v1` int,\n" +
						"  `v2` int,\n" +
						"  PRIMARY KEY (`id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:            "insert into child values (1, 1, 1)",
				Skip: true,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
}
