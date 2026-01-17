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
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ enginetest.CustomValueValidator = &doltCommitValidator{}

var NonlocalScripts = []queries.ScriptTest{
	{
		Name: "basic nonlocal tables use case",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('other')",
			"CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);",
			"INSERT INTO aliased_table VALUES ('amzmapqt');",
			"CALL dolt_checkout('other');",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
				('nonlocal_table', 'main', 'aliased_table', 'immediate')`,
			`INSERT INTO nonlocal_table VALUES ('eesekkgo');`,
			`CREATE TABLE local_table (pk char(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from nonlocal_table;",
				Expected: []sql.Row{{"amzmapqt"}, {"eesekkgo"}},
			},
			{
				Query:    "select * from `mydb/main`.aliased_table;",
				Expected: []sql.Row{{"amzmapqt"}, {"eesekkgo"}},
			},
			{
				Query:    "show create table nonlocal_table;",
				Expected: []sql.Row{{"aliased_table", "CREATE TABLE `aliased_table` (\n  `pk` char(8) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "show create table local_table;",
				Expected: []sql.Row{{"local_table", "CREATE TABLE `local_table` (\n  `pk` char(8) NOT NULL,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `local_table_ibfk_1` FOREIGN KEY (`pk`) REFERENCES `nonlocal_table` (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       `INSERT INTO local_table VALUES ("amzmapqt");`,
				ExpectedErr: nil,
			},
			{
				Query:          `INSERT INTO local_table VALUES ("fdnfjfjf");`,
				ExpectedErrStr: "cannot add or update a child row - Foreign key violation on fk: `local_table_ibfk_1`, table: `local_table`, referenced table: `nonlocal_table`, key: `[fdnfjfjf]`",
			},
			{
				Query:    `CALL DOLT_VERIFY_CONSTRAINTS('--all');`,
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "detect foreign key invalidation is detected when rows are removed",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('other')",
			"CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);",
			"INSERT INTO aliased_table VALUES ('amzmapqt');",
			"CALL dolt_checkout('other');",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
				('nonlocal_table', 'main', 'aliased_table', 'immediate')`,
			`CREATE TABLE local_table (pk char(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));`,
			"INSERT INTO local_table VALUES ('amzmapqt');",
			"DELETE FROM `mydb/main`.aliased_table;",
			"set @@dolt_force_transaction_commit=1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT violation_type FROM dolt_constraint_violations_local_table",
				Expected: []sql.Row{{"foreign key"}},
			},
			{
				// Check that neither command removed the FK relation (this can happen if it thinks the child table was dropped)
				Query:    "SHOW CREATE TABLE local_table;",
				Expected: []sql.Row{{"local_table", "CREATE TABLE `local_table` (\n  `pk` char(8) NOT NULL,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `local_table_ibfk_1` FOREIGN KEY (`pk`) REFERENCES `nonlocal_table` (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "detect foreign key invalidation is detected when the nonlocal table is dropped",
		// DOLT_VERIFY_CONSTRAINTS detects constraint violations by attempting a merge against HEAD.
		// The current behavior for merges is to delete FK constraints if a table doesn't exist after the merge.
		// This is a bug with DOLT_VERIFY_CONSTRAINTS, not with nonlocal_tables. As a workaround,
		// the `dolt constraints verify` CLI command can detect these violations, which we confirm via nonlocal.bats
		Skip: true,
		SetUpScript: []string{
			"CREATE DATABASE IF NOT EXISTS mydb",
			"USE mydb",
			"CALL DOLT_BRANCH('other')",
			"CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);",
			"INSERT INTO aliased_table VALUES ('amzmapqt');",
			"CALL dolt_checkout('other');",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
				('nonlocal_table', 'main', 'aliased_table', 'immediate')`,
			`CREATE TABLE local_table (pk char(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));`,
			"INSERT INTO local_table VALUES ('amzmapqt');",
			"DROP TABLE `mydb/main`.aliased_table;",
			"set @@dolt_force_transaction_commit=1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT violation_type FROM dolt_constraint_violations_local_table",
				Expected: []sql.Row{{"foreign key"}},
			},
			{
				// Check that neither command removed the FK relation (this can happen if it thinks the child table was dropped)
				Query:    "SHOW CREATE TABLE local_table;",
				Expected: []sql.Row{{"local_table", "CREATE TABLE `local_table` (\n  `pk` char(8) NOT NULL,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `local_table_ibfk_1` FOREIGN KEY (`pk`) REFERENCES `nonlocal_table` (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "creating a table matching a nonlocal table rule results in an error",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('other')",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
				("nonlocal_table", "main", "immediate")`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);",
				ExpectedErrStr: "Cannot create table name nonlocal_table because it matches a name present in dolt_nonlocal_tables.",
			},
		},
	},
	{
		Name: "nonlocal tables appear in 'show tables'",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('other')",
			"CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);",
			"CREATE TABLE table_alias_1 (pk char(8) PRIMARY KEY);",
			"CREATE TABLE table_alias_wild_3 (pk char(8) PRIMARY KEY);",
			"INSERT INTO aliased_table VALUES ('amzmapqt');",
			"CALL dolt_checkout('other');",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
				("table_alias_1", "main", "", "immediate"),
				("table_alias_2", "main", "aliased_table", "immediate"),
				("table_alias_wild_*", "main", "", "immediate"),
				("table_alias_missing", "main", "", "immediate");`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show tables",
				Expected: []sql.Row{{"table_alias_1"}, {"table_alias_2"}, {"table_alias_wild_3"}},
			},
		},
	},
	{
		Name: "detect invalid options",
		SetUpScript: []string{
			"CALL dolt_checkout('-b', 'other');",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
				("nonlocal_table", "main", "invalid");`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "select * from nonlocal_table;",
				ExpectedErrStr: "Invalid nonlocal table options invalid: only valid value is 'immediate'.",
			},
		},
	},
	{
		Name: "nonlocal table appears once in show tables when local table exists",
		SetUpScript: []string{
			"CREATE TABLE foo (id int auto_increment primary key);",
			"CALL dolt_commit('-Am', 'create table foo');",
			"CALL dolt_branch('test');",
			"INSERT INTO foo values (1);",
			`INSERT INTO dolt_nonlocal_tables (table_name, target_ref, options) VALUES ('foo', 'main', 'immediate');`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"foo"}},
			},
			{
				Query: "call dolt_checkout('test');",
			},
			// TODO(elianddb): Add an indicator that the current local table (not part of the target reference for the
			//  non-local pattern) is currently shadowed. This would provide actionable feedback, but for now only show
			//  a singular name for the non-local table, as it's the only queryable one.
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"foo"}},
			},
		},
	},
}
