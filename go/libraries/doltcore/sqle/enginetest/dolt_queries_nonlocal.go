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
	"github.com/dolthub/go-mysql-server/testutils"
)

var _ testutils.CustomValueValidator = &doltCommitValidator{}

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
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
				("nonlocal_table", "main", "immediate")`,
			"CALL DOLT_COMMIT('-Am', 'add nonlocal rule')",
			"CALL DOLT_BRANCH('other')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				Query:          "CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);",
				ExpectedErrStr: "Cannot create table name nonlocal_table because it matches a name present in dolt_nonlocal_tables.",
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/11995
	{
		Name: "creating a table matching a nonlocal table rule is only allowed on the rule's target branch",
		SetUpScript: []string{
			`INSERT INTO dolt_nonlocal_tables (table_name, target_ref, options) VALUES
				('global_*', 'main', 'immediate')`,
			"CALL DOLT_COMMIT('-Am', 'add nonlocal rule');",
			"CALL DOLT_BRANCH('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CREATE TABLE global_a (id INT PRIMARY KEY);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE global_b (id INT, CONSTRAINT pk_b PRIMARY KEY (id));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				Query:          "CREATE TABLE global_c (id INT PRIMARY KEY);",
				ExpectedErrStr: "Cannot create table name global_c because it matches a name present in dolt_nonlocal_tables.",
			},
			{
				Query:          "CREATE TABLE global_d (id INT, CONSTRAINT pk_d PRIMARY KEY (id));",
				ExpectedErrStr: "Cannot create table name global_d because it matches a name present in dolt_nonlocal_tables.",
			},
		},
	},
	{
		Name: "creating a table matching a nonlocal table rule with ref_table set or a frozen tag target is rejected on the target branch",
		SetUpScript: []string{
			"CALL DOLT_TAG('v1')",
			`INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
				("renamed_*", "main", "other_name", "immediate"),
				("tagged_*", "v1", "", "immediate")`,
			"CALL DOLT_COMMIT('-Am', 'add nonlocal rules')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CREATE TABLE renamed_a (id INT PRIMARY KEY);",
				ExpectedErrStr: "Cannot create table name renamed_a because it matches a name present in dolt_nonlocal_tables.",
			},
			{
				Query:          "CREATE TABLE tagged_a (id INT PRIMARY KEY);",
				ExpectedErrStr: "Cannot create table name tagged_a because it matches a name present in dolt_nonlocal_tables.",
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
	{
		// https://github.com/dolthub/dolt/issues/10462
		Name: "nonlocal table is not affected by dolt_clean()",
		SetUpScript: []string{
			"CREATE TABLE global_test (id int auto_increment primary key, name varchar(100));",
			"INSERT INTO global_test (id, name) VALUES (1, 'one');",
			"CREATE TABLE foo (id int auto_increment primary key);",
			"INSERT INTO dolt_nonlocal_tables (table_name, target_ref, options) VALUES ('global_*', 'main', 'immediate');",
			"CALL dolt_add('dolt_nonlocal_tables');",
			"CALL dolt_commit('-m', 'set up dolt_nonlocal_tables');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM global_test;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    "CALL dolt_clean();",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * FROM global_test;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{{"global_test"}},
			},
			{
				Query:    "CALL dolt_clean('-x')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * FROM global_test;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{{"global_test"}},
			},
		},
	},
}
