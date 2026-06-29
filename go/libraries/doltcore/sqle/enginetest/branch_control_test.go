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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
)

// BranchControlTest is used to define a test using the branch control system. The root account is used with any queries
// in the SetUpScript.
type BranchControlTest struct {
	Name        string
	SetUpScript []string
	Assertions  []BranchControlTestAssertion
}

// BranchControlTestAssertion is within a BranchControlTest to assert functionality.
type BranchControlTestAssertion struct {
	ExpectedErr    *errors.Kind
	User           string
	Host           string
	Query          string
	ExpectedErrStr string
	Expected       []sql.Row
}

// BranchControlBlockTest are tests for quickly verifying that a command is blocked before the appropriate entry is
// added to the "dolt_branch_control" table. The `TestUserSetUpScripts` are automatically run before every test,
// therefore any set up here is essentially appended to `TestUserSetUpScripts`. In addition, the test user is
// `testuser`@`localhost`.
type BranchControlBlockTest struct {
	ExpectedErr *errors.Kind
	Name        string
	Query       string
	SkipMessage string
	SetUpScript []string
}

// TestUserSetUpScripts creates a user named "testuser@localhost", and grants them privileges on all databases and
// tables. In addition, creates a committed table named "test" with a single value, along with a mirror branch named
// "other".
var TestUserSetUpScripts = []string{
	"DELETE FROM dolt_branch_control WHERE user = '%';",
	"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
	"CREATE USER testuser@localhost;",
	"GRANT ALL ON *.* TO testuser@localhost;",
	"REVOKE SUPER ON *.* FROM testuser@localhost;",
	"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
	"INSERT INTO test VALUES (1, 1);",
	"CALL DOLT_ADD('-A');",
	"CALL DOLT_COMMIT('-m', 'setup commit');",
	"CALL DOLT_BRANCH('other');",
}

var BranchControlBlockTests = []BranchControlBlockTest{
	{
		Name:        "INSERT",
		Query:       "INSERT INTO test VALUES (2, 2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "INSERT on branch db",
		Query:       "INSERT INTO `mydb/other`.test VALUES (2, 2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "REPLACE",
		Query:       "REPLACE INTO test VALUES (2, 2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "UPDATE",
		Query:       "UPDATE test SET pk = 2;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "UPDATE on branch db",
		Query:       "UPDATE `mydb/other`.test SET pk = 2;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DELETE",
		Query:       "DELETE FROM test WHERE pk >= 0;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DELETE from branch table",
		Query:       "DELETE FROM `mydb/other`.test WHERE pk >= 0;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "TRUNCATE",
		Query:       "TRUNCATE TABLE test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE AUTO_INCREMENT",
		SetUpScript: []string{
			"CREATE TABLE test2(pk BIGINT PRIMARY KEY AUTO_INCREMENT);",
		},
		Query:       "ALTER TABLE test2 AUTO_INCREMENT = 20;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ADD CHECK",
		Query:       "ALTER TABLE test ADD CONSTRAINT check_1 CHECK (pk > 0);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP CHECK",
		SetUpScript: []string{
			"ALTER TABLE test ADD CONSTRAINT check_1 CHECK (pk > 0);",
		},
		Query:       "ALTER TABLE test DROP CHECK check_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ALTER COLUMN SET DEFAULT",
		Query:       "ALTER TABLE test ALTER COLUMN v1 SET DEFAULT (5);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ALTER COLUMN DROP DEFAULT",
		SetUpScript: []string{
			"ALTER TABLE test ALTER COLUMN v1 SET DEFAULT (5);",
		},
		Query:       "ALTER TABLE test ALTER COLUMN v1 DROP DEFAULT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ADD FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE test ADD INDEX idx_v1 (v1);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
		},
		Query:       "ALTER TABLE test2 ADD CONSTRAINT fk_1 FOREIGN KEY (v1) REFERENCES test (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE test ADD INDEX idx_v1 (v1);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE, CONSTRAINT fk_1 FOREIGN KEY (v1) REFERENCES test (v1));",
		},
		Query:       "ALTER TABLE test2 DROP FOREIGN KEY fk_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ADD INDEX",
		Query:       "ALTER TABLE test ADD INDEX idx_v1 (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP INDEX",
		SetUpScript: []string{
			"ALTER TABLE test ADD INDEX idx_v1 (v1);",
		},
		Query:       "ALTER TABLE test DROP INDEX idx_v1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE RENAME INDEX",
		SetUpScript: []string{
			"ALTER TABLE test ADD INDEX idx_v1 (v1);",
		},
		Query:       "ALTER TABLE test RENAME INDEX idx_v1 TO idx_v1_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ADD PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test2 (v1 BIGINT, v2 BIGINT);",
		},
		Query:       "ALTER TABLE test2 ADD PRIMARY KEY (v1, v2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE DROP PRIMARY KEY",
		Query:       "ALTER TABLE test DROP PRIMARY KEY;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE RENAME",
		Query:       "ALTER TABLE test RENAME TO test_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "RENAME TABLE",
		Query:       "RENAME TABLE test TO test_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ADD COLUMN",
		Query:       "ALTER TABLE test ADD COLUMN v2 BIGINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE DROP COLUMN",
		Query:       "ALTER TABLE test DROP COLUMN v1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE CHANGE COLUMN",
		Query:       "ALTER TABLE test CHANGE COLUMN v1 v1_new BIGINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE MODIFY COLUMN",
		Query:       "ALTER TABLE test MODIFY COLUMN v1 TINYINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE RENAME COLUMN",
		Query:       "ALTER TABLE test RENAME COLUMN v1 TO v1_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE INDEX",
		Query:       "CREATE INDEX idx_v1 ON test (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP INDEX",
		SetUpScript: []string{
			"CREATE INDEX idx_v1 ON test (v1);",
		},
		Query:       "DROP INDEX idx_v1 ON test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE VIEW",
		Query:       "CREATE VIEW view_1 AS SELECT * FROM TEST;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP VIEW",
		SetUpScript: []string{
			"CREATE VIEW view_1 AS SELECT * FROM TEST;",
		},
		Query:       "DROP VIEW view_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE TRIGGER",
		Query:       "CREATE TRIGGER trigger_1 BEFORE INSERT ON test FOR EACH ROW SET NEW.v1 = 4;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP TRIGGER",
		SetUpScript: []string{
			"CREATE TRIGGER trigger_1 BEFORE INSERT ON test FOR EACH ROW SET NEW.v1 = 4;",
		},
		Query:       "DROP TRIGGER trigger_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE TABLE",
		Query:       "CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE TABLE LIKE",
		Query:       "CREATE TABLE test2 LIKE test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE TABLE AS SELECT",
		Query:       "CREATE TABLE test2 AS SELECT * FROM test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DROP TABLE",
		Query:       "DROP TABLE test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE PROCEDURE",
		Query:       "CREATE PROCEDURE testabc(x DOUBLE, y DOUBLE) SELECT x*y;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP PROCEDURE",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(x DOUBLE, y DOUBLE) SELECT x*y;",
		},
		Query:       "DROP PROCEDURE testabc;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "INSERT SELECT",
		Query:       "INSERT INTO test SELECT pk + 100, v1 FROM test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "INSERT ON DUPLICATE KEY UPDATE",
		Query:       "INSERT INTO test VALUES (1, 1) ON DUPLICATE KEY UPDATE v1 = 50;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "UPDATE with JOIN",
		Query:       "UPDATE test t1 JOIN test t2 ON t1.pk = t2.pk SET t1.v1 = 5;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DELETE with JOIN",
		Query:       "DELETE t1 FROM test t1 JOIN test t2 ON t1.pk = t2.pk;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE CONVERT TO CHARACTER SET",
		Query:       "ALTER TABLE test CONVERT TO CHARACTER SET utf8mb4;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE EVENT",
		Query:       "CREATE EVENT ev1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO test VALUES (99, 99);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP EVENT",
		SetUpScript: []string{
			"CREATE EVENT ev1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO test VALUES (99, 99);",
		},
		Query:       "DROP EVENT ev1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER EVENT",
		SetUpScript: []string{
			"CREATE EVENT ev1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO test VALUES (99, 99);",
		},
		Query:       "ALTER EVENT ev1 DISABLE;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	// Dolt Procedures
	{
		Name: "DOLT_ADD",
		SetUpScript: []string{
			"INSERT INTO test VALUES (2, 2);",
		},
		Query:       "CALL DOLT_ADD('-A');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{ // Normal DOLT_BRANCH is tested in BranchControlTests
		Name:        "DOLT_BRANCH Force Copy",
		Query:       "CALL DOLT_BRANCH('-f', '-c', 'main', 'other');",
		ExpectedErr: branch_control.ErrCannotDeleteBranch,
	},
	{
		Name: "DOLT_BRANCH Force Move",
		SetUpScript: []string{
			"INSERT INTO dolt_branch_control VALUES ('%', 'newother', 'testuser', 'localhost', 'write');",
		},
		Query:       "CALL DOLT_BRANCH('-f', '-m', 'other', 'newother');",
		ExpectedErr: branch_control.ErrCannotDeleteBranch,
	},
	{
		Name:        "DOLT_BRANCH Delete",
		Query:       "CALL DOLT_BRANCH('-d', 'other');",
		ExpectedErr: branch_control.ErrCannotDeleteBranch,
	},
	{
		Name:        "DOLT_CLEAN",
		Query:       "CALL DOLT_CLEAN();",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DOLT_COMMIT",
		SetUpScript: []string{
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
		},
		Query:       "CALL DOLT_COMMIT('-m', 'message');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DOLT_CONFLICTS_RESOLVE",
		Query:       "CALL DOLT_CONFLICTS_RESOLVE('--ours', '.');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DOLT_MERGE",
		SetUpScript: []string{
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'message');",
			"CALL DOLT_CHECKOUT('other');",
		},
		Query:       "CALL DOLT_MERGE('main');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DOLT_RESET",
		Query:       "CALL DOLT_RESET();",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DOLT_REVERT",
		Query:       "CALL DOLT_REVERT('HEAD');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DOLT_VERIFY_CONSTRAINTS",
		Query:       "CALL DOLT_VERIFY_CONSTRAINTS('-a');",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
}

var BranchControlOtherDbBlockTests = []BranchControlBlockTest{
	{
		Name:        "INSERT",
		Query:       "INSERT INTO `mydb/other`.test VALUES (2, 2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "REPLACE",
		Query:       "REPLACE INTO `mydb/other`.test VALUES (2, 2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "UPDATE",
		Query:       "UPDATE `mydb/other`.test SET pk = 2;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DELETE",
		Query:       "DELETE FROM `mydb/other`.test WHERE pk >= 0;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "TRUNCATE",
		Query:       "TRUNCATE TABLE `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE AUTO_INCREMENT",
		SetUpScript: []string{
			"CREATE TABLE `mydb/other`.test2(pk BIGINT PRIMARY KEY AUTO_INCREMENT);",
		},
		Query:       "ALTER TABLE `mydb/other`.test2 AUTO_INCREMENT = 20;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ADD CHECK",
		Query:       "ALTER TABLE `mydb/other`.test ADD CONSTRAINT check_1 CHECK (pk > 0);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP CHECK",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ADD CONSTRAINT check_1 CHECK (pk > 0);",
		},
		Query:       "ALTER TABLE `mydb/other`.test DROP CHECK check_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ALTER COLUMN SET DEFAULT",
		Query:       "ALTER TABLE `mydb/other`.test ALTER COLUMN v1 SET DEFAULT (5);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ALTER COLUMN DROP DEFAULT",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ALTER COLUMN v1 SET DEFAULT (5);",
		},
		Query:       "ALTER TABLE `mydb/other`.test ALTER COLUMN v1 DROP DEFAULT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ADD FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ADD INDEX idx_v1 (v1);",
			"CREATE TABLE `mydb/other`.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
		},
		Query:       "ALTER TABLE `mydb/other`.test2 ADD CONSTRAINT fk_1 FOREIGN KEY (v1) REFERENCES `mydb/other`.test (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ADD INDEX idx_v1 (v1);",
			"CREATE TABLE `mydb/other`.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE, CONSTRAINT fk_1 FOREIGN KEY (v1) REFERENCES `mydb/other`.test (v1));",
		},
		Query:       "ALTER TABLE `mydb/other`.test2 DROP FOREIGN KEY fk_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE ADD INDEX",
		Query:       "ALTER TABLE `mydb/other`.test ADD INDEX idx_v1 (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE DROP INDEX",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ADD INDEX idx_v1 (v1);",
		},
		Query:       "ALTER TABLE `mydb/other`.test DROP INDEX idx_v1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE RENAME INDEX",
		SetUpScript: []string{
			"ALTER TABLE `mydb/other`.test ADD INDEX idx_v1 (v1);",
		},
		Query:       "ALTER TABLE `mydb/other`.test RENAME INDEX idx_v1 TO idx_v1_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "ALTER TABLE ADD PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE `mydb/other`.test2 (v1 BIGINT, v2 BIGINT);",
		},
		Query:       "ALTER TABLE `mydb/other`.test2 ADD PRIMARY KEY (v1, v2);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE DROP PRIMARY KEY",
		Query:       "ALTER TABLE `mydb/other`.test DROP PRIMARY KEY;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE RENAME",
		Query:       "ALTER TABLE `mydb/other`.test RENAME TO test_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name:        "RENAME TABLE",
		Query:       "RENAME TABLE `mydb/other`.test TO test_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name:        "ALTER TABLE ADD COLUMN",
		Query:       "ALTER TABLE `mydb/other`.test ADD COLUMN v2 BIGINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE DROP COLUMN",
		Query:       "ALTER TABLE `mydb/other`.test DROP COLUMN v1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE CHANGE COLUMN",
		Query:       "ALTER TABLE `mydb/other`.test CHANGE COLUMN v1 v1_new BIGINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE MODIFY COLUMN",
		Query:       "ALTER TABLE `mydb/other`.test MODIFY COLUMN v1 TINYINT;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "ALTER TABLE RENAME COLUMN",
		Query:       "ALTER TABLE `mydb/other`.test RENAME COLUMN v1 TO v1_new;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE INDEX",
		Query:       "CREATE INDEX idx_v1 ON `mydb/other`.test (v1);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name: "DROP INDEX",
		SetUpScript: []string{
			"CREATE INDEX idx_v1 ON `mydb/other`.test (v1);",
		},
		Query:       "DROP INDEX idx_v1 ON `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE VIEW",
		Query:       "CREATE VIEW view_1 AS SELECT * FROM `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name: "DROP VIEW",
		SetUpScript: []string{
			"CREATE VIEW view_1 AS SELECT * FROM `mydb/other`.test;",
		},
		Query:       "DROP VIEW view_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name:        "CREATE TRIGGER",
		Query:       "CREATE TRIGGER trigger_1 BEFORE INSERT ON `mydb/other`.test FOR EACH ROW SET NEW.v1 = 4;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name: "DROP TRIGGER",
		SetUpScript: []string{
			"CREATE TRIGGER trigger_1 BEFORE INSERT ON `mydb/other`.test FOR EACH ROW SET NEW.v1 = 4;",
		},
		Query:       "DROP TRIGGER `mydb/other`.trigger_1;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name:        "CREATE TABLE",
		Query:       "CREATE TABLE `mydb/other`.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "CREATE TABLE LIKE",
		Query:       "CREATE TABLE `mydb/other`.test2 LIKE `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
		SkipMessage: "https://github.com/dolthub/dolt/issues/6078",
	},
	{
		Name:        "CREATE TABLE AS SELECT",
		Query:       "CREATE TABLE `mydb/other`.test2 AS SELECT * FROM `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
	{
		Name:        "DROP TABLE",
		Query:       "DROP TABLE `mydb/other`.test;",
		ExpectedErr: branch_control.ErrIncorrectPermissions,
	},
}

var BranchControlTests = []BranchControlTest{
	{
		Name: "Namespace entries block",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{ // Empty table, all branches are allowed
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('otherbranch1');",
				Expected: []sql.Row{{0}},
			},
			{ // Prefix "other" is now locked by root
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'other%', 'root', 'localhost');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BRANCH('otherbranch2');",
				ExpectedErr: branch_control.ErrCannotCreateBranch,
			},
			{ // Allow testuser to use the "other" prefix
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'other%', 'testuser', 'localhost');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('otherbranch2');",
				Expected: []sql.Row{{0}},
			},
			{ // Create a longer match, which takes precedence over shorter matches
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'otherbranch%', 'root', 'localhost');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{ // Matches both "other%" and "otherbranch%", but "otherbranch%" wins by being the longer match
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BRANCH('otherbranch3');",
				ExpectedErr: branch_control.ErrCannotCreateBranch,
			},
			{ // This doesn't match the longer rule, so testuser has access the namespace
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('other3');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'otherbranch%', 'testuser', 'localhost');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('otherbranch3');",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Require admin to modify tables",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER a@localhost;",
			"CREATE USER b@localhost;",
			"GRANT ALL ON *.* TO a@localhost;",
			"REVOKE SUPER ON *.* FROM a@localhost;",
			"GRANT ALL ON *.* TO b@localhost;",
			"REVOKE SUPER ON *.* FROM b@localhost;",
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'a', 'localhost', 'write'), ('%', 'prefix%', 'a', 'localhost', 'admin')",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:        "a",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_branch_control WHERE branch = 'other';",
				ExpectedErr: branch_control.ErrDeletingRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_branch_control WHERE branch = 'other';",
				ExpectedErr: branch_control.ErrDeletingRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_branch_control WHERE branch = 'prefix%';",
				ExpectedErr: branch_control.ErrDeletingRow,
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('%', 'prefix1%', 'b', 'localhost', 'write');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_branch_control WHERE branch = 'prefix1%';",
				ExpectedErr: branch_control.ErrDeletingRow,
			},
			{ // Must have permission on the new name as well
				User:        "a",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_control SET branch = 'other1' WHERE user = 'b' AND branch = 'prefix1%';",
				ExpectedErr: branch_control.ErrUpdatingToRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_control SET permissions = 'admin' WHERE user = 'b' AND branch = 'prefix1%';",
				ExpectedErr: branch_control.ErrUpdatingRow,
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "UPDATE dolt_branch_control SET permissions = 'admin' WHERE user = 'b' AND branch = 'prefix1%';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				User:  "b",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE branch = 'prefix1%';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix1%', 'b', 'localhost', 'admin');",
				ExpectedErr: branch_control.ErrInsertingAccessRow,
			},
			{ // Since "a" has admin on "prefix%", they can also insert into the namespace table
				User:  "a",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'prefix___', 'a', 'localhost');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_namespace_control VALUES ('%', 'prefix', 'b', 'localhost');",
				ExpectedErr: branch_control.ErrInsertingNamespaceRow,
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "UPDATE dolt_branch_namespace_control SET branch = 'prefix%';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				User:        "a",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_namespace_control SET branch = 'other';",
				ExpectedErr: branch_control.ErrUpdatingToRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_branch_namespace_control WHERE branch = 'prefix%';",
				ExpectedErr: branch_control.ErrDeletingRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_namespace_control SET branch = 'anything';",
				ExpectedErr: branch_control.ErrUpdatingRow,
			},
		},
	},
	{
		Name: "Deleting entries works",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost_1', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost_2', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost_3', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost_4', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost_5', 'write');",
			"DELETE FROM dolt_branch_control WHERE host IN ('localhost_2', 'localhost_3');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "%", "testuser", "localhost_1", "write"},
					{"%", "%", "testuser", "localhost", "write"},
					{"%", "%", "testuser", "localhost_4", "write"},
					{"%", "%", "testuser", "localhost_5", "write"},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "INSERT INTO test VALUES (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE host = 'localhost_5';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "%", "testuser", "localhost_1", "write"},
					{"%", "%", "testuser", "localhost", "write"},
					{"%", "%", "testuser", "localhost_4", "write"},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "INSERT INTO test VALUES (2);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE host = 'localhost_1';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "%", "testuser", "localhost", "write"},
					{"%", "%", "testuser", "localhost_4", "write"},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "INSERT INTO test VALUES (3);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE host = 'localhost_4';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "%", "testuser", "localhost", "write"},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE user = 'testuser' AND host = 'localhost';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (5);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control;",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', '%', 'admin');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{
					{"%", "%", "root", "%", "admin"},
				},
			},
		},
	},
	{
		Name: "Subset entries count as duplicates",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"INSERT INTO dolt_branch_control VALUES ('%', 'prefix', 'testuser', 'localhost', 'admin');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'prefix1%', 'testuser', 'localhost', 'admin');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'prefix2_', 'testuser', 'localhost', 'admin');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'prefix3_', 'testuser', 'localhost', 'admin');",
		},
		Assertions: []BranchControlTestAssertion{
			{ // The pre-existing "prefix1%" entry will cover ALL possible matches of "prefix1sub%", so we treat it as a duplicate
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix1sub%', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{ // The ending "%" fully covers "_", so we also treat it as a duplicate
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix1_', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{ // This is the reverse of the above case, so this is NOT a duplicate (although the original is now a subset)
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('%', 'prefix2%', 'testuser', 'localhost', 'admin');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "prefix", "testuser", "localhost", "admin"},
					{"%", "prefix1%", "testuser", "localhost", "admin"},
					{"%", "prefix2_", "testuser", "localhost", "admin"},
					{"%", "prefix2%", "testuser", "localhost", "admin"},
					{"%", "prefix3_", "testuser", "localhost", "admin"},
				},
			},
			{ // Sanity checks to ensure that straight-up duplicates are also caught
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix1%', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'prefix3_', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{ // Verify that creating branches also skips adding an entry if it would be a subset
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'root';",
				Expected: []sql.Row{
					{"%", "%", "root", "localhost", "admin"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('new_root_branch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'root';",
				Expected: []sql.Row{
					{"%", "%", "root", "localhost", "admin"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10352
		Name: "Rebase respects existing branch control privileges",
		SetUpScript: []string{
			"create table tbl (i int);",
			"insert into tbl values (42);",
			"call dolt_commit('-A','-m', 'initial data');",
			"call dolt_branch('branch1');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%','%','root','%', 'admin');",
			"INSERT INTO dolt_branch_control VALUES ('%','branch1','testuser','%', 'write');",
			"call dolt_commit('--allow-empty', '-m', 'empty');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control WHERE user = 'testuser' and branch = 'branch1';",
				Expected: []sql.Row{{"%", "branch1", "testuser", "%", "write"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "call dolt_checkout('branch1');",
				Expected: []sql.Row{{0, "Switched to branch 'branch1'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "insert into tbl values (23);",
				Expected: []sql.Row{{types.OkResult{Info: nil, RowsAffected: 0x1, InsertID: 0x0}}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "call dolt_commit('-a', '-m', 'user1 change');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "call dolt_rebase('--interactive', 'main');",
				Expected: []sql.Row{
					{0, "interactive rebase started on branch dolt_rebase_branch1; adjust the rebase plan in the dolt_rebase table, then continue rebasing by calling dolt_rebase('--continue')"},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "call dolt_rebase('--continue');",
				Expected: []sql.Row{{0, "Successfully rebased and updated refs/heads/branch1"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control WHERE user = 'testuser' and branch = 'branch1';",
				Expected: []sql.Row{{"%", "branch1", "testuser", "%", "write"}},
			},
		},
	},
	{
		Name: "Creating branch creates new entry",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('otherbranch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"mydb", "otherbranch", "testuser", "localhost", "admin"},
				},
			},
		},
	},
	{
		Name: "Renaming branch creates new entry",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"CALL DOLT_BRANCH('otherbranch');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'otherbranch', 'testuser', 'localhost', 'write');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "otherbranch", "testuser", "localhost", "write"},
				},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BRANCH('-f', '-m', 'otherbranch', 'newbranch');",
				ExpectedErr: branch_control.ErrCannotDeleteBranch,
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('-m', 'otherbranch', 'newbranch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "otherbranch", "testuser", "localhost", "write"},  // Original entry remains
					{"mydb", "newbranch", "testuser", "localhost", "admin"}, // New entry is scoped specifically to db
				},
			},
		},
	},
	{
		Name: "Copying branch creates new entry",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"CALL DOLT_BRANCH('otherbranch');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BRANCH('-f', '-c', 'otherbranch', 'newbranch');",
				ExpectedErr: branch_control.ErrCannotDeleteBranch,
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('-c', 'otherbranch', 'newbranch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"mydb", "newbranch", "testuser", "localhost", "admin"},
				},
			},
		},
	},
	{
		Name: "Proper database scoping",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin')," +
				"('dba', 'main', 'testuser', 'localhost', 'write'), ('dbb', 'other', 'testuser', 'localhost', 'write');",
			"CREATE DATABASE dba;", // Implicitly creates "main" branch
			"CREATE DATABASE dbb;", // Implicitly creates "main" branch
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"USE dba;",
			"CALL DOLT_BRANCH('other');",
			"USE dbb;",
			"CALL DOLT_BRANCH('other');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "USE dba;",
				Expected: []sql.Row{},
			},
			{ // On "dba"."main", which we have permissions for
				User:  "testuser",
				Host:  "localhost",
				Query: "CREATE TABLE test (pk BIGINT PRIMARY KEY);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "DROP TABLE test;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{ // On "dba"."other", which we do not have permissions for
				User:        "testuser",
				Host:        "localhost",
				Query:       "CREATE TABLE test (pk BIGINT PRIMARY KEY);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "USE dbb;",
				Expected: []sql.Row{},
			},
			{ // On "dbb"."main", which we do not have permissions for
				User:        "testuser",
				Host:        "localhost",
				Query:       "CREATE TABLE test (pk BIGINT PRIMARY KEY);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{ // On "dbb"."other", which we do not have permissions for
				User:  "testuser",
				Host:  "localhost",
				Query: "CREATE TABLE test (pk BIGINT PRIMARY KEY);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name: "Admin privileges do not give implicit branch permissions",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			// Even though root already has all privileges, this makes the test logic a bit more explicit
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost WITH GRANT OPTION;",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CREATE TABLE test (pk BIGINT PRIMARY KEY);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BRANCH('-m', 'main', 'newbranch');",
				ExpectedErr: branch_control.ErrCannotDeleteBranch,
			},
			{ // Anyone can create a branch as long as it's not blocked by dolt_branch_namespace_control
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('newbranch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"mydb", "newbranch", "testuser", "localhost", "admin"},
				},
			},
		},
	},
	{
		Name: "Database-level admin privileges allow scoped table modifications",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE DATABASE dba;",
			"CREATE DATABASE dbb;",
			"CREATE USER a@localhost;",
			"GRANT ALL ON dba.* TO a@localhost WITH GRANT OPTION;",
			"CREATE USER b@localhost;",
			"GRANT ALL ON dbb.* TO b@localhost WITH GRANT OPTION;",
			// Currently, dolt system tables are scoped to the current database, so this is a workaround for that
			"GRANT ALL ON mydb.* TO a@localhost;",
			"GRANT ALL ON mydb.* TO b@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:  "a",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('dba', 'dummy1', '%', '%', 'write');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "a",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('db_', 'dummy2', '%', '%', 'write');",
				ExpectedErr: branch_control.ErrInsertingAccessRow,
			},
			{
				User:        "a",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('dbb', 'dummy3', '%', '%', 'write');",
				ExpectedErr: branch_control.ErrInsertingAccessRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('dba', 'dummy4', '%', '%', 'write');",
				ExpectedErr: branch_control.ErrInsertingAccessRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('db_', 'dummy5', '%', '%', 'write');",
				ExpectedErr: branch_control.ErrInsertingAccessRow,
			},
			{
				User:  "b",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('dbb', 'dummy6', '%', '%', 'write');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "GRANT SUPER ON *.* TO a@localhost WITH GRANT OPTION;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('db_', 'dummy7', '%', '%', 'write');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{
					{"%", "%", "root", "localhost", "admin"},
					{"dba", "dummy1", "%", "%", "write"},
					{"dbb", "dummy6", "%", "%", "write"},
					{"db_", "dummy7", "%", "%", "write"},
				},
			},
		},
	},
	{
		Name: "Cannot insert an exact match even with elevated permissions",
		Assertions: []BranchControlTestAssertion{
			{
				User:        "root",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', '%', '%', '%', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{ // This will fail if the above succeeded, so kind of a redundant check but ultimately harmless
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE permissions='admin';",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		// Each branch-level dolt procedure gated with Permissions_Write should
		// reject a merge-only user. The existing "Merge permission allows merge
		// but blocks other writes" case already covers dolt_add / dolt_reset /
		// dolt_clean / dolt_revert; this case rounds out the rest of the
		// Write-gated procedures whose MySQL privilege requirements are
		// satisfied by GRANT ALL minus SUPER. dolt_undrop is intentionally not
		// listed — it operates on databases, not branches, and is gated by
		// MySQL SUPER at the grants layer rather than by branch_control.
		Name: "Merge permission blocks Write-gated dolt procedures (no SUPER)",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_CHERRY_PICK('deadbeef');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_REBASE('--interactive', 'HEAD');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_RM('test');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_STASH('push');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_VERIFY_CONSTRAINTS();",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		// Same as the case above, but the procedures here require MySQL SUPER
		// in addition to their branch_control gate. Grant SUPER in the setup
		// so the rejection comes from branch_control rather than the MySQL
		// grants layer.
		Name: "Merge permission blocks Write-gated dolt procedures (SUPER required)",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_BACKUP('add', 'b1', 'file:///tmp/dolt-bc-test');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_FETCH('origin');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_GC();",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_PULL('origin');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_PUSH('origin', 'main');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_REMOTE('add', 'r1', 'http://example.com/r1');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_UPDATE_COLUMN_TAG('test', 'v1', 99999);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		// Read-only procedures should be callable by a user with only read
		// permission on the branch. Adds explicit coverage so a regression
		// that accidentally gates one of these would surface here.
		Name: "Read permission allows read-only dolt procedures",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'read');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'HEAD', '--to', 'HEAD');",
				Expected: []sql.Row{{uint64(0), uint64(0)}},
			},
		},
	},
	{
		// Writes to user-space dolt system tables (dolt_docs, dolt_ignore,
		// dolt_query_catalog, dolt_tests) all flow through
		// createWriteableSystemTable, which is gated by Permissions_Write.
		// dolt_workspace_<t> and dolt_constraint_violations_<t> are also
		// gated but require a complex setup to populate non-empty rows; they
		// are intentionally not exercised here.
		Name: "Merge permission blocks writes to user-space dolt system tables",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_docs VALUES ('README', '# hello');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_ignore VALUES ('tmp_*', true);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_query_catalog VALUES ('q1', 1, 'count', 'SELECT count(*) FROM test', '');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_tests VALUES ('t1', NULL, 'SELECT 1', 'expected_single_value', '==', '1');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		// dolt_workspace_<t> and dolt_constraint_violations_<t> have distinct
		// per-method writer logic (not the createWriteableSystemTable shared
		// helper), so each writer factory needs its own assertion. Rows must
		// be present for the static-error to surface, so the setup creates
		// real workspace entries and a real constraint violation first.
		// ConflictRootObjectTable.Deleter/Updater are also gated with
		// Permissions_Write but are not exercised here — constructing a
		// root-object conflict (vs a row conflict) needs setup that doesn't
		// exist in the core engine tests. The gate matches the pattern used
		// by the other writers in this case, so a regression on root-object
		// writers would still be caught by code review against this file.
		Name: "Merge permission blocks writes to per-table artifact system tables",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			// Constraint-violation setup: a unique-index conflict produced by
			// a merge with @@dolt_force_transaction_commit so the violation
			// persists. Done first because the merge resets the working set.
			"CREATE TABLE cv (a INT, b INT, UNIQUE INDEX (a));",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial');",
			"CALL DOLT_CHECKOUT('-b', 'side');",
			"INSERT INTO cv VALUES (1, 2);",
			"CALL DOLT_COMMIT('-am', 'cv side');",
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO cv VALUES (1, 3);",
			"CALL DOLT_COMMIT('-am', 'cv main');",
			"CALL DOLT_CHECKOUT('side');",
			"SET @@dolt_force_transaction_commit = 1;",
			"CALL DOLT_MERGE('main');", // produces dolt_constraint_violations_cv rows
			// Now create an unstaged change so dolt_workspace_test has rows.
			"INSERT INTO test VALUES (2, 2);",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			// dolt_workspace_<t>.Deleter
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_workspace_test WHERE id = 0;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			// dolt_workspace_<t>.Updater
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "UPDATE dolt_workspace_test SET staged = TRUE WHERE id = 0;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			// dolt_constraint_violations_<t>.Deleter (only writer this table has)
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "DELETE FROM dolt_constraint_violations_cv;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		// Effectively read-only system tables: their writers always return a
		// "read-only" error from Insert/Update/Delete, regardless of
		// branch_control. This case pins that behavior so a refactor that
		// accidentally makes them writable would also have to add a proper
		// branch_control gate to pass.
		Name: "Read-only dolt system tables reject writes for merge permission",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:           "testuser",
				Host:           "localhost",
				Query:          "INSERT INTO dolt_branches (name, hash) SELECT 'newbranch', hash FROM dolt_branches WHERE name = 'main';",
				ExpectedErrStr: "the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes",
			},
			{
				User:           "testuser",
				Host:           "localhost",
				Query:          "DELETE FROM dolt_branches WHERE name = 'other';",
				ExpectedErrStr: "the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes",
			},
			{
				User:           "testuser",
				Host:           "localhost",
				Query:          "INSERT INTO dolt_remotes (name, url, fetch_specs, params) VALUES ('r1', 'http://example.com/r1', '[]', '{}');",
				ExpectedErrStr: "the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes",
			},
		},
	},
	{
		Name: "Merge permission allows merge but blocks other writes",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			// Give testuser merge permission on 'other' branch
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
			// Add more data on main so other can FF merge
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'second commit on main');",
		},
		Assertions: []BranchControlTestAssertion{
			// Verify the merge permission entry is visible in the table
			{
				User:  "testuser",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"%", "other", "testuser", "localhost", "merge"},
				},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			// Merge permission allows reading
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 1}},
			},
			// Merge permission blocks writes
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (3, 3);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "UPDATE test SET v1 = 10 WHERE pk = 1;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "DELETE FROM test WHERE pk = 1;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CREATE TABLE test2 (pk BIGINT PRIMARY KEY);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "DROP TABLE test;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_ADD('-A');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_RESET();",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_CLEAN();",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_REVERT();",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			// Merge permission allows create branch/tag
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_BRANCH('new_branch');",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_TAG('v0');",
				Expected: []sql.Row{{0}},
			},
			// Merge permission allows DOLT_MERGE (FF merge)
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			// After merge, the merged data is visible
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			// Merge permission allows DOLT_COMMIT
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_COMMIT('--allow-empty', '-m', 'msg');",
				Expected: []sql.Row{{doltCommit}},
			},
		},
	},
	{
		Name: "Merge permission allows non-fast-forward merge (creates merge commit)",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			// Make a diverging commit on 'other' so a merge from main will be non-fast-forward
			"CALL DOLT_CHECKOUT('other');",
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on other');",
			// Return to main and make another commit so main and other have diverged
			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (3, 3);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on main');",
			// Give testuser merge permission on 'other'
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},

			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		// DOLT_CHECKOUT has two forms:
		//   DOLT_CHECKOUT('<branch>') — switches the current session's branch (read-ish)
		//   DOLT_CHECKOUT('<table>')  — restores <table>'s working set from HEAD, discarding
		//                               uncommitted changes (a write to the working set)
		// A user with only read or merge permission on main should be able to switch
		// branches, but should not be able to clear working-set changes on main.
		Name: "Merge permission allows DOLT_CHECKOUT('<branch>') but blocks DOLT_CHECKOUT('<table>')",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			// Dirty working set on main that a checkout-of-table would clear.
			"UPDATE test SET v1 = 2 WHERE pk = 1;",
			// testuser has only merge permission on main.
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			// Switching branches is allowed.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			// Clearing a table's working-set changes is a write — should be rejected.
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_CHECKOUT('test');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		// Same scenario as the merge-permission case above, but with read-only
		// permission. Switching branches should still work; clearing working-set
		// changes on a table should still be rejected.
		Name: "Read permission allows DOLT_CHECKOUT('<branch>') but blocks DOLT_CHECKOUT('<table>')",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			"UPDATE test SET v1 = 2 WHERE pk = 1;",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'read');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_CHECKOUT('test');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Merge permission allows merge with conflicts",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			// Make conflicting changes: both branches modify the same row with different values
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE test SET v1 = 100 WHERE pk = 1;",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on other');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET v1 = 200 WHERE pk = 1;",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on main');",
			// Give testuser merge permission on 'other'
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SET @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM dolt_conflicts;",
				Expected: []sql.Row{{int64(1)}},
			},
			// Merge permission lets the user edit conflicted rows via
			// dolt_conflicts_<t> and resolve them.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "UPDATE dolt_conflicts_test SET our_v1 = 50 WHERE our_pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			// The UPDATE on the conflicts table writes through to the source
			// table.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT v1 FROM test WHERE pk = 1;",
				Expected: []sql.Row{{int64(50)}},
			},
			// DOLT_CONFLICTS_RESOLVE clears the conflict marker.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CONFLICTS_RESOLVE('--ours', 'test');",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM dolt_conflicts;",
				Expected: []sql.Row{{int64(0)}},
			},
			// And the merge-only user can finalize the merge.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_COMMIT('-am', 'resolved');",
				Expected: []sql.Row{{doltCommit}},
			},
			// Writes to the source table directly remain rejected — only the
			// dolt_conflicts_<t> path is open to merge-permission callers.
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "UPDATE test SET v1 = 100 WHERE pk = 1;",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Merge permission allows DELETE from dolt_conflicts_<t> to resolve conflicts",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE test SET v1 = 100 WHERE pk = 1;",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on other');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test SET v1 = 200 WHERE pk = 1;",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'commit on main');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SET @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			// Merge-only user can DELETE the conflict row directly. This keeps
			// "our" values on the underlying table.
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "DELETE FROM dolt_conflicts_test;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM dolt_conflicts;",
				Expected: []sql.Row{{int64(0)}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT v1 FROM test WHERE pk = 1;",
				Expected: []sql.Row{{int64(100)}},
			},
		},
	},
	{
		Name: "Merge permission does not allow conflicts writes outside an active merge",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			// No merge is in flight, so the merge-only user cannot resolve
			// conflicts via the procedure. (Direct UPDATE/DELETE on
			// dolt_conflicts_<t> match zero rows when there are no conflicts,
			// so they short-circuit before the writer factory's error
			// surfaces — that's the same behavior as a no-op DELETE on a
			// regular table under the existing WritableDoltTable gate.)
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_CONFLICTS_RESOLVE('--ours', 'test');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			// Direct writes to the source table remain rejected.
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (2, 2);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Read-only user cannot merge",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			// No merge permission given; testuser is read-only on 'other'
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'second commit on main');",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			// Can read
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "SELECT * FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 1}},
			},
			// Cannot write
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (3, 3);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
			// Cannot merge without merge permission
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_MERGE('main');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Merge permission on specific branch does not apply to other branches",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"REVOKE SUPER ON *.* FROM testuser@localhost;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO test VALUES (1, 1);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'initial commit');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_BRANCH('third');",
			// Give testuser merge permission only on 'other', not on 'third'
			"INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'merge');",
			"INSERT INTO test VALUES (2, 2);",
			"CALL DOLT_ADD('-A');",
			"CALL DOLT_COMMIT('-m', 'second commit on main');",
		},
		Assertions: []BranchControlTestAssertion{
			// Merge works on 'other' where testuser has merge permission
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('other');",
				Expected: []sql.Row{{0, "Switched to branch 'other'"}},
			},
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_MERGE('main');",
				Expected: []sql.Row{{doltCommit, 1, 0, "merge successful"}},
			},
			// Merge fails on 'third' where testuser has no merge permission
			{
				User:     "testuser",
				Host:     "localhost",
				Query:    "CALL DOLT_CHECKOUT('third');",
				Expected: []sql.Row{{0, "Switched to branch 'third'"}},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "CALL DOLT_MERGE('main');",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Mixed any match operator (single, prefix, postfix)",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
			"INSERT INTO dolt_branch_control VALUES ('%wy', 'prefix%', '%', '%', 'write');",
			"INSERT INTO dolt_branch_control VALUES ('%wy', 'mask%', '%', '%', 'write');",
			"CREATE TABLE test (pk BIGINT);",
			"CREATE USER testuser@localhost;",
			"GRANT SELECT, INSERT ON *.* TO testuser@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO test VALUES (5);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (6);",
				ExpectedErr: branch_control.ErrIncorrectPermissions,
			},
		},
	},
	{
		Name: "Allow restrictions and supersets",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"INSERT INTO dolt_branch_control VALUES ('%', 'main', '%', '%', 'write');",
			"CREATE USER testuser@localhost;",
			"CREATE USER testuser2@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"GRANT ALL ON *.* TO testuser2@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{ // This is a strict subset and should be blocked
				User:        "root",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', '%', 'write');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{ // This is a restriction
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', '%', 'read');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{ // This is a superset
				User:  "root",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser2', '%', 'admin');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{ // Set to a restriction using UPDATE
				User:  "root",
				Host:  "localhost",
				Query: `UPDATE dolt_branch_control SET user = 'testuser', permissions = 'read' WHERE user = 'testuser2';`,
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{ // Set to a superset using UPDATE
				User:  "root",
				Host:  "localhost",
				Query: "UPDATE dolt_branch_control SET user = 'testuser2', permissions = 'admin' WHERE user = 'testuser';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{ // Ensure that we still block strict subsets
				User:        "root",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_control SET permissions = 'write' WHERE user = 'testuser2';",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
		},
	},
}

func TestBranchControl(t *testing.T) {
	for _, test := range BranchControlTests {
		harness := newDoltHarness(t)
		defer harness.Close()
		t.Run(test.Name, func(t *testing.T) {
			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			ctx := enginetest.NewContext(harness)
			ctx.WithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range test.SetUpScript {
				enginetest.RunQueryWithContext(t, engine, harness, ctx, statement)
			}

			for _, assertion := range test.Assertions {
				user := assertion.User
				host := assertion.Host
				if user == "" {
					user = "root"
				}
				if host == "" {
					host = "localhost"
				}
				ctx = ctx.WithClient(sql.Client{
					User:    user,
					Address: host,
				})

				if assertion.ExpectedErr != nil {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.TestQueryWithContext(t, ctx, engine, harness, assertion.Query, assertion.Expected, nil, nil, nil)
					})
				}
			}
		})
	}
}

func TestBranchControlBlocks(t *testing.T) {
	for _, test := range BranchControlBlockTests {
		t.Run(test.Name, func(t *testing.T) {
			if test.SkipMessage != "" {
				t.Skip(test.SkipMessage)
			}

			harness := newDoltHarness(t)
			defer harness.Close()

			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			rootCtx := enginetest.NewContext(harness)
			rootCtx.WithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range append(TestUserSetUpScripts, test.SetUpScript...) {
				enginetest.RunQueryWithContext(t, engine, harness, rootCtx, statement)
			}

			userCtx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "testuser",
				Address: "localhost",
			})
			enginetest.AssertErrWithCtx(t, engine, harness, userCtx, test.Query, nil, test.ExpectedErr)

			addUserQuery := "INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'write'), ('%', 'other', 'testuser', 'localhost', 'write');"
			addUserQueryResults := []sql.Row{{types.NewOkResult(2)}}
			enginetest.TestQueryWithContext(t, rootCtx, engine, harness, addUserQuery, addUserQueryResults, nil, nil, nil)

			_, iter, _, err := engine.Query(userCtx, test.Query)
			if err == nil {
				_, err = sql.RowIterToRows(userCtx, iter)
			}
			assert.NoError(t, err)
		})
	}

	// These tests are run with permission on main but not other
	for _, test := range BranchControlOtherDbBlockTests {
		t.Run("OtherDB_"+test.Name, func(t *testing.T) {
			if test.SkipMessage != "" {
				t.Skip(test.SkipMessage)
			}

			harness := newDoltHarness(t)
			defer harness.Close()

			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			rootCtx := enginetest.NewContext(harness)
			rootCtx.WithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range append(TestUserSetUpScripts, test.SetUpScript...) {
				enginetest.RunQueryWithContext(t, engine, harness, rootCtx, statement)
			}

			addUserQuery := "INSERT INTO dolt_branch_control VALUES ('%', 'main', 'testuser', 'localhost', 'write');"
			addUserQueryResults := []sql.Row{{types.NewOkResult(1)}}
			enginetest.TestQueryWithContext(t, rootCtx, engine, harness, addUserQuery, addUserQueryResults, nil, nil, nil)

			userCtx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "testuser",
				Address: "localhost",
			})
			enginetest.AssertErrWithCtx(t, engine, harness, userCtx, test.Query, nil, test.ExpectedErr)

			addUserQuery = "INSERT INTO dolt_branch_control VALUES ('%', 'other', 'testuser', 'localhost', 'write');"
			addUserQueryResults = []sql.Row{{types.NewOkResult(1)}}
			enginetest.TestQueryWithContext(t, rootCtx, engine, harness, addUserQuery, addUserQueryResults, nil, nil, nil)

			_, iter, _, err := engine.Query(userCtx, test.Query)
			if err == nil {
				_, err = sql.RowIterToRows(userCtx, iter)
			}
			assert.NoError(t, err)
		})
	}
}

// TestInformationSchemaDoesNotBypassBranchControl is a regression
// test for a cache pollution bug where a SELECT against
// information_schema in a fresh user session would let the next write
// in that session bypass branch_control.
//
// The root cause was that a select against information_Schema.columns
// or tables would DoltDatabaseProvider.AllDatabases and would
// populate the session cache with sqle.DoltTable instances that
// embedded the wrong Database instance (non revision-scoped). This
// was fixed by rebinding Table values to the correct Database
// instance anytime we pull them out of the cache.
//
// The setup must run in a separate session from the writer's session
// so that the table cache is genuinely cold when the writer queries
// information_schema. Otherwise the SetUpScript's CREATE TABLE warms
// the cache with the revisioned db and the pollution path is never
// exercised.  TestBranchControl shares a session between setup and
// assertions, so this test doesn't fit that harness.
func TestInformationSchemaDoesNotBypassBranchControl(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()

	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	rootCtx := enginetest.NewContext(harness)
	rootCtx.WithClient(sql.Client{User: "root", Address: "localhost"})
	engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
	engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

	setup := []string{
		"DELETE FROM dolt_branch_control WHERE user = '%';",
		"INSERT INTO dolt_branch_control VALUES ('%', '%', 'root', 'localhost', 'admin');",
		"CREATE USER testuser@localhost;",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO testuser@localhost;",
		"CREATE TABLE vals (id INT PRIMARY KEY, val INT);",
	}
	for _, q := range setup {
		enginetest.RunQueryWithContext(t, engine, harness, rootCtx, q)
	}

	// Cases differ only in which information_schema table primes the cache;
	// both produce the same un-revisioned WritableDoltTable for `vals` in
	// the session cache. Each case gets its own fresh writer session so
	// nothing pre-warms the cache with a revisioned db.
	cases := []struct {
		name      string
		readQuery string
	}{
		{"information_schema.columns", "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'mydb' AND table_name = 'vals';"},
		{"information_schema.tables", "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'mydb' AND table_name = 'vals';"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			userCtx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "testuser",
				Address: "localhost",
			})

			// Read first to make the cache pollution happen.
			_, iter, _, err := engine.Query(userCtx, c.readQuery)
			require.NoError(t, err)
			_, err = sql.RowIterToRows(userCtx, iter)
			require.NoError(t, err)

			// Pre-fix the first write silently succeeded.
			enginetest.AssertErrWithCtx(t, engine, harness, userCtx,
				"INSERT INTO vals VALUES (1, 1);", nil, branch_control.ErrIncorrectPermissions)
			// Control: the second write at the (now-changed) root would
			// have been caught even pre-fix. Asserting it pins behavior
			// against future regressions that flip the polarity.
			enginetest.AssertErrWithCtx(t, engine, harness, userCtx,
				"INSERT INTO vals VALUES (2, 2);", nil, branch_control.ErrIncorrectPermissions)
		})
	}
}
