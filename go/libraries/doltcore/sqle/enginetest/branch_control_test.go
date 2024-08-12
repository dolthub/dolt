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
	User           string
	Host           string
	Query          string
	Expected       []sql.Row
	ExpectedErr    *errors.Kind
	ExpectedErrStr string
}

// BranchControlBlockTest are tests for quickly verifying that a command is blocked before the appropriate entry is
// added to the "dolt_branch_control" table. The `TestUserSetUpScripts` are automatically run before every test,
// therefore any set up here is essentially appended to `TestUserSetUpScripts`. In addition, the test user is
// `testuser`@`localhost`.
type BranchControlBlockTest struct {
	Name        string
	SetUpScript []string
	Query       string
	ExpectedErr *errors.Kind
	SkipMessage string
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
		Query:       "CALL DOLT_REVERT();",
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
				Query:       "UPDATE dolt_branch_control SET branch = 'other1' WHERE branch = 'prefix1%';",
				ExpectedErr: branch_control.ErrUpdatingToRow,
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "UPDATE dolt_branch_control SET permissions = 'admin' WHERE branch = 'prefix1%';",
				ExpectedErr: branch_control.ErrUpdatingRow,
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "UPDATE dolt_branch_control SET permissions = 'admin' WHERE branch = 'prefix1%';",
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
			ctx.NewCtxWithClient(sql.Client{
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
				ctx = ctx.NewCtxWithClient(sql.Client{
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
			rootCtx.NewCtxWithClient(sql.Client{
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
			rootCtx.NewCtxWithClient(sql.Client{
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
