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
}

// TestUserSetUpScripts creates a user named "testuser@localhost", and grants them privileges on all databases and
// tables. In addition, creates a committed table named "test" with a single value, along with a mirror branch named
// "other".
var TestUserSetUpScripts = []string{
	"DELETE FROM dolt_branch_control WHERE user = '%';",
	"CREATE USER testuser@localhost;",
	"GRANT ALL ON *.* TO testuser@localhost;",
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
		Name:        "DELETE",
		Query:       "DELETE FROM test WHERE pk >= 0;",
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
}

var BranchControlTests = []BranchControlTest{
	{
		Name: "Unable to remove super user",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{
					{"%", "root", "localhost", uint64(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control;",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{
					{"%", "root", "localhost", uint64(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE user = 'root';",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_branch_control;",
				Expected: []sql.Row{
					{"%", "root", "localhost", uint64(1)},
				},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "TRUNCATE TABLE dolt_branch_control;",
				ExpectedErr: plan.ErrTruncateNotSupported,
			},
		},
	},
	{
		Name: "Namespace entries block",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
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
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('other%', 'root', 'localhost');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
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
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('other%', 'testuser', 'localhost');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
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
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('otherbranch%', 'root', 'localhost');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
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
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('otherbranch%', 'testuser', 'localhost');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
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
			"CREATE USER a@localhost;",
			"CREATE USER b@localhost;",
			"GRANT ALL ON *.* TO a@localhost;",
			"GRANT ALL ON *.* TO b@localhost;",
			"INSERT INTO dolt_branch_control VALUES ('other', 'a', 'localhost', 'write'), ('prefix%', 'a', 'localhost', 'admin')",
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
				Query: "INSERT INTO dolt_branch_control VALUES ('prefix1%', 'b', 'localhost', 'write');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
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
					{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				User:  "b",
				Host:  "localhost",
				Query: "DELETE FROM dolt_branch_control WHERE branch = 'prefix1%';",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('prefix1%', 'b', 'localhost', 'admin');",
				ExpectedErr: branch_control.ErrInsertingRow,
			},
			{ // Since "a" has admin on "prefix%", they can also insert into the namespace table
				User:  "a",
				Host:  "localhost",
				Query: "INSERT INTO dolt_branch_namespace_control VALUES ('prefix___', 'a', 'localhost');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				User:        "b",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_namespace_control VALUES ('prefix', 'b', 'localhost');",
				ExpectedErr: branch_control.ErrInsertingRow,
			},
			{
				User:  "a",
				Host:  "localhost",
				Query: "UPDATE dolt_branch_namespace_control SET branch = 'prefix%';",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
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
		Name: "Subset entries count as duplicates",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
			"INSERT INTO dolt_branch_control VALUES ('prefix%', 'testuser', 'localhost', 'admin');",
		},
		Assertions: []BranchControlTestAssertion{
			{ // The pre-existing "prefix%" entry will cover ALL possible matches of "prefixsub%", so we treat it as a duplicate
				User:        "testuser",
				Host:        "localhost",
				Query:       "INSERT INTO dolt_branch_control VALUES ('prefixsub%', 'testuser', 'localhost', 'admin');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
		},
	},
	//TODO: need to add this logic
	/*{
		Name: "Creating branch creates new entry",
		SetUpScript: []string{
			"DELETE FROM dolt_branch_control WHERE user = '%';",
			"CREATE USER testuser@localhost;",
			"GRANT ALL ON *.* TO testuser@localhost;",
		},
		Assertions: []BranchControlTestAssertion{
			{
				User: "testuser",
				Host: "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{},
			},
			{
				User: "testuser",
				Host: "localhost",
				Query: "CALL DOLT_BRANCH('otherbranch');",
				Expected: []sql.Row{{0}},
			},
			{
				User: "testuser",
				Host: "localhost",
				Query: "SELECT * FROM dolt_branch_control WHERE user = 'testuser';",
				Expected: []sql.Row{
					{"otherbranch", "testuser", "localhost", uint64(1)},
				},
			},
		},
	},*/
}

func TestBranchControl(t *testing.T) {
	t.Skip("Branch control isn't globally enabled yet, so tests would fail")
	for _, test := range BranchControlTests {
		harness := newDoltHarness(t)
		t.Run(test.Name, func(t *testing.T) {
			branch_control.Reset()
			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			ctx := enginetest.NewContext(harness)
			ctx.NewCtxWithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

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
				ctx := enginetest.NewContextWithClient(harness, sql.Client{
					User:    user,
					Address: host,
				})

				if assertion.ExpectedErr != nil {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, assertion.ExpectedErr)
					})
				} else if assertion.ExpectedErrStr != "" {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.AssertErrWithCtx(t, engine, harness, ctx, assertion.Query, nil, assertion.ExpectedErrStr)
					})
				} else {
					t.Run(assertion.Query, func(t *testing.T) {
						enginetest.TestQueryWithContext(t, ctx, engine, harness, assertion.Query, assertion.Expected, nil, nil)
					})
				}
			}
		})
	}
}

func TestBranchControlBlocks(t *testing.T) {
	t.Skip("Branch control isn't globally enabled yet, so tests would fail")
	for _, test := range BranchControlBlockTests {
		harness := newDoltHarness(t)
		t.Run(test.Name, func(t *testing.T) {
			branch_control.Reset()
			engine, err := harness.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()

			rootCtx := enginetest.NewContext(harness)
			rootCtx.NewCtxWithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.Analyzer.Catalog.MySQLDb.AddRootAccount()
			engine.Analyzer.Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range append(TestUserSetUpScripts, test.SetUpScript...) {
				enginetest.RunQueryWithContext(t, engine, harness, rootCtx, statement)
			}

			userCtx := enginetest.NewContextWithClient(harness, sql.Client{
				User:    "testuser",
				Address: "localhost",
			})
			enginetest.AssertErrWithCtx(t, engine, harness, userCtx, test.Query, test.ExpectedErr)
			addUserQuery := "INSERT INTO dolt_branch_control VALUES ('main', 'testuser', 'localhost', 'write');"
			addUserQueryResults := []sql.Row{{sql.NewOkResult(1)}}
			enginetest.TestQueryWithContext(t, rootCtx, engine, harness, addUserQuery, addUserQueryResults, nil, nil)
			sch, iter, err := engine.Query(userCtx, test.Query)
			if err == nil {
				_, err = sql.RowIterToRows(userCtx, sch, iter)
			}
			assert.NoError(t, err)
		})
	}
}
