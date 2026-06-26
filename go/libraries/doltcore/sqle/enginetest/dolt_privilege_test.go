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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

var revisionDatabasePrivsSetupPostfix = []string{
	"call dolt_commit('-Am', 'first commit')",
	"call dolt_branch('b1')",
	"use mydb/b1",
}

// The subset of tests in priv_auth_queries.go to run with alternate branch logic. Not all of them are suitable
// because they are too difficult to adapt with simple additions to setup
var revisionDatabasePrivilegeScriptNames = []string{
	"Basic database and table name visibility",
	"Basic SELECT and INSERT privilege checking",
	"Table-level privileges exist",
	"Basic revoke SELECT privilege",
	"Basic revoke all global static privileges",
	"Grant Role with SELECT Privilege",
	"Revoke role currently granted to a user",
	"Drop role currently granted to a user",
	"Anonymous User",
	"IPv4 Loopback == localhost",
	"information_schema.columns table 'privileges' column gets correct values",
	"basic tests on information_schema.SCHEMA_PRIVILEGES table",
	"basic tests on information_schema.TABLE_PRIVILEGES table",
}

// TestRevisionDatabasePrivileges is a spot-check of privilege checking on the original privilege test scripts,
// but with a revisioned database as the current db
func TestRevisionDatabasePrivileges(t *testing.T) {
	testsToRun := make(map[string]bool)
	for _, name := range revisionDatabasePrivilegeScriptNames {
		testsToRun[name] = true
	}

	var scripts []queries.UserPrivilegeTest
	for _, script := range queries.UserPrivTests {
		if testsToRun[script.Name] {
			scripts = append(scripts, script)
		}
	}

	require.Equal(t, len(scripts), len(testsToRun),
		"Error in test setup: one or more expected tests not found. "+
			"Did the name of a test change?")

	mutated := make([]queries.UserPrivilegeTest, len(scripts))
	for i, script := range scripts {
		script.SetUpScript = append(script.SetUpScript, revisionDatabasePrivsSetupPostfix...)
		mutated[i] = script
	}
	runRevisionDBPrivilegeTests(t, mutated)
}

func TestDoltOnlyRevisionTableFunctionPrivileges(t *testing.T) {
	runRevisionDBPrivilegeTests(t, DoltOnlyRevisionTableFunctionPrivilegeTests)
}

func TestDoltOnlyRevisionDatabasePrivileges(t *testing.T) {
	runRevisionDBPrivilegeTests(t, DoltOnlyRevisionDbPrivilegeTests)
}

// runRevisionDBPrivilegeTests runs each script in |scripts| against a fresh engine with the
// current database set to `mydb/b1` for every assertion. The setup script runs as root before
// assertions execute, allowing tests to establish schema, data, users, and grants. The branch
// b1 and the revision database mydb/b1 are not created by this function; each script is
// responsible for creating them in its SetUpScript.
func runRevisionDBPrivilegeTests(t *testing.T, scripts []queries.UserPrivilegeTest) {
	for _, script := range scripts {
		harness := newDoltHarness(t)
		harness.Setup(setup.MydbData, setup.MytableData)
		t.Run(script.Name, func(t *testing.T) {
			engine := mustNewEngine(t, harness)
			defer engine.Close()

			ctx := enginetest.NewContext(harness)
			ctx.WithClient(sql.Client{
				User:    "root",
				Address: "localhost",
			})
			engine.EngineAnalyzer().Catalog.MySQLDb.AddRootAccount()
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPersister(&mysql_db.NoopPersister{})

			for _, statement := range script.SetUpScript {
				enginetest.RunQueryWithContext(t, engine, harness, ctx, statement)
			}

			for _, assertion := range script.Assertions {
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
				ctx.SetCurrentDatabase("mydb/b1")

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

// Privilege test scripts for revision databases. Due to limitations in test construction, test assertions are always
// performed with current db = mydb/b1, write scripts accordingly
var DoltOnlyRevisionDbPrivilegeTests = []queries.UserPrivilegeTest{
	{
		Name: "Basic database and table name visibility",
		SetUpScript: []string{
			"use mydb",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1);",
			"call dolt_commit('-Am', 'first commit')",
			"call dolt_branch('b1')",
			"use mydb/b1",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON mydb.* TO test_role;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;/*2*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*2*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{ // Ensure we've reverted to initial state (all SELECTs after REVOKEs are doing this)
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*3*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*3*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*4*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*4*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*5*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*5*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;/*6*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*6*/",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*7*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*7*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test2 TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*8*/",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*8*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test2 FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;/*9*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*9*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT test_role TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;/*10*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test2;/*10*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "Basic SELECT and INSERT privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (4);",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
		},
	},
	{
		Name: "Basic UPDATE privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "UPDATE test set pk = 4 where pk = 3;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (4);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "UPDATE test set pk = 4 where pk = 3;",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					},
				}}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {4}},
			},
		},
	},
	{
		Name: "Basic DELETE privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "DELETE from test where pk = 3;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT DELETE ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (4);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "DELETE from test where pk = 3;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "Basic CREATE TABLE privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "CREATE TABLE t2 (a int primary key);",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT CREATE ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "CREATE TABLE t2 (a int primary key);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "show tables;",
				Expected: []sql.Row{{"mytable"}, {"myview"}, {"test"}, {"t2"}},
			},
		},
	},
	{
		Name: "Basic DROP TABLE privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "DROP TABLE test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT DROP ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "DROP TABLE TEST",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "show tables;",
				Expected: []sql.Row{{"mytable"}, {"myview"}},
			},
		},
	},
	{
		Name: "Basic ALTER TABLE privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "ALTER TABLE test add column a int;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT ALTER ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "ALTER TABLE test add column a int;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "desc test;",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "", nil, ""},
				},
			},
		},
	},
	{
		Name: "Basic INDEX privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, a int);",
			"INSERT INTO test VALUES (1,1), (2,2), (3,3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "create index t1 on test(a) ;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT select ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "create index t1 on test(a) ;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT index ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "create index t1 on test(a) ;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "desc test;",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE index ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "drop index t1 on test;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT index ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "drop index t1 on test;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "desc test;",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "", nil, ""},
				},
			},
		},
	},
	{
		Name: "Basic constraint privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, a int);",
			"INSERT INTO test VALUES (1,1), (2,2), (3,3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "alter table test add constraint CHECK (NULL = NULL);",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT select ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "alter table test add constraint CHECK (NULL = NULL);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT alter ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "alter table test add constraint chk1 CHECK (a < 10);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "show create table test;",
				Expected: []sql.Row{
					{"test", "CREATE TABLE `test` (\n" +
						"  `pk` bigint NOT NULL,\n" +
						"  `a` int,\n" +
						"  PRIMARY KEY (`pk`),\n" +
						"  CONSTRAINT `chk1` CHECK ((`a` < 10))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE alter ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "alter table test drop check chk1;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT alter ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "alter table test drop check chk1;",
				Expected: []sql.Row{},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "show create table test;",
				Expected: []sql.Row{
					{"test", "CREATE TABLE `test` (\n" +
						"  `pk` bigint NOT NULL,\n" +
						"  `a` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "alter table test add constraint chk1 CHECK (a < 10);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE alter ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "alter table test drop constraint chk1;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT alter ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "alter table test drop constraint chk1;",
				Expected: []sql.Row{},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "show create table test;",
				Expected: []sql.Row{
					{"test", "CREATE TABLE `test` (\n" +
						"  `pk` bigint NOT NULL,\n" +
						"  `a` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "Basic revoke SELECT privilege",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON mydb.* TO tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},
		},
	},
	{
		Name: "Grant Role with SELECT Privilege",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON mydb.* TO test_role;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT test_role TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},
		},
	},
	{
		Name: "Revoke role currently granted to a user",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"call dolt_commit('-Am', 'first commit');",
			"call dolt_branch('b1')",
			"use mydb/b1;",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON mydb.* TO test_role;",
			"GRANT test_role TO tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE test_role FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'test_role';",
				Expected: []sql.Row{{1}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{1}},
			},
		},
	},
}

var DoltOnlyRevisionTableFunctionPrivilegeTests = []queries.UserPrivilegeTest{
	{
		Name: "dolt_schema_diff privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
			"ALTER TABLE test CHANGE COLUMN col1 word varchar(20);",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_schema_diff('HEAD', 'WORKING', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_schema_diff('HEAD', 'WORKING');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_schema_diff('HEAD', 'WORKING', 'test');",
				Expected: []sql.Row{
					{"test", "test",
						"CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `col1` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `word` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_schema_diff('HEAD', 'WORKING');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_schema_diff('HEAD', 'WORKING');",
				Expected: []sql.Row{
					{"test", "test",
						"CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `col1` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
						"CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `word` varchar(20),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
					},
				},
			},
		},
	},
	{
		Name: "dolt_diff privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('HEAD', 'WORKING', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_diff('HEAD', 'WORKING', 'test');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_diff_stat privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_stat('HEAD', 'WORKING', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_stat('HEAD', 'WORKING');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_diff_stat('HEAD', 'WORKING', 'test');",
				Expected: []sql.Row{},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_diff_stat('HEAD', 'WORKING');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_diff_summary privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('HEAD', 'WORKING', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('HEAD', 'WORKING');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_diff_summary('HEAD', 'WORKING', 'test');",
				Expected: []sql.Row{},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_diff_summary('HEAD', 'WORKING');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_log privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT count(*) FROM dolt_log();",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT count(*) FROM dolt_log();",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT count(*) FROM dolt_log('main', '--tables', 'test');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM dolt_log();",
				Expected: []sql.Row{{int64(3)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM dolt_log('main', '--tables', 'test');",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
	{
		Name: "dolt_patch privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_patch('HEAD', 'WORKING', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_patch('HEAD', 'WORKING');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_patch('HEAD', 'WORKING', 'test');",
				Expected: []sql.Row{},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_patch('HEAD', 'WORKING');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_preview_merge_conflicts privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_preview_merge_conflicts('main', 'b1', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_preview_merge_conflicts('main', 'b1', 'test');",
				Expected: []sql.Row{},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_preview_merge_conflicts('main', 'b1', 'test2');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
		},
	},
	{
		Name: "dolt_preview_merge_conflicts_summary privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'b1');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'b1');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'b1');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_query_diff privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test2 VALUES (1, 'a'), (2, 'b');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM test', 'SELECT pk FROM test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT 1 AS pk', 'SELECT 2 AS pk');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test2 TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = 2');",
				Expected: []sql.Row{{1, 2, "modified"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT 1 AS pk', 'SELECT 2 AS pk');",
				Expected: []sql.Row{{1, 2, "modified"}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_query_diff('SELECT pk FROM test limit 1', 'SELECT pk FROM test2 limit 1');",
				Expected: []sql.Row{{1, nil, "deleted"}, {nil, 1, "added"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT pk FROM (SELECT pk FROM test2) s', 'SELECT pk FROM (SELECT pk FROM test) s');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt_query_diff per-table privilege checking with revision database",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test2 VALUES (1, 'a'), (2, 'b');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			// Grant SELECT only on test, not test2
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			// Query touching only test -> allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT pk FROM test', 'SELECT pk FROM test');",
				Expected: []sql.Row{},
			},
			// Query touching test2 which tester has no access to -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM test2', 'SELECT pk FROM test2');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// JOIN touching both tables, but only test is granted -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// query1 touches test (allowed), query2 touches test2 (denied) -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM test', 'SELECT pk FROM test2');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// Subquery referencing test2 -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM (SELECT pk FROM test2) s', 'SELECT pk FROM test');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// Query with no table references -> allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT 1 AS pk', 'SELECT 2 AS pk');",
				Expected: []sql.Row{{1, 2, "modified"}},
			},
			// Now grant test2 as well
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test2 TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			// JOIN touching both tables -> now allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM test t JOIN test2 t2 ON t.pk = t2.pk');",
				Expected: []sql.Row{},
			},
			// Cross-table query args -> now allowed
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_query_diff('SELECT pk FROM test limit 1', 'SELECT pk FROM test2 limit 1');",
				Expected: []sql.Row{{1, nil, "deleted"}, {nil, 1, "added"}},
			},
			// Subquery referencing test2 -> now allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT pk FROM (SELECT pk FROM test2) s', 'SELECT pk FROM (SELECT pk FROM test) s');",
				Expected: []sql.Row{},
			},
			// Revoke test2, verify denial again
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test2 FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM test2', 'SELECT pk FROM test2');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
		},
	},
	{
		Name: "dolt_query_diff privilege checking with qualified table names",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test VALUES (1, 'first row'), (2, 'second row');",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, col1 varchar(20));",
			"INSERT INTO test2 VALUES (1, 'a'), (2, 'b');",
			"call dolt_commit('-Am', 'first commit');",
			"CREATE USER tester@localhost;",
			"call dolt_branch('b1');",
			"use mydb/b1;",
			"GRANT SELECT ON mydb.test TO tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			// Database-qualified table name: mydb.test -> allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT pk FROM mydb.test', 'SELECT pk FROM mydb.test');",
				Expected: []sql.Row{},
			},
			// Database-qualified table name: mydb.test2 -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM mydb.test2', 'SELECT pk FROM mydb.test2');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// Branch-qualified table name using backticks: `mydb/b1`.test -> allowed
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT pk FROM `mydb/b1`.test', 'SELECT pk FROM `mydb/b1`.test');",
				Expected: []sql.Row{},
			},
			// Branch-qualified table name for denied table: `mydb/b1`.test2 -> denied
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT pk FROM `mydb/b1`.test2', 'SELECT pk FROM `mydb/b1`.test2');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// Mixed: one arg qualified, one unqualified
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT * FROM dolt_query_diff('SELECT pk FROM mydb.test', 'SELECT pk FROM test');",
				Expected: []sql.Row{
					{1, nil, "deleted"}, {2, nil, "deleted"},
					{nil, 1, "added"}, {nil, 2, "added"},
				},
			},
			// JOIN with database-qualified table names, missing test2 privilege
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_query_diff('SELECT t.pk FROM mydb.test t JOIN mydb.test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM mydb.test t JOIN mydb.test2 t2 ON t.pk = t2.pk');",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			// Grant test2 and verify qualified JOIN works
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test2 TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM dolt_query_diff('SELECT t.pk FROM mydb.test t JOIN mydb.test2 t2 ON t.pk = t2.pk', 'SELECT t.pk FROM mydb.test t JOIN mydb.test2 t2 ON t.pk = t2.pk');",
				Expected: []sql.Row{},
			},
		},
	},
}
