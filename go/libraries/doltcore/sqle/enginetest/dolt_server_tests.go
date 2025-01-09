// Copyright 2024 Dolthub, Inc.
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
	"context"
	gosql "database/sql"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

// DoltBranchMultiSessionScriptTests contain tests that need to be run in a multi-session server environment
// in order to fully test branch deletion and renaming logic.
var DoltBranchMultiSessionScriptTests = []queries.ScriptTest{
	{
		Name: "Test multi-session behavior for deleting branches",
		SetUpScript: []string{
			"call dolt_branch('branch1');",
			"call dolt_branch('branch2');",
			"call dolt_branch('branch3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('branch1');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-d', 'branch1');",
				ExpectedErrStr: "Error 1105 (HY000): unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch2'"}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-d', 'branch1');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-d', 'branch2');",
				ExpectedErrStr: "Error 1105 (HY000): unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-df', 'branch2');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-d', 'branch3');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
		},
	},
	{
		Name: "Test multi-session behavior for renaming branches",
		SetUpScript: []string{
			"call dolt_branch('branch1');",
			"call dolt_branch('branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('branch1');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-m', 'branch1', 'movedBranch1');",
				ExpectedErrStr: "Error 1105 (HY000): unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-mf', 'branch1', 'movedBranch1');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-m', 'branch2', 'movedBranch2');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
		},
	},
	{
		Name: "Test branch deletion when clients are using a branch-qualified database",
		SetUpScript: []string{
			"call dolt_branch('branch1');",
			"call dolt_branch('branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ use dolt/branch1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.UntypedSqlRow{{"dolt/branch1", "branch1"}},
			},
			{
				Query:    "/* client b */ use dolt/branch2;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.UntypedSqlRow{{"dolt/branch2", "branch2"}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.UntypedSqlRow{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:          "/* client a */ CALL DOLT_BRANCH('-d', 'branch2');",
				ExpectedErrStr: "Error 1105 (HY000): unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_BRANCH('-df', 'branch2');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.UntypedSqlRow{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "/* client a */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.UntypedSqlRow{{"dolt/branch1", "branch1"}},
			},
			{
				// Call a stored procedure since this searches across all databases and will
				// fail if a branch-qualified database exists for a missing branch.
				Query:    "/* client a */ CALL DOLT_BRANCH('branch3');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
		},
	},
	{
		Name: "Test branch renaming when clients are using a branch-qualified database",
		SetUpScript: []string{
			"call dolt_branch('branch1');",
			"call dolt_branch('branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ use dolt/branch1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.UntypedSqlRow{{"dolt/branch1", "branch1"}},
			},
			{
				Query:    "/* client b */ use dolt/branch2;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.UntypedSqlRow{{"dolt/branch2", "branch2"}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.UntypedSqlRow{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:          "/* client a */ CALL DOLT_BRANCH('-m', 'branch2', 'newName');",
				ExpectedErrStr: "Error 1105 (HY000): unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_BRANCH('-mf', 'branch2', 'newName');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.UntypedSqlRow{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				// Call a stored procedure since this searches across all databases and will
				// fail if a branch-qualified database exists for a missing branch.
				Query:    "/* client a */ CALL DOLT_BRANCH('branch3');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
		},
	},
	{
		Name: "Test multi-session behavior for force deleting active branch with autocommit on",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('-b', 'branch1');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:    "/* client b */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches order by name;",
				Expected: []sql.UntypedSqlRow{{"branch1"}, {"main"}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-D', 'branch1');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches;",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
			{
				Query:          "/* client a */ select name from dolt_branches;",
				ExpectedErrStr: "Error 1049 (HY000): database not found: dolt/branch1",
			},
			{
				Query:          "/* client a */ CALL DOLT_CHECKOUT('main');",
				ExpectedErrStr: "Error 1049 (HY000): database not found: dolt/branch1",
			},
			{
				Query:    "/* client a */ USE dolt/main;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
		},
	},
	{
		Name: "Test multi-session behavior for force deleting active branch with autocommit off",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('-b', 'branch1');",
				Expected: []sql.UntypedSqlRow{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"branch1"}},
			},
			{
				Query:    "/* client b */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches order by name;",
				Expected: []sql.UntypedSqlRow{{"branch1"}, {"main"}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-D', 'branch1');",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches;",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
			{
				Query:          "/* client a */ select name from dolt_branches;",
				ExpectedErrStr: "Error 1049 (HY000): database not found: dolt/branch1",
			},
			{
				// TODO: this could be handled better, not the best experience. Maybe kill the session?
				Query:          "/* client a */ CALL DOLT_CHECKOUT('main');",
				ExpectedErrStr: "Error 1049 (HY000): database not found: dolt/branch1",
			},
			{
				Query:    "/* client a */ USE dolt/main;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.UntypedSqlRow{{"main"}},
			},
		},
	},
}

// DropDatabaseMultiSessionScriptTests test that when dropping a database, other sessions are properly updated
// and don't get left with old state that causes incorrect results.
// Note: this test needs to be run against a real Dolt sql-server, and not just with our transaction test scripts,
// because the transaction tests currently have a different behavior for session management and don't emulate prod.
var DropDatabaseMultiSessionScriptTests = []queries.ScriptTest{
	{
		Name: "Test multi-session behavior for dropping databases",
		SetUpScript: []string{
			"create database db01;",
			"create table db01.t01 (pk int primary key);",
			"insert into db01.t01 values (101), (202), (303);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ use db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ use db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ show tables;",
				Expected: []sql.UntypedSqlRow{{"t01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.UntypedSqlRow{{"t01"}},
			},
			{
				Query:    "/* client a */ drop database db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// TODO: This test runner doesn't currently support asserting against null values
				Query:    "/* client a */ select database() is NULL;",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "/* client a */ show databases like 'db01';",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ create database db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select database();",
				Expected: []sql.UntypedSqlRow{{"db01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Test multi-session behavior for dropping databases with a revision db",
		SetUpScript: []string{
			"create database db01;",
			"use db01;",
			"create table db01.t01 (pk int primary key);",
			"insert into db01.t01 values (101), (202), (303);",
			"call dolt_commit('-Am', 'commit on main');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into db01.t01 values (1001), (2002), (3003);",
			"call dolt_commit('-Am', 'commit on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ use db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ use `db01/branch1`;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ show tables;",
				Expected: []sql.UntypedSqlRow{{"t01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.UntypedSqlRow{{"t01"}},
			},
			{
				Query:    "/* client a */ drop database db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// TODO: This test runner doesn't currently support asserting against null values
				Query:    "/* client a */ select database() is NULL;",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "/* client a */ show databases like 'db01';",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ create database db01;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select database();",
				Expected: []sql.UntypedSqlRow{{"db01/branch1"}},
			},
			{
				Query:          "/* client b */ show tables;",
				ExpectedErrStr: "Error 1049 (HY000): database not found: db01/branch1",
			},
		},
	},
}

var PersistVariableTests = []queries.ScriptTest{
	{
		Name: "set persisted variables with on and off",
		SetUpScript: []string{
			"set @@persist.dolt_skip_replication_errors = on;",
			"set @@persist.dolt_read_replica_force_pull = off;",
		},
	},
	{
		Name: "retrieve persisted variables",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select @@dolt_skip_replication_errors",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
			{
				Query: "select @@dolt_read_replica_force_pull",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
		},
	},
	{
		Name: "set persisted variables with 1 and 0",
		SetUpScript: []string{
			"set @@persist.dolt_skip_replication_errors = 0;",
			"set @@persist.dolt_read_replica_force_pull = 1;",
		},
	},
	{
		Name: "retrieve persisted variables",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select @@dolt_skip_replication_errors",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
			{
				Query: "select @@dolt_read_replica_force_pull",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
		},
	},
}

// testSerialSessionScriptTests creates an environment, then for each script starts a server and runs assertions,
// stopping the server in between scripts. Unlike other script test executors, scripts may influence later scripts in
// the block.
func testSerialSessionScriptTests(t *testing.T, tests []queries.ScriptTest) {
	dEnv := dtestutils.CreateTestEnv()
	serverConfig := sqlserver.DefaultCommandLineServerConfig()
	rand.Seed(time.Now().UnixNano())
	port := 15403 + rand.Intn(25)
	serverConfig = serverConfig.WithPort(port)
	defer dEnv.DoltDB.Close()

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			sc, serverConfig := startServerOnEnv(t, serverConfig, dEnv)
			err := sc.WaitForStart()
			require.NoError(t, err)

			conn1, sess1 := newConnection(t, serverConfig)

			t.Run(test.Name, func(t *testing.T) {
				for _, setupStatement := range test.SetUpScript {
					_, err := sess1.Exec(setupStatement)
					require.NoError(t, err)
				}

				for _, assertion := range test.Assertions {
					t.Run(assertion.Query, func(t *testing.T) {
						activeSession := sess1
						rows, err := activeSession.Query(assertion.Query)

						if len(assertion.ExpectedErrStr) > 0 {
							require.EqualError(t, err, assertion.ExpectedErrStr)
						} else if assertion.ExpectedErr != nil {
							require.True(t, assertion.ExpectedErr.Is(err), "expected error %v, got %v", assertion.ExpectedErr, err)
						} else if assertion.Expected != nil {
							require.NoError(t, err)
							assertResultsEqual(t, assertion.Expected, rows)
						} else {
							require.Fail(t, "unsupported ScriptTestAssertion property: %v", assertion)
						}
						if rows != nil {
							require.NoError(t, rows.Close())
						}
					})
				}
			})

			require.NoError(t, conn1.Close())

			sc.Stop()
			err = sc.WaitForStop()
			require.NoError(t, err)
		})
	}
}

func makeDestinationSlice(t *testing.T, columnTypes []*gosql.ColumnType) []interface{} {
	dest := make([]any, len(columnTypes))
	for i, columnType := range columnTypes {
		switch strings.ToLower(columnType.DatabaseTypeName()) {
		case "int", "tinyint", "bigint":
			var integer int
			dest[i] = &integer
		case "text":
			var s string
			dest[i] = &s
		default:
			require.Fail(t, "unsupported type: "+columnType.DatabaseTypeName())
		}
	}

	return dest
}

func startServerOnEnv(t *testing.T, serverConfig servercfg.ServerConfig, dEnv *env.DoltEnv) (*svcs.Controller, servercfg.ServerConfig) {
	sc := svcs.NewController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	return sc, serverConfig
}

// newConnection takes sqlserver.serverConfig and opens a connection, and will return that connection with a new session
func newConnection(t *testing.T, serverConfig servercfg.ServerConfig) (*dbr.Connection, *dbr.Session) {
	const dbName = "dolt"
	conn, err := dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)
	return conn, sess
}

// startServer will start sql-server with given host, unix socket file path and whether to use specific port, which is defined randomly.
func startServer(t *testing.T, withPort bool, host string, unixSocketPath string) (*env.DoltEnv, *svcs.Controller, servercfg.ServerConfig) {
	dEnv := dtestutils.CreateTestEnv()
	serverConfig := sqlserver.DefaultCommandLineServerConfig()
	if withPort {
		rand.Seed(time.Now().UnixNano())
		port := 15403 + rand.Intn(25)
		serverConfig = serverConfig.WithPort(port)
	}
	if host != "" {
		serverConfig = serverConfig.WithHost(host)
	}
	if unixSocketPath != "" {
		serverConfig = serverConfig.WithSocket(unixSocketPath)
	}

	onEnv, config := startServerOnEnv(t, serverConfig, dEnv)
	return dEnv, onEnv, config
}

func assertResultsEqual(t *testing.T, expected []sql.UntypedSqlRow, rows *gosql.Rows) {
	columnTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	dest := makeDestinationSlice(t, columnTypes)

	for _, expectedRow := range expected {
		ok := rows.Next()
		if !ok {
			assert.Fail(t, "Fewer results than expected")
		}
		err := rows.Scan(dest...)
		assert.NoError(t, err)
		assert.Equal(t, expectedRow.Len(), len(dest),
			"Different number of columns returned than expected")

		for j, expectedValue := range expectedRow.Values() {
			switch strings.ToUpper(columnTypes[j].DatabaseTypeName()) {
			case "TEXT":
				actualValue, ok := dest[j].(*string)
				assert.True(t, ok)
				assert.Equal(t, expectedValue, *actualValue)
			case "INT", "TINYINT", "BIGINT":
				actualValue, ok := dest[j].(*int)
				assert.True(t, ok)
				assert.Equal(t, expectedValue, *actualValue)
			default:
				assert.Fail(t, "Unsupported datatype: %s", columnTypes[j].DatabaseTypeName())
			}
		}
	}

	if rows.Next() {
		assert.Fail(t, "More results than expected")
	}
}

func testMultiSessionScriptTests(t *testing.T, tests []queries.ScriptTest) {
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			dEnv, sc, serverConfig := startServer(t, true, "", "")
			err := sc.WaitForStart()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

			conn1, sess1 := newConnection(t, serverConfig)
			conn2, sess2 := newConnection(t, serverConfig)

			t.Run(test.Name, func(t *testing.T) {
				for _, setupStatement := range test.SetUpScript {
					_, err := sess1.Exec(setupStatement)
					require.NoError(t, err)
				}

				for _, assertion := range test.Assertions {
					t.Run(assertion.Query, func(t *testing.T) {
						var activeSession *dbr.Session
						if strings.Contains(strings.ToLower(assertion.Query), "/* client a */") {
							activeSession = sess1
						} else if strings.Contains(strings.ToLower(assertion.Query), "/* client b */") {
							activeSession = sess2
						} else {
							require.Fail(t, "unsupported client specification: "+assertion.Query)
						}

						rows, err := activeSession.Query(assertion.Query)

						if len(assertion.ExpectedErrStr) > 0 {
							require.EqualError(t, err, assertion.ExpectedErrStr)
						} else if assertion.ExpectedErr != nil {
							require.True(t, assertion.ExpectedErr.Is(err), "expected error %v, got %v", assertion.ExpectedErr, err)
						} else if assertion.Expected != nil {
							require.NoError(t, err)
							assertResultsEqual(t, assertion.Expected, rows)
						} else if assertion.SkipResultsCheck {
							// no-op
						} else {
							t.Fatalf("unsupported ScriptTestAssertion property: %v", assertion)
						}
						if rows != nil {
							require.NoError(t, rows.Close())
						}
					})
				}
			})

			require.NoError(t, conn1.Close())
			require.NoError(t, conn2.Close())

			sc.Stop()
			err = sc.WaitForStop()
			require.NoError(t, err)
		})
	}
}
