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
	"context"
	gosql "database/sql"
	"math/rand"
	"runtime"
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
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-d', 'branch1');",
				ExpectedErrStr: "Error 1105: unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-d', 'branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-d', 'branch2');",
				ExpectedErrStr: "Error 1105: unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-df', 'branch2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-d', 'branch3');",
				Expected: []sql.Row{{0}},
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
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:          "/* client b */ CALL DOLT_BRANCH('-m', 'branch1', 'movedBranch1');",
				ExpectedErrStr: "Error 1105: unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-mf', 'branch1', 'movedBranch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-m', 'branch2', 'movedBranch2');",
				Expected: []sql.Row{{0}},
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
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.Row{{"dolt/branch1", "branch1"}},
			},
			{
				Query:    "/* client b */ use dolt/branch2;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.Row{{"dolt/branch2", "branch2"}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:          "/* client a */ CALL DOLT_BRANCH('-d', 'branch2');",
				ExpectedErrStr: "Error 1105: unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_BRANCH('-df', 'branch2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				// Call a stored procedure since this searches across all databases and will
				// fail if a branch-qualified database exists for a missing branch.
				Query:    "/* client a */ CALL DOLT_BRANCH('branch3');",
				Expected: []sql.Row{{0}},
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
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.Row{{"dolt/branch1", "branch1"}},
			},
			{
				Query:    "/* client b */ use dolt/branch2;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT DATABASE(), ACTIVE_BRANCH();",
				Expected: []sql.Row{{"dolt/branch2", "branch2"}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:          "/* client a */ CALL DOLT_BRANCH('-m', 'branch2', 'newName');",
				ExpectedErrStr: "Error 1105: unsafe to delete or rename branches in use in other sessions; use --force to force the change",
			},
			{
				Query:    "/* client a */ CALL DOLT_BRANCH('-mf', 'branch2', 'newName');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ SHOW DATABASES;",
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				// Call a stored procedure since this searches across all databases and will
				// fail if a branch-qualified database exists for a missing branch.
				Query:    "/* client a */ CALL DOLT_BRANCH('branch3');",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Test multi-session behavior for force deleting active branch with autocommit on",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=1;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('-b', 'branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:    "/* client b */ select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches order by name;",
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-D', 'branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches;",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:          "/* client a */ select name from dolt_branches;",
				ExpectedErrStr: "Error 1105: current branch has been force deleted. run 'USE <database>/<branch>' to checkout a different branch, or reconnect to the server",
			},
			{
				Query:          "/* client a */ CALL DOLT_CHECKOUT('main');",
				ExpectedErrStr: "Error 1105: current branch has been force deleted. run 'USE <database>/<branch>' to checkout a different branch, or reconnect to the server",
			},
			{
				Query:    "/* client a */ USE dolt/main;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
		},
	},
	{
		Name: "Test multi-session behavior for force deleting active branch with autocommit off",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET @@autocommit=0;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ CALL DOLT_CHECKOUT('-b', 'branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:    "/* client b */ select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches order by name;",
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "/* client b */ CALL DOLT_BRANCH('-D', 'branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ select name from dolt_branches;",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:          "/* client a */ select name from dolt_branches;",
				ExpectedErrStr: "Error 1105: current branch has been force deleted. run 'USE <database>/<branch>' to checkout a different branch, or reconnect to the server",
			},
			{
				Query:          "/* client a */ CALL DOLT_CHECKOUT('main');",
				ExpectedErrStr: "Error 1105: current branch has been force deleted. run 'USE <database>/<branch>' to checkout a different branch, or reconnect to the server",
			},
			{
				Query:    "/* client a */ USE dolt/main;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select active_branch();",
				Expected: []sql.Row{{"main"}},
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
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ use db01;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ show tables;",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "/* client a */ drop database db01;",
				Expected: []sql.Row{},
			},
			{
				// TODO: This test runner doesn't currently support asserting against null values
				Query:    "/* client a */ select database() is NULL;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ show databases like 'db01';",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ create database db01;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select database();",
				Expected: []sql.Row{{"db01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ use `db01/branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ show tables;",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "/* client b */ show tables;",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "/* client a */ drop database db01;",
				Expected: []sql.Row{},
			},
			{
				// TODO: This test runner doesn't currently support asserting against null values
				Query:    "/* client a */ select database() is NULL;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "/* client a */ show databases like 'db01';",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ create database db01;",
				Expected: []sql.Row{},
			},
			{
				// At this point, this is an invalid revision database, and any queries against it will fail.
				Query:    "/* client b */ select database();",
				Expected: []sql.Row{{"db01/branch1"}},
			},
			{
				Query:          "/* client b */ show tables;",
				ExpectedErrStr: "Error 1105: database not found: db01/branch1",
			},
		},
	},
}

// TestDoltMultiSessionBehavior runs tests that exercise multi-session logic on a running SQL server. Statements
// are sent through the server, from out of process, instead of directly to the in-process engine API.
func TestDoltMultiSessionBehavior(t *testing.T) {
	testMultiSessionScriptTests(t, DoltBranchMultiSessionScriptTests)
}

// TestDropDatabaseMultiSessionBehavior tests that dropping a database from one session correctly updates state
// in other sessions.
func TestDropDatabaseMultiSessionBehavior(t *testing.T) {
	testMultiSessionScriptTests(t, DropDatabaseMultiSessionScriptTests)
}

func testMultiSessionScriptTests(t *testing.T, tests []queries.ScriptTest) {
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			sc, serverConfig := startServer(t, true, "", "")
			err := sc.WaitForStart()
			require.NoError(t, err)

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
							require.True(t, assertion.ExpectedErr.Is(err))
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
			require.NoError(t, conn2.Close())

			sc.StopServer()
			err = sc.WaitForClose()
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

func assertResultsEqual(t *testing.T, expected []sql.Row, rows *gosql.Rows) {
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
		assert.Equal(t, len(expectedRow), len(dest),
			"Different number of columns returned than expected")

		for j, expectedValue := range expectedRow {
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

// startServer will start sql-server with given host, unix socket file path and whether to use specific port, which is defined randomly.
func startServer(t *testing.T, withPort bool, host string, unixSocketPath string) (*sqlserver.ServerController, sqlserver.ServerConfig) {
	dEnv := dtestutils.CreateTestEnv()
	serverConfig := sqlserver.DefaultServerConfig()

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

	sc := sqlserver.NewServerController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	return sc, serverConfig
}

// newConnection takes sqlserver.serverConfig and opens a connection, and will return that connection with a new session
func newConnection(t *testing.T, serverConfig sqlserver.ServerConfig) (*dbr.Connection, *dbr.Session) {
	const dbName = "dolt"
	conn, err := dbr.Open("mysql", sqlserver.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)
	return conn, sess
}

func TestDoltServerRunningUnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets not supported on Windows")
	}
	const defaultUnixSocketPath = "/tmp/mysql.sock"

	// Running unix socket server
	sc, serverConfig := startServer(t, false, "", defaultUnixSocketPath)
	sc.WaitForStart()
	require.True(t, strings.Contains(sqlserver.ConnectionString(serverConfig, "dolt"), "unix"))

	// default unix socket connection works
	localConn, localSess := newConnection(t, serverConfig)
	rows, err := localSess.Query("select 1")
	require.NoError(t, err)
	assertResultsEqual(t, []sql.Row{{1}}, rows)

	t.Run("connecting to local server with tcp connections", func(t *testing.T) {
		// connect with port defined
		serverConfigWithPortOnly := sqlserver.DefaultServerConfig().WithPort(3306)
		conn1, sess1 := newConnection(t, serverConfigWithPortOnly)
		rows1, err := sess1.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows1)

		// connect with host defined
		serverConfigWithPortandHost := sqlserver.DefaultServerConfig().WithHost("127.0.0.1")
		conn2, sess2 := newConnection(t, serverConfigWithPortandHost)
		rows2, err := sess2.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows2)

		// connect with port and host defined
		serverConfigWithPortandHost1 := sqlserver.DefaultServerConfig().WithPort(3306).WithHost("0.0.0.0")
		conn3, sess3 := newConnection(t, serverConfigWithPortandHost1)
		rows3, err := sess3.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows3)

		// close connections
		require.NoError(t, conn3.Close())
		require.NoError(t, conn2.Close())
		require.NoError(t, conn1.Close())
	})

	require.NoError(t, localConn.Close())

	// Stopping unix socket server
	sc.StopServer()
	err = sc.WaitForClose()
	require.NoError(t, err)
	require.NoFileExists(t, defaultUnixSocketPath)

	// Running TCP socket server
	tcpSc, tcpServerConfig := startServer(t, true, "0.0.0.0", "")
	tcpSc.WaitForStart()
	require.False(t, strings.Contains(sqlserver.ConnectionString(tcpServerConfig, "dolt"), "unix"))

	t.Run("host and port specified, there should not be unix socket created", func(t *testing.T) {
		// unix socket connection should fail
		localServerConfig := sqlserver.DefaultServerConfig().WithSocket(defaultUnixSocketPath)
		conn, sess := newConnection(t, localServerConfig)
		_, err := sess.Query("select 1")
		require.Error(t, err)
		require.NoError(t, conn.Close())

		// connection with the host and port define should work
		conn1, sess1 := newConnection(t, tcpServerConfig)
		rows1, err := sess1.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows1)
		require.NoError(t, conn1.Close())
	})

	// Stopping TCP socket server
	tcpSc.StopServer()
	err = tcpSc.WaitForClose()
	require.NoError(t, err)
}
