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
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
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
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"dolt/branch2"}, {"information_schema"}},
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
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}},
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
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"dolt/branch2"}, {"information_schema"}},
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
				Expected: []sql.Row{{"dolt"}, {"dolt/branch1"}, {"information_schema"}},
			},
			{
				// Call a stored procedure since this searches across all databases and will
				// fail if a branch-qualified database exists for a missing branch.
				Query:    "/* client a */ CALL DOLT_BRANCH('branch3');",
				Expected: []sql.Row{{0}},
			},
		},
	},
}

var DoltTransactionMultiSessionScriptTests = []queries.ScriptTest{
	{
		// TODO: Can we repro this with the transaction tests instead?
		Name: "tx isolation testing – https://github.com/dolthub/dolt/issues/3800",
		SetUpScript: []string{
			"create table t(c0 int, c1 varchar(100));",
			"commit",
		},
		/*
		 * Experiment: How do Dolt/MySQL handle tx visibility differently when explicitly starting a
		 * transaction versus just starting a session.
		 *
		 * For each test case below, I tested whether a session can see a committed change (inserting a
		 * row into table t) from another session, with and without starting an explicit session. Both
		 * servers had global configuration for all sessions to disable autocommit.
		 *
		 * MySQL:
		 * Test 1: set @foo='bar'; (start tx: YES) (start session: NO)
		 * Test 2: select 2 from dual; (start tx: YES) (start session: NO)
		 * Test 3: select * from t; (start tx: NO) (start session: NO)
		 * Test 4: select * from t0; (start tx: NO) (start session: NO)
		 *
		 * I assumed starting a session would implicitly create a transaction and have the same behavior,
		 * but it does not appear so.
		 *
		 * Dolt:
		 * Test 1: set @foo='bar'; (start tx: NO) (start session: NO)
		 * Test 2: select 2 from dual; (start tx: NO) (start session: NO)
		 * Test 3: select * from t; (start tx: NO) (start session: NO)
		 * Test 4: select * from t0; (start tx: NO) (start session: NO)
		 *
		 * Dolt is very close to MySQL's behavior, but seems to set the db snapshot for the transaction
		 * more eagerly than MySQL.
		 */
		Assertions: []queries.ScriptTestAssertion{
			{
				// This query should NOT cause the session to lock to a db snapshot version yet
				Query:    "/* client a */ select concat('foo', 'bar');",
				Expected: []sql.Row{{"foobar"}},
			},
			{
				Query:    "/* client b */ insert into t values (0, 'foo');",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t;",
				Expected: []sql.Row{{0, "foo"}},
			},
			{
				// Client A should be able to see the updates from the committed transaction from Client B, since
				// Client A hasn't done anything in their transaction that should have locked the db snapshot.
				Query:    "/* client a */ select * from t;",
				Expected: []sql.Row{{0, "foo"}},
			},
			{
				Query:    "/* client a */ select @@autocommit;",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "/* client b */ select @@autocommit;",
				Expected: []sql.Row{{0}},
			},
		},
	},
}

// TestDoltMultiSessionBehavior runs tests on a sql-server that cover dolt_branch stored procedure behavior across multiple sessions.
func TestDoltBranchMultiSessionBehavior(t *testing.T) {
	testMultiSessionScriptTests(t, DoltBranchMultiSessionScriptTests)
}

// TestDoltTransactionMultiSessionBehavior runs tests on a sql-server that cover transaction behavior across multiple sessions.
func TestDoltTransactionMultiSessionBehavior(t *testing.T) {
	testMultiSessionScriptTests(t, DoltTransactionMultiSessionScriptTests)
}

// testMultiSessionScriptTests launches a Dolt sql-server with autocommit turned off globally and runs the
// specified ScriptTests.
func testMultiSessionScriptTests(t *testing.T, tests []queries.ScriptTest) {
	for _, test := range tests {
		sc, serverConfig := startServer(t)
		sc.WaitForStart()

		conn1, sess1 := newConnection(t, serverConfig)
		conn2, sess2 := newConnection(t, serverConfig)

		t.Run(test.Name, func(t *testing.T) {
			conn3, sess3 := newConnection(t, serverConfig)
			for _, setupStatement := range test.SetUpScript {
				_, err := sess3.Exec(setupStatement)
				require.NoError(t, err)
			}
			conn3.Close()

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
		sc.WaitForClose()
	}
}

func makeDestinationSlice(t *testing.T, columnTypes []*gosql.ColumnType) []interface{} {
	dest := make([]any, len(columnTypes))
	for i, columnType := range columnTypes {
		switch strings.ToLower(columnType.DatabaseTypeName()) {
		case "int", "tinyint", "bigint":
			var integer int
			dest[i] = &integer
		case "text", "varchar":
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
	require.NoError(t, err)
	dest := makeDestinationSlice(t, columnTypes)

	for _, expectedRow := range expected {
		ok := rows.Next()
		if !ok {
			require.Fail(t, "Fewer results than expected")
		}
		err := rows.Scan(dest...)
		require.NoError(t, err)
		require.Equal(t, len(expectedRow), len(dest),
			"Different number of columns returned than expected")

		for j, expectedValue := range expectedRow {
			switch strings.ToUpper(columnTypes[j].DatabaseTypeName()) {
			case "TEXT", "VARCHAR":
				actualValue, ok := dest[j].(*string)
				require.True(t, ok)
				require.Equal(t, expectedValue, *actualValue)
			case "INT", "TINYINT", "BIGINT":
				actualValue, ok := dest[j].(*int)
				require.True(t, ok)
				require.Equal(t, expectedValue, *actualValue)
			default:
				require.Fail(t, "Unsupported datatype: "+columnTypes[j].DatabaseTypeName())
			}
		}
	}

	if rows.Next() {
		require.Fail(t, "More results than expected")
	}
}

func startServer(t *testing.T) (*sqlserver.ServerController, sqlserver.ServerConfig) {
	dEnv := dtestutils.CreateTestEnv()
	rand.Seed(time.Now().UnixNano())
	port := 15403 + rand.Intn(25)
	serverConfig := sqlserver.DefaultServerConfig().WithPort(port).WithAutocommit(false)

	sc := sqlserver.NewServerController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	return sc, serverConfig
}

func newConnection(t *testing.T, serverConfig sqlserver.ServerConfig) (*dbr.Connection, *dbr.Session) {
	const dbName = "dolt"
	conn, err := dbr.Open("mysql", sqlserver.ConnectionString(serverConfig)+dbName, nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)
	return conn, sess
}
