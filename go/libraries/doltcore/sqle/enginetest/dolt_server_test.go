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
}

// TestDoltMultiSessionBehavior runs tests that exercise multi-session logic on a running SQL server. Statements
// are sent through the server, from out of process, instead of directly to the in-process engine API.
func TestDoltMultiSessionBehavior(t *testing.T) {
	// When this test runs with the new storage engine format, we get a panic about an unknown message id.
	// Ex: https://github.com/dolthub/dolt/runs/6679643619?check_suite_focus=true
	skipNewFormat(t)

	testMultiSessionScriptTests(t, DoltBranchMultiSessionScriptTests)
}

func testMultiSessionScriptTests(t *testing.T, tests []queries.ScriptTest) {
	sc, serverConfig := startServer(t)
	defer sc.StopServer()

	for _, test := range tests {
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
			case "TEXT":
				actualValue, ok := dest[j].(*string)
				require.True(t, ok)
				require.Equal(t, expectedValue, *actualValue)
			case "INT", "TINYINT", "BIGINT":
				actualValue, ok := dest[j].(*int)
				require.True(t, ok)
				require.Equal(t, expectedValue, *actualValue)
			default:
				require.Fail(t, "Unsupported datatype: %s", columnTypes[j].DatabaseTypeName())
			}
		}
	}

	if rows.Next() {
		require.Fail(t, "More results than expected")
	}
}

func startServer(t *testing.T) (*sqlserver.ServerController, sqlserver.ServerConfig) {
	dEnv := dtestutils.CreateEnvWithSeedData(t)
	port := 15403 + rand.Intn(25)
	serverConfig := sqlserver.DefaultServerConfig().WithPort(port)

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
