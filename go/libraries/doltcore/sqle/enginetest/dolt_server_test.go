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
	"runtime"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
)

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

// TestPersistVariable tests persisting variables across server starts
func TestPersistVariable(t *testing.T) {
	testSerialSessionScriptTests(t, PersistVariableTests)
}

func TestDoltServerRunningUnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets not supported on Windows")
	}
	const defaultUnixSocketPath = "/tmp/mysql.sock"

	// Running unix socket server
	dEnv, sc, serverConfig := startServer(t, false, "", defaultUnixSocketPath)
	sc.WaitForStart()
	defer dEnv.DoltDB(ctx).Close()
	require.True(t, strings.Contains(servercfg.ConnectionString(serverConfig, "dolt"), "unix"))

	// default unix socket connection works
	localConn, localSess := newConnection(t, serverConfig)
	rows, err := localSess.Query("select 1")
	require.NoError(t, err)
	assertResultsEqual(t, []sql.Row{{1}}, rows)

	t.Run("connecting to local server with tcp connections", func(t *testing.T) {
		// connect with port defined
		serverConfigWithPortOnly := sqlserver.DefaultCommandLineServerConfig().WithPort(3306)
		conn1, sess1 := newConnection(t, serverConfigWithPortOnly)
		rows1, err := sess1.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows1)

		// connect with host defined
		serverConfigWithPortandHost := sqlserver.DefaultCommandLineServerConfig().WithHost("127.0.0.1")
		conn2, sess2 := newConnection(t, serverConfigWithPortandHost)
		rows2, err := sess2.Query("select 1")
		require.NoError(t, err)
		assertResultsEqual(t, []sql.Row{{1}}, rows2)

		// connect with port and host defined
		serverConfigWithPortandHost1 := sqlserver.DefaultCommandLineServerConfig().WithPort(3306).WithHost("0.0.0.0")
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
	sc.Stop()
	err = sc.WaitForStop()
	require.NoError(t, err)
	require.NoFileExists(t, defaultUnixSocketPath)

	// Running TCP socket server
	dEnv, tcpSc, tcpServerConfig := startServer(t, true, "0.0.0.0", "")
	tcpSc.WaitForStart()
	defer dEnv.DoltDB(ctx).Close()
	require.False(t, strings.Contains(servercfg.ConnectionString(tcpServerConfig, "dolt"), "unix"))

	t.Run("host and port specified, there should not be unix socket created", func(t *testing.T) {
		// unix socket connection should fail
		localServerConfig := sqlserver.DefaultCommandLineServerConfig().WithSocket(defaultUnixSocketPath)
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
	tcpSc.Stop()
	err = tcpSc.WaitForStop()
	require.NoError(t, err)
}
