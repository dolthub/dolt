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

package sqlserver

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStartReplicaServer(t *testing.T) {
	// Start a Dolt SQL server...
	serverController := NewServerController()
	go func() {
		dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, createTestRepoDir(t), doltdb.LocalDirDoltDB, "test")

		startServer(context.Background(), "0.0.0", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "root",
			"-p", "",
			"-t", "5",
			"-l", "info",
		}, dEnv, serverController)
	}()
	err := serverController.WaitForStart()
	if err != nil {
		// UnixSocketInUseError is okay to see, but anything else is a test failure
		require.Equal(t, server.UnixSocketInUseError, err)
	}

	// Enable replication
	go func() {
		// TODO: Add starting GTID to config
		replicaConfiguration := NewReplicaConfiguration(
			"9b724fbe-7a7a-11ed-a935-00414aad8698",
			&mysql.ConnParams{
				Host:  "localhost",
				Port:  54321,
				Uname: "root",
				Pass:  "",
			})
		err := replicaBinlogEventHandler(createTestSqlContext(t), replicaConfiguration)
		require.NoError(t, err)
	}()

	// TODO: Why doesn't the very first call to create a db get replicated?

	time.Sleep(60 * time.Second)

	// TODO: Disable replication
}

func createTestRepoDir(t *testing.T) filesys.Filesys {
	tmpDir, err := filesys.LocalFilesysWithWorkingDir("/tmp/")
	require.NoError(t, err)

	exists, _ := tmpDir.Exists("doltReplicaTest")
	if exists {
		err = tmpDir.Delete("doltReplicaTest", true)
		require.NoError(t, err)
	}
	err = tmpDir.MkDirs("doltReplicaTest")
	require.NoError(t, err)

	doltRepoDir, err := tmpDir.WithWorkingDir("doltReplicaTest")
	require.NoError(t, err)

	return doltRepoDir
}

func createTestSqlContext(t *testing.T) *sql.Context {
	server := sqlserver.GetRunningServer()
	if server == nil {
		panic("unable to access running SQL server")
	}

	// TODO: Hack up a fake connection so we can get a sql.Context to work with...
	//       This seems to work... but is super hacky and will cause problems when
	//       a real connection comes in with connection ID 123456
	conn := mysql.Conn{
		ConnectionID: 123456,
	}

	ctx, err := server.SessionManager().NewContext(&conn)
	require.NoError(t, err)

	// TODO: Is this still needed?
	ctx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})

	return ctx
}
