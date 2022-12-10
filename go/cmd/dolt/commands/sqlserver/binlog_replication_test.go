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
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStartReplicaServer(t *testing.T) {
	serverController := NewServerController()
	go func() {
		ctx := context.Background()
		dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")

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
		require.Equal(t, server.UnixSocketInUseError, err)
	}

	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/", nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	time.Sleep(60 * time.Second)
}
