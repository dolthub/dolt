// Copyright 2023 Dolthub, Inc.
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

package binlogreplication

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// TestBinlogReplicationServerRestart tests that a replica can be configured and started, then the server process can
// be restarted and replica can be restarted without problems.
func TestBinlogReplicationServerRestart(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	var wg sync.WaitGroup

	// Launch a goroutine that inserts data for 5 seconds
	primaryDatabase.MustExec("create table t (pk int auto_increment primary key)")
	wg.Add(1)
	go func() {
		defer wg.Done()
		limit := 5 * time.Second
		for startTime := time.Now(); time.Now().Sub(startTime) <= limit; {
			primaryDatabase.MustExec("insert into t values (DEFAULT);")
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Let replication run for a second, then restart the Dolt sql-server
	time.Sleep(1 * time.Second)
	stopDoltSqlServer(t)
	time.Sleep(1 * time.Second)
	var err error
	doltPort, doltProcess, err = startDoltSqlServer(testDir)
	require.NoError(t, err)

	// Check replication status on the replica and assert configuration is still present
	// TODO: This currently fails, because there is no replica configuration/status persisted, so when the
	//       Dolt sql-server starts up again, `show replica status` returns zero rows.
	status := showReplicaStatus(t)
	convertByteArraysToStrings(status)
	require.Equal(t, "5", status["Connect_Retry"])
	require.Equal(t, "86400", status["Source_Retry_Count"])
	require.Equal(t, "localhost", status["Source_Host"])
	require.NotEmpty(t, status["Source_Port"])
	require.NotEmpty(t, status["Source_User"])

	// Restart replication on replica
	replicaDatabase.MustExec("START REPLICA")

	// Assert that all changes have replicated from the primary
	wg.Wait()
	primaryRows, err := primaryDatabase.Queryx("SELECT COUNT(*) AS count FROM t;")
	require.NoError(t, err)
	replicaRows, err := replicaDatabase.Queryx("SELECT COUNT(*) AS count FROM t;")
	require.NoError(t, err)
	primaryRow := readNextRow(t, primaryRows)
	replicaRow := readNextRow(t, replicaRows)
	require.Equal(t, primaryRow["count"], replicaRow["count"])
}
