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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBinlogReplicationServerRestart tests that a replica can be configured and started, then the
// server process can be restarted and replica can be restarted without problems.
func TestBinlogReplicationServerRestart(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicaSystemVars)
	h.startReplicationAndCreateTestDb(h.mySqlPort)

	h.primaryDatabase.MustExec("create table t (pk int auto_increment primary key)")

	// Launch a goroutine that inserts data for 5 seconds
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		limit := 5 * time.Second
		for startTime := time.Now(); time.Now().Sub(startTime) <= limit; {
			h.primaryDatabase.MustExec("insert into t values (DEFAULT);")
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Let the replica process a few transactions, then stop the server and pause a second
	h.waitForReplicaToReachGtid(3)
	h.stopDoltSqlServer()
	time.Sleep(1000 * time.Millisecond)

	var err error
	h.doltPort, h.doltProcess, err = h.startDoltSqlServer(nil)
	require.NoError(t, err)

	// Check replication status on the replica and assert configuration persisted
	status := h.showReplicaStatus()
	// The default Connect_Retry interval is 60s; but some tests configure a faster connection retry interval
	require.True(t, status["Connect_Retry"] == "5" || status["Connect_Retry"] == "60")
	require.Equal(t, "86400", status["Source_Retry_Count"])
	require.Equal(t, "localhost", status["Source_Host"])
	require.NotEmpty(t, status["Source_Port"])
	require.NotEmpty(t, status["Source_User"])

	// Restart replication on replica
	// TODO: For now, we have to set server_id each time we start the service.
	//       Turn this into a persistent sys var
	h.replicaDatabase.MustExec("set @@global.server_id=123;")
	h.replicaDatabase.MustExec("START REPLICA")

	// Assert that all changes have replicated from the primary
	wg.Wait()
	h.waitForReplicaToCatchUp()
	countMaxQuery := "SELECT COUNT(pk) AS count, MAX(pk) as max FROM db01.t;"
	primaryRows, err := h.primaryDatabase.Queryx(countMaxQuery)
	require.NoError(t, err)
	replicaRows, err := h.replicaDatabase.Queryx(countMaxQuery)
	require.NoError(t, err)
	primaryRow := convertMapScanResultToStrings(readNextRow(t, primaryRows))
	replicaRow := convertMapScanResultToStrings(readNextRow(t, replicaRows))
	require.Equal(t, primaryRow["count"], replicaRow["count"])
	require.Equal(t, primaryRow["max"], replicaRow["max"])
	require.NoError(t, replicaRows.Close())
}
