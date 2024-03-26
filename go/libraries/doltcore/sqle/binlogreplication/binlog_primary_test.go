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

package binlogreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBinlogPrimary runs a simple sanity check that a MySQL replica can connect to a Dolt primary and receive
// binlog events.
func TestBinlogPrimary(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// Swap the replica and primary databases, since we're
	// replicating in the other direction in this test.
	var tempDatabase = primaryDatabase
	primaryDatabase = replicaDatabase
	replicaDatabase = tempDatabase

	// Clear out any existing GTID record on the replica
	replicaDatabase.MustExec("reset binary logs and gtids;")

	// On the Primary, we need to set @@server_uuid to a valid UUID:
	primaryDatabase.MustExec("set GLOBAL server_uuid='3ab04dd4-8c9e-471e-a223-9712a3b7c37e';")

	// On the Primary, turn on GTID mode
	// NOTE: Dolt doesn't currently require moving through the GTID_MODE states like this, but
	//       MySQL does, so we do it here anyway.
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='OFF_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL ENFORCE_GTID_CONSISTENCY='ON';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON';")

	// On the Primary, make sure we have a non-zero SERVER_ID set
	primaryDatabase.MustExec("set GLOBAL SERVER_ID=42;")

	// Create the replication user on the Dolt primary server
	// TODO: this should be done on both as part of the shared setup code
	primaryDatabase.MustExec("CREATE USER 'replicator'@'%' IDENTIFIED BY 'Zqr8_blrGm1!';")
	primaryDatabase.MustExec("GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';")

	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(500 * time.Millisecond)

	// Sanity check on SHOW REPLICA STATUS
	rows, err := replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)
	allRows := readAllRows(t, rows)
	require.Equal(t, 1, len(allRows))
	require.NoError(t, rows.Close())
	fmt.Printf("SHOW REPLICA STATUS: %v\n", allRows)
	require.Equal(t, "3ab04dd4-8c9e-471e-a223-9712a3b7c37e:1-2", allRows[0]["Executed_Gtid_Set"])
	require.Equal(t, "", allRows[0]["Last_IO_Error"])
	require.Equal(t, "", allRows[0]["Last_SQL_Error"])
	require.Equal(t, "Yes", allRows[0]["Replica_IO_Running"])
	require.Equal(t, "Yes", allRows[0]["Replica_SQL_Running"])

	// Test that the table was created and one row inserted
	rows, err = replicaDatabase.Queryx("select * from db01.t;")
	require.NoError(t, err)
	allRows = readAllRows(t, rows)
	require.Equal(t, 1, len(allRows))
	require.NoError(t, rows.Close())
	require.Equal(t, "1076895760", allRows[0]["pk"])
	require.Equal(t, "abcd", allRows[0]["c1"])
}
