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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBinlogReplicationFilters_ignoreTablesOnly tests that the ignoreTables replication
// filtering option is correctly applied and honored.
func TestBinlogReplicationFilters_ignoreTablesOnly(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplication(t, mySqlPort)

	// Ignore replication events for db01.t2. Also tests that the first filter setting is overwritten by
	// the second and that db and that db and table names are case-insensitive.
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);")
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(DB01.T2);")

	// Assert that status shows replication filters
	status := showReplicaStatus(t)
	require.Equal(t, "db01.t2", status["Replicate_Ignore_Table"])
	require.Equal(t, "", status["Replicate_Do_Table"])

	// Make changes on the primary
	primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	waitForReplicaToCatchUp(t)

	// Verify that all changes from t1 were applied on the replica
	rows, err := replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that no changes from t2 were applied on the replica
	rows, err = replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_doTablesOnly tests that the doTables replication
// filtering option is correctly applied and honored.
func TestBinlogReplicationFilters_doTablesOnly(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplication(t, mySqlPort)

	// Do replication events for db01.t1. Also tests that the first filter setting is overwritten by
	// the second and that db and that db and table names are case-insensitive.
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(db01.t2);")
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(DB01.T1);")

	// Assert that status shows replication filters
	status := showReplicaStatus(t)
	require.Equal(t, "db01.t1", status["Replicate_Do_Table"])
	require.Equal(t, "", status["Replicate_Ignore_Table"])

	// Make changes on the primary
	primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	waitForReplicaToCatchUp(t)

	// Verify that all changes from t1 were applied on the replica
	rows, err := replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that no changes from t2 were applied on the replica
	rows, err = replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_doTablesAndIgnoreTables tests that the doTables and ignoreTables
// replication filtering options are correctly applied and honored when used together.
func TestBinlogReplicationFilters_doTablesAndIgnoreTables(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplication(t, mySqlPort)

	// Do replication events for db01.t1, and db01.t2
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(db01.t1, db01.t2);")
	// Ignore replication events for db01.t2
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t2);")

	// Assert that replica status shows replication filters
	status := showReplicaStatus(t)
	require.True(t, status["Replicate_Do_Table"] == "db01.t1,db01.t2" ||
		status["Replicate_Do_Table"] == "db01.t2,db01.t1")
	require.Equal(t, "db01.t2", status["Replicate_Ignore_Table"])

	// Make changes on the primary
	primaryDatabase.MustExec("CREATE TABLE db01.t1 (pk INT PRIMARY KEY);")
	primaryDatabase.MustExec("CREATE TABLE db01.t2 (pk INT PRIMARY KEY);")
	for i := 1; i < 12; i++ {
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t1 VALUES (%d);", i))
		primaryDatabase.MustExec(fmt.Sprintf("INSERT INTO db01.t2 VALUES (%d);", i))
	}
	primaryDatabase.MustExec("UPDATE db01.t1 set pk = pk-1;")
	primaryDatabase.MustExec("UPDATE db01.t2 set pk = pk-1;")
	primaryDatabase.MustExec("DELETE FROM db01.t1 WHERE pk = 10;")
	primaryDatabase.MustExec("DELETE FROM db01.t2 WHERE pk = 10;")

	// Pause to let the replica catch up
	waitForReplicaToCatchUp(t)

	// Verify that all changes from t1 were applied on the replica
	rows, err := replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])
	require.NoError(t, rows.Close())

	// Verify that no changes from t2 were applied on the replica
	rows, err = replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationFilters_errorCases test returned errors for various error cases.
func TestBinlogReplicationFilters_errorCases(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// All tables must be qualified with a database
	_, err := replicaDatabase.Queryx("CHANGE REPLICATION FILTER REPLICATE_DO_TABLE=(t1);")
	require.Error(t, err)
	require.ErrorContains(t, err, "no database specified for table")

	_, err = replicaDatabase.Queryx("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(t1);")
	require.Error(t, err)
	require.ErrorContains(t, err, "no database specified for table")
}
