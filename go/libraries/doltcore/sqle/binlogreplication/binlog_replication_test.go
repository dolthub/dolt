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

package binlogreplication

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

var mySqlPort, doltPort int
var primaryDatabase, replicaDatabase *sqlx.DB
var mySqlProcess, doltProcess *os.Process
var doltLogFilePath, oldDoltLogFilePath, mysqlLogFilePath string
var doltLogFile, mysqlLogFile *os.File
var testDir string
var originalWorkingDir string

// doltReplicaSystemVars are the common system variables that need
// to be set on a Dolt replica before replication is turned on.
var doltReplicaSystemVars = map[string]string{
	"server_id": "42",
}

func teardown(t *testing.T) {
	if mySqlProcess != nil {
		stopMySqlServer(t)
	}
	if doltProcess != nil {
		stopDoltSqlServer(t)
	}
	if mysqlLogFile != nil {
		mysqlLogFile.Close()
	}
	if doltLogFile != nil {
		doltLogFile.Close()
	}

	// Output server logs on failure for easier debugging
	if t.Failed() {
		if oldDoltLogFilePath != "" {
			fmt.Printf("\nDolt server log from %s:\n", oldDoltLogFilePath)
			printFile(oldDoltLogFilePath)
		}

		fmt.Printf("\nDolt server log from %s:\n", doltLogFilePath)
		printFile(doltLogFilePath)
		fmt.Printf("\nMySQL server log from %s:\n", mysqlLogFilePath)
		printFile(mysqlLogFilePath)
		mysqlErrorLogFilePath := filepath.Join(filepath.Dir(mysqlLogFilePath), "error_log.err")
		fmt.Printf("\nMySQL server error log from %s:\n", mysqlErrorLogFilePath)
		printFile(mysqlErrorLogFilePath)
	} else {
		// clean up temp files on clean test runs
		defer os.RemoveAll(testDir)
	}

	if toxiClient != nil {
		proxies, _ := toxiClient.Proxies()
		for _, value := range proxies {
			value.Delete()
		}
	}
}

// TestBinlogReplicationSanityCheck performs the simplest possible binlog replication test. It starts up
// a MySQL primary and a Dolt replica, and asserts that a CREATE TABLE statement properly replicates to the
// Dolt replica, along with simple insert, update, and delete statements.
func TestBinlogReplicationSanityCheck(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Create a table on the primary and verify on the replica
	primaryDatabase.MustExec("create table tableT (pk int primary key)")
	waitForReplicaToCatchUp(t)
	assertCreateTableStatement(t, replicaDatabase, "tableT",
		"CREATE TABLE tableT ( pk int NOT NULL, PRIMARY KEY (pk)) "+
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin")
	assertRepoStateFileExists(t, "db01")

	// Insert/Update/Delete on the primary
	primaryDatabase.MustExec("insert into tableT values(100), (200)")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT", [][]any{{"100"}, {"200"}})
	primaryDatabase.MustExec("delete from tableT where pk = 100")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT", [][]any{{"200"}})
	primaryDatabase.MustExec("update tableT set pk = 300")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT", [][]any{{"300"}})
}

// TestBinlogReplicationWithHundredsOfDatabases asserts that we can efficiently replicate the creation of hundreds of databases.
func TestBinlogReplicationWithHundredsOfDatabases(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Create a table on the primary and verify on the replica
	primaryDatabase.MustExec("create table tableT (pk int primary key)")
	waitForReplicaToCatchUp(t)
	assertCreateTableStatement(t, replicaDatabase, "tableT",
		"CREATE TABLE tableT ( pk int NOT NULL, PRIMARY KEY (pk)) "+
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin")
	assertRepoStateFileExists(t, "db01")

	// Create a few hundred databases on the primary and let them replicate to the replica
	dbCount := 300
	startTime := time.Now()
	for i := range dbCount {
		dbName := fmt.Sprintf("db%03d", i)
		primaryDatabase.MustExec(fmt.Sprintf("create database %s", dbName))
	}
	waitForReplicaToCatchUp(t)
	endTime := time.Now()
	logrus.Infof("Time to replicate %d databases: %v", dbCount, endTime.Sub(startTime))

	// Spot check the presence of a database on the replica
	assertRepoStateFileExists(t, "db042")

	// Insert some data in one database
	startTime = time.Now()
	primaryDatabase.MustExec("use db042;")
	primaryDatabase.MustExec("create table t (pk int primary key);")
	primaryDatabase.MustExec("insert into t values (100), (101), (102);")

	// Verify the results on the replica
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db042.t;", [][]any{{"100"}, {"101"}, {"102"}})
	endTime = time.Now()
	logrus.Infof("Time to replicate inserts to 1 database (out of %d): %v", endTime.Sub(startTime), dbCount)
}

// TestAutoRestartReplica tests that a Dolt replica automatically starts up replication if
// replication was running when the replica was shut down.
func TestAutoRestartReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)

	// Assert that replication is not running yet
	status := queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// Start up replication and replicate some test data
	startReplicationAndCreateTestDb(t, mySqlPort)
	primaryDatabase.MustExec("create table db01.autoRestartTest(pk int primary key);")
	waitForReplicaToCatchUp(t)
	primaryDatabase.MustExec("insert into db01.autoRestartTest values (100);")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.autoRestartTest;", [][]any{{"100"}})

	// Test for the presence of the replica-running state file
	require.True(t, fileExists(filepath.Join(testDir, "dolt", ".doltcfg", "replica-running")))

	// Restart the Dolt replica
	stopDoltSqlServer(t)
	var err error
	doltPort, doltProcess, err = startDoltSqlServer(testDir, nil)
	require.NoError(t, err)

	// Assert that some test data replicates correctly
	primaryDatabase.MustExec("insert into db01.autoRestartTest values (200);")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.autoRestartTest;",
		[][]any{{"100"}, {"200"}})

	// SHOW REPLICA STATUS should show that replication is running, with no errors
	status = queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "Yes", status["Replica_IO_Running"])
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// Stop replication and assert the replica-running marker file is removed
	replicaDatabase.MustExec("stop replica")
	require.False(t, fileExists(filepath.Join(testDir, "dolt", ".doltcfg", "replica-running")))

	// Restart the Dolt replica
	stopDoltSqlServer(t)
	doltPort, doltProcess, err = startDoltSqlServer(testDir, nil)
	require.NoError(t, err)

	// SHOW REPLICA STATUS should show that replication is NOT running, with no errors
	status = queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])
}

// TestBinlogSystemUserIsLocked tests that the binlog applier user is locked and cannot be used to connect to the server.
func TestBinlogSystemUserIsLocked(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	dsn := fmt.Sprintf("%s@tcp(127.0.0.1:%v)/", binlogApplierUser, doltPort)
	db, err := sqlx.Open("mysql", dsn)
	require.NoError(t, err)

	// Before starting replication, the system account does not exist
	err = db.Ping()
	require.Error(t, err)
	require.ErrorContains(t, err, "No authentication")

	// After starting replication, the system account is locked
	startReplicationAndCreateTestDb(t, mySqlPort)
	err = db.Ping()
	require.Error(t, err)
	require.ErrorContains(t, err, "Access denied for user")
}

// TestFlushLogs tests that binary logs can be flushed on the primary, which forces a new binlog file to be written,
// including sending new Rotate and FormatDescription events to the replica. This is a simple sanity tests that we can
// process the events without errors.
func TestFlushLogs(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Make changes on the primary and verify on the replica
	primaryDatabase.MustExec("create table t (pk int primary key)")
	waitForReplicaToCatchUp(t)
	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	assertCreateTableStatement(t, replicaDatabase, "t", expectedStatement)

	primaryDatabase.MustExec("flush binary logs;")
	waitForReplicaToCatchUp(t)

	primaryDatabase.MustExec("insert into t values (1), (2), (3);")
	waitForReplicaToCatchUp(t)

	requireReplicaResults(t, "select * from db01.t;", [][]any{
		{"1"}, {"2"}, {"3"},
	})
}

// TestResetReplica tests that "RESET REPLICA" and "RESET REPLICA ALL" correctly clear out
// replication configuration and metadata.
func TestResetReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// RESET REPLICA returns an error if replication is running
	_, err := replicaDatabase.Queryx("RESET REPLICA")
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to reset replica while replication is running")

	// Calling RESET REPLICA clears out any errors
	replicaDatabase.MustExec("STOP REPLICA;")
	rows, err := replicaDatabase.Queryx("RESET REPLICA;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	status := queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_Errno"])
	require.Equal(t, "", status["Last_Error"])
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "", status["Last_IO_Error_Timestamp"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_SQL_Error_Timestamp"])

	// Calling RESET REPLICA ALL clears out all replica configuration
	rows, err = replicaDatabase.Queryx("RESET REPLICA ALL;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	status = queryReplicaStatus(t)
	require.Equal(t, "", status["Source_Host"])
	require.Equal(t, "", status["Source_User"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// Now try querying the status using the older, deprecated 'show slave status' statement
	// and spot check that the data is the same, but the column names have changed
	status = querySlaveStatus(t)
	require.Equal(t, "", status["Master_Host"])
	require.Equal(t, "", status["Master_User"])
	require.Equal(t, "No", status["Slave_IO_Running"])
	require.Equal(t, "No", status["Slave_SQL_Running"])

	rows, err = replicaDatabase.Queryx("select * from mysql.slave_master_info;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Start replication again and verify that we can still query replica status
	startReplicationAndCreateTestDb(t, mySqlPort)
	replicaStatus := showReplicaStatus(t)
	require.Equal(t, "0", replicaStatus["Last_Errno"])
	require.Equal(t, "", replicaStatus["Last_Error"])
	require.True(t, replicaStatus["Replica_IO_Running"] == binlogreplication.ReplicaIoRunning ||
		replicaStatus["Replica_IO_Running"] == binlogreplication.ReplicaIoConnecting)
}

// TestStartReplicaErrors tests that the "START REPLICA" command returns appropriate responses
// for various error conditions.
func TestStartReplicaErrors(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)

	// START REPLICA returns an error when no replication source is configured
	_, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, ErrServerNotConfiguredAsReplica.Error())

	// For an incomplete source configuration, throw an error as early as possible to make sure the user notices it.
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, SOURCE_HOST='localhost';")
	rows, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid (empty) username")
	require.Nil(t, rows)

	// SOURCE_AUTO_POSITION cannot be disabled – we only support GTID positioning
	rows, err = replicaDatabase.Queryx("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, " +
		"SOURCE_HOST='localhost', SOURCE_USER='replicator', SOURCE_AUTO_POSITION=0;")
	require.Error(t, err)
	require.ErrorContains(t, err, "Error 1105 (HY000): SOURCE_AUTO_POSITION cannot be disabled")
	require.Nil(t, rows)

	// START REPLICA logs a warning if replication is already running
	startReplicationAndCreateTestDb(t, mySqlPort)
	replicaDatabase.MustExec("START REPLICA;")
	assertWarning(t, replicaDatabase, 3083, "Replication thread(s) for channel '' are already running.")
}

// TestShowReplicaStatus tests various cases "SHOW REPLICA STATUS" that aren't covered by other tests.
func TestShowReplicaStatus(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// Assert that very long hostnames are handled correctly
	longHostname := "really.really.really.really.long.host.name.012345678901234567890123456789012345678901234567890123456789.com"
	replicaDatabase.MustExec(fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s';", longHostname))
	status := showReplicaStatus(t)
	require.Equal(t, longHostname, status["Source_Host"])
}

// TestStopReplica tests that STOP REPLICA correctly stops the replication process, and that
// warnings are logged when STOP REPLICA is invoked when replication is not running.
func TestStopReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)

	// STOP REPLICA logs a warning if replication is not running
	replicaDatabase.MustExec("STOP REPLICA;")
	assertWarning(t, replicaDatabase, 3084, "Replication thread(s) for channel '' are already stopped.")

	// Start replication with bad connection params
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_HOST='doesnotexist', SOURCE_PORT=111, SOURCE_USER='nobody';")
	replicaDatabase.MustExec("START REPLICA;")
	time.Sleep(200 * time.Millisecond)
	status := showReplicaStatus(t)
	require.Equal(t, "Connecting", status["Replica_IO_Running"])
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// STOP REPLICA works when replication cannot establish a connection
	replicaDatabase.MustExec("STOP REPLICA;")
	status = showReplicaStatus(t)
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// START REPLICA and verify status
	startReplicationAndCreateTestDb(t, mySqlPort)
	time.Sleep(100 * time.Millisecond)
	status = showReplicaStatus(t)
	require.True(t, status["Replica_IO_Running"] == "Connecting" || status["Replica_IO_Running"] == "Yes")
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// STOP REPLICA stops replication when it is running and connected to the source
	replicaDatabase.MustExec("STOP REPLICA;")
	status = showReplicaStatus(t)
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// STOP REPLICA logs a warning if replication is not running
	replicaDatabase.MustExec("STOP REPLICA;")
	assertWarning(t, replicaDatabase, 3084, "Replication thread(s) for channel '' are already stopped.")
}

// TestDoltCommits tests that Dolt commits are created and use correct transaction boundaries.
func TestDoltCommits(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// First transaction (DDL)
	primaryDatabase.MustExec("create table t1 (pk int primary key);")

	// Second transaction (DDL)
	primaryDatabase.MustExec("create table t2 (pk int primary key);")

	// Third transaction (autocommit DML)
	primaryDatabase.MustExec("insert into t2 values (0);")

	// Disable autocommit so we can manually control transactions
	primaryDatabase.MustExec("set autocommit=0;")

	// Fourth transaction (explicitly controlled transaction)
	primaryDatabase.MustExec("start transaction;")
	primaryDatabase.MustExec("insert into t1 values(1);")
	primaryDatabase.MustExec("insert into t1 values(2);")
	primaryDatabase.MustExec("insert into t1 values(3);")
	primaryDatabase.MustExec("insert into t2 values(3), (2), (1);")
	primaryDatabase.MustExec("commit;")

	// Verify Dolt commit on replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select count(*) as count from db01.dolt_log;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "5", row["count"])
	require.NoError(t, rows.Close())

	// Use dolt_diff so we can see what tables were edited and schema/data changes
	replicaDatabase.MustExec("use db01;")
	// Note: we don't use an order by clause, since the commits come in so quickly that they get the same timestamp
	rows, err = replicaDatabase.Queryx("select * from db01.dolt_diff;")
	require.NoError(t, err)

	// Fourth transaction
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t1", row["table_name"])
	commitId := row["commit_hash"]
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])
	require.Equal(t, commitId, row["commit_hash"])

	// Third transaction
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])

	// Second transaction
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])

	// First transaction
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.Equal(t, "t1", row["table_name"])

	require.NoError(t, rows.Close())

	// Verify that commit timestamps are unique
	rows, err = replicaDatabase.Queryx("select distinct date from db01.dolt_log;")
	require.NoError(t, err)
	allRows := readAllRowsIntoMaps(t, rows)
	require.Equal(t, 5, len(allRows)) // 4 transactions + 1 initial commit
}

// TestForeignKeyChecks tests that foreign key constraints replicate correctly when foreign key checks are
// enabled and disabled.
func TestForeignKeyChecks(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Test that we can execute statement-based replication that requires foreign_key_checks
	// being turned off (referenced table doesn't exist yet).
	primaryDatabase.MustExec("SET foreign_key_checks = 0;")
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, color varchar(100), FOREIGN KEY (color) REFERENCES colors(name));")
	primaryDatabase.MustExec("CREATE TABLE colors (name varchar(100) primary key);")
	primaryDatabase.MustExec("SET foreign_key_checks = 1;")

	// Insert a record with foreign key checks enabled
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("INSERT INTO colors VALUES ('green'), ('red'), ('blue');")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (1, 'red'), (2, 'green');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Insert path with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("SET foreign_key_checks = 0;")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (3, 'not-a-color');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Update and Delete paths with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("DELETE FROM colors WHERE name='red';")
	primaryDatabase.MustExec("UPDATE t1 SET color='still-not-a-color' WHERE pk=2;")
	primaryDatabase.MustExec("COMMIT;")

	// Verify the changes on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.t1 order by pk;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	require.Equal(t, "red", row["color"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	require.Equal(t, "still-not-a-color", row["color"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	require.Equal(t, "not-a-color", row["color"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.colors order by name;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "blue", row["name"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "green", row["name"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestCharsetsAndCollations tests that we can successfully replicate data using various charsets and collations.
func TestCharsetsAndCollations(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Use non-default charset/collations to create data on the primary
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, c1 varchar(255) COLLATE ascii_general_ci, c2 varchar(255) COLLATE utf16_general_ci);")
	primaryDatabase.MustExec("insert into t1 values (1, \"one\", \"one\");")

	// Verify on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("show create table db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Contains(t, row["Create Table"], "ascii_general_ci")
	require.Contains(t, row["Create Table"], "utf16_general_ci")
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.t1;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "one", row["c1"])
	require.Equal(t, "\x00o\x00n\x00e", row["c2"])
	require.NoError(t, rows.Close())
}

//
// Test Helper Functions
//

// waitForReplicaToCatchUp waits (up to 30s) for the replica to catch up with the primary database. The
// lag is measured by checking that gtid_executed is the same on the primary and replica.
func waitForReplicaToCatchUp(t *testing.T) {
	timeLimit := 30 * time.Second

	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		replicaGtid := queryGtid(t, replicaDatabase)
		primaryGtid := queryGtid(t, primaryDatabase)

		if primaryGtid == replicaGtid {
			return
		} else {
			fmt.Printf("primary and replica not in sync yet... (primary: %s, replica: %s)\n", primaryGtid, replicaGtid)
			time.Sleep(250 * time.Millisecond)
		}
	}

	// Log some status of the replica, before failing the test
	outputShowReplicaStatus(t)
	t.Fatal("primary and replica did not synchronize within " + timeLimit.String())
}

// waitForReplicaToReachGtid waits (up to 10s) for the replica's @@gtid_executed sys var to show that
// it has executed the |target| gtid transaction number.
func waitForReplicaToReachGtid(t *testing.T, target int) {
	timeLimit := 10 * time.Second
	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		time.Sleep(250 * time.Millisecond)
		replicaGtid := queryGtid(t, replicaDatabase)

		if replicaGtid != "" {
			components := strings.Split(replicaGtid, ":")
			require.Equal(t, 2, len(components))
			sourceGtid := components[1]
			if strings.Contains(sourceGtid, "-") {
				gtidRange := strings.Split(sourceGtid, "-")
				require.Equal(t, 2, len(gtidRange))
				sourceGtid = gtidRange[1]
			}

			i, err := strconv.Atoi(sourceGtid)
			require.NoError(t, err)
			if i >= target {
				return
			}
		}

		fmt.Printf("replica has not reached transaction %d yet; currently at: %s \n", target, replicaGtid)
	}

	t.Fatal("replica did not reach target GTID within " + timeLimit.String())
}

// assertWarning asserts that the specified |database| has a warning with |code| and |message|,
// otherwise it will fail the current test.
func assertWarning(t *testing.T, database *sqlx.DB, code int, message string) {
	rows, err := database.Queryx("SHOW WARNINGS;")
	require.NoError(t, err)
	warning := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, strconv.Itoa(code), warning["Code"])
	require.Equal(t, message, warning["Message"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

func queryGtid(t *testing.T, database *sqlx.DB) string {
	rows, err := database.Queryx("SELECT @@global.gtid_executed as gtid_executed;")
	require.NoError(t, err)
	defer rows.Close()
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	if row["gtid_executed"] == nil {
		t.Fatal("no value for @@GLOBAL.gtid_executed")
	}
	return row["gtid_executed"].(string)
}

func readNextRow(t *testing.T, rows *sqlx.Rows) map[string]interface{} {
	row := make(map[string]interface{})
	require.True(t, rows.Next())
	err := rows.MapScan(row)
	require.NoError(t, err)
	return row
}

// readAllRowsIntoMaps reads all data from |rows| and returns a slice of maps, where each key
// in the map is the field name, and each value is the string representation of the field value.
func readAllRowsIntoMaps(t *testing.T, rows *sqlx.Rows) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for {
		row := make(map[string]interface{})
		if rows.Next() == false {
			return result
		}
		err := rows.MapScan(row)
		require.NoError(t, err)
		row = convertMapScanResultToStrings(row)
		result = append(result, row)
	}
}

// readAllRowsIntoSlices reads all data from |rows| and returns a slice of slices, with
// all values converted to strings.
func readAllRowsIntoSlices(t *testing.T, rows *sqlx.Rows) [][]any {
	result := make([][]any, 0)
	for {
		if rows.Next() == false {
			return result
		}
		row, err := rows.SliceScan()
		require.NoError(t, err)
		row = convertSliceScanResultToStrings(row)
		result = append(result, row)
	}
}

// startSqlServers starts a MySQL server and a Dolt sql-server for use in tests.
func startSqlServers(t *testing.T) {
	startSqlServersWithDoltSystemVars(t, nil)
}

// startSqlServersWithDoltSystemVars starts a MySQL server and a Dolt sql-server for use in tests. Before the
// Dolt sql-server is started, the specified |doltPersistentSystemVars| are persisted in the Dolt sql-server's
// local configuration. These are useful when you need to set system variables that must be available when the
// sql-server starts up, such as replication system variables.
func startSqlServersWithDoltSystemVars(t *testing.T, doltPersistentSystemVars map[string]string) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping binlog replication integ tests on Windows OS")
	} else if runtime.GOOS == "darwin" && os.Getenv("CI") == "true" {
		t.Skip("Skipping binlog replication integ tests in CI environment on Mac OS")
	}

	testDir = filepath.Join(os.TempDir(), fmt.Sprintf("%s-%v", t.Name(), time.Now().Unix()))
	err := os.MkdirAll(testDir, 0777)

	cmd := exec.Command("chmod", "777", testDir)
	_, err = cmd.Output()
	if err != nil {
		panic(err)
	}

	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", testDir)

	// Start up primary and replica databases
	mySqlPort, mySqlProcess, err = startMySqlServer(testDir)
	require.NoError(t, err)
	doltPort, doltProcess, err = startDoltSqlServer(testDir, doltPersistentSystemVars)
	require.NoError(t, err)
}

// stopMySqlServer stops the running MySQL server. If any errors are encountered while stopping
// the MySQL server, this function will fail the current test.
func stopMySqlServer(t *testing.T) {
	require.NoError(t, mySqlProcess.Kill())
}

// stopDoltSqlServer stops the running Dolt sql-server. If any errors are encountered while
// stopping the Dolt sql-server, this function will fail the current test.
func stopDoltSqlServer(t *testing.T) {
	// Use the negative process ID so that we grab the entire process group.
	// This is necessary to kill all the processes the child spawns.
	// Note that we use os.FindProcess, instead of syscall.Kill, since syscall.Kill
	// is not available on windows.
	p, err := os.FindProcess(-doltProcess.Pid)
	require.NoError(t, err)

	err = p.Signal(syscall.SIGKILL)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)

	// Remove the sql-server lock file so that we can restart cleanly
	lockFilepath := filepath.Join(testDir, "dolt", "db01", ".dolt", "sql-server.lock")
	stat, _ := os.Stat(lockFilepath)
	if stat != nil {
		err = os.Remove(lockFilepath)
		require.NoError(t, err)
	}
	// Remove the global sql-server lock file as well
	lockFilepath = filepath.Join(testDir, "dolt", ".dolt", "sql-server.lock")
	stat, _ = os.Stat(lockFilepath)
	if stat != nil {
		err = os.Remove(lockFilepath)
		require.NoError(t, err)
	}
}

// startReplication configures the replication source on the replica and runs the START REPLICA statement.
func startReplication(_ *testing.T, port int) {
	replicaDatabase.MustExec(
		fmt.Sprintf("change replication source to SOURCE_HOST='localhost', "+
			"SOURCE_USER='replicator', SOURCE_PASSWORD='Zqr8_blrGm1!', "+
			"SOURCE_PORT=%v, SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=5;", port))

	replicaDatabase.MustExec("start replica;")
}

// startReplicationAndCreateTestDb starts up replication on the replica, connecting to |port| on the primary,
// creates the test database, db01, on the primary, and ensures it gets replicated to the replica.
func startReplicationAndCreateTestDb(t *testing.T, port int) {
	startReplicationAndCreateTestDbWithDelay(t, port, 100*time.Millisecond)
}

// startReplicationAndCreateTestDbWithDelay starts up replication on the replica, connecting to |port| on the primary,
// pauses for |delay| before creating the test database, db01, on the primary, and ensures it
// gets replicated to the replica.
func startReplicationAndCreateTestDbWithDelay(t *testing.T, port int, delay time.Duration) {
	startReplication(t, port)
	time.Sleep(delay)

	// Look to see if the test database, db01, has been created yet. If not, create it and wait for it to
	// replicate to the replica. Note that when re-starting replication in certain tests, we can't rely on
	// the replica to contain all GTIDs (i.e. Dolt -> MySQL replication when restarting the replica, since
	// Dolt doesn't yet resend events that occurred while the replica wasn't connected).
	dbNames := mustListDatabases(t, primaryDatabase)
	if !slices.Contains(dbNames, "db01") {
		primaryDatabase.MustExec("create database db01;")
		waitForReplicaToCatchUp(t)
	}
	primaryDatabase.MustExec("use db01;")
	_, _ = replicaDatabase.Exec("use db01;")
}

func assertCreateTableStatement(t *testing.T, database *sqlx.DB, table string, expectedStatement string) {
	rows, err := database.Queryx("show create table db01." + table + ";")
	require.NoError(t, err)
	var actualTable, actualStatement string
	require.True(t, rows.Next())
	err = rows.Scan(&actualTable, &actualStatement)
	require.NoError(t, err)
	require.Equal(t, table, actualTable)
	require.NotNil(t, actualStatement)
	actualStatement = sanitizeCreateTableString(actualStatement)
	require.Equal(t, expectedStatement, actualStatement)
}

func sanitizeCreateTableString(statement string) string {
	statement = strings.ReplaceAll(statement, "`", "")
	statement = strings.ReplaceAll(statement, "\n", "")
	regex := regexp.MustCompile("\\s+")
	return regex.ReplaceAllString(statement, " ")
}

// findFreePort returns an available port that can be used for a server. If any errors are
// encountered, this function will panic and fail the current test.
func findFreePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}
	freePort := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}

	if freePort < 0 {
		panic(fmt.Sprintf("unable to find available TCP port; found port %v", freePort))
	}

	return freePort
}

// startMySqlServer configures a starts a fresh MySQL server instance and returns the port it is running on,
// and the os.Process handle. If unable to start up the MySQL server, an error is returned.
func startMySqlServer(dir string) (int, *os.Process, error) {
	originalCwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	dir = dir + string(os.PathSeparator) + "mysql" + string(os.PathSeparator)
	dataDir := dir + "mysql_data"
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return -1, nil, err
	}
	cmd := exec.Command("chmod", "777", dir)
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	err = os.Chdir(dir)
	if err != nil {
		return -1, nil, err
	}

	mySqlPort = findFreePort()

	// MySQL will NOT start up as the root user, so if we're running as root
	// (e.g. in a CI env), use the "mysql" user instead.
	user, err := user.Current()
	if err != nil {
		panic("unable to determine current user: " + err.Error())
	}
	username := user.Username
	if username == "root" {
		fmt.Printf("overriding current user (root) to run mysql as 'mysql' user instead\n")
		username = "mysql"
	}

	// Check to see if the MySQL data directory has the "mysql" directory in it, which
	// tells us whether this MySQL instance has been initialized yet or not.
	initialized := directoryExists(filepath.Join(dataDir, "mysql"))
	if !initialized {
		// Create a fresh MySQL server for the primary
		chmodCmd := exec.Command("mysqld",
			"--no-defaults",
			"--user="+username,
			"--initialize-insecure",
			"--datadir="+dataDir,
			"--default-authentication-plugin=mysql_native_password")
		output, err = chmodCmd.CombinedOutput()
		if err != nil {
			return -1, nil, fmt.Errorf("unable to execute command %v: %v – %v", cmd.String(), err.Error(), string(output))
		}
	}

	cmd = exec.Command("mysqld",
		"--no-defaults",
		"--user="+username,
		"--datadir="+dataDir,
		"--gtid-mode=ON",
		"--skip-replica-start=ON",
		"--enforce-gtid-consistency=ON",
		fmt.Sprintf("--port=%v", mySqlPort),
		"--server-id=11223344",
		fmt.Sprintf("--socket=mysql-%v.sock", mySqlPort),
		"--general_log_file="+dir+"general_log",
		"--slow_query_log_file="+dir+"slow_query_log",
		"--log-error="+dir+"error_log",
		fmt.Sprintf("--pid-file="+dir+"pid-%v.pid", mySqlPort))

	mysqlLogFilePath = filepath.Join(dir, fmt.Sprintf("mysql-%d.out.log", time.Now().Unix()))
	mysqlLogFile, err = os.Create(mysqlLogFilePath)
	if err != nil {
		return -1, nil, err
	}
	fmt.Printf("MySQL server logs at: %s \n", mysqlLogFilePath)
	cmd.Stdout = mysqlLogFile
	cmd.Stderr = mysqlLogFile
	err = cmd.Start()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to start process %q: %v", cmd.String(), err.Error())
	}

	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%v)/", mySqlPort)
	primaryDatabase = sqlx.MustOpen("mysql", dsn)

	err = waitForSqlServerToStart(primaryDatabase)
	if err != nil {
		return -1, nil, err
	}

	// Ensure the replication user exists with the right grants when we initialize
	// the MySQL server for the first time
	if !initialized {
		mustCreateReplicatorUser(primaryDatabase)
	}

	dsn = fmt.Sprintf("root@tcp(127.0.0.1:%v)/", mySqlPort)
	primaryDatabase = sqlx.MustOpen("mysql", dsn)

	os.Chdir(originalCwd)
	fmt.Printf("MySQL server started on port %v \n", mySqlPort)

	return mySqlPort, cmd.Process, nil
}

// directoryExists returns true if the specified |path| is to a directory that exists, otherwise,
// if the path doesn't exist or isn't a directory, false is returned.
func directoryExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

var cachedDoltDevBuildPath = ""

func initializeDevDoltBuild(dir string, goDirPath string) string {
	if cachedDoltDevBuildPath != "" {
		return cachedDoltDevBuildPath
	}

	// If we're not in a CI environment, don't worry about building a dev build
	if os.Getenv("CI") != "true" {
		return ""
	}

	basedir := filepath.Dir(filepath.Dir(dir))
	fullpath := filepath.Join(basedir, fmt.Sprintf("devDolt-%d", os.Getpid()))

	_, err := os.Stat(fullpath)
	if err == nil {
		return fullpath
	}

	fmt.Printf("building dolt dev build at: %s \n", fullpath)
	cmd := exec.Command("go", "build", "-o", fullpath, "./cmd/dolt")
	cmd.Dir = goDirPath

	output, err := cmd.CombinedOutput()
	if err != nil {
		panic("unable to build dolt for binlog integration tests: " + err.Error() + "\nFull output: " + string(output) + "\n")
	}
	cachedDoltDevBuildPath = fullpath

	return cachedDoltDevBuildPath
}

// startDoltSqlServer starts a Dolt sql-server on a free port from the specified directory |dir|. If
// |doltPeristentSystemVars| is populated, then those system variables will be set, persistently, for
// the Dolt database, before the Dolt sql-server is started.
func startDoltSqlServer(dir string, doltPersistentSystemVars map[string]string) (int, *os.Process, error) {
	dir = filepath.Join(dir, "dolt")
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return -1, nil, err
	}

	// If we already assigned a port, re-use it. This is useful when testing restarting a primary, since
	// we want the primary to come back up on the same port, so the replica can reconnect.
	if doltPort < 1 {
		doltPort = findFreePort()
	}
	fmt.Printf("Starting Dolt sql-server on port: %d, with data dir %s\n", doltPort, dir)

	// take the CWD and move up four directories to find the go directory
	if originalWorkingDir == "" {
		var err error
		originalWorkingDir, err = os.Getwd()
		if err != nil {
			panic(err)
		}
	}
	goDirPath := filepath.Join(originalWorkingDir, "..", "..", "..", "..")
	err = os.Chdir(goDirPath)
	if err != nil {
		panic(err)
	}

	socketPath := filepath.Join("/tmp", fmt.Sprintf("dolt.%v.sock", doltPort))

	// use an admin user NOT named "root" to test that we don't require the "root" account
	adminUser := "admin"

	if doltPersistentSystemVars != nil && len(doltPersistentSystemVars) > 0 {
		// Initialize the dolt directory first
		err = runDoltCommand(dir, goDirPath, "init", "--name=binlog-test", "--email=binlog@test")
		if err != nil {
			return -1, nil, err
		}

		for systemVar, value := range doltPersistentSystemVars {
			query := fmt.Sprintf("SET @@PERSIST.%s=%s;", systemVar, value)
			err = runDoltCommand(dir, goDirPath, "sql", fmt.Sprintf("-q=%s", query))
			if err != nil {
				return -1, nil, err
			}
		}
	}

	args := []string{"go", "run", "./cmd/dolt",
		"sql-server",
		fmt.Sprintf("-u%s", adminUser),
		"--loglevel=TRACE",
		fmt.Sprintf("--data-dir=%s", dir),
		fmt.Sprintf("--port=%v", doltPort),
		fmt.Sprintf("--socket=%s", socketPath)}

	// If we're running in CI, use a precompiled dolt binary instead of go run
	devDoltPath := initializeDevDoltBuild(dir, goDirPath)
	if devDoltPath != "" {
		args[2] = devDoltPath
		args = args[2:]
	}
	cmd := exec.Command(args[0], args[1:]...)

	// Set a unique process group ID so that we can cleanly kill this process, as well as
	// any spawned child processes later. Mac/Unix can set the "Setpgid" field directly, but
	// on windows, this field isn't present, so we need to use reflection so that this code
	// can still compile for windows, even though we don't run it there.
	procAttr := &syscall.SysProcAttr{}
	ps := reflect.ValueOf(procAttr)
	s := ps.Elem()
	f := s.FieldByName("Setpgid")
	f.SetBool(true)
	cmd.SysProcAttr = procAttr

	// Some tests restart the Dolt sql-server, so if we have a current log file, save a reference
	// to it so we can print the results later if the test fails.
	if doltLogFilePath != "" {
		oldDoltLogFilePath = doltLogFilePath
	}

	doltLogFilePath = filepath.Join(dir, fmt.Sprintf("dolt-%d.out.log", time.Now().Unix()))
	doltLogFile, err = os.Create(doltLogFilePath)
	if err != nil {
		return -1, nil, err
	}
	fmt.Printf("dolt sql-server logs at: %s \n", doltLogFilePath)
	cmd.Stdout = doltLogFile
	cmd.Stderr = doltLogFile
	err = cmd.Start()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to execute command %v: %v", cmd.String(), err.Error())
	}

	fmt.Printf("Dolt CMD: %s\n", cmd.String())

	dsn := fmt.Sprintf("%s@tcp(127.0.0.1:%v)/", adminUser, doltPort)
	replicaDatabase = sqlx.MustOpen("mysql", dsn)

	err = waitForSqlServerToStart(replicaDatabase)
	if err != nil {
		return -1, nil, err
	}

	mustCreateReplicatorUser(replicaDatabase)
	fmt.Printf("Dolt server started on port %v \n", doltPort)

	return doltPort, cmd.Process, nil
}

// mustCreateReplicatorUser creates the replicator user on the specified |db| and grants them replication slave privs.
func mustCreateReplicatorUser(db *sqlx.DB) {
	db.MustExec("CREATE USER if not exists 'replicator'@'%' IDENTIFIED BY 'Zqr8_blrGm1!';")
	db.MustExec("GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';")
}

// runDoltCommand runs a short-lived dolt CLI command with the specified arguments from |doltArgs|. The Dolt data
// directory is specified from |doltDataDir| and |goDirPath| is the path to the go directory within the Dolt repo.
// This function will only return when the Dolt CLI command has completed, so it is not suitable for running
// long-lived commands such as "sql-server". If the command fails, an error is returned with the combined output.
func runDoltCommand(doltDataDir string, goDirPath string, doltArgs ...string) error {
	// If we're running in CI, use a precompiled dolt binary instead of go run
	devDoltPath := initializeDevDoltBuild(doltDataDir, goDirPath)

	args := append([]string{"go", "run", "./cmd/dolt",
		fmt.Sprintf("--data-dir=%s", doltDataDir)},
		doltArgs...)
	if devDoltPath != "" {
		args[2] = devDoltPath
		args = args[2:]
	}
	cmd := exec.Command(args[0], args[1:]...)
	fmt.Printf("Running Dolt CMD: %s\n", cmd.String())
	output, err := cmd.CombinedOutput()
	fmt.Printf("Dolt CMD output: %s\n", string(output))
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(output))
	}
	return nil
}

// waitForSqlServerToStart polls the specified database to wait for it to become available, pausing
// between retry attempts, and returning an error if it is not able to verify that the database is
// available.
func waitForSqlServerToStart(database *sqlx.DB) error {
	fmt.Printf("Waiting for server to start...\n")
	for counter := 0; counter < 30; counter++ {
		if database.Ping() == nil {
			return nil
		}
		fmt.Printf("not up yet; waiting...\n")
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}

// printFile opens the specified filepath |path| and outputs the contents of that file to stdout.
func printFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Unable to open file: %s \n", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		s, err := reader.ReadString(byte('\n'))
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		fmt.Print(s)
	}
	fmt.Println()
}

// assertRepoStateFileExists asserts that the repo_state.json file is present for the specified
// database |db|.
func assertRepoStateFileExists(t *testing.T, db string) {
	repoStateFile := filepath.Join(testDir, "dolt", db, ".dolt", "repo_state.json")

	_, err := os.Stat(repoStateFile)
	require.NoError(t, err)
}

// requireReplicaResults runs the specified |query| on the replica database and asserts that the results match
// |expectedResults|. Note that the actual results are converted to string values in almost all cases, due to
// limitations in the SQL library we use to query the replica database, so |expectedResults| should generally
// be expressed in strings.
func requireReplicaResults(t *testing.T, query string, expectedResults [][]any) {
	requireResults(t, replicaDatabase, query, expectedResults)
}

// requireReplicaResults runs the specified |query| on the primary database and asserts that the results match
// |expectedResults|. Note that the actual results are converted to string values in almost all cases, due to
// limitations in the SQL library we use to query the replica database, so |expectedResults| should generally
// be expressed in strings.
func requirePrimaryResults(t *testing.T, query string, expectedResults [][]any) {
	requireResults(t, primaryDatabase, query, expectedResults)
}

func requireResults(t *testing.T, db *sqlx.DB, query string, expectedResults [][]any) {
	rows, err := db.Queryx(query)
	require.NoError(t, err)
	allRows := readAllRowsIntoSlices(t, rows)
	require.Equal(t, len(expectedResults), len(allRows), "Expected %v, got %v", expectedResults, allRows)
	for i := range expectedResults {
		require.Equal(t, expectedResults[i], allRows[i], "Expected %v, got %v", expectedResults[i], allRows[i])
	}
	require.NoError(t, rows.Close())
}

// queryReplicaStatus returns the results of `SHOW REPLICA STATUS` as a map, for the replica
// database. If any errors are encountered, this function will fail the current test.
func queryReplicaStatus(t *testing.T) map[string]any {
	rows, err := replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	status := convertMapScanResultToStrings(readNextRow(t, rows))
	require.NoError(t, rows.Close())
	return status
}

// querySlaveStatus returns the results of `SHOW SLAVE STATUS` as a map, for the replica
// database. If any errors are encountered, this function will fail the current test.
// The queryReplicaStatus() function should generally be favored over this function for
// getting the status of a replica. This function exists only to help test that the
// deprecated 'show slave status' statement works.
func querySlaveStatus(t *testing.T) map[string]any {
	rows, err := replicaDatabase.Queryx("SHOW SLAVE STATUS;")
	require.NoError(t, err)
	status := convertMapScanResultToStrings(readNextRow(t, rows))
	require.NoError(t, rows.Close())
	return status
}

// mustListDatabases returns a string slice of the databases (i.e. schemas) available on the specified |db|. If
// any errors are encountered, this function will fail the current test.
func mustListDatabases(t *testing.T, db *sqlx.DB) []string {
	rows, err := db.Queryx("show databases;")
	require.NoError(t, err)
	allRows := readAllRowsIntoSlices(t, rows)
	dbNames := make([]string, len(allRows))
	for i, row := range allRows {
		dbNames[i] = row[0].(string)
	}
	return dbNames
}
