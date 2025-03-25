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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// doltReplicationPrimarySystemVars holds the system variables that must be set when the Dolt sql-server launches
// in order for it to operate as a binlog primary server. Changes to some of these system variables (i.e. log_bin)
// are not reflected once the SQL engine has been instantiated, so to change them, they must be persisted and the
// SQL server needs to be restarted. Other system variables (i.e. enforce_gtid_consistency, gtid_mode) can be set
// on a running server, but are set here for convenience.
var doltReplicationPrimarySystemVars = map[string]string{
	"log_bin":                  "1",
	"enforce_gtid_consistency": "ON",
	"gtid_mode":                "ON",
	"server_id":                "42",
}

// TestBinlogPrimary_BinlogNotEnabled tests that when binary logging is NOT enabled, primary commands such as
// SHOW BINARY LOGS still work, and that attempts to start replication fail with an error.
func TestBinlogPrimary_BinlogNotEnabled(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(map[string]string{
		"enforce_gtid_consistency": "ON",
		"gtid_mode":                "ON",
	})
	h.setupForDoltToMySqlReplication()

	// When binary logging is NOT enabled, binary log commands such as SHOW MASTER STATUS, SHOW BINARY LOG STATUS,
	// and SHOW BINARY LOGS should not error out.
	h.requirePrimaryResults("SHOW MASTER STATUS", [][]any{})
	h.requirePrimaryResults("SHOW BINARY LOG STATUS", [][]any{})
	h.requirePrimaryResults("SHOW BINARY LOGS", [][]any{})

	h.startReplicationAndCreateTestDb(h.doltPort)
	time.Sleep(1 * time.Second)
	status := h.queryReplicaStatus()
	require.Equal(t, "13120", status["Last_IO_Errno"])
	require.Contains(t, status["Last_IO_Error"],
		"Source command COM_REGISTER_REPLICA failed: unknown error: no binlog currently being recorded")
}

// TestBinlogPrimary_GtidModeNotEnabled asserts that when @@gtid_mode is NOT enabled,
// attempting to start replication will fail with an error visible in the replica's status.
func TestBinlogPrimary_GtidModeNotEnabled(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(map[string]string{"log_bin": "1"})
	h.setupForDoltToMySqlReplication()

	h.requirePrimaryResults("SHOW MASTER STATUS", [][]any{{"binlog-main.000001", "151", "", "", ""}})
	h.requirePrimaryResults("SHOW BINARY LOG STATUS", [][]any{{"binlog-main.000001", "151", "", "", ""}})
	h.requirePrimaryResults("SHOW BINARY LOGS", [][]any{{"binlog-main.000001", "151", "No"}})

	h.startReplication(h.doltPort)
	time.Sleep(1 * time.Second)
	status := h.queryReplicaStatus()
	require.Equal(t, "13117", status["Last_IO_Errno"])
	require.Contains(t, status["Last_IO_Error"],
		"The replication receiver thread cannot start because the source has GTID_MODE = OFF and this server has GTID_MODE = ON")
}

// TestBinlogPrimary_EnforceGtidConsistencyNotEnabled asserts that when @@enforce_gtid_consistency is NOT enabled,
// attempting to start replication will fail with an error visible in the replica's status.
func TestBinlogPrimary_EnforceGtidConsistencyNotEnabled(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(map[string]string{"log_bin": "1", "gtid_mode": "ON"})
	h.setupForDoltToMySqlReplication()

	h.requirePrimaryResults("SHOW MASTER STATUS", [][]any{{"binlog-main.000001", "151", "", "", ""}})
	h.requirePrimaryResults("SHOW BINARY LOG STATUS", [][]any{{"binlog-main.000001", "151", "", "", ""}})
	h.requirePrimaryResults("SHOW BINARY LOGS", [][]any{{"binlog-main.000001", "151", "No"}})

	h.startReplication(h.doltPort)
	time.Sleep(1 * time.Second)
	status := h.queryReplicaStatus()
	require.Equal(t, "13114", status["Last_IO_Errno"])
	require.Contains(t, status["Last_IO_Error"],
		"@@enforce_gtid_consistency must be enabled for binlog replication")
}

// TestBinlogPrimary runs a simple sanity check that a MySQL replica can connect to a Dolt primary and receive
// binlog events from a wide variety of SQL data types.
func TestBinlogPrimary(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (" +
		"pk int primary key, " +
		"c1 varchar(10), c2 int, c3 varchar(100), c4 tinyint, c5 smallint, c6 mediumint, c7 bigint, " +
		"uc1 tinyint unsigned, uc2 smallint unsigned, uc3 mediumint unsigned, uc4 int unsigned, uc5 bigint unsigned, " +
		"f1 float, f2 double, " +
		"t1 year, t2 date, t3 time(6), " +
		"t4 datetime, t5 datetime(1), t6 datetime(2), t7 datetime(3), t8 datetime(4), t9 datetime(5), t10 datetime(6), " +
		"t11 timestamp, t12 timestamp(1), t13 timestamp(2), t14 timestamp(3), t15 timestamp(4), t16 timestamp(5), t17 timestamp(6), " +
		"b1 bit(10), " +
		"e1 enum('blue', 'green', 'red'), s1 set('pants','shirt','tie','belt'), " +
		"ch1 char(10), ch2 char(255)," +
		"d1 decimal(14, 4), d2 decimal(14, 4), d3 decimal(14, 4), d4 decimal(10, 0), " +
		"bl1 blob, " +
		"tx1 text," +
		"bin1 binary(10), vbin1 varbinary(20)," +
		"geo1 geometry, json1 json, json2 json, json3 json, json4 json, json5 json, " +
		"json6 json, json7 json" +
		");")

	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t"}})

	h.primaryDatabase.MustExec("insert into db01.t values (" +
		"1, " +
		"'42', NULL, NULL, 123, 123, 123, 123, 200, 200, 200, 200, 200, " +
		"1.0101, 2.02030405060708, " +
		"1981, '1981-02-16', '-123:45:30.123456', " +
		"'1981-02-16 06:01:02.234567', '1981-02-16 06:01:02.234567', '1981-02-16 06:01:02.234567', '1981-02-16 06:01:02.234567', " +
		"'1981-02-16 06:01:02.234567', '1981-02-16 06:01:02.234567', '1981-02-16 06:01:02.234567', " +
		"'2024-04-08 10:30:42.876543', '2024-04-08 10:30:42.876543', '2024-04-08 10:30:42.876543', '2024-04-08 10:30:42.876543'," +
		"'2024-04-08 10:30:42.876543', '2024-04-08 10:30:42.876543', '2024-04-08 10:30:42.876543'," +
		"b'0100000011', " +
		"'green', 'pants,tie,belt'," +
		"'purple', 'abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz'," +
		"1234567890.1234, -1234567890.1234, 1234567890.0001, 10.0," +
		"0x010203," +
		"'text text text'," +
		"0x0102030405, 0x0102030405060708090a," +
		`POINT(1,1), 'true', '[true, false]', '[true, [true, false]]', '["foo","bar"]', '["baz", 1.0, 2.0, "bash"]', ` +
		`'{"foo":"bar"}', '{"foo": {"baz": "bar"}}'` +
		");")
	h.waitForReplicaToCatchUp()

	// Debugging output – useful to see deeper status from replica when tests are failing
	h.outputReplicaApplierStatus()
	h.outputShowReplicaStatus()

	// Sanity check on SHOW REPLICA STATUS
	rows, err := h.replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)
	allRows := readAllRowsIntoMaps(t, rows)
	require.Equal(t, 1, len(allRows))
	require.NoError(t, rows.Close())
	uuid := h.queryPrimaryServerUuid()
	require.Equal(t, uuid+":1-3", allRows[0]["Executed_Gtid_Set"])
	require.Equal(t, "", allRows[0]["Last_IO_Error"])
	require.Equal(t, "", allRows[0]["Last_SQL_Error"])
	require.Equal(t, "Yes", allRows[0]["Replica_IO_Running"])
	require.Equal(t, "Yes", allRows[0]["Replica_SQL_Running"])

	// Test that the table was created and one row inserted
	h.requireReplicaResults("select * from db01.t;", [][]any{
		{"1", "42", nil, nil, "123", "123", "123", "123", "200", "200", "200", "200", "200",
			float32(1.0101), float64(2.02030405060708),
			"1981", "1981-02-16", "-123:45:30.123456",
			"1981-02-16 06:01:02", "1981-02-16 06:01:02.2", "1981-02-16 06:01:02.23", "1981-02-16 06:01:02.235",
			"1981-02-16 06:01:02.2346", "1981-02-16 06:01:02.23457", "1981-02-16 06:01:02.234567",
			"2024-04-08 10:30:43", "2024-04-08 10:30:42.9", "2024-04-08 10:30:42.88", "2024-04-08 10:30:42.877",
			"2024-04-08 10:30:42.8765", "2024-04-08 10:30:42.87654", "2024-04-08 10:30:42.876543",
			"\x01\x03",
			"green", "pants,tie,belt",
			"purple", "abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz",
			"1234567890.1234", "-1234567890.1234", "1234567890.0001", "10",
			"\x01\x02\x03",
			"text text text",
			"\x01\x02\x03\x04\x05\x00\x00\x00\x00\x00", "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a",
			"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3F\x00\x00\x00\x00\x00\x00\xf0\x3F",
			"true", "[true, false]", "[true, [true, false]]", `["foo", "bar"]`, `["baz", 1.0, 2.0, "bash"]`,
			`{"foo": "bar"}`, `{"foo": {"baz": "bar"}}`,
		},
	})

	h.requirePrimaryResults("SHOW BINARY LOG STATUS", [][]any{
		{"binlog-main.000001", "2377", "", "", uuid + ":1-3"}})
}

// TestBinlogPrimary_Rotation tests how a Dolt primary server handles rotating the binary log file when the
// size threshold is reached.
func TestBinlogPrimary_Rotation(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Change the binlog rotation threshold on the primary to 10KB (instead of the default 1GB)
	h.primaryDatabase.MustExec("SET @@GLOBAL.max_binlog_size = 10240;")

	// Generate enough data to trigger a logfile rotation
	h.primaryDatabase.MustExec("create table t (n int);")
	for i := range 100 {
		h.primaryDatabase.MustExec(fmt.Sprintf("insert into t values (%d);", i))
	}
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("SELECT MAX(n) FROM t;", [][]any{{"99"}})

	// Check the binary log file status and ensure the file has been rotated
	uuid := h.queryPrimaryServerUuid()
	h.requirePrimaryResults("show binary log status;", [][]any{
		{"binlog-main.000003", "1027", "", "", uuid + ":1-102"},
	})

	h.requirePrimaryResults("show binary logs;", [][]any{
		{"binlog-main.000001", "10318", "No"},
		{"binlog-main.000002", "10481", "No"},
		{"binlog-main.000003", "1027", "No"},
	})
}

// TestBinlogPrimary_AutoPurging tests that the primary server correctly purges binary log files older than
// @@binlog_expire_logs_seconds on restart.
func TestBinlogPrimary_AutoPurging(t *testing.T) {
	h := newHarness(t)
	mapCopy := copyMap(doltReplicationPrimarySystemVars)
	mapCopy["binlog_expire_logs_seconds"] = "1"
	h.startSqlServersWithDoltSystemVars(mapCopy)
	h.setupForDoltToMySqlReplication()

	// Generate binary log content
	h.primaryDatabase.MustExec("create database db01;")
	h.primaryDatabase.MustExec("create table db01.t (n int);")
	for i := range 100 {
		h.primaryDatabase.MustExec(fmt.Sprintf("insert into db01.t values (%d);", i))
	}
	h.requirePrimaryResults("SHOW BINARY LOGS;", [][]any{
		{"binlog-main.000001", "21346", "No"},
	})

	// Restart and confirm the binary log has been purged
	h.stopDoltSqlServer()
	time.Sleep(1 * time.Second)
	h.mustRestartDoltPrimaryServer()
	h.requirePrimaryResults("SHOW BINARY LOGS;", [][]any{
		{"binlog-main.000002", "191", "No"},
	})

	// Check the value of @@gtid_purged
	h.requirePrimaryResults("SELECT @@gtid_purged;", [][]any{
		{fmt.Sprintf("%s:1-102", h.queryPrimaryServerUuid())},
	})

	// Verify the replica reports an error about the GTIDs not being available
	h.startReplicationAndCreateTestDb(h.doltPort)
	time.Sleep(1 * time.Second)
	status := h.queryReplicaStatus()
	require.Equal(t, "13114", status["Last_IO_Errno"])
	require.Contains(t, status["Last_IO_Error"],
		"Got fatal error 1236 from source when reading data from binary log: "+
			"'Cannot replicate because the source purged required binary logs.")
}

// TestBinlogPrimary_InitializeGTIDPurged asserts that @@gtid_purged is set correctly in a variety of
// scenarios, such as when a fresh server starts up, or when a server is restarted multiple times.
func TestBinlogPrimary_InitializeGTIDPurged(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()

	// On a fresh server, @@gtid_purged and @@gtid_executed should be empty
	h.requirePrimaryResults("SELECT @@gtid_executed;", [][]any{{""}})
	h.requirePrimaryResults("SELECT @@gtid_purged;", [][]any{{""}})

	// Create a GTID in the first binary log file, and restart the server to rotate to a new binary log file
	// After the first restart, @@gtid_purged should be empty and @@gtid_executed should be the first GTID
	h.primaryDatabase.MustExec("CREATE DATABASE db01;")
	h.stopDoltSqlServer()
	h.mustRestartDoltPrimaryServer()
	h.requirePrimaryResults("SELECT @@gtid_executed;", [][]any{
		{fmt.Sprintf("%s:1", h.queryPrimaryServerUuid())},
	})
	h.requirePrimaryResults("SELECT @@gtid_purged;", [][]any{{""}})

	// Manually remove the first binary log file, containing GTID 1 and restart the server
	// When no GTID is found in any available logs, @@gtid_purged should be set to @@gtid_executed
	require.NoError(t, os.Remove(filepath.Join(h.testDir, "dolt", ".dolt", "binlog", "binlog-main.000001")))
	h.stopDoltSqlServer()
	h.mustRestartDoltPrimaryServer()
	h.requirePrimaryResults("SELECT @@gtid_executed;", [][]any{
		{fmt.Sprintf("%s:1", h.queryPrimaryServerUuid())},
	})
	h.requirePrimaryResults("SELECT @@gtid_purged;", [][]any{
		{fmt.Sprintf("%s:1", h.queryPrimaryServerUuid())},
	})

	// Create a new GTID in the current binary log file, restart, and test @@gtid_executed and @@gtid_purged
	h.primaryDatabase.MustExec("CREATE DATABASE db02;")
	h.stopDoltSqlServer()
	h.mustRestartDoltPrimaryServer()
	h.requirePrimaryResults("SELECT @@gtid_executed;", [][]any{
		{fmt.Sprintf("%s:1-2", h.queryPrimaryServerUuid())},
	})
	h.requirePrimaryResults("SELECT @@gtid_purged;", [][]any{
		{fmt.Sprintf("%s:1", h.queryPrimaryServerUuid())},
	})
}

// TestBinlogPrimary_ReplicaAndPrimaryRestart tests that a replica can disconnect and reconnect to the primary to
// restart the replication stream, even when the primary has been restarted and log files have rotated.
func TestBinlogPrimary_ReplicaAndPrimaryRestart(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Change the binlog rotation threshold on the primary to 10KB (instead of the default 1GB) so
	// that log files will rotate more often
	h.primaryDatabase.MustExec("SET @@GLOBAL.max_binlog_size = 10240;")

	// Create a table on the primary and assert that it gets replicated
	h.primaryDatabase.MustExec("create table db01.t1 (pk int primary key, c1 varchar(255));")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}})

	// Assert that the executed GTID position on the replica contains GTIDs 1 and 2
	serverUuid := h.queryPrimaryServerUuid()
	status := h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-2", status["Executed_Gtid_Set"])

	// Stop the MySQL replica server and wait for a few seconds
	h.stopMySqlServer()
	time.Sleep(2_000 * time.Millisecond)

	// Generate enough data to trigger a logfile rotation
	h.primaryDatabase.MustExec("create table t (n int);")
	for i := range 100 {
		h.primaryDatabase.MustExec(fmt.Sprintf("insert into t values (%d);", i))
	}

	// Stop the primary and restart it to test that it creates a new log file and
	// applies a rotate event to the last log file
	h.stopDoltSqlServer()

	// Restart the Dolt primary server
	h.mustRestartDoltPrimaryServer()

	// Generate more data on the primary after restarting
	h.primaryDatabase.MustExec("use db01;")
	for i := range 100 {
		h.primaryDatabase.MustExec(fmt.Sprintf("insert into t values (%d);", i+100))
	}

	// Restart the MySQL replica and reconnect to the Dolt primary
	h.mustRestartMySqlReplicaServer()
	h.startReplicationAndCreateTestDb(h.doltPort)
	h.waitForReplicaToCatchUp()

	// Assert the executed GTID position now contains all GTIDs
	status = h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-203", status["Executed_Gtid_Set"])
	h.requireReplicaResults("SELECT MAX(n) FROM t;", [][]any{{"199"}})
}

// TestBinlogPrimary_Heartbeats tests that heartbeats sent from the primary to the replica are well-formed and
// don't cause the replica to close the stream. For example, if the nextLogPosition field in the heartbeat event
// doesn't match up with the nextLogPosition from the previous event, then the replica will quit the connection.
func TestBinlogPrimary_Heartbeats(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()

	// Start replication, with a 45s delay before any commands are sent to the primary.
	// This gives enough time for the first heartbeat event to be sent, before any user
	// initiated binlog events, so we can test that scenario.
	h.startReplicationAndCreateTestDbWithDelay(h.doltPort, 45*time.Second)

	// Insert a row every second, for 70s, which gives the server a chance to send two heartbeats
	h.primaryDatabase.MustExec("create table db01.heartbeatTest(pk int);")
	endTime := time.Now().Add(70 * time.Second)
	maxInsertValue := 0
	for time.Now().Before(endTime) {
		maxInsertValue += 1
		h.primaryDatabase.MustExec(fmt.Sprintf("insert into db01.heartbeatTest values (%d);", maxInsertValue))
		time.Sleep(1 * time.Second)
	}

	// Ensure the replica is still in sync
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select count(*) from db01.heartbeatTest;",
		[][]any{{fmt.Sprintf("%d", maxInsertValue)}})

	// Make sure no errors have occurred
	time.Sleep(1 * time.Second)
	status := h.queryReplicaStatus()
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "0", status["Last_IO_Errno"])
}

// TestBinlogPrimary_ReplicaRestart tests that the Dolt primary server behaves correctly when the
// replica server is stopped, and then later reconnects.
func TestBinlogPrimary_ReplicaRestart(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Create a table on the primary and assert that it gets replicated
	h.primaryDatabase.MustExec("create table db01.t1 (pk int primary key, c1 varchar(255));")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}})

	// Assert that the executed GTID position on the replica contains GTIDs 1 and 2
	serverUuid := h.queryPrimaryServerUuid()
	status := h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-2", status["Executed_Gtid_Set"])

	// Stop the MySQL replica server and wait for a few seconds
	h.stopMySqlServer()
	time.Sleep(4_000 * time.Millisecond)

	// Make a change while the replica is stopped to test that the server
	// doesn't error out when a registered replica is not available.
	h.primaryDatabase.MustExec("insert into db01.t1 values (1, 'one');")

	// Restart the MySQL replica and reconnect to the Dolt primary
	prevPrimaryDatabase := h.primaryDatabase
	var err error
	h.mySqlPort, h.mySqlProcess, err = h.startMySqlServer()
	require.NoError(t, err)
	h.replicaDatabase = h.primaryDatabase
	h.primaryDatabase = prevPrimaryDatabase
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Create another table and assert that it gets replicated
	h.primaryDatabase.MustExec("create table db01.t2 (pk int primary key, c1 varchar(255));")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}, {"t2"}})

	// Assert the executed GTID position now contains all GTIDs
	status = h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-4", status["Executed_Gtid_Set"])
}

// TestBinlogPrimary_PrimaryRestart tests that a Dolt primary server can be restarted and that a replica
// will successfully reconnect and continue replicating binlog events.
func TestBinlogPrimary_PrimaryRestart(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Only one binary log file should be present on a fresh server
	h.requirePrimaryResults("show binary logs;", [][]any{
		{"binlog-main.000001", "263", "No"},
	})

	// Create a table on the primary and assert that it gets replicated
	h.primaryDatabase.MustExec("create table db01.t1 (pk int primary key, c1 varchar(255));")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}})

	// Assert that the executed GTID position on the replica contains GTIDs 1 and 2
	serverUuid := h.queryPrimaryServerUuid()
	status := h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-2", status["Executed_Gtid_Set"])

	// Stop the Dolt primary server
	h.stopDoltSqlServer()
	time.Sleep(2_000 * time.Millisecond)

	// Restart the Dolt primary server
	h.mustRestartDoltPrimaryServer()
	h.waitForReplicaToReconnect()

	// A new binary log file is created on each server restart
	h.requirePrimaryResults("show binary logs;", [][]any{
		{"binlog-main.000001", "549", "No"},
		{"binlog-main.000002", "191", "No"},
	})

	// Create another table and assert that it gets replicated
	h.primaryDatabase.MustExec("create table db01.t2 (pk int primary key, c1 varchar(255));")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}, {"t2"}})

	// Assert the executed GTID position now contains GTID #2 and GTID #3
	status = h.queryReplicaStatus()
	require.Equal(t, serverUuid+":1-3", status["Executed_Gtid_Set"])
}

// TestBinlogPrimary_PrimaryRestartBeforeReplicaConnects tests that a MySQL replica can connect to a Dolt primary
// when the Dolt primary has multiple binlog files and the replica needs events from a non-current binlog file.
func TestBinlogPrimary_PrimaryRestartBeforeReplicaConnects(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()

	// Create a test database to trigger the first GTID binlog event
	h.primaryDatabase.MustExec("CREATE DATABASE db02;")

	// Restart the Dolt primary server to trigger a binlog file rotation
	h.stopDoltSqlServer()
	h.mustRestartDoltPrimaryServer()

	// Start replication and verify the replica receives the CREATE DATABASE event from the first binlog file
	h.startReplication(h.doltPort)
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("SHOW DATABASES;", [][]any{
		{"db02"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"},
	})

	// Verify that the Dolt primary server has two binary log files
	h.requirePrimaryResults("SHOW BINARY LOGS;", [][]any{
		{"binlog-main.000001", "312", "No"},
		{"binlog-main.000002", "191", "No"},
	})
}

// TestBinlogPrimary_DisallowBranchesWithSlashes asserts that trying to set @@log_bin_branch to
// a branch name containing a slash results in an error.
func TestBinlogPrimary_DisallowBranchesWithSlashes(t *testing.T) {
	h := newHarness(t)
	mapCopy := copyMap(doltReplicationPrimarySystemVars)
	mapCopy["log_bin_branch"] = "'branch/withslash'"
	h.startSqlServersWithDoltSystemVars(mapCopy)
	h.setupForDoltToMySqlReplication()

	// Because the replication branch was invalid, the binary log status should be
	// empty, indicating that no binary logs are being recorded.
	h.requirePrimaryResults("SHOW BINARY LOG STATUS;", [][]any{})
}

// TestBinlogPrimary_ChangeReplicationBranch asserts that the log_bin_branch system variable can
// be used to control what branch is replicated.
func TestBinlogPrimary_ChangeReplicationBranch(t *testing.T) {
	h := newHarness(t)
	mapCopy := copyMap(doltReplicationPrimarySystemVars)
	mapCopy["log_bin_branch"] = "branch1"
	h.startSqlServersWithDoltSystemVars(mapCopy)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// No events should be generated when we're not updating the configured replication branch
	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{})

	// Create the branch1 branch and make sure it gets replicated
	h.primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	h.primaryDatabase.MustExec("insert into db01.t values('hundred', 100, 2000);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t"}})
	h.requireReplicaResults("select * from db01.t;", [][]any{{"hundred", "100", "2000"}})

	// Insert another row on main and make sure it doesn't get replicated
	h.primaryDatabase.MustExec("call dolt_checkout('main');")
	h.primaryDatabase.MustExec("insert into db01.t values('two hundred', 200, 2000);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"hundred", "100", "2000"}})

	// Assert that changing log_bin_branch while the server is running has no effect
	h.primaryDatabase.MustExec("SET @@GLOBAL.log_bin_branch='main';")
	h.primaryDatabase.MustExec("call dolt_checkout('main');")
	h.primaryDatabase.MustExec("insert into db01.t values('three hundred', 300, 2023);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"hundred", "100", "2000"}})
}

// TestBinlogPrimary_SimpleSchemaChangesWithAutocommit tests that we can make simple schema changes (e.g. create table,
// alter table, drop table) and replicate the DDL statements correctly.
func TestBinlogPrimary_SimpleSchemaChangesWithAutocommit(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Create a table
	h.primaryDatabase.MustExec("create table db01.t1 (pk int primary key, c1 varchar(255) NOT NULL comment 'foo bar baz');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}})
	h.requireReplicaResults("show create table db01.t1;", [][]any{{"t1",
		"CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `c1` varchar(255) COLLATE utf8mb4_0900_bin NOT NULL COMMENT 'foo bar baz',\n" +
			"  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})

	// Insert some data
	h.primaryDatabase.MustExec("insert into db01.t1 (pk, c1) values (1, 'foo');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t1;", [][]any{{"1", "foo"}})

	// Modify the table
	h.primaryDatabase.MustExec("alter table db01.t1 rename column c1 to z1;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show create table db01.t1;", [][]any{{"t1",
		"CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `z1` varchar(255) COLLATE utf8mb4_0900_bin NOT NULL COMMENT 'foo bar baz',\n" +
			"  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})

	// Insert some data
	h.primaryDatabase.MustExec("insert into db01.t1 (pk, z1) values (2, 'bar');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t1;", [][]any{{"1", "foo"}, {"2", "bar"}})

	// Drop the table
	h.primaryDatabase.MustExec("drop table db01.t1;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{})

	// Rename a table
	h.primaryDatabase.MustExec("create table originalName(pk1 int, pk2 int, c1 varchar(200), c2 varchar(200), primary key (pk1, pk2));")
	h.primaryDatabase.MustExec("insert into originalName values (1, 2, 'one', 'two');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"originalName"}})
	h.requireReplicaResults("select * from originalName;", [][]any{{"1", "2", "one", "two"}})
	h.primaryDatabase.MustExec("rename table originalName to newName;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"newName"}})
	h.requireReplicaResults("select * from newName;", [][]any{{"1", "2", "one", "two"}})
}

// TestBinlogPrimary_SchemaChangesWithManualCommit tests that manually managed transactions, which
// contain a mix of schema and data changes, can be correctly replicated.
func TestBinlogPrimary_SchemaChangesWithManualCommit(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Create table
	h.primaryDatabase.MustExec("set @@autocommit=0;")
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("create table t (pk int primary key, c1 varchar(100), c2 int);")
	h.primaryDatabase.MustExec("insert into t values (1, 'one', 1);")
	h.primaryDatabase.MustExec("commit;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show create table t;", [][]any{{"t", "CREATE TABLE `t` (\n  " +
		"`pk` int NOT NULL,\n  `c1` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  " +
		"`c2` int DEFAULT NULL,\n  PRIMARY KEY (`pk`)\n) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})
	h.requireReplicaResults("select * from t;", [][]any{{"1", "one", "1"}})

	// Alter column and update
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("alter table t modify column c2 varchar(100);")
	h.primaryDatabase.MustExec("update t set c2='foo';")
	h.primaryDatabase.MustExec("commit;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show create table t;", [][]any{{"t", "CREATE TABLE `t` (\n  " +
		"`pk` int NOT NULL,\n  `c1` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  " +
		"`c2` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  PRIMARY KEY (`pk`)\n) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})
	h.requireReplicaResults("select * from t;", [][]any{{"1", "one", "foo"}})
}

// TestBinlogPrimary_Rollback asserts that rolled back transactions are not sent to replicas.
func TestBinlogPrimary_Rollback(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Create a test table
	h.primaryDatabase.MustExec("set @@autocommit=0;")
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("create table t1 (pk int primary key, c1 varchar(100), c2 int);")
	h.primaryDatabase.MustExec("commit;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}})
	h.requireReplicaResults("select * from t1;", [][]any{})

	// Insert data, but roll back the transaction
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("insert into t1 values (1, 'two', 3);")
	h.primaryDatabase.MustExec("rollback;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from t1;", [][]any{})
}

// TestBinlogPrimary_MultipleTablesManualCommit tests that binlog events are generated correctly
// when multiple tables are changed in a single SQL commit.
func TestBinlogPrimary_MultipleTablesManualCommit(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Insert to multiple tables in a single SQL transaction
	h.primaryDatabase.MustExec("set @@autocommit=0;")
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("create table t1 (pk int primary key, c1 varchar(100), c2 int);")
	h.primaryDatabase.MustExec("insert into t1 values (1, 'one', 1);")
	h.primaryDatabase.MustExec("create table t2 (pk int primary key, c1 varchar(100), c2 int);")
	h.primaryDatabase.MustExec("insert into t2 values (1, 'eins', 1);")
	h.primaryDatabase.MustExec("commit;")

	// Verify the results on the replica
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show tables;", [][]any{{"t1"}, {"t2"}})
	h.requireReplicaResults("select * from t1;", [][]any{{"1", "one", "1"}})
	h.requireReplicaResults("select * from t2;", [][]any{{"1", "eins", "1"}})

	// Update to multiple tables in a single SQL transaction
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("update t1 set c2=1111;")
	h.primaryDatabase.MustExec("update t2 set c2=2222;")
	h.primaryDatabase.MustExec("commit;")

	// Verify the results on the replica
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from t1;", [][]any{{"1", "one", "1111"}})
	h.requireReplicaResults("select * from t2;", [][]any{{"1", "eins", "2222"}})

	// Delete from multiple tables in a single SQL transaction
	h.primaryDatabase.MustExec("start transaction;")
	h.primaryDatabase.MustExec("delete from t1 where c2=1111;")
	h.primaryDatabase.MustExec("delete from t2 where c2=2222;")
	h.primaryDatabase.MustExec("commit;")

	// Verify the results on the replica
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from t1;", [][]any{})
	h.requireReplicaResults("select * from t2;", [][]any{})
}

// TestBinlogPrimary_ReplicateCreateDropDatabase tests that Dolt can correctly replicate statements to create,
// drop, and undrop databases.
func TestBinlogPrimary_ReplicateCreateDropDatabase(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	// Test CREATE DATABASE
	h.primaryDatabase.MustExec("create database foobar1;")
	h.primaryDatabase.MustExec("create table foobar1.table1 (c1 enum('red', 'green', 'blue'));")
	h.primaryDatabase.MustExec("insert into foobar1.table1 values ('blue');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show databases;", [][]any{
		{"db01"}, {"foobar1"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})
	h.requireReplicaResults("select * from foobar1.table1;", [][]any{{"blue"}})

	// Test DROP DATABASE
	h.primaryDatabase.MustExec("drop database foobar1;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show databases;", [][]any{
		{"db01"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})

	// Test DOLT_UNDROP()
	h.primaryDatabase.MustExec("call dolt_undrop('foobar1');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("show databases;", [][]any{
		{"db01"}, {"foobar1"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})
	h.requireReplicaResults("select * from foobar1.table1;", [][]any{{"blue"}})
}

// TestBinlogPrimary_InsertUpdateDelete tests that insert, update, and delete statements can be executed correctly
// in autocommit transactions, and also when they mixed together in the same explicit SQL transaction.
func TestBinlogPrimary_InsertUpdateDelete(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")

	// Insert multiple rows
	h.primaryDatabase.MustExec("insert into db01.t values ('1', 1, 1981), ('2', 2, 1982), ('3', 3, 1983), ('4', 4, 1984);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by pk;", [][]any{
		{"1", "1", "1981"}, {"2", "2", "1982"}, {"3", "3", "1983"}, {"4", "4", "1984"}})

	// Delete multiple rows
	h.primaryDatabase.MustExec("delete from db01.t where pk in ('1', '3');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by pk;", [][]any{
		{"2", "2", "1982"}, {"4", "4", "1984"}})

	// Update multiple rows
	h.primaryDatabase.MustExec("update db01.t set c2 = 1942;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by pk;", [][]any{
		{"2", "2", "1942"}, {"4", "4", "1942"}})

	// Turn off @@autocommit and mix inserts/updates/deletes in the same transaction
	h.primaryDatabase.MustExec("SET @@autocommit=0;")
	h.primaryDatabase.MustExec("insert into db01.t values ('10', 10, 2020), ('11', 11, 2021), ('12', 12, 2022), ('13', 13, 2023);")
	h.primaryDatabase.MustExec("delete from db01.t where pk in ('11', '13');")
	h.primaryDatabase.MustExec("update db01.t set c2 = 2042 where c2 > 2000;")
	h.primaryDatabase.MustExec("COMMIT;")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by pk;", [][]any{
		{"10", "10", "2042"}, {"12", "12", "2042"},
		{"2", "2", "1942"}, {"4", "4", "1942"},
	})
}

// TestBinlogPrimary_OnlyReplicateMainBranch tests that binlog events are only generated for the main branch of a Dolt repository.
func TestBinlogPrimary_OnlyReplicateMainBranch(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// No events should be generated when we're not updating the main branch
	h.primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	h.primaryDatabase.MustExec("insert into db01.t values('hundred', 100, 2000);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Insert another row on branch1 and make sure it doesn't get replicated
	h.primaryDatabase.MustExec("insert into db01.t values('two hundred', 200, 2000);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Events should be generated from the main branch
	h.primaryDatabase.MustExec("call dolt_checkout('main');")
	h.primaryDatabase.MustExec("insert into db01.t values('42', 42, 2042);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"42", "42", "2042"}})
}

// TestBinlogPrimary_KeylessTables tests that Dolt can replicate changes to keyless tables.
func TestBinlogPrimary_KeylessTables(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (c1 varchar(100), c2 int, c3 int unsigned);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Test inserts
	h.primaryDatabase.MustExec("insert into db01.t values('one', 1, 11), ('two', 2, 22);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by c2;", [][]any{{"one", "1", "11"}, {"two", "2", "22"}})

	// Test inserting duplicate rows
	h.primaryDatabase.MustExec("insert into db01.t values('one', 1, 11), ('one', 1, 11);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by c2;", [][]any{
		{"one", "1", "11"}, {"one", "1", "11"}, {"one", "1", "11"}, {"two", "2", "22"}})

	// Test updating multiple rows
	h.primaryDatabase.MustExec("update db01.t set c1='uno' where c1='one';")
	h.primaryDatabase.MustExec("update db01.t set c1='zwei' where c1='two';")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by c2;", [][]any{
		{"uno", "1", "11"}, {"uno", "1", "11"}, {"uno", "1", "11"}, {"zwei", "2", "22"}})

	// Test deleting multiple rows
	h.primaryDatabase.MustExec("delete from db01.t where c1='uno';")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t order by c2;", [][]any{{"zwei", "2", "22"}})
}

// TestBinlogPrimary_Merge tests that the binlog is updated when data is merged in from another branch.
func TestBinlogPrimary_Merge(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// No events should be generated when we're not updating the main branch
	h.primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	h.primaryDatabase.MustExec("insert into db01.t values('hundred', 100, 2000), ('two-hundred', 200, 2001);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'inserting rows 100 and 200 on branch1');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Make a commit on main, so that we don't get a fast-forward merge later
	h.primaryDatabase.MustExec("call dolt_checkout('main');")
	h.primaryDatabase.MustExec("insert into db01.t values('42', 42, 2042);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'inserting row 42 on main');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"42", "42", "2042"}})

	// Merge branch1 into main
	h.primaryDatabase.MustExec("call dolt_merge('branch1');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{
		{"42", "42", "2042"}, {"hundred", "100", "2000"}, {"two-hundred", "200", "2001"}})
}

// TestBinlogPrimary_Cherrypick tests binlog replication when dolt_cherry_pick() is used to cherry-pick commits.
func TestBinlogPrimary_Cherrypick(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Make a couple of commits on branch1 so that we can cherry-pick them
	h.primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	h.primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	h.primaryDatabase.MustExec("SET @RowOneCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	h.primaryDatabase.MustExec("SET @RowTwoCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	h.primaryDatabase.MustExec("SET @RowThreeCommit=dolt_hashof('HEAD');")

	// Cherry-pick a commit from branch1 onto main
	h.primaryDatabase.MustExec("call dolt_checkout('main');")
	h.primaryDatabase.MustExec("call dolt_cherry_pick(@RowTwoCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"02", "2"}})

	// Cherry-pick another commit from branch1 onto main
	h.primaryDatabase.MustExec("call dolt_cherry_pick(@RowThreeCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"02", "2"}, {"03", "3"}})
}

// TestBinlogPrimary_Revert tests binlog replication when dolt_revert() is used to revert commits.
func TestBinlogPrimary_Revert(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Make a couple of commits on main so that we can revert one
	h.primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	h.primaryDatabase.MustExec("SET @RowOneCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	h.primaryDatabase.MustExec("SET @RowTwoCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	h.primaryDatabase.MustExec("SET @RowThreeCommit=dolt_hashof('HEAD');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})

	// Revert a commit
	h.primaryDatabase.MustExec("call dolt_revert(@RowTwoCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}, {"03", "3"}})

	// Revert another commit
	h.primaryDatabase.MustExec("call dolt_revert(@RowOneCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"03", "3"}})
}

// TestBinlogPrimary_Reset tests that the binlog is updated when a branch head is reset to a different commit.
func TestBinlogPrimary_Reset(t *testing.T) {
	h := newHarness(t)
	h.startSqlServersWithDoltSystemVars(doltReplicationPrimarySystemVars)
	h.setupForDoltToMySqlReplication()
	h.startReplicationAndCreateTestDb(h.doltPort)

	h.primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	h.primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	h.primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Make a couple of commits on main so that we can test resetting to them
	h.primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	h.primaryDatabase.MustExec("SET @OneRowCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	h.primaryDatabase.MustExec("SET @TwoRowsCommit=dolt_hashof('HEAD');")
	h.primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	h.primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	h.primaryDatabase.MustExec("SET @ThreeRowsCommit=dolt_hashof('HEAD');")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})

	// Reset back to the first commit when no rows are present
	h.primaryDatabase.MustExec("call dolt_reset('--hard', @EmptyTableCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{})

	// Reset back to the second commit when only one row is present
	h.primaryDatabase.MustExec("call dolt_reset('--hard', @OneRowCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}})

	// Reset back to the second commit when only one row is present
	h.primaryDatabase.MustExec("call dolt_reset('--hard', @TwoRowsCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}})

	// Reset back to the second commit when only one row is present
	h.primaryDatabase.MustExec("call dolt_reset('--hard', @ThreeRowsCommit);")
	h.waitForReplicaToCatchUp()
	h.requireReplicaResults("select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})
}

func (h *harness) setupForDoltToMySqlReplication() {
	// Swap the replica and primary databases, since we're
	// replicating in the other direction in this test.
	var tempDatabase = h.primaryDatabase
	h.primaryDatabase = h.replicaDatabase
	h.replicaDatabase = tempDatabase

	// Set the session's timezone to UTC, to avoid TIMESTAMP test values changing
	// when they are converted to UTC for storage.
	h.replicaDatabase.MustExec("SET @@time_zone = '+0:00';")

	// Reset binary logs and gtids on the replica, so that @@gtid_executed doesn't contain any
	// executed GTIDs from the replica server.
	h.replicaDatabase.MustExec("reset master;")
}

// outputReplicaApplierStatus prints out the replica applier status information from the
// performance_schema replication_applier_status_by_worker table. This is useful for debugging
// replication from a Dolt primary to a MySQL replica, since this often contains more detailed
// information about why MySQL failed to apply a binlog event.
func (h *harness) outputReplicaApplierStatus() {
	newRows, err := h.replicaDatabase.Queryx("select * from performance_schema.replication_applier_status_by_worker;")
	require.NoError(h.t, err)
	allNewRows := readAllRowsIntoMaps(h.t, newRows)
	h.t.Logf("\n\nreplication_applier_status_by_worker: %v\n", allNewRows)
}

// outputShowReplicaStatus prints out replica status information. This is useful for debugging
// replication failures in tests since status will show whether the replica is successfully connected,
// any recent errors, and what GTIDs have been executed.
func (h *harness) outputShowReplicaStatus() {
	newRows, err := h.replicaDatabase.Queryx("show replica status;")
	require.NoError(h.t, err)
	allNewRows := readAllRowsIntoMaps(h.t, newRows)
	h.t.Logf("\n\nSHOW REPLICA STATUS: %v\n", allNewRows)
}

// copyMap returns a copy of the specified map |m|.
func copyMap(m map[string]string) map[string]string {
	mapCopy := make(map[string]string)
	for key, value := range m {
		mapCopy[key] = value
	}
	return mapCopy
}

// queryPrimaryServerUuid queries the primary server for its server UUID. If any errors are encountered,
// this function will fail the current test.
func (h *harness) queryPrimaryServerUuid() string {
	rows, err := h.primaryDatabase.Queryx("SELECT @@server_uuid;")
	require.NoError(h.t, err)
	serverUuid := convertMapScanResultToStrings(readNextRow(h.t, rows))
	require.NoError(h.t, rows.Close())
	return serverUuid["@@server_uuid"].(string)
}

// waitForReplicaToReconnect will poll the status of the replica and return when the status indicates
// the replica has reconnected to the primary. If after 60s the replica hasn't reconnected, this
// function will fail the current test.
func (h *harness) waitForReplicaToReconnect() {
	startTime := time.Now()
	for {
		time.Sleep(500 * time.Millisecond)

		status := h.queryReplicaStatus()
		if status["Replica_IO_Running"] == "Yes" {
			break
		}

		if startTime.Add(time.Second * 60).Before(time.Now()) {
			h.t.Fatalf("Unable to detect replica reconnect after 60s")
		}
	}
}

// mustRestartDoltPrimaryServer starts up the Dolt sql-server, after it has already been stopped before this function
// is called, and configures it as the primary database.
func (h *harness) mustRestartDoltPrimaryServer() {
	var err error
	prevReplicaDatabase := h.replicaDatabase
	h.doltPort, h.doltProcess, err = h.startDoltSqlServer(nil)
	require.NoError(h.t, err)
	h.primaryDatabase = h.replicaDatabase
	h.replicaDatabase = prevReplicaDatabase
}

// mustRestartMySqlReplicaServer starts up the MySQL server, after it has already been stopped before this function
// is called, and configures it as the replica database.
func (h *harness) mustRestartMySqlReplicaServer() {
	var err error
	prevPrimaryDatabase := h.primaryDatabase
	h.mySqlPort, h.mySqlProcess, err = h.startMySqlServer()
	require.NoError(h.t, err)
	h.replicaDatabase = h.primaryDatabase
	h.primaryDatabase = prevPrimaryDatabase
}
