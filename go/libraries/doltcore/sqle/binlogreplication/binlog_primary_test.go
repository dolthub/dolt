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

func setupForDoltToMySqlReplication() {
	// Swap the replica and primary databases, since we're
	// replicating in the other direction in this test.
	var tempDatabase = primaryDatabase
	primaryDatabase = replicaDatabase
	replicaDatabase = tempDatabase

	// Clear out any existing GTID record on the replica
	replicaDatabase.MustExec("reset binary logs and gtids;")

	// On the Primary, turn on GTID mode
	// NOTE: Dolt doesn't currently require moving through the GTID_MODE states like this, but
	//       MySQL does, so we do it here anyway.
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='OFF_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL ENFORCE_GTID_CONSISTENCY='ON';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON';")

	// Create the replication user on the Dolt primary server
	// TODO: this should probably be done on both primary and replica as part of the shared setup code
	primaryDatabase.MustExec("CREATE USER 'replicator'@'%' IDENTIFIED BY 'Zqr8_blrGm1!';")
	primaryDatabase.MustExec("GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';")

	// On the Primary, make sure we have a non-zero SERVER_ID set
	// TODO: Technically, we should be setting this persistently and we should restart the sql-server
	// TODO: Do we still need to do this? it would default to 1 and MySQL would fail to replica, right?
	primaryDatabase.MustExec("set GLOBAL SERVER_ID=42;")

	// Set the session's timezone to UTC, to avoid TIMESTAMP test values changing
	// when they are converted to UTC for storage.
	replicaDatabase.MustExec("SET @@time_zone = '+0:00';")
}

// TestBinlogPrimary runs a simple sanity check that a MySQL replica can connect to a Dolt primary and receive
// binlog events.
func TestBinlogPrimary(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()

	// TODO: We don't support replicating DDL statements yet, so for now, set up the DDL before
	//       starting up replication.
	primaryDatabase.MustExec("create database db01;")
	testTableCreateStatement := "create table db01.t (pk int primary key, c1 varchar(10), c2 int, c3 varchar(100), " +
		"c4 tinyint, c5 smallint, c6 mediumint, c7 bigint, uc1 tinyint unsigned, uc2 smallint unsigned, uc3 mediumint unsigned, uc4 int unsigned, uc5 bigint unsigned, t1 year, t2 datetime, t3 timestamp, t4 date, t5 time);"
	primaryDatabase.MustExec(testTableCreateStatement)
	replicaDatabase.MustExec(testTableCreateStatement)

	// Because we have executed other statements, we need to reset GTIDs on the replica
	replicaDatabase.MustExec("reset binary logs and gtids;")

	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(250 * time.Millisecond)

	primaryDatabase.MustExec("insert into db01.t values (1, '42', NULL, NULL, 123, 123, 123, 123, 200, 200, 200, 200, 200, " +
		"1981, '1981-02-16 06:01:02', '2024-04-08 10:30:42', '1981-02-16', '-123:45:30');")
	time.Sleep(250 * time.Millisecond)

	// Debugging output
	outputReplicaApplierStatus(t)
	outputShowReplicaStatus(t)

	// Sanity check on SHOW REPLICA STATUS
	rows, err := replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)
	allRows := readAllRowsIntoMaps(t, rows)
	require.Equal(t, 1, len(allRows))
	require.NoError(t, rows.Close())
	//require.Equal(t, "3ab04dd4-8c9e-471e-a223-9712a3b7c37e:1-2", allRows[0]["Executed_Gtid_Set"])
	require.Equal(t, "", allRows[0]["Last_IO_Error"])
	require.Equal(t, "", allRows[0]["Last_SQL_Error"])
	require.Equal(t, "Yes", allRows[0]["Replica_IO_Running"])
	require.Equal(t, "Yes", allRows[0]["Replica_SQL_Running"])

	// Test that the table was created and one row inserted
	requireReplicaResults(t, "select * from db01.t;", [][]any{
		{"1", "42", nil, nil,
			"123", "123", "123", "123", "200", "200", "200", "200", "200",
			"1981", "1981-02-16 06:01:02", "2024-04-08 10:30:42", "1981-02-16", "-123:45:30"},
	})
}

func TestBinlogPrimary_InsertUpdateDelete(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()

	// TODO: We don't support replicating DDL statements yet, so for now, set up the DDL before
	//       starting up replication.
	primaryDatabase.MustExec("create database db01;")
	testTableCreateStatement := "create table db01.t (pk varchar(100) primary key, c1 int, c2 year);"
	primaryDatabase.MustExec(testTableCreateStatement)
	replicaDatabase.MustExec(testTableCreateStatement)

	// Because we have executed other statements, we need to reset GTIDs on the replica
	replicaDatabase.MustExec("reset binary logs and gtids;")

	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(450 * time.Millisecond)

	// Insert multiple rows
	primaryDatabase.MustExec("insert into db01.t values ('1', 1, 1981), ('2', 2, 1982), ('3', 3, 1983), ('4', 4, 1984);")
	time.Sleep(450 * time.Millisecond)
	outputReplicaApplierStatus(t)
	requireReplicaResults(t, "select * from db01.t order by pk;", [][]any{
		{"1", "1", "1981"}, {"2", "2", "1982"}, {"3", "3", "1983"}, {"4", "4", "1984"}})

	// Delete multiple rows
	primaryDatabase.MustExec("delete from db01.t where pk in ('1', '3');")
	time.Sleep(250 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by pk;", [][]any{
		{"2", "2", "1982"}, {"4", "4", "1984"}})

	// Update multiple rows
	primaryDatabase.MustExec("update db01.t set c2 = 1942;")
	time.Sleep(250 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by pk;", [][]any{
		{"2", "2", "1942"}, {"4", "4", "1942"}})

	// Turn off @@autocommit and mix inserts/updates/deletes in the same transaction
	primaryDatabase.MustExec("SET @@autocommit=0;")
	primaryDatabase.MustExec("insert into db01.t values ('10', 10, 2020), ('11', 11, 2021), ('12', 12, 2022), ('13', 13, 2023);")
	primaryDatabase.MustExec("delete from db01.t where pk in ('11', '13');")
	primaryDatabase.MustExec("update db01.t set c2 = 2042 where c2 > 2000;")
	primaryDatabase.MustExec("COMMIT;")
	time.Sleep(250 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by pk;", [][]any{
		{"10", "10", "2042"}, {"12", "12", "2042"},
		{"2", "2", "1942"}, {"4", "4", "1942"},
	})
}

// requireReplicaResults runs the specified |query| on the replica database and asserts that the results match
// |expectedResults|. Note that the actual results are converted to string values in almost all cases, due to
// limitations in the SQL library we use to query the replica database, so |expectedResults| should generally
// be expressed in strings.
//
// TODO: Extract to binlog_test_utils
func requireReplicaResults(t *testing.T, query string, expectedResults [][]any) {
	rows, err := replicaDatabase.Queryx(query)
	require.NoError(t, err)
	allRows := readAllRowsIntoSlices(t, rows)
	require.Equal(t, len(expectedResults), len(allRows), "Expected %v, got %v", expectedResults, allRows)
	for i := range expectedResults {
		require.Equal(t, expectedResults[i], allRows[i], "Expected %v, got %v", expectedResults[i], allRows[i])
	}
	require.NoError(t, rows.Close())
}

// outputReplicaApplierStatus prints out the replica applier status information from the
// performance_schema replication_applier_status_by_worker table. This is useful for debugging
// replication from a Dolt primary to a MySQL replica, since this often contains more detailed
// information about why MySQL failed to apply a binlog event.
func outputReplicaApplierStatus(t *testing.T) {
	newRows, err := replicaDatabase.Queryx("select * from performance_schema.replication_applier_status_by_worker;")
	require.NoError(t, err)
	allNewRows := readAllRowsIntoMaps(t, newRows)
	fmt.Printf("\n\nreplication_applier_status_by_worker: %v\n", allNewRows)
}

// outputShowReplicaStatus prints out replica status information. This is useful for debugging
// replication failures in tests since status will show whether the replica is successfully connected,
// any recent errors, and what GTIDs have been executed.
func outputShowReplicaStatus(t *testing.T) {
	newRows, err := replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)
	allNewRows := readAllRowsIntoMaps(t, newRows)
	fmt.Printf("\n\nSHOW REPLICA STATUS: %v\n", allNewRows)
}
