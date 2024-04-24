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
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (" +
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
		"d1 decimal(14, 4), d2 decimal(14, 4), d3 decimal(14, 4)," +
		"bl1 blob, " +
		"tx1 text," +
		"bin1 binary(10), vbin1 varbinary(20)," +
		"geo1 geometry" +
		");")

	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(250 * time.Millisecond)
	requireReplicaResults(t, "show tables;", [][]any{{"t"}})

	primaryDatabase.MustExec("insert into db01.t values (" +
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
		"1234567890.1234, -1234567890.1234, 1234567890.0001," +
		"0x010203," +
		"'text text text'," +
		"0x0102030405, 0x0102030405060708090a," +
		"POINT(1,1) " +
		");")
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
		{"1", "42", nil, nil, "123", "123", "123", "123", "200", "200", "200", "200", "200",
			float32(1.0101), float64(2.02030405060708),
			"1981", "1981-02-16", "-123:45:30.123456",
			"1981-02-16 06:01:02", "1981-02-16 06:01:02.2", "1981-02-16 06:01:02.23", "1981-02-16 06:01:02.234",
			"1981-02-16 06:01:02.2345", "1981-02-16 06:01:02.23456", "1981-02-16 06:01:02.234567",
			"2024-04-08 10:30:42", "2024-04-08 10:30:42.8", "2024-04-08 10:30:42.87", "2024-04-08 10:30:42.876",
			"2024-04-08 10:30:42.8765", "2024-04-08 10:30:42.87654", "2024-04-08 10:30:42.876543",
			"\x01\x03",
			"green", "pants,tie,belt",
			"purple", "abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz",
			"1234567890.1234", "-1234567890.1234", "1234567890.0001",
			"\x01\x02\x03",
			"text text text",
			"\x01\x02\x03\x04\x05\x00\x00\x00\x00\x00", "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a",
			"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3F\x00\x00\x00\x00\x00\x00\xf0\x3F",
		},
	})
}

// TestBinlogPrimary_SimpleSchemaChangesWithAutocommit tests that we can make simple schema changes (e.g. create table,
// alter table, drop table) and replicate the DDL statements correctly.
func TestBinlogPrimary_SimpleSchemaChangesWithAutocommit(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	// Create a table
	primaryDatabase.MustExec("create table db01.t1 (pk int primary key, c1 varchar(255) NOT NULL comment 'foo bar baz');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show tables;", [][]any{{"t1"}})
	requireReplicaResults(t, "show create table db01.t1;", [][]any{{"t1",
		"CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `c1` varchar(255) COLLATE utf8mb4_0900_bin NOT NULL COMMENT 'foo bar baz',\n" +
			"  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})

	// Insert some data
	primaryDatabase.MustExec("insert into db01.t1 (pk, c1) values (1, 'foo');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t1;", [][]any{{"1", "foo"}})

	// Modify the table
	primaryDatabase.MustExec("alter table db01.t1 rename column c1 to z1;")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show create table db01.t1;", [][]any{{"t1",
		"CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `z1` varchar(255) COLLATE utf8mb4_0900_bin NOT NULL COMMENT 'foo bar baz',\n" +
			"  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})

	// Insert some data
	primaryDatabase.MustExec("insert into db01.t1 (pk, z1) values (2, 'bar');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t1;", [][]any{{"1", "foo"}, {"2", "bar"}})

	// Drop the table
	primaryDatabase.MustExec("drop table db01.t1;")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show tables;", [][]any{})

	// Rename a table
	primaryDatabase.MustExec("create table originalName(pk1 int, pk2 int, c1 varchar(200), c2 varchar(200), primary key (pk1, pk2));")
	primaryDatabase.MustExec("insert into originalName values (1, 2, 'one', 'two');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show tables;", [][]any{{"originalName"}})
	requireReplicaResults(t, "select * from originalName;", [][]any{{"1", "2", "one", "two"}})
	primaryDatabase.MustExec("rename table originalName to newName;")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show tables;", [][]any{{"newName"}})
	requireReplicaResults(t, "select * from newName;", [][]any{{"1", "2", "one", "two"}})
}

// TestBinlogPrimary_SchemaChangesWithManualCommit tests that manually managed transactions, which
// contain a mix of schema and data changes, can be correctly replicated.
func TestBinlogPrimary_SchemaChangesWithManualCommit(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	time.Sleep(100 * time.Millisecond)

	// Create table
	primaryDatabase.MustExec("set @@autocommit=0;")
	primaryDatabase.MustExec("start transaction;")
	primaryDatabase.MustExec("create table t (pk int primary key, c1 varchar(100), c2 int);")
	primaryDatabase.MustExec("insert into t values (1, 'one', 1);")
	primaryDatabase.MustExec("commit;")
	time.Sleep(100 * time.Millisecond)
	requireReplicaResults(t, "show create table t;", [][]any{{"t", "CREATE TABLE `t` (\n  " +
		"`pk` int NOT NULL,\n  `c1` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  " +
		"`c2` int DEFAULT NULL,\n  PRIMARY KEY (`pk`)\n) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})
	requireReplicaResults(t, "select * from t;", [][]any{{"1", "one", "1"}})

	// Alter column and update
	primaryDatabase.MustExec("start transaction;")
	primaryDatabase.MustExec("alter table t modify column c2 varchar(100);")
	primaryDatabase.MustExec("update t set c2='foo';")
	primaryDatabase.MustExec("commit;")
	time.Sleep(100 * time.Millisecond)
	requireReplicaResults(t, "show create table t;", [][]any{{"t", "CREATE TABLE `t` (\n  " +
		"`pk` int NOT NULL,\n  `c1` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  " +
		"`c2` varchar(100) COLLATE utf8mb4_0900_bin DEFAULT NULL,\n  PRIMARY KEY (`pk`)\n) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}})
	requireReplicaResults(t, "select * from t;", [][]any{{"1", "one", "foo"}})
}

// TestBinlogPrimary_ReplicateCreateDropDatabase tests that Dolt can correctly replicate statements to create,
// drop, and undrop databases.
func TestBinlogPrimary_ReplicateCreateDropDatabase(t *testing.T) {
	// TODO: Seems like we need a database-level hook for create/delete database in order to replicate them
	t.Skipf("Dolt is currently unable to replicate database level statements")

	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	time.Sleep(100 * time.Millisecond)

	// Test CREATE DATABASE
	primaryDatabase.MustExec("create database foobar1;")
	time.Sleep(100 * time.Millisecond)
	requireReplicaResults(t, "show databases;", [][]any{
		{"db01"}, {"foobar1"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})

	// Test DROP DATABASE
	primaryDatabase.MustExec("drop database foobar1;")
	time.Sleep(100 * time.Millisecond)
	requireReplicaResults(t, "show databases;", [][]any{
		{"db01"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})

	// Test DOLT_UNDROP_DATABASE()
	primaryDatabase.MustExec("call dolt_undrop_database('foobar1');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "show databases;", [][]any{
		{"db01"}, {"foobar1"}, {"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}})
}

func TestBinlogPrimary_InsertUpdateDelete(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")

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

// TestBinlogPrimary_OnlyReplicateMainBranch tests that binlog events are only generated for the main branch of a Dolt repository.
func TestBinlogPrimary_OnlyReplicateMainBranch(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// No events should be generated when we're not updating the main branch
	primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	primaryDatabase.MustExec("insert into db01.t values('hundred', 100, 2000);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Insert another row on branch1 and make sure it doesn't get replicated
	primaryDatabase.MustExec("insert into db01.t values('two hundred', 200, 2000);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Events should be generated from the main branch
	primaryDatabase.MustExec("call dolt_checkout('main');")
	primaryDatabase.MustExec("insert into db01.t values('42', 42, 2042);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"42", "42", "2042"}})
}

// TestBinlogPrimary_KeylessTables tests that Dolt can replicate changes to keyless tables.
func TestBinlogPrimary_KeylessTables(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (c1 varchar(100), c2 int, c3 int unsigned);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Test inserts
	primaryDatabase.MustExec("insert into db01.t values('one', 1, 11), ('two', 2, 22);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by c2;", [][]any{{"one", "1", "11"}, {"two", "2", "22"}})

	// Test inserting duplicate rows
	primaryDatabase.MustExec("insert into db01.t values('one', 1, 11), ('one', 1, 11);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by c2;", [][]any{
		{"one", "1", "11"}, {"one", "1", "11"}, {"one", "1", "11"}, {"two", "2", "22"}})

	// Test updating multiple rows
	primaryDatabase.MustExec("update db01.t set c1='uno' where c1='one';")
	primaryDatabase.MustExec("update db01.t set c1='zwei' where c1='two';")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by c2;", [][]any{
		{"uno", "1", "11"}, {"uno", "1", "11"}, {"uno", "1", "11"}, {"zwei", "2", "22"}})

	// Test deleting multiple rows
	primaryDatabase.MustExec("delete from db01.t where c1='uno';")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t order by c2;", [][]any{{"zwei", "2", "22"}})
}

// TestBinlogPrimary_Merge tests that the binlog is updated when data is merged in from another branch.
func TestBinlogPrimary_Merge(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int, c2 year);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// No events should be generated when we're not updating the main branch
	primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	primaryDatabase.MustExec("insert into db01.t values('hundred', 100, 2000), ('two-hundred', 200, 2001);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'inserting rows 100 and 200 on branch1');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Make a commit on main, so that we don't get a fast-forward merge later
	primaryDatabase.MustExec("call dolt_checkout('main');")
	primaryDatabase.MustExec("insert into db01.t values('42', 42, 2042);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'inserting row 42 on main');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"42", "42", "2042"}})

	// Merge branch1 into main
	primaryDatabase.MustExec("call dolt_merge('branch1');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{
		{"42", "42", "2042"}, {"hundred", "100", "2000"}, {"two-hundred", "200", "2001"}})
}

// TestBinlogPrimary_Cherrypick tests binlog replication when dolt_cherry_pick() is used to cherry-pick commits.
func TestBinlogPrimary_Cherrypick(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Make a couple of commits on branch1 so that we can cherry-pick them
	primaryDatabase.MustExec("call dolt_checkout('-b', 'branch1');")
	primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	primaryDatabase.MustExec("SET @RowOneCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	primaryDatabase.MustExec("SET @RowTwoCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	primaryDatabase.MustExec("SET @RowThreeCommit=dolt_hashof('HEAD');")

	// Cherry-pick a commit from branch1 onto main
	primaryDatabase.MustExec("call dolt_checkout('main');")
	primaryDatabase.MustExec("call dolt_cherry_pick(@RowTwoCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"02", "2"}})

	// Cherry-pick another commit from branch1 onto main
	primaryDatabase.MustExec("call dolt_cherry_pick(@RowThreeCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"02", "2"}, {"03", "3"}})
}

// TestBinlogPrimary_Revert tests binlog replication when dolt_revert() is used to revert commits.
func TestBinlogPrimary_Revert(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Make a couple of commits on main so that we can revert one
	primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	primaryDatabase.MustExec("SET @RowOneCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	primaryDatabase.MustExec("SET @RowTwoCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	primaryDatabase.MustExec("SET @RowThreeCommit=dolt_hashof('HEAD');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})

	// Revert a commit
	primaryDatabase.MustExec("call dolt_revert(@RowTwoCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}, {"03", "3"}})

	// Revert another commit
	primaryDatabase.MustExec("call dolt_revert(@RowOneCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"03", "3"}})
}

// TestBinlogPrimary_Reset tests that the binlog is updated when a branch head is reset to a different commit.
func TestBinlogPrimary_Reset(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	setupForDoltToMySqlReplication()
	startReplication(t, doltPort)
	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	//       Here we just pause to let the hardcoded binlog events be delivered
	time.Sleep(100 * time.Millisecond)

	primaryDatabase.MustExec("create table db01.t (pk varchar(100) primary key, c1 int);")
	primaryDatabase.MustExec("call dolt_commit('-Am', 'creating table t');")
	primaryDatabase.MustExec("SET @EmptyTableCommit=dolt_hashof('HEAD');")
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Make a couple of commits on main so that we can test resetting to them
	primaryDatabase.MustExec("insert into db01.t values('01', 1);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 01');")
	primaryDatabase.MustExec("SET @OneRowCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('02', 2);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 02');")
	primaryDatabase.MustExec("SET @TwoRowsCommit=dolt_hashof('HEAD');")
	primaryDatabase.MustExec("insert into db01.t values('03', 3);")
	primaryDatabase.MustExec("call dolt_commit('-am', 'inserting 03');")
	primaryDatabase.MustExec("SET @ThreeRowsCommit=dolt_hashof('HEAD');")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})

	// Reset back to the first commit when no rows are present
	primaryDatabase.MustExec("call dolt_reset('--hard', @EmptyTableCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{})

	// Reset back to the second commit when only one row is present
	primaryDatabase.MustExec("call dolt_reset('--hard', @OneRowCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}})

	// Reset back to the second commit when only one row is present
	primaryDatabase.MustExec("call dolt_reset('--hard', @TwoRowsCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}})

	// Reset back to the second commit when only one row is present
	primaryDatabase.MustExec("call dolt_reset('--hard', @ThreeRowsCommit);")
	time.Sleep(200 * time.Millisecond)
	requireReplicaResults(t, "select * from db01.t;", [][]any{{"01", "1"}, {"02", "2"}, {"03", "3"}})
}

func setupForDoltToMySqlReplication() {
	// Swap the replica and primary databases, since we're
	// replicating in the other direction in this test.
	var tempDatabase = primaryDatabase
	primaryDatabase = replicaDatabase
	replicaDatabase = tempDatabase

	// On the Primary, turn on GTID mode
	// NOTE: Dolt doesn't currently require moving through the GTID_MODE states like this, but
	//       MySQL does, so we do it here anyway.
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='OFF_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON_PERMISSIVE';")
	primaryDatabase.MustExec("set GLOBAL ENFORCE_GTID_CONSISTENCY='ON';")
	primaryDatabase.MustExec("set GLOBAL GTID_MODE='ON';")

	// Create the db01 database that our tests will use
	primaryDatabase.MustExec("create database db01;")
	primaryDatabase.MustExec("use db01;")

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
