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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBinlogReplicationMultiDb tests that binlog events spanning multiple databases are correctly
// applied by a replica.
func TestBinlogReplicationMultiDb(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// Make changes on the primary to db01 and db02
	primaryDatabase.MustExec("create database db02;")
	primaryDatabase.MustExec("use db01;")
	primaryDatabase.MustExec("create table t01 (pk int primary key, c1 int default (0))")
	primaryDatabase.MustExec("use db02;")
	primaryDatabase.MustExec("create table t02 (pk int primary key, c1 int default (0))")
	primaryDatabase.MustExec("use db01;")
	primaryDatabase.MustExec("insert into t01 (pk) values (1), (3), (5), (8), (9);")
	primaryDatabase.MustExec("use db02;")
	primaryDatabase.MustExec("insert into t02 (pk) values (2), (4), (6), (7), (10);")
	primaryDatabase.MustExec("use db01;")
	primaryDatabase.MustExec("delete from t01 where pk=9;")
	primaryDatabase.MustExec("delete from db02.t02 where pk=10;")
	primaryDatabase.MustExec("use db02;")
	primaryDatabase.MustExec("update db01.t01 set pk=7 where pk=8;")
	primaryDatabase.MustExec("update t02 set pk=8 where pk=7;")

	// Verify the changes in db01 on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.t01 order by pk asc;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "5", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "7", row["pk"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Close())

	// Verify db01.dolt_diff
	replicaDatabase.MustExec("use db01;")
	rows, err = replicaDatabase.Queryx("select * from db01.dolt_diff;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "1", row["data_change"])
	require.EqualValues(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "1", row["data_change"])
	require.EqualValues(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "1", row["data_change"])
	require.EqualValues(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "0", row["data_change"])
	require.EqualValues(t, "1", row["schema_change"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Close())

	// Verify the changes in db02 on the replica
	replicaDatabase.MustExec("use db02;")
	rows, err = replicaDatabase.Queryx("select * from db02.t02 order by pk asc;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "4", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "6", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "8", row["pk"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Verify db02.dolt_diff
	rows, err = replicaDatabase.Queryx("select * from db02.dolt_diff;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestBinlogReplicationMultiDbTransactions tests that binlog events for transactions that span
// multiple DBs are applied correctly to a replica.
func TestBinlogReplicationMultiDbTransactions(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// Make changes on the primary to db01 and db02
	primaryDatabase.MustExec("create database db02;")
	primaryDatabase.MustExec("create table db01.t01 (pk int primary key, c1 int default (0))")
	primaryDatabase.MustExec("create table db02.t02 (pk int primary key, c1 int default (0))")
	primaryDatabase.MustExec("set @autocommit = 0;")

	primaryDatabase.MustExec("start transaction;")
	primaryDatabase.MustExec("insert into db01.t01 (pk) values (1), (3), (5), (8), (9);")
	primaryDatabase.MustExec("insert into db02.t02 (pk) values (2), (4), (6), (7), (10);")
	primaryDatabase.MustExec("delete from db01.t01 where pk=9;")
	primaryDatabase.MustExec("delete from db02.t02 where pk=10;")
	primaryDatabase.MustExec("update db01.t01 set pk=7 where pk=8;")
	primaryDatabase.MustExec("update db02.t02 set pk=8 where pk=7;")
	primaryDatabase.MustExec("commit;")

	// Verify the changes in db01 on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.t01 order by pk asc;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "5", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "7", row["pk"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Verify db01.dolt_diff
	replicaDatabase.MustExec("use db01;")
	rows, err = replicaDatabase.Queryx("select * from db01.dolt_diff;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "1", row["data_change"])
	require.EqualValues(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t01", row["table_name"])
	require.EqualValues(t, "0", row["data_change"])
	require.EqualValues(t, "1", row["schema_change"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Verify the changes in db02 on the replica
	waitForReplicaToCatchUp(t)
	replicaDatabase.MustExec("use db02;")
	rows, err = replicaDatabase.Queryx("select * from db02.t02 order by pk asc;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "4", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "6", row["pk"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "8", row["pk"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Verify db02.dolt_diff
	rows, err = replicaDatabase.Queryx("select * from db02.dolt_diff;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "t02", row["table_name"])
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}
