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
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBinlogPrimary(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// Swap the replica and primary databases, since we're
	// replicating in the other direction in this test.
	var tempDatabase = primaryDatabase
	primaryDatabase = replicaDatabase
	replicaDatabase = tempDatabase

	// TODO: On MySQL Replica, need to run:
	//       set GLOBAL GTID_MODE="OFF_PERMISSIVE";
	//       set GLOBAL GTID_MODE="ON_PERMISSIVE";
	//       set GLOBAL ENFORCE_GTID_CONSISTENCY="ON";
	//       set GLOBAL GTID_MODE="ON";

	// Create the replication user on the Dolt primary server
	// TODO: this should be done on both as part of the shared setup code
	primaryDatabase.MustExec("CREATE USER 'replicator'@'%' IDENTIFIED BY 'Zqr8_blrGm1!';")
	primaryDatabase.MustExec("GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';")

	// TODO: We need to set SOURCE_AUTO_POSITION=1 on the replica!!
	//       This wasn't required for Dolt, because we defaulted to
	//       auto positioning, but it is required for MySQL.

	// TODO: Need to set GTID system vars on the Dolt SQL server:
	//       set @@GLOBAL.GTID_MODE=ON
	//       set @@GLOBAL.ENFORCE_GTID_CONSISTENCY=ON

	// change replication source to SOURCE_HOST='localhost', SOURCE_USER='root', SOURCE_PASSWORD='', SOURCE_PORT=11229, SOURCE_AUTO_POSITION=1;

	startReplication(t, doltPort)

	// TODO: For now, we manually create the database and the table on the primary
	//       since we don't support replicating DDL statements yet.
	primaryDatabase.MustExec("create database db01")
	primaryDatabase.MustExec("use db01")
	createTableStatement := "create table t (pk int primary key, c1 varchar(100))"
	primaryDatabase.MustExec(createTableStatement)
	replicaDatabase.MustExec(createTableStatement)

	// Insert rows into the primary database and assert they get replicated
	primaryDatabase.MustExec("insert into t values (1, 'one')")

	// NOTE: waitForReplicaToCatchUp won't work until we implement GTID support
	time.Sleep(500 * time.Millisecond)

	rows, err := replicaDatabase.Queryx("select * from db01.t;")
	require.NoError(t, err)
	allRows := readAllRows(t, rows)
	require.Equal(t, 1, len(allRows))
	require.NoError(t, rows.Close())
	require.Equal(t, 1, allRows[0]["pk"])
	require.Equal(t, "one", allRows[0]["c1"])
}
