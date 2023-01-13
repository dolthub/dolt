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
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestBinlogReplicationFilters tests that replication filtering options are correctly
// applied and honored.
func TestBinlogReplicationFilters(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	// Ignore replication events for db01.t2
	replicaDatabase.MustExec("CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t2);")

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

	time.Sleep(100 * time.Millisecond)

	// Verify that all changes from t1 were applied on the replica
	rows, err := replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t1;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "10", row["count"])
	require.Equal(t, "0", row["min"])
	require.Equal(t, "9", row["max"])

	// Verify that no changes from t2 were applied on the replica
	rows, err = replicaDatabase.Queryx("SELECT COUNT(pk) as count, MIN(pk) as min, MAX(pk) as max from db01.t2;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["count"])
	require.Equal(t, nil, row["min"])
	require.Equal(t, nil, row["max"])
}
