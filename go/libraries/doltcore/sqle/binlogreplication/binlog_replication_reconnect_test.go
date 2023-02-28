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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/toxiproxy/v2"
	toxiproxyclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var toxiClient *toxiproxyclient.Client
var mysqlProxy *toxiproxyclient.Proxy
var proxyPort int

// TestBinlogReplicationReconnection tests that the replica's connection to the primary is correctly
// reestablished if it drops.
func TestBinlogReplicationReconnection(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	configureToxiProxy(t)
	configureFastConnectionRetry(t)
	startReplication(t, proxyPort)

	testInitialReplicaStatus(t)

	primaryDatabase.MustExec("create table reconnect_test(pk int primary key, c1 varchar(255));")
	for i := 0; i < 1000; i++ {
		value := "foobarbazbashfoobarbazbashfoobarbazbashfoobarbazbashfoobarbazbash"
		primaryDatabase.MustExec(fmt.Sprintf("insert into reconnect_test values (%v, %q)", i, value))
	}
	// Remove the limit_data toxic so that a connection can be reestablished
	mysqlProxy.RemoveToxic("limit_data")

	// Assert that all records get written to the table
	waitForReplicaToCatchUp(t)

	rows, err := replicaDatabase.Queryx("select min(pk) as min, max(pk) as max, count(pk) as count from db01.reconnect_test;")
	require.NoError(t, err)

	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["min"])
	require.Equal(t, "999", row["max"])
	require.Equal(t, "1000", row["count"])
	require.NoError(t, rows.Close())

	// Assert that show replica status show reconnection IO error
	status := showReplicaStatus(t)
	require.Equal(t, "1158", status["Last_IO_Errno"])
	require.Equal(t, "unexpected EOF", status["Last_IO_Error"])
	requireRecentTimeString(t, status["Last_IO_Error_Timestamp"])
}

// configureFastConnectionRetry configures the replica to retry a failed connection after 5s, instead of the default 60s
// connection retry interval. This is used for testing connection retry logic without waiting the full default period.
func configureFastConnectionRetry(_ *testing.T) {
	replicaDatabase.MustExec(
		fmt.Sprintf("change replication source to SOURCE_CONNECT_RETRY=5;"))
}

// testInitialReplicaStatus tests the data returned by SHOW REPLICA STATUS and errors
// out if any values are not what is expected for a replica that has just connected
// to a MySQL primary.
func testInitialReplicaStatus(t *testing.T) {
	status := showReplicaStatus(t)

	// Positioning settings
	require.Equal(t, "true", status["Auto_Position"])

	// Connection settings
	require.Equal(t, "5", status["Connect_Retry"])
	require.Equal(t, "86400", status["Source_Retry_Count"])
	require.Equal(t, "localhost", status["Source_Host"])
	require.NotEmpty(t, status["Source_Port"])
	require.NotEmpty(t, status["Source_User"])

	// Error status
	require.Equal(t, "0", status["Last_Errno"])
	require.Equal(t, "", status["Last_Error"])
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "", status["Last_IO_Error_Timestamp"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_SQL_Error_Timestamp"])

	// Empty filter configuration
	require.Equal(t, "", status["Replicate_Do_Table"])
	require.Equal(t, "", status["Replicate_Ignore_Table"])

	// Thread status
	require.True(t,
		status["Replica_IO_Running"] == "Yes" ||
			status["Replica_IO_Running"] == "Connecting")
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// Unsupported fields
	require.Equal(t, "INVALID", status["Source_Log_File"])
	require.Equal(t, "Ignored", status["Source_SSL_Allowed"])
	require.Equal(t, "None", status["Until_Condition"])
	require.Equal(t, "0", status["SQL_Delay"])
	require.Equal(t, "0", status["SQL_Remaining_Delay"])
	require.Equal(t, "0", status["Seconds_Behind_Source"])
}

// requireRecentTimeString asserts that the specified |datetime| is a non-nil timestamp string
// with a value less than five minutes ago.
func requireRecentTimeString(t *testing.T, datetime interface{}) {
	require.NotNil(t, datetime)
	datetimeString := datetime.(string)

	datetime, err := time.Parse(time.UnixDate, datetimeString)
	require.NoError(t, err)
	require.LessOrEqual(t, time.Now().Add(-5*time.Minute), datetime)
	require.GreaterOrEqual(t, time.Now(), datetime)
}

// showReplicaStatus returns a map with the results of SHOW REPLICA STATUS, keyed by the
// name of each column.
func showReplicaStatus(t *testing.T) map[string]interface{} {
	rows, err := replicaDatabase.Queryx("show replica status;")
	defer rows.Close()
	require.NoError(t, err)
	return convertByteArraysToStrings(readNextRow(t, rows))
}

func configureToxiProxy(t *testing.T) {
	toxiproxyPort := findFreePort()

	metrics := toxiproxy.NewMetricsContainer(prometheus.NewRegistry())
	toxiproxyServer := toxiproxy.NewServer(metrics, zerolog.Nop())
	go func() {
		toxiproxyServer.Listen("localhost", strconv.Itoa(toxiproxyPort))
	}()
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Toxiproxy server running on port %d \n", toxiproxyPort)

	toxiClient = toxiproxyclient.NewClient(fmt.Sprintf("localhost:%d", toxiproxyPort))

	proxyPort = findFreePort()
	var err error
	mysqlProxy, err = toxiClient.CreateProxy("mysql",
		fmt.Sprintf("localhost:%d", proxyPort), // downstream
		fmt.Sprintf("localhost:%d", mySqlPort)) // upstream
	if err != nil {
		panic(fmt.Sprintf("unable to create toxiproxy: %v", err.Error()))
	}

	mysqlProxy.AddToxic("limit_data", "limit_data", "downstream", 1.0, toxiproxyclient.Attributes{
		"bytes": 1_000,
	})
	fmt.Printf("Toxiproxy proxy with limit_data toxic (1KB) started on port %d \n", proxyPort)
}

// convertByteArraysToStrings converts each []byte value in the specified map |m| into a string.
// This is necessary because MapScan doesn't honor (or know about) the correct underlying SQL types – it
// gets all results back as strings, typed as []byte.
// More info at the end of this issue: https://github.com/jmoiron/sqlx/issues/225
func convertByteArraysToStrings(m map[string]interface{}) map[string]interface{} {
	for key, value := range m {
		if bytes, ok := value.([]byte); ok {
			m[key] = string(bytes)
		}
	}

	return m
}
