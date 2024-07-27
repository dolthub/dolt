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
	"strings"
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

// TestBinlogReplicationAutoReconnect tests that the replica's connection to the primary is correctly
// reestablished if it drops.
func TestBinlogReplicationAutoReconnect(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	configureToxiProxy(t)
	configureFastConnectionRetry(t)
	startReplicationAndCreateTestDb(t, proxyPort)

	// Get the replica started up and ensure it's in sync with the primary before turning on the limit_data toxic
	testInitialReplicaStatus(t)
	primaryDatabase.MustExec("create table reconnect_test(pk int primary key, c1 varchar(255));")
	waitForReplicaToCatchUp(t)
	turnOnLimitDataToxic(t)

	for i := 0; i < 1000; i++ {
		value := "foobarbazbashfoobarbazbashfoobarbazbashfoobarbazbashfoobarbazbash"
		primaryDatabase.MustExec(fmt.Sprintf("insert into reconnect_test values (%v, %q)", i, value))
	}
	// Remove the limit_data toxic so that a connection can be reestablished
	err := mysqlProxy.RemoveToxic("limit_data")
	require.NoError(t, err)
	t.Logf("Toxiproxy proxy limit_data toxic removed")

	// Assert that all records get written to the table
	waitForReplicaToCatchUp(t)

	rows, err := replicaDatabase.Queryx("select min(pk) as min, max(pk) as max, count(pk) as count from db01.reconnect_test;")
	require.NoError(t, err)

	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["min"])
	require.Equal(t, "999", row["max"])
	require.Equal(t, "1000", row["count"])
	require.NoError(t, rows.Close())

	// Assert that show replica status show reconnection IO error
	status := showReplicaStatus(t)
	require.Equal(t, "1158", status["Last_IO_Errno"])
	require.True(t, strings.Contains(status["Last_IO_Error"].(string), "EOF"))
	requireRecentTimeString(t, status["Last_IO_Error_Timestamp"])
}

// configureFastConnectionRetry configures the replica to retry a failed connection after 5s, instead of the default 60s
// connection retry interval. This is used for testing connection retry logic without waiting the full default period.
func configureFastConnectionRetry(_ *testing.T) {
	replicaDatabase.MustExec(
		"change replication source to SOURCE_CONNECT_RETRY=5;")
}

// testInitialReplicaStatus tests the data returned by SHOW REPLICA STATUS and errors
// out if any values are not what is expected for a replica that has just connected
// to a MySQL primary.
func testInitialReplicaStatus(t *testing.T) {
	status := showReplicaStatus(t)

	// Positioning settings
	require.Equal(t, "1", status["Auto_Position"])

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
	require.NoError(t, err)
	defer rows.Close()
	return convertMapScanResultToStrings(readNextRow(t, rows))
}

func configureToxiProxy(t *testing.T) {
	toxiproxyPort := findFreePort()

	metrics := toxiproxy.NewMetricsContainer(prometheus.NewRegistry())
	toxiproxyServer := toxiproxy.NewServer(metrics, zerolog.Nop())
	go func() {
		toxiproxyServer.Listen("localhost", strconv.Itoa(toxiproxyPort))
	}()
	time.Sleep(500 * time.Millisecond)
	t.Logf("Toxiproxy control plane running on port %d", toxiproxyPort)

	toxiClient = toxiproxyclient.NewClient(fmt.Sprintf("localhost:%d", toxiproxyPort))

	proxyPort = findFreePort()
	var err error
	mysqlProxy, err = toxiClient.CreateProxy("mysql",
		fmt.Sprintf("localhost:%d", proxyPort), // downstream
		fmt.Sprintf("localhost:%d", mySqlPort)) // upstream
	if err != nil {
		panic(fmt.Sprintf("unable to create toxiproxy: %v", err.Error()))
	}
	t.Logf("Toxiproxy proxy started on port %d", proxyPort)
}

// turnOnLimitDataToxic adds a limit_data toxic to the active Toxiproxy, which prevents more than 1KB of data
// from being sent from the primary through the proxy to the replica. Callers MUST call configureToxiProxy
// before calling this function.
func turnOnLimitDataToxic(t *testing.T) {
	require.NotNil(t, mysqlProxy)
	_, err := mysqlProxy.AddToxic("limit_data", "limit_data", "downstream", 1.0, toxiproxyclient.Attributes{
		"bytes": 1_000,
	})
	require.NoError(t, err)
	t.Logf("Toxiproxy proxy with limit_data toxic (1KB) started on port %d", proxyPort)
}

// convertMapScanResultToStrings converts each value in the specified map |m| into a string.
// This is necessary because MapScan doesn't honor (or know about) the correct underlying SQL types – it
// gets results back as strings, typed as []byte. Results also get returned as int64, which are converted to strings
// for ease of testing.
// More info at the end of this issue: https://github.com/jmoiron/sqlx/issues/225
func convertMapScanResultToStrings(m map[string]interface{}) map[string]interface{} {
	for key, value := range m {
		switch v := value.(type) {
		case []uint8:
			m[key] = string(v)
		case int64:
			m[key] = strconv.FormatInt(v, 10)
		case uint64:
			m[key] = strconv.FormatUint(v, 10)
		}
	}

	return m
}

// convertSliceScanResultToStrings returns a new slice, formed by converting each value in the slice |ss| into a string.
// This is necessary because SliceScan doesn't honor (or know about) the correct underlying SQL types –it
// gets results back as strings, typed as []byte, or as int64 values.
// More info at the end of this issue: https://github.com/jmoiron/sqlx/issues/225
func convertSliceScanResultToStrings(ss []any) []any {
	row := make([]any, len(ss))
	for i, value := range ss {
		switch v := value.(type) {
		case []uint8:
			row[i] = string(v)
		case int64:
			row[i] = strconv.FormatInt(v, 10)
		case uint64:
			row[i] = strconv.FormatUint(v, 10)
		default:
			row[i] = v
		}
	}

	return row
}
