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
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var toxiClient *toxiproxy.Client
var mysqlProxy *toxiproxy.Proxy
var proxyPort int

// TestBinlogReplicationReconnection tests that the replica's connection to the primary is correctly
// reestablished if it drops.
func TestBinlogReplicationReconnection(t *testing.T) {
	createServers(t)
	configureToxiProxy(t)
	startReplication(t, proxyPort)
	defer teardown(t)

	primaryDatabase.MustExec("create table reconnect_test(pk int primary key, c1 varchar(255));")
	for i := 0; i < 1000; i++ {
		value := "foobarbazbashfoobarbazbashfoobarbazbashfoobarbazbashfoobarbazbash"
		primaryDatabase.MustExec(fmt.Sprintf("insert into reconnect_test values (%v, %q)", i, value))
	}
	// Remove the limit_data toxic so that a connection can be reestablished
	mysqlProxy.RemoveToxic("limit_data")

	// Assert that all records get written to the table
	time.Sleep(5 * time.Second)
	rows, err := replicaDatabase.Queryx("select min(pk) as min, max(pk) as max, count(pk) as count from reconnect_test;")
	require.NoError(t, err)

	row := readNextRow(t, rows)
	require.Equal(t, "0", toString(row["min"]))
	require.Equal(t, "999", toString(row["max"]))
	require.Equal(t, "1000", toString(row["count"]))

	// Assert that show replica status show reconnection events
	rows, err = replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)

	row = readNextRow(t, rows)
	// TODO: Assert that show replica status show reconnection events
	//       There isn't a reconnects counter, but we could probably grab the last IO thread error and confirm
	//       an unexpected EOF was seen and recovered from... Or... if we do have the reconnect time be 60s, that would
	//       be plenty of time to check that the status is not connected and then that it gets reconnected.
	fmt.Printf("REPLICA STATUS: %q \n", row)
}

func configureToxiProxy(t *testing.T) {
	// TODO: This depends on the toxiproxy-server already being running on this port!
	//       Change this to launch and manage the server
	toxiproxyPort := 8474
	toxiClient = toxiproxy.NewClient(fmt.Sprintf("localhost:%d", toxiproxyPort))
	fmt.Printf("Toxiproxy server running on port %d \n", toxiproxyPort)

	proxyPort = findFreePort()
	var err error
	mysqlProxy, err = toxiClient.CreateProxy("mysql",
		fmt.Sprintf("localhost:%d", proxyPort), // downstream
		fmt.Sprintf("localhost:%d", mySqlPort)) // upstream
	if err != nil {
		panic(fmt.Sprintf("unable to create toxiproxy: %v", err.Error()))
	}

	mysqlProxy.AddToxic("limit_data", "limit_data", "downstream", 1.0, toxiproxy.Attributes{
		"bytes": 1_000,
	})
	fmt.Printf("Toxiproxy proxy with limit_data toxic started on port %d \n", proxyPort)
}
