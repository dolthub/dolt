// Copyright 2026 Dolthub, Inc.
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

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestClusterStandbyKeepsLocalUserAcrossUsersAndGrants asserts that a standby
// which applies a primary's users and grants keeps the ephemeral account the
// CLI connects to a running server with. That account is created by the server
// instance itself and is never part of a replicated payload, so applying one
// must leave it in place.
func TestClusterStandbyKeepsLocalUserAcrossUsersAndGrants(t *testing.T) {
	t.Parallel()

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	primaryPort := ports.GetOrAllocatePort("server1")
	primaryCluster := ports.GetOrAllocatePort("server1_cluster")
	standbyPort := ports.GetOrAllocatePort("server2")
	standbyCluster := ports.GetOrAllocatePort("server2_cluster")

	primaryConfig := fmt.Sprintf(`
log_level: trace
listener:
  host: 0.0.0.0
  port: %d
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:%d/{database}
  bootstrap_role: primary
  bootstrap_epoch: 1
  remotesapi:
    port: %d
`, primaryPort, standbyCluster, primaryCluster)

	standbyConfig := fmt.Sprintf(`
log_level: trace
listener:
  host: 0.0.0.0
  port: %d
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:%d/{database}
  bootstrap_role: standby
  bootstrap_epoch: 1
  remotesapi:
    port: %d
`, standbyPort, primaryCluster, standbyCluster)

	primary, _ := makeClusterServer(t, &ports, "server1", "server1", primaryConfig)
	standby, standbyStore := makeClusterServer(t, &ports, "server2", "server2", standbyConfig)

	ctx := t.Context()

	requireDoltSQLConnects(t, standbyStore, "before users and grants replicated")

	primaryDB, err := primary.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { primaryDB.Close() })
	primaryConn, err := primaryDB.Conn(ctx)
	require.NoError(t, err)
	defer primaryConn.Close()

	for _, stmt := range []string{
		"SET @@GLOBAL.dolt_cluster_ack_writes_timeout_secs = 10",
		"create database repo1",
		"use repo1",
		"create table vals (i int primary key)",
		`create user "aaron"@"%" identified by "aaronspassword"`,
		`grant ALL ON *.* to "aaron"@"%"`,
	} {
		_, err := primaryConn.ExecContext(ctx, stmt)
		require.NoErrorf(t, err, "statement: %s", stmt)
	}

	// The standby accepting a connection as the replicated user is how we know
	// it has applied the users and grants payload.
	require.Eventually(t, func() bool {
		db, err := standby.DB(driver.Connection{User: "aaron", Pass: "aaronspassword"})
		if err != nil {
			return false
		}
		defer db.Close()
		conn, err := db.Conn(ctx)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 30*time.Second, 100*time.Millisecond, "replicated user never became usable on the standby")

	requireDoltSQLConnects(t, standbyStore, "after users and grants replicated")
}

// requireDoltSQLConnects asserts that `dolt sql` in |rs| can connect to the
// sql-server running there, which it does as the server's ephemeral local user.
func requireDoltSQLConnects(t *testing.T, rs driver.RepoStore, when string) {
	t.Helper()
	cmd := rs.DoltCmd("sql", "-q", "select 1")
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "dolt sql %s: %s", when, string(output))
}
