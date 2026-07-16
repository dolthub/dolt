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
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestClusterDropDatabaseParticipatesInAckWritesTimeout asserts that DROP
// DATABASE replication participates in dolt_cluster_ack_writes_timeout_secs,
// exactly like normal writes, users/grants, and branch control do.
//
// With ack writes enabled, a write on the primary blocks until it has been
// acknowledged by the standby replicas, or until the ack-writes timeout
// elapses (at which point it returns with a replication-timeout warning). This
// is how a client learns whether its write actually made it to the standby.
//
// The scenario:
//   - A primary (server1) and standby (server2) replicate two databases,
//     repo_up and repo_down. dolt_cluster_ack_writes_timeout_secs is set.
//   - While the standby is up and caught up, the primary drops repo_up. The
//     drop replicates promptly, so the statement returns well before the
//     timeout and emits no timeout warning. This proves the wait is driven by
//     actual replication acknowledgement, not a blind sleep.
//   - The standby is stopped. The primary drops repo_down. The drop cannot be
//     acknowledged by the (stopped) standby, so a correct implementation blocks
//     until the ack-writes timeout elapses and then returns with a
//     replication-timeout warning.
//
// Before the fix, DROP DATABASE replication was entirely fire-and-forget: the
// drop hook launched background goroutines and returned immediately, never
// plumbing a ReplicationStatusController into WaitForReplicationController. As
// a result the statement returned instantly regardless of whether the drop had
// replicated, and dolt_cluster_ack_writes_timeout_secs had no effect on it.
func TestClusterDropDatabaseParticipatesInAckWritesTimeout(t *testing.T) {
	t.Parallel()

	const ackTimeoutSecs = 5

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	server1Port := ports.GetOrAllocatePort("server1")
	server1Cluster := ports.GetOrAllocatePort("server1_cluster")
	server2Port := ports.GetOrAllocatePort("server2")
	server2Cluster := ports.GetOrAllocatePort("server2_cluster")

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
`, server1Port, server2Cluster, server1Cluster)

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
`, server2Port, server1Cluster, server2Cluster)

	primary := makeClusterServer(t, &ports, "server1", "server1", primaryConfig)
	standby := makeClusterServer(t, &ports, "server2", "server2", standbyConfig)

	ctx := t.Context()

	primaryDB, err := primary.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { primaryDB.Close() })

	primaryConn, err := primaryDB.Conn(ctx)
	require.NoError(t, err)
	defer primaryConn.Close()

	for _, stmt := range []string{
		fmt.Sprintf("SET @@GLOBAL.dolt_cluster_ack_writes_timeout_secs = %d", ackTimeoutSecs),
		"create database repo_up",
		"use repo_up",
		"create table vals (i int primary key)",
		"insert into vals values (0),(1),(2),(3),(4)",
		"create database repo_down",
		"use repo_down",
		"create table vals (i int primary key)",
		"insert into vals values (0),(1),(2),(3),(4)",
	} {
		_, err := primaryConn.ExecContext(ctx, stmt)
		require.NoErrorf(t, err, "statement: %s", stmt)
	}

	// Wait for both databases to replicate to the standby so that the standby
	// will accept (and acknowledge) their drops.
	standbyDB, err := standby.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { standbyDB.Close() })
	waitForDatabasesOnStandby(t, ctx, standbyDB, []string{"dolt_cluster", "information_schema", "mysql", "repo_down", "repo_up"})
	standbyDB.Close()

	// Control: with the standby up and caught up, dropping repo_up replicates
	// promptly. The statement must return well before the ack-writes timeout
	// and emit no replication-timeout warning. This proves the wait is
	// satisfied by a real acknowledgement, not by always sleeping the timeout.
	start := time.Now()
	_, err = primaryConn.ExecContext(ctx, "drop database repo_up")
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.Lessf(t, elapsed, (ackTimeoutSecs-1)*time.Second,
		"drop of repo_up with a live, caught-up standby should return promptly, took %v", elapsed)
	require.Empty(t, replicationTimeoutWarnings(t, ctx, primaryConn),
		"drop of repo_up replicated to a live standby, so it should not emit a replication-timeout warning")

	// Stop the standby so that the DROP DATABASE below cannot be acknowledged.
	require.NoError(t, standby.GracefulStop())

	// Gap: dropping repo_down cannot be acknowledged by the stopped standby.
	// With ack writes enabled, the statement must block until the ack-writes
	// timeout elapses and then return with a replication-timeout warning.
	start = time.Now()
	_, err = primaryConn.ExecContext(ctx, "drop database repo_down")
	require.NoError(t, err)
	elapsed = time.Since(start)
	require.GreaterOrEqualf(t, elapsed, (ackTimeoutSecs-2)*time.Second,
		"drop of repo_down should block on the ack-writes timeout (~%ds) while the standby is down, but returned after %v",
		ackTimeoutSecs, elapsed)
	require.NotEmptyf(t, replicationTimeoutWarnings(t, ctx, primaryConn),
		"drop of repo_down could not replicate to the stopped standby, so it should emit a replication-timeout warning")
}

// replicationTimeoutWarnings returns the messages of any replication-timeout
// warnings (as emitted by WaitForReplicationController) attached to the most
// recently executed statement on conn.
func replicationTimeoutWarnings(t *testing.T, ctx context.Context, conn *sql.Conn) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	require.NoError(t, err)
	defer rows.Close()
	var msgs []string
	for rows.Next() {
		var level, message string
		var code int
		require.NoError(t, rows.Scan(&level, &code, &message))
		if strings.Contains(message, "Timed out replication") {
			msgs = append(msgs, message)
		}
	}
	require.NoError(t, rows.Err())
	return msgs
}
