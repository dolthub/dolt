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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestClusterTransitionToStandbyWaitsForDropDatabase asserts that
// dolt_cluster_transition_to_standby does not consider a standby replica to be
// fully caught up while a DROP DATABASE has not yet been replicated to it.
//
// The scenario:
//   - A primary (server1) and standby (server2) replicate a database, repo1.
//     dolt_cluster_ack_writes_timeout_secs is set so the writes block until
//     they (and users/grants and branch control) have replicated. This ensures
//     everything except the upcoming drop is fully quiesced and caught up.
//   - The standby is stopped.
//   - The primary drops repo1. The drop cannot be replicated to the (stopped)
//     standby, so it stays outstanding.
//   - The primary is asked to transition to standby, requiring 1 caught-up
//     replica.
//
// Users/grants and branch control are already replicated and quiesced, so they
// report caught up even with the standby down. The only outstanding
// replication is the drop of repo1. A correct implementation must therefore
// refuse to consider the standby caught up and fail the transition.
//
// Before the fix, the transition ignored outstanding DROP DATABASE
// replications entirely and reported the standby as fully caught up, silently
// succeeding while the standby still held repo1.
func TestClusterTransitionToStandbyWaitsForDropDatabase(t *testing.T) {
	t.Parallel()

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

	// Create repo1 with data and touch users/grants and branch control on the
	// primary. With ack writes enabled, each of these statements blocks until
	// it has replicated to the standby, so once they return there is no
	// outstanding repo1/users/grants/branch-control novelty pending
	// replication.
	for _, stmt := range []string{
		"SET @@GLOBAL.dolt_cluster_ack_writes_timeout_secs = 10",
		"create database repo1",
		"use repo1",
		"create table vals (i int primary key)",
		"insert into vals values (0),(1),(2),(3),(4)",
		`create user "replprobe"@"%" identified by "replprobepassword"`,
		`grant all on *.* to "replprobe"@"%"`,
		"delete from dolt_branch_control",
		`insert into dolt_branch_control values ("repo1", "main", "replprobe", "%", "admin")`,
	} {
		_, err := primaryConn.ExecContext(ctx, stmt)
		require.NoErrorf(t, err, "statement: %s", stmt)
	}

	// Sanity check: repo1 has replicated to the standby.
	standbyDB, err := standby.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { standbyDB.Close() })
	waitForDatabasesOnStandby(t, ctx, standbyDB, []string{"dolt_cluster", "information_schema", "mysql", "repo1"})
	standbyDB.Close()

	// Stop the standby so that the DROP DATABASE below cannot be replicated.
	require.NoError(t, standby.GracefulStop())

	// Drop repo1 on the primary. Everything else is already fully replicated
	// and quiesced, so the only outstanding replication to the standby is this
	// drop.
	_, err = primaryConn.ExecContext(ctx, "drop database repo1")
	require.NoError(t, err)

	// Ask the primary to transition to standby, requiring 1 caught-up replica.
	// The only standby is down with an outstanding drop, so no replica is
	// fully caught up and the transition must fail.
	rows, err := primaryConn.QueryContext(ctx, "call dolt_cluster_transition_to_standby('2', '1')")
	if err == nil {
		reported := collectTransitionRows(t, rows)
		t.Fatalf("expected dolt_cluster_transition_to_standby to fail because the DROP DATABASE of repo1 "+
			"has not replicated to the (stopped) standby, but it succeeded and reported: %v", reported)
	}
	require.ErrorContains(t, err, "could not ensure 1 replicas were caught up")
}

// makeClusterServer starts a single sql-server in its own repo store, using the
// provided cluster config written to server.yaml. portName must already be
// allocated in ports and must match the listener port embedded in config.
func makeClusterServer(t *testing.T, ports *DynamicResources, name, portName, config string) *driver.SqlServer {
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	f := driver.WithFile{Name: "server.yaml", Contents: config}
	require.NoError(t, f.WriteAtDir(rs.Dir))

	server := MakeServer(t, rs, &driver.Server{
		Name:        name,
		Args:        []string{"--config", "server.yaml"},
		DynamicPort: portName,
	}, ports)
	require.NotNil(t, server)
	return server
}

func waitForDatabasesOnStandby(t *testing.T, ctx context.Context, db *sql.DB, want []string) {
	t.Helper()
	var last []string
	require.Eventuallyf(t, func() bool {
		conn, err := db.Conn(ctx)
		if err != nil {
			return false
		}
		defer conn.Close()
		got, err := showDatabases(ctx, conn)
		if err != nil {
			return false
		}
		last = got
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "standby never converged to databases %v; last saw %v", want, &last)
}

func showDatabases(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "show databases")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(dbs)
	return dbs, nil
}

func collectTransitionRows(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		row := ""
		for i, c := range cols {
			if i > 0 {
				row += ", "
			}
			row += fmt.Sprintf("%s=%s", c, vals[i].String)
		}
		out = append(out, "{"+row+"}")
	}
	require.NoError(t, rows.Err())
	return out
}
