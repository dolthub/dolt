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

// TestClusterStandbyAuthenticatesRestartedPrimaryWithManyDatabases restarts
// the primary of a two node cluster which has many databases. The restarted
// primary signs with a new key, and every database's replication thread calls
// the standby at once. The standby should look the new key up for all of those
// calls, and should not refuse any of them as unauthenticated.
func TestClusterStandbyAuthenticatesRestartedPrimaryWithManyDatabases(t *testing.T) {
	t.Parallel()

	const numDatabases = 16

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	primaryPort := ports.GetOrAllocatePort("server1")
	primaryCluster := ports.GetOrAllocatePort("server1_cluster")
	standbyPort := ports.GetOrAllocatePort("server2")
	standbyCluster := ports.GetOrAllocatePort("server2_cluster")

	primaryConfig := fmt.Sprintf(`
log_level: info
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
log_level: info
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
	standby, _ := makeClusterServer(t, &ports, "server2", "server2", standbyConfig)

	ctx := t.Context()

	databases := make([]string, numDatabases)
	for i := range databases {
		databases[i] = fmt.Sprintf("repo%02d", i)
	}

	func() {
		db, err := primary.DB(driver.Connection{User: "root"})
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, name := range databases {
			for _, stmt := range []string{
				"create database " + name,
				"create table " + name + ".vals (i int primary key)",
				"insert into " + name + ".vals values (1)",
			} {
				_, err := conn.ExecContext(ctx, stmt)
				require.NoErrorf(t, err, "statement: %s", stmt)
			}
		}
	}()
	waitForPrimaryCaughtUp(t, ctx, primary, databases)

	standbyLogBeforeRestart := standby.Output.Len()
	require.NoError(t, primary.Restart(nil, nil))
	waitForPrimaryCaughtUp(t, ctx, primary, databases)

	standbyLog := standby.Output.String()[standbyLogBeforeRestart:]
	var refused []string
	for _, line := range strings.Split(standbyLog, "\n") {
		if strings.Contains(line, "authorization header failed to verify") {
			refused = append(refused, line)
		}
	}
	require.Emptyf(t, refused, "standby refused %d calls from the restarted primary:\n%s",
		len(refused), strings.Join(refused, "\n"))
}

// waitForPrimaryCaughtUp waits until a write to every one of |databases| on
// |primary| has been acknowledged by the standby.
func waitForPrimaryCaughtUp(t *testing.T, ctx context.Context, primary *driver.SqlServer, databases []string) {
	t.Helper()
	db, err := primary.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		notCaughtUp, err := primaryCaughtUp(ctx, db, databases)
		if err == nil && notCaughtUp == "" {
			return
		}
		if err != nil {
			notCaughtUp = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("primary never caught up its standby: %s", notCaughtUp)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// primaryCaughtUp returns "" when every database has replicated with no
// error, and otherwise a description of one which has not.
func primaryCaughtUp(ctx context.Context, db *sql.DB, databases []string) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "select `database`, replication_lag_millis, current_error from dolt_cluster.dolt_cluster_status")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	seen := make(map[string]bool)
	for rows.Next() {
		var name string
		var lag sql.NullInt64
		var currentError sql.NullString
		if err := rows.Scan(&name, &lag, &currentError); err != nil {
			return "", err
		}
		if !lag.Valid || lag.Int64 != 0 || currentError.Valid {
			return fmt.Sprintf("%s: replication_lag_millis %v, current_error %v", name, lag, currentError), nil
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	for _, name := range databases {
		if !seen[name] {
			return name + " is not in dolt_cluster_status", nil
		}
	}
	return "", nil
}
