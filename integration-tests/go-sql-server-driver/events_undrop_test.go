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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestEventsUndrop verifies that events continue to fire after a database is
// dropped and then undropped. This exercises the event executor's quiesce/wake
// path: after the undrop the executor must discover the restored events and
// resume executing them.
func TestEventsUndrop(t *testing.T) {
	t.Parallel()
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("events_undrop_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
		Envs:        []string{"DOLT_EVENT_SCHEDULER_PERIOD=1"},
	}
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "events_undrop_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// Set up the table and a recurring event that increments a counter every second.
	_, err = conn.ExecContext(ctx, "CREATE TABLE counter (val INT NOT NULL)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO counter VALUES (0)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE EVENT inc_counter ON SCHEDULE EVERY 1 SECOND DO UPDATE counter SET val = val + 1")
	require.NoError(t, err)

	// Wait for the event to fire at least once.
	require.Eventually(t, func() bool {
		var val int
		err := conn.QueryRowContext(ctx, "SELECT val FROM events_undrop_test.counter").Scan(&val)
		return err == nil && val >= 1
	}, 10*time.Second, 500*time.Millisecond, "event did not fire before drop")

	// Drop and undrop the database.
	_, err = conn.ExecContext(ctx, "DROP DATABASE events_undrop_test")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CALL dolt_undrop('events_undrop_test')")
	require.NoError(t, err)

	// Read the counter value right after undrop so we can assert it increments further.
	var valAfterUndrop int
	err = conn.QueryRowContext(ctx, "SELECT val FROM events_undrop_test.counter").Scan(&valAfterUndrop)
	require.NoError(t, err)

	// Wait for the event to fire again after the undrop.
	require.Eventually(t, func() bool {
		var val int
		err := conn.QueryRowContext(ctx, "SELECT val FROM events_undrop_test.counter").Scan(&val)
		return err == nil && val > valAfterUndrop
	}, 10*time.Second, 500*time.Millisecond, "event did not fire after undrop")

	var finalVal int
	err = conn.QueryRowContext(ctx, "SELECT val FROM events_undrop_test.counter").Scan(&finalVal)
	require.NoError(t, err)
	assert.Greater(t, finalVal, valAfterUndrop, "counter should have incremented after undrop")
}
