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
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestGCWithPendingEdits tests that a session-aware GC succeeds, and keeps what the session needs, while a session
// has pending edits to a table. The writer of those edits holds the table's primary index root, which is stored
// inline in the table's chunk, so GC must not be asked to keep the root by its address.
func TestGCWithPendingEdits(t *testing.T) {
	t.Parallel()
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("pending_edits_gc_test")
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args:          []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort:   "server_port",
		LogNotMatches: []string{"caught panic"},
	}, &ports)
	server.DBName = "pending_edits_gc_test"
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	ctx := t.Context()
	a, err := db.Conn(ctx)
	require.NoError(t, err)
	defer a.Close()
	b, err := db.Conn(ctx)
	require.NoError(t, err)
	defer b.Close()

	const rows = 5000
	_, err = a.ExecContext(ctx, "CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(200))")
	require.NoError(t, err)
	// Enough rows for a tree with internal nodes, so that the writer reads leaves from the store.
	_, err = a.ExecContext(ctx, `INSERT INTO t
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
SELECT i, 0, REPEAT('x', 150) FROM n`, rows)
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "CREATE TABLE scratch (id INT AUTO_INCREMENT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "CALL dolt_commit('-Am', 'create tables')")
	require.NoError(t, err)

	// Collect the separate copy of the primary index root that writing the table left behind.
	_, err = b.ExecContext(ctx, "CALL dolt_gc()")
	require.NoError(t, err)

	_, err = a.ExecContext(ctx, "START TRANSACTION")
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "UPDATE t SET v = 1 WHERE id = 1")
	require.NoError(t, err)

	// Without a write since the last GC, the GC returns early without visiting any session.
	_, err = b.ExecContext(ctx, "INSERT INTO scratch VALUES ()")
	require.NoError(t, err)
	_, err = b.ExecContext(ctx, "CALL dolt_gc()")
	require.NoError(t, err)

	// Reads a leaf the first UPDATE didn't.
	_, err = a.ExecContext(ctx, "UPDATE t SET v = 1 WHERE id = ?", rows)
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "COMMIT")
	require.NoError(t, err)

	_, err = b.ExecContext(ctx, "CALL dolt_gc()")
	require.NoError(t, err)
	var sum int
	require.NoError(t, b.QueryRowContext(ctx, "SELECT SUM(v) FROM t").Scan(&sum))
	require.Equal(t, 2, sum)
}
