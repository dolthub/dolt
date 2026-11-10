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

// TestSavepointSurvivesGC tests that edits made in a transaction before a savepoint are kept by a concurrent GC, so
// that rolling back to the savepoint can restore them.
func TestSavepointSurvivesGC(t *testing.T) {
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
	repo, err := rs.MakeRepo("savepoint_gc_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
	}
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "savepoint_gc_test"

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

	_, err = a.ExecContext(ctx, "CREATE TABLE t (a INT PRIMARY KEY, b LONGTEXT)")
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "CALL dolt_commit('-Am', 'create t')")
	require.NoError(t, err)

	_, err = a.ExecContext(ctx, "START TRANSACTION")
	require.NoError(t, err)
	// The value is stored out of band, so after the UPDATE only the savepoint's root references it.
	_, err = a.ExecContext(ctx, "INSERT INTO t VALUES (1, REPEAT(MD5(RAND()), 20000))")
	require.NoError(t, err)
	var expected string
	require.NoError(t, a.QueryRowContext(ctx, "SELECT b FROM t WHERE a = 1").Scan(&expected))
	_, err = a.ExecContext(ctx, "SAVEPOINT s")
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "UPDATE t SET b = 'y' WHERE a = 1")
	require.NoError(t, err)

	_, err = b.ExecContext(ctx, "CALL dolt_gc()")
	require.NoError(t, err)

	_, err = a.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s")
	require.NoError(t, err)
	var actual string
	require.NoError(t, a.QueryRowContext(ctx, "SELECT b FROM t WHERE a = 1").Scan(&actual))
	require.Equal(t, expected, actual)
	_, err = a.ExecContext(ctx, "COMMIT")
	require.NoError(t, err)

	b2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer b2.Close()
	require.NoError(t, b2.QueryRowContext(ctx, "SELECT b FROM t WHERE a = 1").Scan(&actual))
	require.Equal(t, expected, actual)
}
