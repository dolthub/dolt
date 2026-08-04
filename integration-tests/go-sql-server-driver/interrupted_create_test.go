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
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// startServerOverOrphanedDir seeds a healthy database named good, lets |makeOrphan| place an orphaned database
// directory in the data directory, and starts a server that must log |logMatch| and still serve the healthy
// database. It returns a connection to the server and the data directory. The orphaned state is reproduced
// directly rather than by racing a real kill, so tests stay deterministic.
func startServerOverOrphanedDir(t *testing.T, makeOrphan func(rs driver.RepoStore), logMatch string) (*sql.Conn, string) {
	t.Helper()

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	good, err := rs.MakeRepo("good")
	require.NoError(t, err)
	require.NoError(t, good.DoltExec("sql", "-q", "CREATE TABLE t (i INT PRIMARY KEY)"))

	makeOrphan(rs)

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	server := MakeServer(t, rs, &driver.Server{
		Args:        []string{"--port", `{{get_port "server_one"}}`},
		DynamicPort: "server_one",
		LogMatches:  []string{logMatch},
	}, &ports)
	server.DBName = "good"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// An orphaned directory must not break INFORMATION_SCHEMA queries server-wide.
	var count int
	require.NoError(t, conn.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'good'").Scan(&count))
	require.Equal(t, 1, count)

	return conn, rs.Dir
}

func TestServerIgnoresInterruptedCreateDatabase(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11206
	t.Parallel()

	conn, dataDir := startServerOverOrphanedDir(t, func(rs driver.RepoStore) {
		_, err := rs.MakeRepo("pending")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(rs.Dir, "pending", ".dolt_safe_to_ignore"), nil, 0o644))
	}, "skipping in-progress database directory")

	var served string
	require.NoError(t, conn.QueryRowContext(t.Context(),
		"SELECT GROUP_CONCAT(schema_name) FROM information_schema.schemata WHERE schema_name IN ('good', 'pending')").Scan(&served))
	require.Equal(t, "good", served, "only the healthy database must be served")

	// Ignoring is non-destructive because another process could be completing the same create.
	_, statErr := os.Stat(filepath.Join(dataDir, "pending"))
	require.NoError(t, statErr, "orphaned directory should be left on disk, not deleted")
}

func TestServerSkipsIncompleteDatabaseDirectory(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11206
	t.Parallel()

	startServerOverOrphanedDir(t, func(rs driver.RepoStore) {
		// A database whose creation was interrupted leaves behind storage files
		// without the repo state that marks it ready.
		incompleteNoms := filepath.Join(rs.Dir, "incomplete", ".dolt", "noms")
		require.NoError(t, os.MkdirAll(incompleteNoms, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(incompleteNoms, "LOCK"), nil, 0o644))
	}, "skipping incomplete database directory")
}
