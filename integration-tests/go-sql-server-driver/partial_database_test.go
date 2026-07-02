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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestServerSkipsPartialDatabaseDirectory verifies that a server started over a
// data directory holding a database that never finished initializing still
// answers INFORMATION_SCHEMA queries against the healthy databases beside it.
func TestServerSkipsPartialDatabaseDirectory(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11206
	t.Parallel()

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	good, err := rs.MakeRepo("good")
	require.NoError(t, err)
	require.NoError(t, good.DoltExec("sql", "-q", "CREATE TABLE t (i INT PRIMARY KEY)"))
	require.NoError(t, good.DoltExec("add", "."))
	require.NoError(t, good.DoltExec("commit", "-m", "init"))

	// A database whose creation was interrupted leaves behind storage files
	// without the repo state that marks it ready.
	incompleteNoms := filepath.Join(rs.Dir, "incomplete", ".dolt", "noms")
	require.NoError(t, os.MkdirAll(incompleteNoms, 0755))
	f, err := os.Create(filepath.Join(incompleteNoms, "LOCK"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	server := MakeServer(t, rs, &driver.Server{
		Args:        []string{"--port", `{{get_port "server_one"}}`},
		DynamicPort: "server_one",
		LogMatches:  []string{"skipping incomplete database directory"},
	}, &ports)
	require.NotNil(t, server)
	server.DBName = "good"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	var count int
	err = conn.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'good'",
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
