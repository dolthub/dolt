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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestServerInMultiDatabaseDirectory starts a server in a directory of
// databases which is not itself a database, and asserts that each database is
// writable. A process which opened those databases twice would hold their
// locks with one store and be left read-only with the other.
func TestServerInMultiDatabaseDirectory(t *testing.T) {
	t.Parallel()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	dbNames := []string{"db_one", "db_two", "db_three"}
	for _, name := range dbNames {
		_, err := rs.MakeRepo(name)
		require.NoError(t, err)
	}
	_, err = os.Stat(filepath.Join(rs.Dir, ".dolt"))
	require.ErrorIs(t, err, fs.ErrNotExist)

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, rs, &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}, &ports)
	require.NotNil(t, server)

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for _, name := range dbNames {
		for _, q := range []string{
			fmt.Sprintf("use %s", name),
			"create table t (pk int primary key)",
			"insert into t values (1)",
			"call dolt_commit('-Am', 'written by the server')",
		} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, "database %s: %s", name, q)
		}
	}
}
