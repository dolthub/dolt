// Copyright 2023 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	sqldriver "database/sql/driver"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestGCConjoinsOldgen(t *testing.T) {
	u, err := driver.NewDoltUser(t.TempDir() )
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("concurrent_gc_test")
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{})
	server.DBName = "concurrent_gc_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create the database...
	func() {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "create table vals (id bigint primary key, val bigint)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
		require.NoError(t, err)
	}()

	// Each 1024 leaf entries is approximately one chunk.
	// We create 512 commits, each of which adds the next 1024 rows sequentially.
	// After we create each commit, we dolt_gc(), which moves the new chunks to
	// the oldgen. At the end of the test, we will assert that there are not
	// too many table files in the old gen.
	for i := 0; i < 512; i++ {
		var vals []string
		for j := i*1024; j < (i+1)*1024; j++ {
			vals = append(vals, "(" + strconv.Itoa(j) + ",0)")
		}
		func() {
			conn, err := db.Conn(ctx)
			defer func() {
				// After calling dolt_gc, the connection is bad. Remove it from the connection pool.
				conn.Raw(func(_ any) error {
					return sqldriver.ErrBadConn
				})
			}()

			_, err = conn.ExecContext(ctx, "insert into vals values " + strings.Join(vals, ","))
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from " + strconv.Itoa(i*1024) + "')")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "call dolt_gc()")
			require.NoError(t, err)
		}()
	}

	entries, err := os.ReadDir(filepath.Join(repo.Dir, ".dolt/noms/oldgen"))
	require.NoError(t, err)
	require.Greater(t, len(entries), 2)
	// defaultMaxTables == 256, plus a few files extra like |manifest| and |LOCK|.
	require.Less(t, len(entries), 272)
}
