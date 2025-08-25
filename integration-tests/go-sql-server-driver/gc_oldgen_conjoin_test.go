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
	sqldriver "database/sql/driver"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestGCConjoinsOldgen(t *testing.T) {
	t.Parallel()

	type gcMode int
	const (
		modeNever gcMode = iota
		modeAlways
		modeQuarter
	)

	cases := []struct {
		name string
		mode gcMode
	}{
		{"never", modeNever},
		{"always", modeAlways},
		{"quarter", modeQuarter},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// NOTE: avoid t.Parallel() here to keep dynamic ports simple.
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t

			u, err := driver.NewDoltUser()
			require.NoError(t, err)
			t.Cleanup(func() { u.Cleanup() })

			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			repoName := "concurrent_gc_test_" + tc.name
			repo, err := rs.MakeRepo(repoName)
			require.NoError(t, err)

			server := MakeServer(t, repo, &driver.Server{
				Args:        []string{"--port", `{{get_port "server"}}`},
				DynamicPort: "server",
			}, &ports)
			server.DBName = repoName

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
			// We create 512 commits, each adding the next 1024 rows sequentially.
			// After each commit we run dolt_gc(), varying the args per test case.
			for i := 0; i < 512; i++ {
				var vals []string
				for j := i * 1024; j < (i+1)*1024; j++ {
					vals = append(vals, "("+strconv.Itoa(j)+",0)")
				}
				func(iter int) {
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					defer func() {
						// After calling dolt_gc, the connection is bad. Remove it from the connection pool.
						conn.Raw(func(_ any) error {
							return sqldriver.ErrBadConn
						})
					}()

					_, err = conn.ExecContext(ctx, "insert into vals values "+strings.Join(vals, ","))
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(iter*1024)+"')")
					require.NoError(t, err)

					gcSQL := "call dolt_gc()"
					switch tc.mode {
					case modeAlways:
						gcSQL = "call dolt_gc('--archive-level','1')"
					case modeQuarter:
						if iter%4 == 0 {
							gcSQL = "call dolt_gc('--archive-level','1')"
						}
					case modeNever:
						// leave as "call dolt_gc()"
					}

					_, err = conn.ExecContext(ctx, gcSQL)
					require.NoError(t, err)
				}(i)
			}

			entries, err := os.ReadDir(filepath.Join(repo.Dir, ".dolt/noms/oldgen"))
			require.NoError(t, err)
			require.Greater(t, len(entries), 2)
			// defaultMaxTables == 256, plus a few files extra like |manifest| and |LOCK|.
			require.Less(t, len(entries), 272)
		})
	}
}
