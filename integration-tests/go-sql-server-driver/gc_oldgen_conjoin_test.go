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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestGCConjoinsOldgen(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		everyN int
	}{
		{"never_archive", 1024},
		{"always_archive", 1},
		{"half_archive", 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t

			u, err := driver.NewDoltUser()
			require.NoError(t, err)
			t.Cleanup(func() { u.Cleanup() })

			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			repoName := "gc_oldgen_conjoin_test_" + tc.name
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

			ctx := t.Context()

			const NumCommits = 288

			func() {
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				for i := range NumCommits {
					_, err = conn.ExecContext(ctx, "call dolt_commit('--allow-empty', '-am', 'commit "+strconv.Itoa(i)+"')")
					require.NoError(t, err)
					gcSQL := "call dolt_gc('--archive-level', '0')"
					if ((i + 1) % tc.everyN) == 0 {
						gcSQL = "call dolt_gc('--archive-level', '1')"
					}
					_, err = conn.ExecContext(ctx, gcSQL)
					require.NoError(t, err)
				}
			}()

			count := CountTableFiles(t, filepath.Join(repo.Dir, ".dolt/noms/oldgen"))
			// The conjoin is triggered on the first 257 table files that are added, as the 258th table file is added.
			// In our situation, the conjoin always leaves 2 table files over.
			require.Equal(t, NumCommits-256, count)
			require.Contains(t, server.Output.String(), "conjoin completed successfully")
			err = server.Restart(nil, nil)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				return db.PingContext(t.Context()) == nil
			}, 5*time.Second, 50*time.Millisecond)
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			row := conn.QueryRowContext(ctx, "select count(*) from dolt_log")
			require.NoError(t, row.Err())
			var commitCnt int
			require.NoError(t, row.Scan(&commitCnt))
			// The + 1 is for the initial commit.
			require.Equal(t, NumCommits+1, commitCnt)

		})
	}
}
