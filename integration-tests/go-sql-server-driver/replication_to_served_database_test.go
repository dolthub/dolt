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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestReplicationToServedDatabaseIsDisabled starts a server over databases |a| and |b|, where |a| replicates through
// a file remote pointing at |b|'s own storage. The server already holds |b|'s chunk journal, so the remote can't be
// opened a second time: the server logs that, starts with replication for |a| disabled, and leaves |b| alone.
func TestReplicationToServedDatabaseIsDisabled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		sysvars string
	}{
		{"push_on_write", "  dolt_replicate_to_remote: origin\n"},
		{"read_replica", "  dolt_read_replica_remote: origin\n  dolt_replicate_heads: main\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			u, err := driver.NewDoltUser()
			require.NoError(t, err)
			t.Cleanup(func() { u.Cleanup() })
			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			b, err := rs.MakeRepo("b")
			require.NoError(t, err)
			require.NoError(t, b.DoltExec("sql", "-q", "create table bt (x int primary key); insert into bt values (1)"))
			require.NoError(t, b.DoltExec("commit", "-Am", "written to b"))
			a, err := rs.MakeRepo("a")
			require.NoError(t, err)
			// Resolve symlinks so the remote names the same path the server opens |b| at.
			bDir, err := filepath.EvalSymlinks(b.Dir)
			require.NoError(t, err)
			require.NoError(t, a.CreateRemote("origin", "file://"+filepath.ToSlash(filepath.Join(bDir, ".dolt", "noms"))))

			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			port := ports.GetOrAllocatePort("server")
			config := fmt.Sprintf("listener:\n  host: 127.0.0.1\n  port: %d\nsystem_variables:\n%s", port, tc.sysvars)
			require.NoError(t, driver.WithFile{Name: "server.yaml", Contents: config}.WriteAtDir(rs.Dir))
			server := MakeServer(t, rs, &driver.Server{
				Args:        []string{"--config", "server.yaml"},
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

			for _, q := range []string{
				"use a",
				"create table at (y int primary key)",
				"call dolt_commit('-Am', 'written to a')",
			} {
				_, err := conn.ExecContext(ctx, q)
				require.NoError(t, err, q)
			}
			var tables int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema = 'a' and table_name = 'bt'").Scan(&tables))
			require.Zero(t, tables, "a must not have pulled from b")
			var msg string
			require.NoError(t, conn.QueryRowContext(ctx, "select message from b.dolt_log limit 1").Scan(&msg))
			require.Equal(t, "written to b", msg, "a must not have pushed to b")

			require.Contains(t, server.Output.String(), "cannot create NBS store for directory containing chunk journal")
		})
	}
}
