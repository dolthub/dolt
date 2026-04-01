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

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestGCIncremental(t *testing.T) {
	t.Parallel()

	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repoName := "incremental_gc_test"
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
		// Create some chunks that will be collected into oldgen because they're referenced by a commit
		_, err = conn.ExecContext(ctx, "create table vals (id bigint primary key, val bigint)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
		require.NoError(t, err)
		// Create some chunks that will be collected into newgen because they aren't referenced by a commit
		_, err = conn.ExecContext(ctx, "create table vals2 (id bigint primary key, val bigint)")
		require.NoError(t, err)
		// An incremental file size of 1 means that every leaf node will receive its own chunk file
		gcSQL := "call dolt_gc('--archive-level','1','--incremental-file-size','1');"
		_, err = conn.ExecContext(ctx, gcSQL)
		require.NoError(t, err)
	}()

	repoSize, err := GetRepoSize(repo.Dir)
	require.NoError(t, err)
	// Both newgen and oldgen should contain multiple chunk files
	require.Greater(t, repoSize.NewGenC, 1)
	require.Greater(t, repoSize.OldGenC, 1)
}
