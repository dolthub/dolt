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

// TestCloneAfterPushStaleSingleton surfaces a bug in the interaction
// between NomsBlockStore's TableFileSource (Sources()) implementation
// and the dbfactory singleton cache for file:// URLs.
//
// When a long-running dolt process clones a file:// URL, the source
// NomsBlockStore is opened and stored in the dbfactory singleton cache,
// keyed by path. A subsequent clone of the same file:// URL reuses that
// cached NomsBlockStore. Meanwhile, an external process can push new
// writes to the file remote, adding new table files and rewriting its
// manifest on disk.
//
// On the second clone, NomsBlockStore.Sources() re-reads the on-disk
// manifest (which now references the newly-pushed table files), but
// builds its chunkSource map from the cached, in-memory table set, which
// is stale and does not contain those new table files. The lookup fails
// with ErrSpecWithoutChunkSource ("manifest referenced table file for
// which there is no chunkSource."), causing the second clone to fail.
func TestCloneAfterPushStaleSingleton(t *testing.T) {
	t.Parallel()
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	ctx := t.Context()

	// The "running dolt process" which performs the clones. Its
	// singleton cache will hold the source NomsBlockStore across both
	// clones.
	clonerRS, err := u.MakeRepoStore()
	require.NoError(t, err)
	clonerRepo, err := clonerRS.MakeRepo("cloner")
	require.NoError(t, err)
	clonerSrvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "cloner_port"}}`},
		DynamicPort: "cloner_port",
	}
	clonerServer := MakeServer(t, clonerRepo, clonerSrvSettings, &ports)
	clonerServer.DBName = "cloner"

	// A separate process which owns the source database and pushes new
	// writes to the file remote in between the two clones.
	remoteRS, err := u.MakeRepoStore()
	require.NoError(t, err)
	remoteRepo, err := remoteRS.MakeRepo("source")
	require.NoError(t, err)
	remoteSrvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "remote_port"}}`},
		DynamicPort: "remote_port",
	}
	remoteServer := MakeServer(t, remoteRepo, remoteSrvSettings, &ports)
	remoteServer.DBName = "source"

	// The file:// remote which the source pushes to and the cloner
	// clones from.
	remoteDir := t.TempDir()
	remoteURL := "file://" + remoteDir

	clonerDB, err := clonerServer.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, clonerDB.Close())
	})

	remoteDB, err := remoteServer.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, remoteDB.Close())
	})

	// Set up the file remote on the source server and push an initial
	// commit to it.
	func() {
		conn, err := remoteDB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "CALL DOLT_REMOTE('add', 'rendezvous', '"+remoteURL+"')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE t (pk int primary key, v int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "INSERT INTO t VALUES (1, 1)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '-m', 'first commit')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_PUSH('rendezvous', 'main:main')")
		require.NoError(t, err)
	}()

	// First clone. This opens the source NomsBlockStore and caches it in
	// the cloner process's dbfactory singleton cache.
	func() {
		conn, err := clonerDB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "CALL DOLT_CLONE('"+remoteURL+"', 'clone_one')")
		require.NoError(t, err)
	}()

	// Push more writes to the file remote from the source process. This
	// adds a new table file and rewrites the manifest on disk. The
	// cloner process's cached NomsBlockStore is unaware of this change.
	func() {
		conn, err := remoteDB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "INSERT INTO t VALUES (2, 2)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '-m', 'second commit')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_PUSH('rendezvous', 'main:main')")
		require.NoError(t, err)
	}()

	// Second clone of the same file:// URL. It reuses the cached, stale
	// source NomsBlockStore. Before the fix, Sources() reads the fresh
	// on-disk manifest but cannot find the newly-pushed table file in its
	// stale in-memory table set, and this clone fails with
	// "manifest referenced table file for which there is no chunkSource."
	func() {
		conn, err := clonerDB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "CALL DOLT_CLONE('"+remoteURL+"', 'clone_two')")
		require.NoError(t, err)
	}()
}
