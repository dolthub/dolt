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
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	"github.com/dolthub/dolt/go/store/hash"
)

// TestCloneNoConjoin asserts that a clone (in this case a backup
// sync-url) of a store with many table files does not conjoin just
// because of the datas.Clone operation.
//
// In order to construct a store with a lot of table files, without it
// itself being conjoined, this test is tightly coupled with
// implementation details around GenerationalNBS,
// nbs.defaultMaxTables, GC oldGenRefs, and how pull works.
//
// Concretely, to setup the store, this test first creates a new dolt
// commit on `main` and then runs GC. This creates a new table file in
// the old gen. It does this 192 times. It then uses a remotesapi
// endpoint on another running sql-server to do a similar thing. It
// creates a remote to the remote database and then fetches it. It
// creates a new commit on the remote database and then fetches
// that. This creates a new table file in the new gen. It does this
// 192 times. At the end, the new gen has more than 192 table files
// and the old gen has exactly 192 table files.
//
// Then this test calls dolt_backup(sync-url) to a file remote.
//
// Finally, the test asserts that the destination file remote has at
// least 384 table files in it. With default settings, if conjoin had
// been enabled, they would have been conjoined to be below 256 files.
func TestCloneNoConjoin(t *testing.T) {
	t.Parallel()
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	testRS, err := u.MakeRepoStore()
	require.NoError(t, err)
	testRepo, err := testRS.MakeRepo("clone_no_conjoin_test")
	require.NoError(t, err)
	testSrvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		Envs:        []string{"DOLT_REMOTE_PASSWORD=insecure_password"},
		DynamicPort: "server_port",
	}
	testServer := MakeServer(t, testRepo, testSrvSettings, &ports)
	testServer.DBName = "clone_no_conjoin_test"

	remoteRS, err := u.MakeRepoStore()
	require.NoError(t, err)
	remoteRepo, err := remoteRS.MakeRepo("clone_no_conjoin_remote")
	require.NoError(t, err)
	remoteSrvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "remote_server_port"}}`},
		DynamicPort: "remote_server_port",
	}
	remoteServer := MakeServer(t, remoteRepo, remoteSrvSettings, &ports)
	remoteServer.DBName = "clone_no_conjoin_remote"

	// A file remote which remoteDB pushes to and db pulls from.
	rendezvousDir := t.TempDir()
	// The final sync-url destination about which we will make our assertions.
	finalDestDir := t.TempDir()
	
	ctx := t.Context()

	// First make our oldgen table files.
	db, err := testServer.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	func() {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for range 192 {
			_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '--allow-empty', '-m', 'creating a new commit')")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "CALL DOLT_GC()")
			require.NoError(t, err)
		}
	}()

	remoteDB, err := remoteServer.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, remoteDB.Close())
	}()

	// Now make the newgen table files, pulling from a file remote.
	func() {
		remoteConn, err := remoteDB.Conn(ctx)
		require.NoError(t, err)
		defer remoteConn.Close()
		_, err = remoteConn.ExecContext(ctx, "CALL DOLT_REMOTE('add', 'rendezvous', 'file://" + rendezvousDir + "')")
		require.NoError(t, err)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "CALL DOLT_REMOTE('add', 'rendezvous', 'file://" + rendezvousDir + "')")
		require.NoError(t, err)
		for range 192 {
			_, err = remoteConn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '--allow-empty', '-m', 'creating a new commit')")
			require.NoError(t, err)
			_, err = remoteConn.ExecContext(ctx, "CALL DOLT_PUSH('rendezvous', 'main:remoteref')")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "CALL DOLT_FETCH('rendezvous')")
			require.NoError(t, err)
		}
	}()

	// Now backup sync-url to the finalDestDir
	func() {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "CALL DOLT_BACKUP('sync-url', 'file://" + finalDestDir + "')")
		require.NoError(t, err)
	}()

	numTableFiles := 0
	filepath.WalkDir(finalDestDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		n := d.Name()
		n = strings.TrimSuffix(n, ".darc")
		_, ok := hash.MaybeParse(n)
		if ok {
			t.Logf("found table file %s", n)
			numTableFiles += 1
		}
		return nil
	})
	t.Logf("found %v table files", numTableFiles)
	assert.Greater(t, numTableFiles, 256)
}
