// Copyright 2025 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// A simple test to ensure that temptf is cleaned up when we push.
//
// This is inconvenient to do from somewhere like `bats` because
// the failure mode we are looking for is before process shutdown,
// i.e., when there is a long running server.
func TestPushTemptfCleanup(t *testing.T) {
	t.Parallel()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	dbOne, err := rs.MakeRepo("db_one")
	require.NoError(t, err)
	dir := t.TempDir()
	dbOne.CreateRemote("origin", "file://"+dir)
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, rs, &driver.Server{
		Args:        []string{"--port", `{{get_port "server_one"}}`},
		DynamicPort: "server_one",
	}, &ports)
	require.NotNil(t, server)
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "USE db_one")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "CALL dolt_push('origin', 'main')")
	require.NoError(t, err)
	cnt := 0
	err = fs.WalkDir(os.DirFS(filepath.Join(dbOne.Dir, ".dolt/temptf")), ".", func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		cnt += 1
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
}
