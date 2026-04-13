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
	"github.com/dolthub/dolt/go/store/chunks"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestGCIncremental(t *testing.T) {
	t.Parallel()

	archiveLevels := []chunks.GCArchiveLevel{chunks.NoArchive, chunks.SimpleArchive}

	// small_file_size is chosen so that every leaf chunk gets put in its own table file. In this test, we know
	// exactly how many table files to expect after GC.
	smallFileSize := 1

	// medium_file_size is chosen so that multiple chunks are put into each table file, and ensures that we correctly handle
	// an in-progress table file at the end of the mark-and-sweep.
	mediumFileSize := 5_000

	// large_file_size is arbitrarily large and tests the case where all leaf chunks fit in a single table file.
	largeFileSize := 1_000_0000

	fileSizes := []int{smallFileSize, mediumFileSize, largeFileSize}

	for _, archiveLevel := range archiveLevels {
		t.Run(fmt.Sprintf("archiveLevel=%d", archiveLevel), func(t *testing.T) {
			t.Parallel()
			for _, fileSize := range fileSizes {
				t.Run(fmt.Sprintf("fileSize=%d", fileSize), func(t *testing.T) {
					t.Parallel()
					for _, full := range []bool{true, false} {
						t.Run(fmt.Sprintf("full=%v", full), func(t *testing.T) {
							t.Parallel()

							runGCIncrementalTest(t, archiveLevel, fileSize, full)
						})
					}
				})
			}
		})
	}
}

func runGCIncrementalTest(t *testing.T, archiveLevel chunks.GCArchiveLevel, fileSize int, full bool) {
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

	ctx := context.Background()

	server := MakeServer(t, repo, &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}, &ports)
	server.DBName = repoName

	// Create the database and run GC
	func() {

		db, err := server.DB(driver.Connection{User: "root"})
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		// Create some chunks that will be collected into oldgen because they're referenced by a commit
		_, err = conn.ExecContext(ctx, "create table vals (id bigint primary key, val bigint)")
		require.NoError(t, err)
		vals := []string{}
		for i := 0; i <= 1024; i++ {
			vals = append(vals, fmt.Sprintf("(%d,0)", i))
		}
		_, err = conn.ExecContext(ctx, "insert into vals values "+strings.Join(vals, ","))
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
		require.NoError(t, err)

		// Create some chunks that will be collected into newgen because they aren't referenced by a commit
		_, err = conn.ExecContext(ctx, "create table vals2 (id bigint primary key, val bigint)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into vals2 values "+strings.Join(vals, ","))
		require.NoError(t, err)
		var gcSQL string
		if full {
			gcSQL = "call dolt_gc('--archive-level',?,'--incremental-file-size',?,'--full');"
		} else {
			gcSQL = "call dolt_gc('--archive-level',?,'--incremental-file-size',?);"
		}
		_, err = conn.ExecContext(ctx, gcSQL, archiveLevel, fileSize)
		require.NoError(t, err)
	}()

	// Verify that the DB is in a valid state and there are no dropped chunks.
	err = repo.DoltExec("fsck")
	require.NoError(t, err)

	repoSize, err := GetRepoSize(repo.Dir)
	require.NoError(t, err)
	// Both newgen and oldgen should contain multiple chunk files
	require.Greater(t, repoSize.NewGenC, 1)
	require.Greater(t, repoSize.OldGenC, 1)
}

type RepoSize struct {
	Journal int64
	NewGen  int64
	NewGenC int
	OldGen  int64
	OldGenC int
}

func GetRepoSize(dir string) (RepoSize, error) {
	skipFile := func(filename string) bool {
		return filename == "manifest" || filename == "LOCK"
	}
	var ret RepoSize
	entries, err := os.ReadDir(filepath.Join(dir, ".dolt/noms"))
	if err != nil {
		return ret, err
	}
	for _, e := range entries {
		if skipFile(e.Name()) {
			continue
		}
		stat, err := e.Info()
		if err != nil {
			return ret, err
		}
		if stat.IsDir() {
			continue
		}
		ret.NewGen += stat.Size()
		ret.NewGenC += 1
		if e.Name() == "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" {
			ret.Journal += stat.Size()
		}
	}
	entries, err = os.ReadDir(filepath.Join(dir, ".dolt/noms/oldgen"))
	if err != nil {
		return ret, err
	}
	for _, e := range entries {
		if skipFile(e.Name()) {
			continue
		}
		stat, err := e.Info()
		if err != nil {
			return ret, err
		}
		if stat.IsDir() {
			continue
		}
		ret.OldGen += stat.Size()
		ret.OldGenC += 1
	}
	return ret, nil
}
