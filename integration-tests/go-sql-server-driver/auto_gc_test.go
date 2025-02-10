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
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestAutoGC(t *testing.T) {
	var enabled_16, final_16, disabled, final_disabled RepoSize
	t.Run("Enable", func(t *testing.T) {
		t.Run("CommitEvery16", func(t *testing.T) {
			enabled_16, final_16 = runAutoGCTest(t, true, 16)
			t.Logf("repo size before final gc: %v", enabled_16)
			t.Logf("repo size after final gc: %v", final_16)
		})
	})
	t.Run("Disabled", func(t *testing.T) {
		disabled, final_disabled = runAutoGCTest(t, false, 128)
		t.Logf("repo size before final gc: %v", disabled)
		t.Logf("repo size after final gc: %v", final_disabled)
	})
	if enabled_16.NewGen > 0 && disabled.NewGen > 0 {
		assert.Greater(t, enabled_16.OldGen, disabled.OldGen)
		assert.Greater(t, enabled_16.OldGenC, disabled.OldGenC)
		assert.Greater(t, enabled_16.NewGen-enabled_16.Journal, enabled_16.Journal)
		assert.Less(t, disabled.NewGen-disabled.Journal, disabled.Journal)
	}
}

func setupAutoGCTest(ctx context.Context, t *testing.T, enable bool) (string, *sql.DB) {
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("auto_gc_test")
	require.NoError(t, err)

	err = driver.WithFile{
		Name: "server.yaml",
		Contents: fmt.Sprintf(`
behavior:
  auto_gc_behavior:
    enable: %v
`, enable),
	}.WriteAtDir(repo.Dir)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args: []string{"--config", "server.yaml"},
	})
	server.DBName = "auto_gc_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	// Create the database...
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `
create table vals (
    id bigint primary key,
    v1 bigint,
    v2 bigint,
    v3 bigint,
    v4 bigint,
    index (v1),
    index (v2),
    index (v3),
    index (v4),
    index (v1,v2),
    index (v1,v3),
    index (v1,v4),
    index (v2,v3),
    index (v2,v4),
    index (v2,v1),
    index (v3,v1),
    index (v3,v2),
    index (v3,v4),
    index (v4,v1),
    index (v4,v2),
    index (v4,v3)
)
`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	return repo.Dir, db
}

func autoGCInsertStatement(i int) string {
	var vals []string
	for j := i * 1024; j < (i+1)*1024; j++ {
		var vs [4]string
		vs[0] = strconv.Itoa(rand.Int())
		vs[1] = strconv.Itoa(rand.Int())
		vs[2] = strconv.Itoa(rand.Int())
		vs[3] = strconv.Itoa(rand.Int())
		val := "(" + strconv.Itoa(j) + "," + strings.Join(vs[:], ",") + ")"
		vals = append(vals, val)
	}
	return "insert into vals values " + strings.Join(vals, ",")
}

func runAutoGCTest(t *testing.T, enable bool, commitEvery int) (RepoSize, RepoSize) {
	// A simple auto-GC test, where we run
	// operations on an auto GC server and
	// ensure that the database is getting
	// collected.
	ctx := context.Background()
	dir, db := setupAutoGCTest(ctx, t, enable)

	for i := 0; i < 64; i++ {
		stmt := autoGCInsertStatement(i)
		conn, err := db.Conn(ctx)
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
		if i%commitEvery == 0 {
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(i*1024)+"')")
			require.NoError(t, err)
		}
		require.NoError(t, conn.Close())
	}

	before, err := GetRepoSize(dir)
	require.NoError(t, err)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_gc('--full')")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	after, err := GetRepoSize(dir)
	require.NoError(t, err)
	return before, after
}

type RepoSize struct {
	Journal int64
	NewGen  int64
	NewGenC int
	OldGen  int64
	OldGenC int
}

func GetRepoSize(dir string) (RepoSize, error) {
	var ret RepoSize
	entries, err := os.ReadDir(filepath.Join(dir, ".dolt/noms"))
	if err != nil {
		return ret, err
	}
	for _, e := range entries {
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

func (rs RepoSize) String() string {
	return fmt.Sprintf("journal: %v, new gen: %v (%v files), old gen: %v (%v files)", rs.Journal, rs.NewGen, rs.NewGenC, rs.OldGen, rs.OldGenC)
}
