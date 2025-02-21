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
			var s AutoGCTest
			s.Enable = true
			enabled_16, final_16 = runAutoGCTest(t, &s, 64, 16)
			assert.Contains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
			t.Logf("repo size before final gc: %v", enabled_16)
			t.Logf("repo size after final gc: %v", final_16)
		})
		t.Run("ClusterReplication", func(t *testing.T) {
			// This test does not work yet, because remotsrv Commits
			// do not go through the doltdb.hooksDatabase hooks
			// machinery.
			t.Skip()
			var s AutoGCTest
			s.Enable = true
			s.Replicate = true
			enabled_16, final_16 = runAutoGCTest(t, &s, 256, 16)
			assert.Contains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
			assert.Contains(t, string(s.StandbyServer.Output.Bytes()), "Successfully completed auto GC")
			t.Logf("repo size before final gc: %v", enabled_16)
			t.Logf("repo size after final gc: %v", final_16)
			rs, err := GetRepoSize(s.StandbyDir)
			require.NoError(t, err)
			t.Logf("standby size: %v", rs)
		})
	})
	t.Run("Disabled", func(t *testing.T) {
		var s AutoGCTest
		disabled, final_disabled = runAutoGCTest(t, &s, 64, 128)
		assert.NotContains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
		t.Logf("repo size before final gc: %v", disabled)
		t.Logf("repo size after final gc: %v", final_disabled)
	})
	if enabled_16.NewGen > 0 && disabled.NewGen > 0 {
		assert.Greater(t, enabled_16.OldGen, disabled.OldGen)
		assert.Greater(t, enabled_16.OldGenC, disabled.OldGenC)
		assert.Less(t, disabled.NewGen-disabled.Journal, disabled.Journal)
	}
}

type AutoGCTest struct {
	Enable        bool
	PrimaryDir    string
	PrimaryServer *driver.SqlServer
	PrimaryDB     *sql.DB

	Replicate     bool
	StandbyDir    string
	StandbyServer *driver.SqlServer
	StandbyDB     *sql.DB
}

func (s *AutoGCTest) Setup(ctx context.Context, t *testing.T) {
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	s.CreatePrimaryServer(ctx, t, u)

	if s.Replicate {
		u, err := driver.NewDoltUser()
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})
		s.CreateStandbyServer(ctx, t, u)
	}

	s.CreatePrimaryDatabase(ctx, t)
}

func (s *AutoGCTest) CreatePrimaryServer(ctx context.Context, t *testing.T, u driver.DoltUser) {
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("auto_gc_test")
	require.NoError(t, err)

	behaviorFragment := fmt.Sprintf(`
behavior:
  auto_gc_behavior:
    enable: %v
`, s.Enable)

	var clusterFragment string
	if s.Replicate {
		clusterFragment = `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:3852/{database}
  bootstrap_role: primary
  bootstrap_epoch: 1
  remotesapi:
    port: 3851
`
	}

	err = driver.WithFile{
		Name: "server.yaml",
		Contents: behaviorFragment + clusterFragment,
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

	s.PrimaryDir = repo.Dir
	s.PrimaryDB = db
	s.PrimaryServer = server
}

func (s *AutoGCTest) CreateStandbyServer(ctx context.Context, t *testing.T, u driver.DoltUser) {
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("auto_gc_test")
	require.NoError(t, err)

	behaviorFragment := fmt.Sprintf(`
listener:
  host: 0.0.0.0
  port: 3308
behavior:
  auto_gc_behavior:
    enable: %v
`, s.Enable)

	var clusterFragment string
	if s.Replicate {
		clusterFragment = `
cluster:
  standby_remotes:
  - name: primary
    remote_url_template: http://localhost:3851/{database}
  bootstrap_role: standby
  bootstrap_epoch: 1
  remotesapi:
    port: 3852
`
	}

	err = driver.WithFile{
		Name: "server.yaml",
		Contents: behaviorFragment + clusterFragment,
	}.WriteAtDir(repo.Dir)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args: []string{"--config", "server.yaml"},
		Port: 3308,
	})
	server.DBName = "auto_gc_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	s.StandbyDir = repo.Dir
	s.StandbyDB = db
	s.StandbyServer = server
}

func (s *AutoGCTest) CreatePrimaryDatabase(ctx context.Context, t *testing.T) {
	// Create the database...
	conn, err := s.PrimaryDB.Conn(ctx)
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

func runAutoGCTest(t *testing.T, s *AutoGCTest, numStatements int, commitEvery int) (RepoSize, RepoSize) {
	// A simple auto-GC test, where we run
	// operations on an auto GC server and
	// ensure that the database is getting
	// collected.
	ctx := context.Background()
	s.Setup(ctx, t)

	for i := 0; i < numStatements; i++ {
		stmt := autoGCInsertStatement(i)
		conn, err := s.PrimaryDB.Conn(ctx)
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
		if i%commitEvery == 0 {
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(i*1024)+"')")
			require.NoError(t, err)
		}
		require.NoError(t, conn.Close())
	}

	before, err := GetRepoSize(s.PrimaryDir)
	require.NoError(t, err)
	conn, err := s.PrimaryDB.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_gc('--full')")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	after, err := GetRepoSize(s.PrimaryDir)
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
