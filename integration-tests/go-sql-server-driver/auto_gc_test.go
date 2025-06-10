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
	t.Parallel()
	var enabled_16, final_16, disabled, final_disabled RepoSize
	numStatements, numCommits := 512, 16
	if testing.Short() || os.Getenv("CI") != "" {
		numStatements = 64
	}
	t.Run("Enable", func(t *testing.T) {
		t.Parallel()
		for _, sa := range []struct {
			archive bool
			name    string
		}{{true, "Archive"}, {false, "NoArchive"}} {
			t.Run(sa.name, func(t *testing.T) {
				t.Run("CommitEvery16", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.Archive = sa.archive
					enabled_16, final_16 = runAutoGCTest(t, &s, numStatements, numCommits)
					assert.Contains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
					assert.NotContains(t, string(s.PrimaryServer.Output.Bytes()), "dangling references requested during GC")
					t.Logf("repo size before final gc: %v", enabled_16)
					t.Logf("repo size after final gc: %v", final_16)
				})
				t.Run("ClusterReplication", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.Replicate = true
					s.Archive = sa.archive
					enabled_16, final_16 = runAutoGCTest(t, &s, numStatements, numCommits)
					assert.Contains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
					assert.Contains(t, string(s.StandbyServer.Output.Bytes()), "Successfully completed auto GC")
					assert.NotContains(t, string(s.PrimaryServer.Output.Bytes()), "dangling references requested during GC")
					assert.NotContains(t, string(s.StandbyServer.Output.Bytes()), "dangling references requested during GC")
					t.Logf("repo size before final gc: %v", enabled_16)
					t.Logf("repo size after final gc: %v", final_16)
					rs, err := GetRepoSize(s.StandbyDir)
					require.NoError(t, err)
					t.Logf("standby size: %v", rs)
				})
				t.Run("PushToRemotesAPI", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.EnableRemotesAPI = true
					s.Archive = sa.archive
					enabled_16, final_16 = runAutoGCTest(t, &s, numStatements, 2)
					assert.Contains(t, string(s.PrimaryServer.Output.Bytes()), "Successfully completed auto GC")
					assert.Contains(t, string(s.StandbyServer.Output.Bytes()), "Successfully completed auto GC")
					assert.NotContains(t, string(s.PrimaryServer.Output.Bytes()), "dangling references requested during GC")
					assert.NotContains(t, string(s.StandbyServer.Output.Bytes()), "dangling references requested during GC")
				})
			})
		}
	})
	t.Run("Disabled", func(t *testing.T) {
		t.Parallel()
		var s AutoGCTest
		disabled, final_disabled = runAutoGCTest(t, &s, numStatements, 128)
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
	Archive       bool
	PrimaryDir    string
	PrimaryServer *driver.SqlServer
	PrimaryDB     *sql.DB

	Replicate     bool
	StandbyDir    string
	StandbyServer *driver.SqlServer
	StandbyDB     *sql.DB

	EnableRemotesAPI bool

	Ports *DynamicResources
}

func (s *AutoGCTest) Setup(ctx context.Context, t *testing.T) {
	s.Ports = &DynamicResources{
		global: &GlobalPorts,
		t:      t,
	}
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	s.CreatePrimaryServer(ctx, t, u)

	if s.Replicate || s.EnableRemotesAPI {
		u, err := driver.NewDoltUser()
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})
		s.CreateStandbyServer(ctx, t, u)
	}

	s.CreatePrimaryDatabase(ctx, t)

	if s.EnableRemotesAPI {
		s.CreateUsersAndRemotes(ctx, t, u)
	}
}

func (s *AutoGCTest) CreatePrimaryServer(ctx context.Context, t *testing.T, u driver.DoltUser) {
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("auto_gc_test")
	require.NoError(t, err)

	archiveFragment := ``
	if s.Archive {
		archiveFragment = `
    archive_level: 1`
	}

	behaviorFragment := fmt.Sprintf(`
behavior:
  auto_gc_behavior:
    enable: %v%v
listener:
  port: {{get_port "primary_server_port"}}
`, s.Enable, archiveFragment)

	var clusterFragment string
	if s.Replicate {
		clusterFragment = `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:{{get_port "standby_server_cluster_port"}}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 1
  remotesapi:
    port: {{get_port "primary_server_cluster_port"}}
`
	}

	err = driver.WithFile{
		Name:     "server.yaml",
		Contents: behaviorFragment + clusterFragment,
		Template: s.Ports.ApplyTemplate,
	}.WriteAtDir(repo.Dir)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args:        []string{"--config", "server.yaml"},
		DynamicPort: "primary_server_port",
		Envs:        []string{"DOLT_REMOTE_PASSWORD=insecurepassword"},
	}, s.Ports)
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

	archiveFragment := ``
	if s.Archive {
		archiveFragment = `
    archive_level: 1`
	}

	behaviorFragment := fmt.Sprintf(`
listener:
  host: 0.0.0.0
  port: {{get_port "standby_server_port"}}
behavior:
  auto_gc_behavior:
    enable: %v%v
`, s.Enable, archiveFragment)

	var remotesapiFragment string
	if s.EnableRemotesAPI {
		remotesapiFragment = `
remotesapi:
  port: {{get_port "standby_remotesapi_port"}}
  read_only: false
`
	}

	var clusterFragment string
	if s.Replicate {
		clusterFragment = `
cluster:
  standby_remotes:
  - name: primary
    remote_url_template: http://localhost:{{get_port "primary_server_cluster_port"}}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 1
  remotesapi:
    port: {{get_port "standby_server_cluster_port"}}
`
	}

	err = driver.WithFile{
		Name:     "server.yaml",
		Contents: behaviorFragment + remotesapiFragment + clusterFragment,
		Template: s.Ports.ApplyTemplate,
	}.WriteAtDir(repo.Dir)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args:        []string{"--config", "server.yaml"},
		DynamicPort: "standby_server_port",
	}, s.Ports)
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

// Create a user on the stanby used for pushing from the primary to the standby.
// Create the remote itself on the primary, pointing to the standby.
func (s *AutoGCTest) CreateUsersAndRemotes(ctx context.Context, t *testing.T, u driver.DoltUser) {
	conn, err := s.StandbyDB.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create user remoteuser@'%' identified by 'insecurepassword'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "grant all on *.* to remoteuser@'%'")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	conn, err = s.PrimaryDB.Conn(ctx)
	require.NoError(t, err)
	port, ok := s.Ports.GetPort("standby_remotesapi_port")
	require.True(t, ok)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("call dolt_remote('add', 'origin', 'http://localhost:%d/auto_gc_test')", port))
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

	var pushAttempts, pushSuccesses int

	for i := 0; i < numStatements; i++ {
		stmt := autoGCInsertStatement(i)
		conn, err := s.PrimaryDB.Conn(ctx)
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
		if i%commitEvery == 0 {
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(i*1024)+"')")
			require.NoError(t, err)
			if s.EnableRemotesAPI {
				// Pushes are allowed to fail transiently, since the pushed data can be GCd before
				// it is added to the store. But pushes should mostly succeed.
				pushAttempts += 1
				_, err = conn.ExecContext(ctx, "call dolt_push('origin', '--force', '--user', 'remoteuser', 'main')")
				if err == nil {
					pushSuccesses += 1
				}
			}
		}
		require.NoError(t, conn.Close())
	}

	if s.EnableRemotesAPI {
		// Pushes should succeed at least 33% of the time.
		// This is a conservative lower bound.
		require.Less(t, float64(pushAttempts)*.33, float64(pushSuccesses))
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
