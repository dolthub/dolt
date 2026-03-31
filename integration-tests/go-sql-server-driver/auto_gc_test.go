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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestAutoGC(t *testing.T) {
	t.Parallel()
	t.Run("Enable", func(t *testing.T) {
		t.Parallel()
		for _, sa := range []struct {
			archive bool
			name    string
		}{{true, "Archive"}, {false, "NoArchive"}} {
			t.Run(sa.name, func(t *testing.T) {
				t.Parallel()
				t.Run("CommitEvery16", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.Archive = sa.archive
					runAutoGCTestUntilGC(t, &s, 3, 16)
				})
				t.Run("ClusterReplication", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.Replicate = true
					s.Archive = sa.archive
					runAutoGCTestUntilGC(t, &s, 3, 16)
				})
				t.Run("PushToRemotesAPI", func(t *testing.T) {
					t.Parallel()
					var s AutoGCTest
					s.Enable = true
					s.EnableRemotesAPI = true
					s.Archive = sa.archive
					runAutoGCTestUntilGC(t, &s, 3, 2)
				})
			})
		}
	})
	t.Run("Disabled", func(t *testing.T) {
		t.Parallel()
		var s AutoGCTest
		runAutoGCTestDisabled(t, &s, 64, 16)
	})
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

	primaryGCCount atomic.Int32
	standbyGCCount atomic.Int32
	sawDanglingRef atomic.Bool
}

func (s *AutoGCTest) gcVisitor(counter *atomic.Int32) func(string) {
	return func(line string) {
		if strings.Contains(line, "Successfully completed auto GC") {
			counter.Add(1)
		}
		if strings.Contains(line, "dangling references requested during GC") {
			s.sawDanglingRef.Store(true)
		}
	}
}

func (s *AutoGCTest) allServersReachedGCCount(target int) bool {
	if s.primaryGCCount.Load() < int32(target) {
		return false
	}
	if (s.Replicate || s.EnableRemotesAPI) && s.standbyGCCount.Load() < int32(target) {
		return false
	}
	return true
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
		Name:        "primary",
		Args:        []string{"--config", "server.yaml"},
		DynamicPort: "primary_server_port",
		Envs:        []string{"DOLT_REMOTE_PASSWORD=insecurepassword"},
	}, s.Ports, driver.WithOutputVisitor(s.gcVisitor(&s.primaryGCCount)))
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
		Name:        "standby",
		Args:        []string{"--config", "server.yaml"},
		DynamicPort: "standby_server_port",
	}, s.Ports, driver.WithOutputVisitor(s.gcVisitor(&s.standbyGCCount)))
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

// runAutoGCTestUntilGC runs insert+commit cycles until auto GC has
// completed targetGCCount times on every running server, or fails if
// that doesn't happen within a reasonable number of iterations. It
// fails immediately if any server logs a dangling references message.
func runAutoGCTestUntilGC(t *testing.T, s *AutoGCTest, targetGCCount int, commitEvery int) {
	const maxStatements = 1024
	ctx := t.Context()
	s.Setup(ctx, t)

	var pushAttempts, pushSuccesses int

	for i := range maxStatements {
		require.False(t, s.sawDanglingRef.Load(), "saw dangling references message during auto GC")
		if s.allServersReachedGCCount(targetGCCount) {
			t.Logf("all servers reached %d auto GCs after %d statements", targetGCCount, i)
			break
		}

		stmt := autoGCInsertStatement(i)
		conn, err := s.PrimaryDB.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
		if (i+1)%commitEvery == 0 {
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(i*1024)+"')")
			require.NoError(t, err)
			if s.EnableRemotesAPI {
				pushAttempts += 1
				_, err = conn.ExecContext(ctx, "call dolt_push('origin', '--force', '--user', 'remoteuser', 'main')")
				if err == nil {
					pushSuccesses += 1
				}
			}
		}
		require.NoError(t, conn.Close())
	}

	require.False(t, s.sawDanglingRef.Load(), "saw dangling references message during auto GC")
	require.True(t, s.allServersReachedGCCount(targetGCCount),
		"did not reach %d auto GCs within %d statements (primary: %d, standby: %d)",
		targetGCCount, maxStatements, s.primaryGCCount.Load(), s.standbyGCCount.Load())

	if s.EnableRemotesAPI {
		require.Less(t, float64(pushAttempts)*.33, float64(pushSuccesses))
	}

	t.Logf("primary auto GC count: %d", s.primaryGCCount.Load())
	if s.Replicate || s.EnableRemotesAPI {
		t.Logf("standby auto GC count: %d", s.standbyGCCount.Load())
	}
}

// runAutoGCTestDisabled runs a fixed number of insert+commit cycles
// against a server with auto GC disabled, verifying it never fires.
func runAutoGCTestDisabled(t *testing.T, s *AutoGCTest, numStatements int, commitEvery int) {
	ctx := t.Context()
	s.Setup(ctx, t)

	for i := range numStatements {
		stmt := autoGCInsertStatement(i)
		conn, err := s.PrimaryDB.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
		if (i+1)%commitEvery == 0 {
			_, err = conn.ExecContext(ctx, "call dolt_commit('-am', 'insert from "+strconv.Itoa(i*1024)+"')")
			require.NoError(t, err)
		}
		require.NoError(t, conn.Close())
	}

	require.Zero(t, s.primaryGCCount.Load(), "auto GC should not run when disabled")
}

