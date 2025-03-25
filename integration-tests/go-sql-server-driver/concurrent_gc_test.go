// Copyright 2023 Dolthub, Inc.
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
	sqldriver "database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestConcurrentGC(t *testing.T) {
	t.Parallel()
	type dimension struct {
		names   []string
		factors func(gcTest) []gcTest
	}
	commits := dimension{
		names: []string{"NoCommits", "WithCommits"},
		factors: func(base gcTest) []gcTest {
			no, yes := base, base
			no.commit = false
			yes.commit = true
			return []gcTest{no, yes}
		},
	}
	full := dimension{
		names: []string{"NotFull", "Full"},
		factors: func(base gcTest) []gcTest {
			no, yes := base, base
			no.full = false
			yes.full = true
			return []gcTest{no, yes}
		},
	}
	safepoint := dimension{
		names: []string{"KillConnections", "SessionAware"},
		factors: func(base gcTest) []gcTest {
			no, yes := base, base
			no.sessionAware = false
			yes.sessionAware = true
			return []gcTest{no, yes}
		},
	}
	var doDimensions func(t *testing.T, base gcTest, dims []dimension)
	doDimensions = func(t *testing.T, base gcTest, dims []dimension) {
		if len(dims) == 0 {
			base.run(t)
			return
		}
		dim, dims := dims[0], dims[1:]
		dimf := dim.factors(base)
		for i := range dim.names {
			t.Run(dim.names[i], func(t *testing.T) {
				t.Parallel()
				doDimensions(t, dimf[i], dims)
			})
		}
	}
	dimensions := []dimension{commits, full, safepoint}
	doDimensions(t, gcTest{
		numThreads: 8,
		duration:   10 * time.Second,
	}, dimensions)
}

type gcTest struct {
	numThreads   int
	duration     time.Duration
	commit       bool
	full         bool
	sessionAware bool
}

func (gct gcTest) createDB(t *testing.T, ctx context.Context, db *sql.DB) {
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// We're going to bootstrap the database with a table which has id, val, id == [0,7*1024], val == 0.
	_, err = conn.ExecContext(ctx, "create table vals (id int primary key, val int)")
	require.NoError(t, err)
	vals := []string{}
	for i := 0; i <= (gct.numThreads-1)*1024; i++ {
		vals = append(vals, fmt.Sprintf("(%d,0)", i))
	}
	_, err = conn.ExecContext(ctx, "insert into vals values "+strings.Join(vals, ","))
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
	require.NoError(t, err)
}

// When running with kill_connections GC safepoints, asserts that the
// error we got is not an error that was not allowed.
func assertExpectedGCError(t *testing.T, err error) bool {
	if !assert.NotContains(t, err.Error(), "dangling ref") {
		return false
	}
	if !assert.NotContains(t, err.Error(), "is unexpected noms value") {
		return false
	}
	if !assert.NotContains(t, err.Error(), "interface conversion: types.Value is nil") {
		return false
	}
	return true
}

func (gct gcTest) doUpdate(t *testing.T, ctx context.Context, db *sql.DB, i int) error {
	conn, err := db.Conn(ctx)
	if gct.sessionAware {
		if !assert.NoError(t, err) {
			return nil
		}
	} else if err != nil {
		require.NotContains(t, err.Error(), "connection refused")
		t.Logf("err in Conn: %v", err)
		return nil
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if gct.sessionAware {
		assert.NoError(t, err)
	}
	if err != nil {
		// Ignore and try with a different connection...
		return nil
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "update vals set val = val+1 where id = ?", i)
	if gct.sessionAware {
		assert.NoError(t, err)
	} else if err != nil {
		if !assertExpectedGCError(t, err) {
			return err
		}
		t.Logf("err in Exec update: %v", err)
	}
	if err != nil {
		// Early return so we do not try to continue using the
		// broken connection or attempt to commit something
		// with no changes.
		return nil
	}
	if gct.commit {
		_, err = tx.ExecContext(ctx, fmt.Sprintf("call dolt_commit('-am', 'increment vals id = %d')", i))
		if gct.sessionAware {
			assert.NoError(t, err)
		} else if err != nil {
			if !assertExpectedGCError(t, err) {
				return err
			}
			t.Logf("err in Exec call dolt_commit: %v", err)
		}
	} else {
		err = tx.Commit()
		if gct.sessionAware {
			assert.NoError(t, err)
		} else if err != nil {
			if !assertExpectedGCError(t, err) {
				return err
			}
			t.Logf("err in tx commit: %v", err)
		}
	}
	return nil
}

func (gct gcTest) doGC(t *testing.T, ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if gct.sessionAware {
		if !assert.NoError(t, err) {
			return nil
		}
	} else if err != nil {
		require.NotContains(t, err.Error(), "connection refused")
		t.Logf("err in Conn for dolt_gc: %v", err)
		return nil
	}
	if !gct.sessionAware {
		defer func() {
			// After calling dolt_gc, the connection is bad. Remove it from the connection pool.
			conn.Raw(func(_ any) error {
				return sqldriver.ErrBadConn
			})
		}()
	} else {
		defer conn.Close()
	}
	b := time.Now()
	if !gct.full {
		_, err = conn.ExecContext(ctx, "call dolt_gc()")
	} else {
		_, err = conn.ExecContext(ctx, `call dolt_gc("--full")`)
	}
	if assert.NoError(t, err) {
		t.Logf("successful dolt_gc took %v", time.Since(b))
	}
	return nil
}

func (gct gcTest) finalize(t *testing.T, ctx context.Context, db *sql.DB) {
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	var ids []any
	var qmarks []string
	for i := 0; i < gct.numThreads*1024; i += 1024 {
		ids = append(ids, i)
		qmarks = append(qmarks, "?")
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("select val from vals where id in (%s)", strings.Join(qmarks, ",")), ids...)
	require.NoError(t, err)
	i := 0
	cnt := 0
	for rows.Next() {
		var val int
		i += 1
		require.NoError(t, rows.Scan(&val))
		cnt += val
	}
	require.Equal(t, len(ids), i)
	t.Logf("successfully updated val %d times", cnt)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())

	rows, err = conn.QueryContext(ctx, "select count(*) from dolt_log")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&cnt))
	t.Logf("database has %d commit(s)", cnt)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	require.NoError(t, conn.Close())
}

func (gct gcTest) run(t *testing.T) {
	var ports DynamicPorts
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("concurrent_gc_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
	}
	if gct.sessionAware {
		srvSettings.Envs = append(srvSettings.Envs, "DOLT_GC_SAFEPOINT_CONTROLLER_CHOICE=session_aware")
	} else {
		srvSettings.Envs = append(srvSettings.Envs, "DOLT_GC_SAFEPOINT_CONTROLLER_CHOICE=kill_connections")
	}

	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "concurrent_gc_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	gct.createDB(t, context.Background(), db)

	start := time.Now()

	eg, egCtx := errgroup.WithContext(context.Background())

	// We're going to spawn 8 threads, each running mutations on their own part of the table...
	for i := 0; i < gct.numThreads; i++ {
		i := i * 1024
		eg.Go(func() error {
			for j := 0; time.Since(start) < gct.duration && egCtx.Err() == nil; j++ {
				if err := gct.doUpdate(t, egCtx, db, i); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// We spawn a thread which calls dolt_gc() periodically
	eg.Go(func() error {
		for time.Since(start) < gct.duration && egCtx.Err() == nil {
			if err := gct.doGC(t, egCtx, db); err != nil {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	require.NoError(t, eg.Wait())

	// Recreate the connection pool here, since idle connections in the
	// connection pool may be stale.
	db.Close()
	db, err = server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)

	gct.finalize(t, context.Background(), db)
}
