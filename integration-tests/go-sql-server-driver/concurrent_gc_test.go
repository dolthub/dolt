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
	t.Run("NoCommits", func(t *testing.T) {
		t.Run("Normal", func(t *testing.T) {
			var gct = gcTest{
				numThreads: 8,
				duration:   10 * time.Second,
			}
			gct.run(t)
		})
		t.Run("Full", func(t *testing.T) {
			var gct = gcTest{
				numThreads: 8,
				duration:   10 * time.Second,
				full:       true,
			}
			gct.run(t)
		})
	})
	t.Run("WithCommits", func(t *testing.T) {
		t.Run("Normal", func(t *testing.T) {
			var gct = gcTest{
				numThreads: 8,
				duration:   10 * time.Second,
				commit:     true,
			}
			gct.run(t)
		})
		t.Run("Full", func(t *testing.T) {
			var gct = gcTest{
				numThreads: 8,
				duration:   10 * time.Second,
				commit:     true,
				full:       true,
			}
			gct.run(t)
		})
	})
}

type gcTest struct {
	numThreads int
	duration   time.Duration
	commit     bool
	full       bool
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

func (gct gcTest) doUpdate(t *testing.T, ctx context.Context, db *sql.DB, i int) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Logf("err in Conn: %v", err)
		return nil
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "update vals set val = val+1 where id = ?", i)
	if err != nil {
		if !assert.NotContains(t, err.Error(), "dangling ref") {
			return err
		}
		if !assert.NotContains(t, err.Error(), "is unexpected noms value") {
			return err
		}
		if !assert.NotContains(t, err.Error(), "interface conversion: types.Value is nil") {
			return err
		}
		t.Logf("err in Exec update: %v", err)
	}
	if gct.commit {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("call dolt_commit('-am', 'increment vals id = %d')", i))
		if err != nil {
			if !assert.NotContains(t, err.Error(), "dangling ref") {
				return err
			}
			if !assert.NotContains(t, err.Error(), "is unexpected noms value") {
				return err
			}
			if !assert.NotContains(t, err.Error(), "interface conversion: types.Value is nil") {
				return err
			}
			t.Logf("err in Exec call dolt_commit: %v", err)
		}
	}
	return nil
}

func (gct gcTest) doGC(t *testing.T, ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Logf("err in Conn for dolt_gc: %v", err)
		return nil
	}
	defer func() {
		// After calling dolt_gc, the connection is bad. Remove it from the connection pool.
		conn.Raw(func(_ any) error {
			return sqldriver.ErrBadConn
		})
	}()
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
	u, err := driver.NewDoltUser(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("concurrent_gc_test")
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{})
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
