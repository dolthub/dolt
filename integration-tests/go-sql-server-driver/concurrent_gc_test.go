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
	"testing"
	"strings"
	"time"
	"fmt"
	sqldriver "database/sql/driver"

	"golang.org/x/sync/errgroup"
        "github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestConcurrentGC(t *testing.T) {
	NumThreads := 8
	Duration := 30 * time.Second

        u, err := driver.NewDoltUser()
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

	func() {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		// We're going to bootstrap the database with a table which has id, val, id == [0,7*1024], val == 0.
		_, err = conn.ExecContext(context.Background(), "create table vals (id int primary key, val int)")
		require.NoError(t, err)
		vals := []string{}
		for i := 0; i <= 7 * 1024; i++ {
			vals = append(vals, fmt.Sprintf("(%d,0)", i))
		}
		_, err = conn.ExecContext(context.Background(), "insert into vals values " + strings.Join(vals, ","))
		require.NoError(t, err)
	}()

	start := time.Now()

	var eg errgroup.Group

	// We're going to spawn 8 threads, each running mutations on their own part of the table...
	for i := 0; i < NumThreads; i++ {
		i := i * 1024
		eg.Go(func() error {
			for j := 0; time.Since(start) < Duration; j++ {
				func() {
					conn, err := db.Conn(context.Background())
					if err != nil {
						t.Logf("err in Conn: %v", err)
						return
					}
					defer conn.Close()
					_, err = conn.ExecContext(context.Background(), "update vals set val = val+1 where id = ?", i)
					if err != nil {
						t.Logf("err in Exec: %v", err)
					}
				}()
			}
			return nil
		})
	}

	// We spawn a thread which calls dolt_gc() periodically
	eg.Go(func() error {
		for time.Since(start) < Duration {
			func() {
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Logf("err in Conn for dolt_gc: %v", err)
					return
				}
				b := time.Now()
				_, err = conn.ExecContext(context.Background(), "call dolt_gc()")
				if err != nil {
					t.Logf("err in Exec dolt_gc: %v", err)
				} else {
					t.Logf("successful dolt_gc took %v", time.Since(b))
				}
				// After calling dolt_gc, the connection is bad. Remove it from the connection pool.
				conn.Raw(func(_ any) error {
					return sqldriver.ErrBadConn
				})
			}()
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	eg.Wait()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	rows, err := conn.QueryContext(context.Background(), "select val from vals where id in (0, 1024, 2048, 3072, 4096, 5120, 6144, 7168)")
	i := 0
	cnt := 0
	for rows.Next() {
		var val int
		i += 1
		require.NoError(t, rows.Scan(&val))
		cnt += val
	}
	require.Equal(t, 8, i)
	t.Logf("successfully updated val %d times", cnt)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	require.NoError(t, conn.Close())
}
