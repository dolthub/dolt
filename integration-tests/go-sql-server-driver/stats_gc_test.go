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
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestStatsGCConcurrency(t *testing.T) {
	t.Parallel()
	// At one time, it was possible for a GC of stats to block the
	// analyzer from accessing stats for a long time. This test sets
	// up the following condition:
	//
	// * 100 branches, each with 10 tables, each with 10 indexes.
	//   All of the branches and tables and indexes are non-empty.
	//
	// * One thread calls `call dolt_stats_gc()`
	//
	// * Other threads continue to connect to the database and run
	//   qureies.
	//
	// * We expect all queries to complete without much delay. If
	//   there is a long delay on the read queries, this fails the
	//   test.
	ctx := context.Background()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("stats_gc_concurrency_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "stats_gc_concurrency_test"

	// Connect to the database
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)

	numbranches := 3

	var branches = []string{"main"}
	for i := 0; i < numbranches; i++ {
		branches = append(branches, uuid.New().String())
	}

	var tables = []string{
		"t0", "t1", "t2", "t3", "t4",
		"t5", "t6", "t7", "t8", "t9",
	}
	var columns = []string{
		"c0", "c1", "c2", "c3", "c4",
		"c5", "c6", "c7", "c8", "c9",
	}
	var columndefs = []string{}
	for _, col := range columns {
		columndefs = append(columndefs, fmt.Sprintf("`%s` bigint unique", col))
	}

	for _, table := range tables {
		_, err = db.ExecContext(ctx, fmt.Sprintf(strings.Join([]string{
			"CREATE TABLE `%s` (",
			"id BIGINT AUTO_INCREMENT PRIMARY KEY,",
			strings.Join(columndefs, ",\n"),
			");",
		}, "\n"), table))
		require.NoError(t, err)
	}

	_, err = db.ExecContext(ctx, "call dolt_commit('-Am', 'initial commit')")
	require.NoError(t, err)

	for _, branch := range branches[1:] {
		_, err = db.ExecContext(ctx, fmt.Sprintf("call dolt_checkout('-b', '%s')", branch))
		require.NoError(t, err)
		for _, table := range tables {
			_, err = db.ExecContext(ctx, fmt.Sprintf("insert into `%s` values %s", table, t0_values_str()))
			require.NoError(t, err)
		}
	}
	
	_, err = db.ExecContext(ctx, "call dolt_checkout('main')")
	require.NoError(t, err)
	for _, table := range tables {
		_, err = db.ExecContext(ctx, fmt.Sprintf("insert into `%s` values %s", table, t0_values_str()))
		require.NoError(t, err)
	}

	numThreads := 10
	// The workers will run until this is closed. This will be closed after the stats_gc() is finished.
	stop := make(chan struct{})
	maxQuery := make([]time.Duration, numThreads)
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < numThreads; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-stop:
					return nil
				default:
				}

				err := func() (err error) {
					start := time.Now()
					defer func() {
						elapsed := time.Since(start)
						if elapsed > maxQuery[i] {
							maxQuery[i] = elapsed
						}
					}()
					conn, err := db.Conn(egCtx)
					if err != nil {
						return err
					}
					defer func() {
						err = errors.Join(err, conn.Close())
					}()
					branch := branches[rand.IntN(len(branches))]
					_, err = conn.ExecContext(egCtx, fmt.Sprintf("call dolt_checkout('%s')", branch))
					if err != nil {
						return err
					}
					t1 := tables[rand.IntN(len(tables))]
					t2 := tables[rand.IntN(len(tables))]
					for t2 == t1 {
						t2 = tables[rand.IntN(len(tables))]
					}
					c1 := columns[rand.IntN(len(columns))]
					c2 := columns[rand.IntN(len(columns))]
					rows, err := conn.QueryContext(egCtx, fmt.Sprintf("select * from %s join %s on %s.%s = %s.%s",
						t1, t2, t1, c1, t2, c2))
					if err != nil {
						return err
					}
					defer func() {
						err = errors.Join(err, rows.Close())
					}()
					for rows.Next() {
					}
					return nil
				}()
				if err != nil {
					return err
				}
			}
		})
	}

	var gcDuration time.Duration
	eg.Go(func() (err error) {
		defer close(stop)
		start := time.Now()
		defer func() {
			gcDuration = time.Since(start)
		}()
		conn, err := db.Conn(egCtx)
		if err != nil {
			return err
		}
		defer func() {
			err = errors.Join(err, conn.Close())
		}()
		_, err = conn.ExecContext(egCtx, "call dolt_stats_gc()")
		if err != nil {
			return err
		}
		return nil
	})

	require.NoError(t, eg.Wait())

	for _, d := range maxQuery {
		assert.Greater(t, 3 * time.Second, d)
		assert.Greater(t, gcDuration, d)
	}
}

func TestStatsAnalyzeTableSpeed(t *testing.T) {
	t.Parallel()
	// At one time, calling Analyze Table would go through the
	// rate-limiting system that background stats refresh goes
	// through. This would make ANALYZE TABLE take a longer time
	// than necessary, and it would be very dependent on the stats
	// timer that were in effect.
	//
	// This tests that even with very large stats timers, ANALYZE
	// TABLE behaves reasonably.
	ctx := context.Background()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("stats_analyze_table_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "stats_analyze_table_test"

	// Connect to the database
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	// Task rate-limiting of one per minute will make things more
	// than slow enough that ANALYZE TABLE down below will fail on
	// the context timeout if these rates are effecting the
	// ANALYZE TABLE call.
	_, err = db.ExecContext(ctx, "call dolt_stats_timers(60000000000, 60000000000)")
	require.NoError(t, err)

	var columns = []string{
		"c0", "c1", "c2", "c3", "c4",
		"c5", "c6", "c7", "c8", "c9",
	}
	var columndefs = []string{}
	for _, col := range columns {
		columndefs = append(columndefs, fmt.Sprintf("`%s` bigint unique", col))
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(strings.Join([]string{
		"CREATE TABLE `%s` (",
		"id BIGINT AUTO_INCREMENT PRIMARY KEY,",
		strings.Join(columndefs, ",\n"),
		");",
	}, "\n"), "t0"))
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "call dolt_commit('-Am', 'initial commit')")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "call dolt_checkout('main')")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("insert into `%s` values %s", "t0", t0_values_strs(65536)))
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second * 30)
	defer cancel()
	_, err = db.ExecContext(timeoutCtx, "ANALYZE TABLE t0")
	require.NoError(t, err)
}

func t0_values_str() string {
	return fmt.Sprintf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)",
		rand.IntN(1<<48), rand.IntN(1<<48), rand.IntN(1<<48), rand.IntN(1<<48),
		rand.IntN(1<<48), rand.IntN(1<<48), rand.IntN(1<<48), rand.IntN(1<<48),
		rand.IntN(1<<48), rand.IntN(1<<48), rand.IntN(1<<48))
}

func t0_values_strs(n int) string {
	vals := make([]string, n)
	for i := range vals {
		vals[i] = t0_values_str()
	}
	return strings.Join(vals, ",")
}
