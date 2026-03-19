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
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestconcurrentDropDatabase is a regression test for #10692.
//
// A lock-ordering bug between *DatabaseProvider and *DoltSession meant that
// DROP DATABASE during concurrency could cause the DatabaseProvider to
// deadlock and the server databases to become unavailable.
func TestConcurrentDropDatabase(t *testing.T) {
	// Not parallel. Senseitive to timing.
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo("concurrent_drop_database_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
	}
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "concurrent_drop_database_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	db.SetMaxIdleConns(0)
	defer func() {
		require.NoError(t, db.Close())
	}()
	ctx := t.Context()

	eg, ctx := errgroup.WithContext(ctx)
	start := time.Now()

	var numcreates int32 = 0
	const numWriters = 8
	const testDuration = 8 * time.Second
	for i := range numWriters {
		eg.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, testDuration*4)
			defer cancel()
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			j := 0
			for {
				if time.Since(start) > testDuration {
					return nil
				}
				if ctx.Err() != nil {
					return context.Cause(ctx)
				}
				database := fmt.Sprintf("db%08d%08d", i, j)
				_, err := conn.ExecContext(ctx, "CREATE DATABASE "+database)
				if err != nil {
					return err
				}
				atomic.AddInt32(&numcreates, 1)
				_, err = conn.ExecContext(ctx, "DROP DATABASE "+database)
				if err != nil {
					return err
				}
			}
		})
	}
	require.NoError(t, eg.Wait())
	ctx, cancel := context.WithTimeout(t.Context(), 2 * time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "SHOW DATABASES")
	require.NoError(t, err)
	defer rows.Close()
	var databases []string
	for rows.Next() {
		var db string
		err = rows.Scan(&db)
		require.NoError(t, err)
		databases = append(databases, db)
	}
	require.NoError(t, rows.Err())
	sort.Strings(databases)
	require.Equal(t, []string{"concurrent_drop_database_test", "information_schema", "mysql"}, databases)
	t.Logf("created %d databases", numcreates)
}
