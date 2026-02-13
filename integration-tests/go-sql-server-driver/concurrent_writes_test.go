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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestConcurrentWrites verifies concurrent write behavior and transaction locking in the SQL server driver.
func TestConcurrentWrites(t *testing.T) {
	t.Parallel()
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

	repo, err := rs.MakeRepo("concurrent_writes_test")
	require.NoError(t, err)

	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
	}
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "concurrent_writes_test"

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	db.SetMaxIdleConns(0)
	defer func() {
		require.NoError(t, db.Close())
	}()
	ctx := t.Context()
	func() {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		// Create table and initial data.
		_, err = conn.ExecContext(ctx, "CREATE TABLE data (id VARCHAR(64) PRIMARY KEY, worker INT, data TEXT, created_at TIMESTAMP)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'init with table')")
		require.NoError(t, err)
	}()

	eg, ctx := errgroup.WithContext(ctx)
	start := time.Now()

	nextInt := uint32(0)
	const numWriters = 32
	const testDuration = 8 * time.Second
	startCh := make(chan struct{})
	for i := range numWriters {
		eg.Go(func() error {
			select {
			case <-startCh:
			case <-ctx.Done():
				return nil
			}
			db, err := server.DB(driver.Connection{User: "root"})
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
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
					return nil
				}
				key := fmt.Sprintf("main-%d-%d", i, j)
				_, err := conn.ExecContext(ctx, "INSERT INTO data VALUES (?,?,?,?)", key, i, key, time.Now())
				if err != nil {
					return err
				}
				atomic.AddUint32(&nextInt, 1)
				_, err = conn.ExecContext(ctx, fmt.Sprintf("CALL DOLT_COMMIT('-Am', 'insert %s')", key))
				if err != nil {
					return err
				}
				j += 1
			}
		})
	}
	time.Sleep(500 * time.Millisecond)
	close(startCh)
	require.NoError(t, eg.Wait())
	t.Logf("wrote %d", nextInt)
	ctx = t.Context()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func () {
		require.NoError(t, conn.Close())
	}()
	var i int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&i)
	require.NoError(t, err)
	t.Logf("read %d", i)
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_log").Scan(&i)
	require.NoError(t, err)
	t.Logf("created %d commits", i)
}
