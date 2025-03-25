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
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestSqlServerMaxConns(t *testing.T) {
	t.Parallel()
	t.Run("MaxConns 3", testMaxConns3)
	t.Run("MaxConns 3 BackLog 0", testMaxConns3BackLog0)
	t.Run("MaxConns 3 BackLog 1", testMaxConns3BackLog1)
	t.Run("MaxConns 3 MaxConnectionsTimeout 10s", testMaxConns3Timeout10s)
}

func setupMaxConnsTest(t *testing.T, ctx context.Context, args ...string) (*sql.DB, []*sql.Conn) {
	t.Parallel()
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("max_conns_test")
	require.NoError(t, err)
	args = append(args, "--max-connections", "3")
	args = append(args, "--port", `{{get_port "server"}}`)
	srvSettings := &driver.Server{
		Args:        args,
		DynamicPort: "server",
	}
	var ports DynamicPorts
	ports.global = &GlobalPorts
	ports.t = t
	server := MakeServer(t, repo, srvSettings, &ports)
	server.DBName = "max_conns_test"
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	db.SetMaxIdleConns(0)
	
	var conns []*sql.Conn
	t.Cleanup(func() {
		closeAll(conns)
	})
	for i := 0; i < 3; i++ {
		conn, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	_, err = conns[0].ExecContext(ctx, `
CREATE TABLE test_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  str VARCHAR(20)
);`)
	require.NoError(t, err)

	return db, conns
}

func closeAll(conns []*sql.Conn) {
	for i, c := range conns {
		if c != nil {
			c.Close()
		}
		conns[i] = nil
	}
}

func testMaxConns3BackLog0(t *testing.T) {
	ctx := context.Background()
	db, _ := setupMaxConnsTest(t, ctx, "--back-log", "0")
	if t.Failed() {
		return
	}
	_, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
	require.ErrorIs(t, err, mysql.ErrInvalidConn)
}

func testMaxConns3Timeout10s(t *testing.T) {
	ctx := context.Background()
	db, _ := setupMaxConnsTest(t, ctx, "--max-connections-timeout", "10s")
	if t.Failed() {
		return
	}
	start := time.Now()
	_, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
	elapsed := time.Since(start)
	require.ErrorIs(t, err, mysql.ErrInvalidConn)
	require.True(t, elapsed > 9 * time.Second, "it took more than 9 seconds to fail")
	require.True(t, elapsed < 12 * time.Second, "it took less than 12 seconds to fail")
}

func testMaxConns3(t *testing.T) {
	ctx := context.Background()
	db, conns := setupMaxConnsTest(t, ctx)
	if t.Failed() {
		return
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		conn, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "insert into test_table (str) values ('test4223')")
		return err
	})
	eg.Go(func() error {
		conn, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "insert into test_table (str) values ('test9119')")
		return err
	})
	conns[0].Close()
	conns[0] = nil
	require.NoError(t, eg.Wait())
	ctx = context.Background()
	rows, err := conns[1].QueryContext(ctx, `SELECT * FROM test_table ORDER BY str ASC`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var id int
	var str string
	require.NoError(t, rows.Scan(&id, &str))
	require.Equal(t, "test4223", str)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &str))
	require.Equal(t, "test9119", str)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func testMaxConns3BackLog1(t *testing.T) {
	ctx := context.Background()
	db, conns := setupMaxConnsTest(t, ctx, "--back-log", "1")
	if t.Failed() {
		return
	}
	eg, ctx := errgroup.WithContext(ctx)
	done := make(chan struct{})
	eg.Go(func() error {
		conn, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
		if err != nil {
			return err
		}
		defer func() {
			// Keep this connection alive until the other function
			// has a chance to try to connect and fail.
			<-done
			conn.Close()
		}()
		_, err = conn.ExecContext(ctx, "insert into test_table (str) values ('test4223')")
		return err
	})
	eg.Go(func() error {
		defer close(done)
		time.Sleep(1 * time.Second)
		_, err := db.Conn(driver.WithConnectRetriesDisabled(ctx))
		if !assert.ErrorIs(t, err, mysql.ErrInvalidConn) {
			return errors.New("unexpected test failure")
		}
		return nil
	})
	<-done
	conns[0].Close()
	conns[0] = nil
	require.NoError(t, eg.Wait())
	ctx = context.Background()
	rows, err := conns[1].QueryContext(ctx, `SELECT * FROM test_table`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var id int
	var str string
	require.NoError(t, rows.Scan(&id, &str))
	require.Equal(t, "test4223", str)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}
