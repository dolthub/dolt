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
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestGCWithOpenSavepoint runs dolt_gc() on one connection while another
// connection has an open transaction with savepoints. The root value of a
// savepoint taken after a write exists only in memory until GC needs to keep
// it, and it must remain usable for ROLLBACK TO SAVEPOINT after the GC.
func TestGCWithOpenSavepoint(t *testing.T) {
	t.Parallel()

	// The savepoint is never used, but GC must keep its root.
	t.Run("CommitAfterGC", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec("INSERT INTO t VALUES (1, 'one')")
		s.exec("SAVEPOINT s")
		s.exec("INSERT INTO t VALUES (2, 'two')")
		s.gc()
		s.exec("COMMIT")
		require.Equal(t, []string{"1:one", "2:two"}, s.rows(s.gcConn))
	})

	t.Run("RollbackAfterGC", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec("INSERT INTO t VALUES (1, 'one')")
		s.exec("SAVEPOINT s")
		s.exec("INSERT INTO t VALUES (2, 'two')")
		s.gc()
		s.exec("ROLLBACK TO SAVEPOINT s")
		require.Equal(t, []string{"1:one"}, s.rows(s.conn))
		s.exec("INSERT INTO t VALUES (3, 'three')")
		s.exec("COMMIT")
		want := []string{"1:one", "3:three"}
		require.Equal(t, want, s.rows(s.gcConn))
		s.restart()
		require.Equal(t, want, s.rows(s.gcConn))
	})

	// A savepoint taken before any write has the transaction's starting
	// root, which is already stored.
	t.Run("RollbackToTransactionStartAfterGC", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec("SAVEPOINT s")
		s.exec("INSERT INTO t VALUES (1, 'one')")
		s.gc()
		s.exec("ROLLBACK TO SAVEPOINT s")
		require.Empty(t, s.rows(s.conn))
		s.exec("INSERT INTO t VALUES (2, 'two')")
		s.exec("COMMIT")
		require.Equal(t, []string{"2:two"}, s.rows(s.gcConn))
	})

	t.Run("RollbackToEachOfTwoSavepoints", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec("INSERT INTO t VALUES (1, 'one')")
		s.exec("SAVEPOINT s1")
		s.exec("INSERT INTO t VALUES (2, 'two')")
		s.exec("SAVEPOINT s2")
		s.exec("INSERT INTO t VALUES (3, 'three')")
		s.gc()
		s.exec("ROLLBACK TO SAVEPOINT s2")
		require.Equal(t, []string{"1:one", "2:two"}, s.rows(s.conn))
		s.exec("ROLLBACK TO SAVEPOINT s1")
		require.Equal(t, []string{"1:one"}, s.rows(s.conn))
		s.exec("COMMIT")
		require.Equal(t, []string{"1:one"}, s.rows(s.gcConn))
	})

	// The first GC stores the savepoint's root, so the second finds it
	// already present and must still keep it.
	t.Run("RollbackAfterTwoGCs", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec("INSERT INTO t VALUES (1, 'one')")
		s.exec("SAVEPOINT s")
		s.exec("INSERT INTO t VALUES (2, 'two')")
		s.gc()
		s.exec("UPDATE t SET b = 'changed'")
		s.gcFull()
		s.exec("ROLLBACK TO SAVEPOINT s")
		require.Equal(t, []string{"1:one"}, s.rows(s.conn))
		s.exec("COMMIT")
		require.Equal(t, []string{"1:one"}, s.rows(s.gcConn))
	})

	// After the DELETE, only the savepoint reaches the multi-level trees of
	// the primary and secondary index.
	t.Run("RollbackToTreesOnlyReachableFromSavepoint", func(t *testing.T) {
		t.Parallel()
		const count = 5000
		s := newGCSavepointServer(t)
		s.exec("START TRANSACTION")
		s.exec(fmt.Sprintf(`INSERT INTO t
			WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
			SELECT i, CONCAT('value-', i) FROM n`, count))
		s.exec("SAVEPOINT s")
		s.exec("DELETE FROM t")
		s.gc()
		s.exec("ROLLBACK TO SAVEPOINT s")
		s.exec("COMMIT")
		for _, from := range []string{"t", "t FORCE INDEX (bidx) WHERE b > ''"} {
			query := "SELECT COUNT(*), CAST(SUM(b = CONCAT('value-', a)) AS SIGNED) FROM " + from
			var gotRows, gotMatching int
			require.NoError(t, s.gcConn.QueryRowContext(s.ctx, query).Scan(&gotRows, &gotMatching))
			require.Equal(t, count, gotRows, query)
			require.Equal(t, count, gotMatching, query)
		}
	})

	// Collecting the savepoint's root must not dirty the store, or a later GC
	// would rewrite it instead of taking the no-op path. The other session's
	// visit can land after the first GC's no-op check, so that takes two GCs.
	t.Run("SavepointOnUnchangedDatabaseIsNoOp", func(t *testing.T) {
		t.Parallel()
		s := newGCSavepointServer(t)
		s.gc()
		before := s.storeFiles()
		s.exec("START TRANSACTION")
		s.exec("SAVEPOINT s")
		s.gc()
		s.gc()
		require.Equal(t, before, s.storeFiles())
		s.exec("ROLLBACK TO SAVEPOINT s")
		s.exec("COMMIT")
	})
}

type gcSavepointServer struct {
	t      *testing.T
	ctx    context.Context
	server *driver.SqlServer
	db     *sql.DB
	dir    string
	// conn holds the transaction and gcConn runs GC and reads committed rows.
	conn   *sql.Conn
	gcConn *sql.Conn
}

func newGCSavepointServer(t *testing.T) *gcSavepointServer {
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	const repoName = "gc_savepoint_test"
	repo, err := rs.MakeRepo(repoName)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}, &ports)
	server.DBName = repoName

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	s := &gcSavepointServer{t: t, ctx: t.Context(), server: server, db: db, dir: repo.Dir}
	s.connect()
	s.exec("CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR(32), KEY bidx (b))")
	s.exec("CALL dolt_commit('-Am', 'base')")
	return s
}

func (s *gcSavepointServer) connect() {
	var err error
	s.conn, err = s.db.Conn(s.ctx)
	require.NoError(s.t, err)
	s.t.Cleanup(func() { s.conn.Close() })
	s.gcConn, err = s.db.Conn(s.ctx)
	require.NoError(s.t, err)
	s.t.Cleanup(func() { s.gcConn.Close() })
}

func (s *gcSavepointServer) exec(query string) {
	_, err := s.conn.ExecContext(s.ctx, query)
	require.NoError(s.t, err, query)
}

func (s *gcSavepointServer) gc() {
	s.gcExec("CALL dolt_gc()")
}

func (s *gcSavepointServer) gcFull() {
	s.gcExec("CALL dolt_gc('--full')")
}

func (s *gcSavepointServer) gcExec(query string) {
	_, err := s.gcConn.ExecContext(s.ctx, query)
	require.NoError(s.t, err, query)
}

// rows returns the rows of t, as seen by |conn|, formatted as "a:b".
func (s *gcSavepointServer) rows(conn *sql.Conn) []string {
	rows, err := conn.QueryContext(s.ctx, "SELECT a, b FROM t ORDER BY a")
	require.NoError(s.t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var a int
		var b string
		require.NoError(s.t, rows.Scan(&a, &b))
		ret = append(ret, fmt.Sprintf("%d:%s", a, b))
	}
	require.NoError(s.t, rows.Err())
	return ret
}

func (s *gcSavepointServer) restart() {
	require.NoError(s.t, s.conn.Close())
	require.NoError(s.t, s.gcConn.Close())
	require.NoError(s.t, s.server.Restart(nil, nil))
	require.Eventually(s.t, func() bool {
		return s.db.PingContext(s.ctx) == nil
	}, 5*time.Second, 50*time.Millisecond)
	s.connect()
}

// storeFiles describes every file in the store, by path, size and mod time.
func (s *gcSavepointServer) storeFiles() map[string]string {
	noms := filepath.Join(s.dir, ".dolt", "noms")
	ret := make(map[string]string)
	err := filepath.WalkDir(noms, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		ret[path] = fmt.Sprintf("%d %s", fi.Size(), fi.ModTime())
		return nil
	})
	require.NoError(s.t, err)
	return ret
}
