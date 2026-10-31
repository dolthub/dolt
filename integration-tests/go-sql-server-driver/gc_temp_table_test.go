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

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// gcTempTableRows is enough rows for |tt| to have a multi-level tree. Its root
// lives in memory, so only a read which descends the tree hits the store.
const gcTempTableRows = 5000

// TestGCWithTemporaryTable runs dolt_gc() while a session holds a temporary
// table. No branch, working set or transaction reaches a temporary table, so
// its chunks stay in the store only if the session reports them as GC roots.
func TestGCWithTemporaryTable(t *testing.T) {
	t.Parallel()

	for name, args := range map[string][]string{"Default": nil, "Full": {"--full"}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newGCTempTableServer(t)
			s.createTempTable()
			s.gc(args...)
			s.requireTempRows(gcTempTableRows, gcTempTableRows)
			s.exec("UPDATE tt SET v = 'changed' WHERE id = 1")
			s.requireTempRows(gcTempTableRows, gcTempTableRows-1)
		})
	}

	t.Run("BothGCModes", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.gc()
		s.gc("--full")
		s.requireTempRows(gcTempTableRows, gcTempTableRows)
	})

	t.Run("WriteAfterGCThenSecondGC", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.gc()
		s.exec(gcTempTableInsert(gcTempTableRows, 2*gcTempTableRows))
		s.requireTempRows(2*gcTempTableRows, 2*gcTempTableRows)
		s.gc()
		s.requireTempRows(2*gcTempTableRows, 2*gcTempTableRows)
		s.exec("DELETE FROM tt WHERE id >= 100")
		s.requireTempRows(100, 100)
		s.gc("--full")
		s.requireTempRows(100, 100)
		// The chunks which GC kept can't have left the store dangling.
		s.exec("INSERT INTO t VALUES (1)")
		s.exec("CALL dolt_commit('-Am', 'after gc')")
	})

	// GC visits the session which called it separately from all the others.
	t.Run("GCOnOwningConnection", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.gcConn = s.conn
		s.gc()
		s.requireTempRows(gcTempTableRows, gcTempTableRows)
	})

	// The index is built by DDL, which leaves it in the table but out of the
	// root of the table's write session.
	t.Run("IndexAddedToPopulatedTable", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.exec("ALTER TABLE tt ADD INDEX vidx (v)")
		s.gc()
		s.requireTempRows(gcTempTableRows, gcTempTableRows)
		s.requireInt(gcTempTableRows, "SELECT COUNT(DISTINCT id) FROM tt FORCE INDEX (vidx) WHERE v > ''")
		s.exec("UPDATE tt SET v = 'changed' WHERE id = 1")
		s.requireTempRows(gcTempTableRows, gcTempTableRows-1)
	})

	// The tree is only a root held in memory, so the chunks of the value are all
	// that a read needs from the store.
	t.Run("OutOfLineValues", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.exec("CREATE TEMPORARY TABLE lt (id INT PRIMARY KEY, b LONGTEXT)")
		s.exec("INSERT INTO lt VALUES (1, REPEAT(MD5(1), 4000))")
		s.gc()
		s.requireInt(32*4000, "SELECT LENGTH(b) FROM lt WHERE id = 1")
	})

	t.Run("OpenTransactionWithSavepoint", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.exec("START TRANSACTION")
		s.exec("SAVEPOINT s")
		s.exec("UPDATE tt SET v = 'changed' WHERE id = 1")
		s.gc()
		s.requireTempRows(gcTempTableRows, gcTempTableRows-1)
		// Temporary tables are not part of a savepoint.
		s.exec("ROLLBACK TO SAVEPOINT s")
		s.requireTempRows(gcTempTableRows, gcTempTableRows-1)
		s.exec("COMMIT")
		s.requireTempRows(gcTempTableRows, gcTempTableRows-1)
	})

	t.Run("RevisionDatabase", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.exec("CALL dolt_branch('other')")
		s.exec(fmt.Sprintf("USE `%s/other`", gcTempTableRepo))
		s.createTempTable()
		s.gc()
		s.requireTempRows(gcTempTableRows, gcTempTableRows)
	})

	// A temporary table outlives DROP DATABASE and holds on to the closed store.
	// It must not stop GC of a new database with the same name.
	for name, dropConn := range map[string]func(*gcTempTableServer) *sql.Conn{
		"OtherConnectionRecreatesDatabase":  func(s *gcTempTableServer) *sql.Conn { return s.gcConn },
		"OwningConnectionRecreatesDatabase": func(s *gcTempTableServer) *sql.Conn { return s.conn },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newGCTempTableServer(t)
			s.exec("CREATE DATABASE d1")
			s.exec("USE d1")
			s.createTempTable()
			for _, query := range []string{
				"DROP DATABASE d1",
				"CREATE DATABASE d1",
				"USE d1",
				"CREATE TABLE t (a INT PRIMARY KEY)",
				"INSERT INTO t VALUES (1)",
				"CALL dolt_commit('-Am', 'recreated')",
			} {
				_, err := dropConn(s).ExecContext(s.ctx, query)
				require.NoError(t, err, query)
			}
			_, err := s.gcConn.ExecContext(s.ctx, "USE d1")
			require.NoError(t, err)
			s.gc()
			s.gc("--full")
			s.gcConn = s.conn
			s.gc()
			s.requireInt(1, "SELECT COUNT(*) FROM d1.t")
		})
	}

	// With enough tables, the table map in the root of the temporary table's
	// write session has a chunk which only that root references. The next write
	// to the temporary table edits that map.
	t.Run("MultiLevelTableMap", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		for i := range 300 {
			s.exec(fmt.Sprintf("CREATE TABLE a_table_with_quite_a_long_name_%d (a INT PRIMARY KEY)", i))
		}
		s.exec("CALL dolt_commit('-Am', 'tables')")
		s.exec("CREATE TEMPORARY TABLE tt (id INT PRIMARY KEY, v VARCHAR(32))")
		s.exec("INSERT INTO tt VALUES (1, MD5(1))")
		s.gc()
		s.exec("INSERT INTO tt VALUES (2, MD5(2))")
		s.requireTempRows(2, 2)
	})

	// Visiting the table must not dirty the store, or every GC would rewrite it
	// instead of taking the no-op path. The other session's visit can land
	// after the first GC's no-op check, so that takes two GCs.
	t.Run("UnchangedDatabaseIsNoOp", func(t *testing.T) {
		t.Parallel()
		s := newGCTempTableServer(t)
		s.createTempTable()
		s.gc()
		s.gc()
		before := s.storeFiles()
		s.gc()
		s.gc()
		require.Equal(t, before, s.storeFiles())
		s.requireTempRows(gcTempTableRows, gcTempTableRows)
	})
}

const gcTempTableRepo = "gc_temp_table_test"

type gcTempTableServer struct {
	t   *testing.T
	ctx context.Context
	dir string
	// conn holds the temporary table and gcConn runs GC.
	conn   *sql.Conn
	gcConn *sql.Conn
}

func newGCTempTableServer(t *testing.T) *gcTempTableServer {
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() { u.Cleanup() })

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	repo, err := rs.MakeRepo(gcTempTableRepo)
	require.NoError(t, err)

	server := MakeServer(t, repo, &driver.Server{
		Args:          []string{"--port", `{{get_port "server"}}`},
		DynamicPort:   "server",
		LogNotMatches: []string{"caught panic"},
	}, &ports)
	server.DBName = gcTempTableRepo

	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	s := &gcTempTableServer{t: t, ctx: t.Context(), dir: repo.Dir}
	s.conn, err = db.Conn(s.ctx)
	require.NoError(t, err)
	t.Cleanup(func() { s.conn.Close() })
	s.gcConn, err = db.Conn(s.ctx)
	require.NoError(t, err)
	t.Cleanup(func() { s.gcConn.Close() })

	s.exec("CREATE TABLE t (a INT PRIMARY KEY)")
	s.exec("CALL dolt_commit('-Am', 'base')")
	return s
}

// gcTempTableInsert returns a statement filling |tt| with the ids in [start, end).
func gcTempTableInsert(start, end int) string {
	return fmt.Sprintf(`INSERT INTO tt
		WITH RECURSIVE n(i) AS (SELECT %d UNION ALL SELECT i + 1 FROM n WHERE i < %d)
		SELECT i, MD5(i) FROM n`, start, end-1)
}

func (s *gcTempTableServer) createTempTable() {
	s.exec("CREATE TEMPORARY TABLE tt (id INT PRIMARY KEY, v VARCHAR(32))")
	s.exec(gcTempTableInsert(0, gcTempTableRows))
}

func (s *gcTempTableServer) exec(query string) {
	_, err := s.conn.ExecContext(s.ctx, query)
	require.NoError(s.t, err, query)
}

func (s *gcTempTableServer) gc(args ...string) {
	query := "CALL dolt_gc()"
	if len(args) > 0 {
		query = fmt.Sprintf("CALL dolt_gc('%s')", args[0])
	}
	_, err := s.gcConn.ExecContext(s.ctx, query)
	require.NoError(s.t, err)
}

func (s *gcTempTableServer) requireInt(want int, query string) {
	var got int
	require.NoError(s.t, s.conn.QueryRowContext(s.ctx, query).Scan(&got), query)
	require.Equal(s.t, want, got, query)
}

// requireTempRows reads every row of |tt|, and checks how many there are and
// how many still have their original value. An index lookup on a temporary
// table currently repeats rows for each partition, so this counts distinct ids.
func (s *gcTempTableServer) requireTempRows(wantRows, wantOriginal int) {
	s.requireInt(wantRows, "SELECT COUNT(DISTINCT id) FROM tt WHERE v IS NOT NULL")
	s.requireInt(wantOriginal, "SELECT COUNT(DISTINCT id) FROM tt WHERE v = MD5(id)")
}

// storeFiles describes every file in the store, by path, size and mod time.
func (s *gcTempTableServer) storeFiles() map[string]string {
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
