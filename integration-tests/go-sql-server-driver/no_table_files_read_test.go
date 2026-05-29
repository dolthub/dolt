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
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// These tests assert the behavior of an optimization where dolt CLI commands
// avoid loading potentially-large local database files when the command can
// instead be serviced by a running `dolt sql-server`.
//
// The hook for asserting that no table files are loaded is the
// DOLT_TEST_ASSERT_NO_TABLE_FILES_READ environment variable. When it is set,
// dolt panics if it ever bootstraps the chunk journal or opens a table file
// from disk. We use it to prove that, when a server is running, the CLI never
// touches the local database files.
//
// Every scenario uses a data dir root which contains multiple databases
// (db_one and db_two) as subdirectories, and exercises CLI invocations both
// from the data dir root and from within a particular database subdirectory.
// At the multi-db root there is no "current database", so the commands which
// require one (`dolt log`, `dolt remote -v`) are only exercised from the
// subdirectory; from the root we use `dolt sql -q` with database-qualified
// names.
//
// The test matrix has three top-level scenarios:
//
//  1. A dolt sql-server is running in the data dir root. The CLI is invoked
//     with DOLT_TEST_ASSERT_NO_TABLE_FILES_READ=1 and must succeed, because it
//     dispatches to the server without reading any table files.
//  2. No server is running, but a stale sql-server.info file is present at the
//     data dir root. The CLI is invoked (executing locally) and must succeed.
//  3. No server is running, but a concurrent `dolt sql` process holds the
//     write lock on db_one. The CLI is invoked (executing locally); reads
//     succeed, writes to db_one fail because they cannot acquire the write
//     lock, and writes to the unlocked db_two succeed.
//
// In scenarios 2 and 3 the CLI must read the local database, so we do not set
// DOLT_TEST_ASSERT_NO_TABLE_FILES_READ for those invocations.

// makePopulatedRepoStore creates a fresh repo store (the data dir root) with
// two initialized databases below it, each with a committed table, some rows,
// and a remote configured.
func makePopulatedRepoStore(t *testing.T, u driver.DoltUser) (driver.RepoStore, driver.Repo, driver.Repo) {
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	dbOne := makePopulatedDB(t, rs, "db_one")
	dbTwo := makePopulatedDB(t, rs, "db_two")
	return rs, dbOne, dbTwo
}

func makePopulatedDB(t *testing.T, rs driver.RepoStore, name string) driver.Repo {
	repo, err := rs.MakeRepo(name)
	require.NoError(t, err)
	err = repo.DoltExec("sql", "-q", "create table vals (id int primary key auto_increment, v int);"+
		"insert into vals (v) values (1), (2), (3);"+
		"call dolt_commit('-Am', 'initial data')")
	require.NoError(t, err)
	require.NoError(t, repo.CreateRemote("origin", "file://"+filepath.Join(repo.Dir, "remote")))
	return repo
}

// writeStaleInfoFile writes a stale sql-server.info file into the .dolt
// directory of the data dir root. Credentials lookup walks up from the cwd, so
// invocations from both the root and a database subdirectory will discover it.
func writeStaleInfoFile(t *testing.T, rs driver.RepoStore) {
	dotDolt := filepath.Join(rs.Dir, ".dolt")
	require.NoError(t, os.MkdirAll(dotDolt, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dotDolt, "sql-server.info"), []byte("1:3306:this_is_not_a_real_secret"), 0600))
}

// withNoTableFilesRead sets DOLT_TEST_ASSERT_NO_TABLE_FILES_READ=1 on |cmd| so
// that the command panics if it loads any table files from disk.
func withNoTableFilesRead(cmd *exec.Cmd) *exec.Cmd {
	cmd.Env = append(cmd.Env, dconfig.EnvAssertNoTableFilesRead+"=1")
	return cmd
}

func assertCmdSucceeds(t *testing.T, cmd *exec.Cmd) string {
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "expected command to succeed, output:\n%s", string(out))
	return string(out)
}

func assertCmdFails(t *testing.T, cmd *exec.Cmd) string {
	out, err := cmd.CombinedOutput()
	require.Error(t, err, "expected command to fail, output:\n%s", string(out))
	return string(out)
}

func TestNoTableFilesRead(t *testing.T) {
	t.Parallel()

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	// Sanity check: with the assertion hook enabled and no server running, CLI
	// commands which read the database must fail. This proves the hook itself
	// works and the success cases below are meaningful. We check both from the
	// data dir root (database-qualified read) and from a database subdirectory.
	t.Run("AssertHookFailsWhenLoadingTableFiles", func(t *testing.T) {
		rs, dbOne, _ := makePopulatedRepoStore(t, u)
		t.Run("from data dir root", func(t *testing.T) {
			out := assertCmdFails(t, withNoTableFilesRead(rs.DoltCmd("sql", "-q", "select count(*) from db_one.vals")))
			require.Regexp(t, "loading table files is disabled", out)
		})
		t.Run("from database subdirectory", func(t *testing.T) {
			out := assertCmdFails(t, withNoTableFilesRead(dbOne.DoltCmd("log")))
			require.Regexp(t, "loading table files is disabled", out)
		})
	})

	// Scenario 1: a dolt sql-server is running in the data dir root. All CLI
	// invocations set the assertion hook and must succeed by dispatching to the
	// server.
	t.Run("WithRunningServer", func(t *testing.T) {
		rs, dbOne, _ := makePopulatedRepoStore(t, u)

		var ports DynamicResources
		ports.global = &GlobalPorts
		ports.t = t
		RunServerUntilEndOfTest(t, rs, &driver.Server{
			Args:        []string{"--port", `{{get_port "server"}}`},
			DynamicPort: "server",
		}, &ports)

		t.Run("from data dir root", func(t *testing.T) {
			t.Run("show databases", func(t *testing.T) {
				out := assertCmdSucceeds(t, withNoTableFilesRead(rs.DoltCmd("sql", "-q", "show databases")))
				require.Regexp(t, "db_one", out)
				require.Regexp(t, "db_two", out)
			})
			t.Run("sql read", func(t *testing.T) {
				out := assertCmdSucceeds(t, withNoTableFilesRead(rs.DoltCmd("sql", "-q", "select count(*) from db_one.vals")))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write of db_two", func(t *testing.T) {
				assertCmdSucceeds(t, withNoTableFilesRead(rs.DoltCmd("sql", "-q", "insert into db_two.vals (v) values (10)")))
			})
		})
		t.Run("from database subdirectory", func(t *testing.T) {
			t.Run("sql read", func(t *testing.T) {
				out := assertCmdSucceeds(t, withNoTableFilesRead(dbOne.DoltCmd("sql", "-q", "select count(*) from vals")))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write", func(t *testing.T) {
				assertCmdSucceeds(t, withNoTableFilesRead(dbOne.DoltCmd("sql", "-q", "insert into vals (v) values (11)")))
			})
			t.Run("remote -v", func(t *testing.T) {
				out := assertCmdSucceeds(t, withNoTableFilesRead(dbOne.DoltCmd("remote", "-v")))
				require.Regexp(t, "origin", out)
			})
			t.Run("log", func(t *testing.T) {
				out := assertCmdSucceeds(t, withNoTableFilesRead(dbOne.DoltCmd("log")))
				require.Regexp(t, "initial data", out)
			})
		})
	})

	// Scenario 2: no server running, but a stale sql-server.info file is
	// present at the data dir root. The CLI executes locally and must work.
	t.Run("WithStaleInfoFile", func(t *testing.T) {
		rs, dbOne, _ := makePopulatedRepoStore(t, u)
		writeStaleInfoFile(t, rs)

		t.Run("from data dir root", func(t *testing.T) {
			t.Run("sql read", func(t *testing.T) {
				out := assertCmdSucceeds(t, rs.DoltCmd("sql", "-q", "select count(*) from db_one.vals"))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write of db_two", func(t *testing.T) {
				assertCmdSucceeds(t, rs.DoltCmd("sql", "-q", "insert into db_two.vals (v) values (20)"))
			})
		})
		t.Run("from database subdirectory", func(t *testing.T) {
			t.Run("sql read", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("sql", "-q", "select count(*) from vals"))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write", func(t *testing.T) {
				assertCmdSucceeds(t, dbOne.DoltCmd("sql", "-q", "insert into vals (v) values (21)"))
			})
			t.Run("remote -v", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("remote", "-v"))
				require.Regexp(t, "origin", out)
			})
			t.Run("log", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("log"))
				require.Regexp(t, "initial data", out)
			})
		})
	})

	// Scenario 3: no server running, but a concurrent `dolt sql` process holds
	// the write lock on db_one. The CLI executes locally: reads succeed, writes
	// to db_one fail because they cannot acquire the exclusive write lock, and
	// writes to the unlocked db_two succeed.
	t.Run("WithConcurrentDoltSql", func(t *testing.T) {
		rs, dbOne, _ := makePopulatedRepoStore(t, u)
		RunDoltSQLUntilEndOfTest(t, dbOne)

		t.Run("from data dir root", func(t *testing.T) {
			t.Run("sql read of locked db_one", func(t *testing.T) {
				out := assertCmdSucceeds(t, rs.DoltCmd("sql", "-q", "select count(*) from db_one.vals"))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write of locked db_one fails", func(t *testing.T) {
				out := assertCmdFails(t, rs.DoltCmd("sql", "-q", "insert into db_one.vals (v) values (30)"))
				require.Regexp(t, "(?i)read only", out)
			})
			t.Run("sql write of unlocked db_two succeeds", func(t *testing.T) {
				assertCmdSucceeds(t, rs.DoltCmd("sql", "-q", "insert into db_two.vals (v) values (31)"))
			})
		})
		t.Run("from database subdirectory", func(t *testing.T) {
			t.Run("sql read", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("sql", "-q", "select count(*) from vals"))
				require.Regexp(t, "3", out)
			})
			t.Run("sql write fails", func(t *testing.T) {
				out := assertCmdFails(t, dbOne.DoltCmd("sql", "-q", "insert into vals (v) values (32)"))
				require.Regexp(t, "(?i)read only", out)
			})
			t.Run("remote -v", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("remote", "-v"))
				require.Regexp(t, "origin", out)
			})
			t.Run("log", func(t *testing.T) {
				out := assertCmdSucceeds(t, dbOne.DoltCmd("log"))
				require.Regexp(t, "initial data", out)
			})
		})
	})
}
