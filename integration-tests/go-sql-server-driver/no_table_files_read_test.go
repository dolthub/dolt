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
// The test matrix has three top-level scenarios:
//
//  1. A dolt sql-server is running. The CLI is invoked with
//     DOLT_TEST_ASSERT_NO_TABLE_FILES_READ=1 and must succeed, because it
//     dispatches to the server without reading any table files.
//  2. No server is running, but a stale sql-server.info file is present. The
//     CLI is invoked (executing locally) and must still succeed.
//  3. No server is running, but a concurrent `dolt sql` process holds the
//     database lock. The CLI is invoked (executing locally); reads succeed and
//     writes fail because they cannot acquire the write lock.
//
// In scenarios 2 and 3 the CLI must read the local database, so we do not set
// DOLT_TEST_ASSERT_NO_TABLE_FILES_READ for those invocations.

// makePopulatedRepo creates a fresh repo store and an initialized repo named
// |name| with a committed table, some rows, and a remote configured.
func makePopulatedRepo(t *testing.T, u driver.DoltUser, name string) driver.Repo {
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo(name)
	require.NoError(t, err)
	err = repo.DoltExec("sql", "-q", "create table vals (id int primary key auto_increment, v int);"+
		"insert into vals (v) values (1), (2), (3);"+
		"call dolt_commit('-Am', 'initial data')")
	require.NoError(t, err)
	require.NoError(t, repo.CreateRemote("origin", "file://"+filepath.Join(repo.Dir, "remote")))
	return repo
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

	// Sanity check: with the assertion hook enabled and no server running, a
	// CLI command which reads the database must fail. This proves the hook
	// itself works and the success cases below are meaningful.
	t.Run("AssertHookFailsWhenLoadingTableFiles", func(t *testing.T) {
		repo := makePopulatedRepo(t, u, "assert_hook")
		out := assertCmdFails(t, withNoTableFilesRead(repo.DoltCmd("log")))
		require.Regexp(t, "loading table files is disabled", out)
	})

	// Scenario 1: a dolt sql-server is running. All CLI invocations set the
	// assertion hook and must succeed by dispatching to the server.
	t.Run("WithRunningServer", func(t *testing.T) {
		repo := makePopulatedRepo(t, u, "with_running_server")

		var ports DynamicResources
		ports.global = &GlobalPorts
		ports.t = t
		RunServerUntilEndOfTest(t, repo, &driver.Server{
			Args:        []string{"--port", `{{get_port "server"}}`},
			DynamicPort: "server",
		}, &ports)

		t.Run("sql read", func(t *testing.T) {
			out := assertCmdSucceeds(t, withNoTableFilesRead(repo.DoltCmd("sql", "-q", "select count(*) from vals")))
			require.Regexp(t, "3", out)
		})
		t.Run("sql write", func(t *testing.T) {
			assertCmdSucceeds(t, withNoTableFilesRead(repo.DoltCmd("sql", "-q", "insert into vals (v) values (10)")))
		})
		t.Run("remote -v", func(t *testing.T) {
			out := assertCmdSucceeds(t, withNoTableFilesRead(repo.DoltCmd("remote", "-v")))
			require.Regexp(t, "origin", out)
		})
		t.Run("log", func(t *testing.T) {
			out := assertCmdSucceeds(t, withNoTableFilesRead(repo.DoltCmd("log")))
			require.Regexp(t, "initial data", out)
		})
	})

	// Scenario 2: no server running, but a stale sql-server.info file is
	// present. The CLI executes locally and must still work.
	t.Run("WithStaleInfoFile", func(t *testing.T) {
		repo := makePopulatedRepo(t, u, "with_stale_info_file")
		path := filepath.Join(repo.Dir, ".dolt/sql-server.info")
		require.NoError(t, os.WriteFile(path, []byte("1:3306:this_is_not_a_real_secret"), 0600))

		t.Run("sql read", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("sql", "-q", "select count(*) from vals"))
			require.Regexp(t, "3", out)
		})
		t.Run("sql write", func(t *testing.T) {
			assertCmdSucceeds(t, repo.DoltCmd("sql", "-q", "insert into vals (v) values (20)"))
		})
		t.Run("remote -v", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("remote", "-v"))
			require.Regexp(t, "origin", out)
		})
		t.Run("log", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("log"))
			require.Regexp(t, "initial data", out)
		})
	})

	// Scenario 3: no server running, but a concurrent `dolt sql` process holds
	// the database lock. The CLI executes locally: reads succeed and writes
	// fail because they cannot acquire the exclusive write lock.
	t.Run("WithConcurrentDoltSql", func(t *testing.T) {
		repo := makePopulatedRepo(t, u, "with_concurrent_dolt_sql")
		RunDoltSQLUntilEndOfTest(t, repo)

		t.Run("sql read", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("sql", "-q", "select count(*) from vals"))
			require.Regexp(t, "3", out)
		})
		t.Run("sql write fails", func(t *testing.T) {
			out := assertCmdFails(t, repo.DoltCmd("sql", "-q", "insert into vals (v) values (30)"))
			require.Regexp(t, "(?i)read only", out)
		})
		t.Run("remote -v", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("remote", "-v"))
			require.Regexp(t, "origin", out)
		})
		t.Run("log", func(t *testing.T) {
			out := assertCmdSucceeds(t, repo.DoltCmd("log"))
			require.Regexp(t, "initial data", out)
		})
	})
}
