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
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

func TestSQLServerInfoFile(t *testing.T) {
	t.Parallel()
	t.Run("With Two Repos", func(t *testing.T) {
		u, err := driver.NewDoltUser()
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})

		rs, err := u.MakeRepoStore()
		require.NoError(t, err)
		dbOne, err := rs.MakeRepo("db_one")
		require.NoError(t, err)
		dbTwo, err := rs.MakeRepo("db_two")
		require.NoError(t, err)

		t.Run("With server running in root", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			RunServerUntilEndOfTest(t, rs, &driver.Server{
				Args: []string{"--port", `{{get_port "server_one"}}`},
				DynamicPort: "server_one",
			}, &ports)

			t.Run("sql-server.info file exists", func(t *testing.T) {
				location := filepath.Join(rs.Dir, ".dolt/sql-server.info")
				f, err := os.Open(location)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			})
			t.Run("Running again in root fails", func(t *testing.T) {
				_ = MakeServer(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running in db_one fails", func(t *testing.T) {
				_ = MakeServer(t, dbOne, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running dolt sql -q in /server connects to server", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("Running dolt sql -q in /server/db_two connects to server on db_two", func(t *testing.T) {
				cmd := dbTwo.DoltCmd("sql", "-q", "select database()")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("Running dolt sql -q in /server/db_two/.dolt/noms connects to server on no database", func(t *testing.T) {
				cmd := dbTwo.DoltCmd("sql", "-q", "select database()")
				cmd.Dir = filepath.Join(cmd.Dir, ".dolt/noms")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "NULL", outstr)

				cmd = dbTwo.DoltCmd("sql", "-q", "show databases")
				cmd.Dir = filepath.Join(cmd.Dir, ".dolt/noms")
				output, err = cmd.CombinedOutput()
				outstr = string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
		})
		t.Run("sql-server.info in root no longer exists", func(t *testing.T) {
			location := filepath.Join(rs.Dir, ".dolt/sql-server.info")
			f, err := os.Open(location)
			if f != nil {
				defer f.Close()
			}
			require.ErrorIs(t, err, fs.ErrNotExist)
		})
		t.Run("With server running in db_one", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			RunServerUntilEndOfTest(t, dbOne, &driver.Server{
				Args: []string{"--port", `{{get_port "server_one"}}`},
				DynamicPort: "server_one",
			}, &ports)

			t.Run("Running server in root fails", func(t *testing.T) {
				_ = MakeServer(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running server in db_two succeeds", func(t *testing.T) {
				RunServerUntilEndOfTest(t, dbTwo, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
				}, &ports)
			})
			t.Run("dolt sql -q in root succeeds", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("dolt sql -q in root can write to db_two", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "create table db_two.vals (id int primary key)")
				_, err := cmd.CombinedOutput()
				require.NoError(t, err)
			})
			t.Run("dolt sql -q in root cannot write to db_one", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "create table db_one.vals (id int primary key)")
				_, err := cmd.CombinedOutput()
				require.Error(t, err)
			})
		})
		t.Run("Given a running dolt sql process in root", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			RunDoltSQLUntilEndOfTest(t, rs)

			t.Run("Running server in root fails", func(t *testing.T) {
				_ = MakeServer(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running server in db_one fails", func(t *testing.T) {
				_ = MakeServer(t, dbOne, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("dolt sql -q in root succeeds", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("dolt sql -q in root cannot write to db_one", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "create table db_one.vals (id int primary key)")
				_, err := cmd.CombinedOutput()
				require.Error(t, err)
			})
		})
		t.Run("Given a running dolt sql process in db_one", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			RunDoltSQLUntilEndOfTest(t, dbOne)

			t.Run("Running server in root fails", func(t *testing.T) {
				_ = MakeServer(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running server in db_one fails", func(t *testing.T) {
				_ = MakeServer(t, dbOne, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
					ErrorMatches: []string{
						"locked by another dolt process",
					},
				}, &ports)
			})
			t.Run("Running server in db_two succeeds", func(t *testing.T) {
				RunServerUntilEndOfTest(t, dbTwo, &driver.Server{
					Args: []string{"--port", `{{get_port "server_two"}}`},
					DynamicPort: "server_two",
				}, &ports)
			})
			t.Run("dolt sql -q in root succeeds", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("dolt sql -q in root can write to db_two", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "create table db_two.another_vals (id int primary key)")
				_, err := cmd.CombinedOutput()
				require.NoError(t, err)
			})
			t.Run("dolt sql -q in root cannot write to db_one", func(t *testing.T) {
				cmd := rs.DoltCmd("sql", "-q", "create table db_one.vals (id int primary key)")
				_, err := cmd.CombinedOutput()
				require.Error(t, err)
			})
		})
		t.Run("With stale sql-server.info file in root", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			Setup := func(t *testing.T) {
				path := filepath.Join(rs.Dir, ".dolt/sql-server.info")
				err := os.WriteFile(path, []byte("1:3306:this_is_not_a_real_secret"), 0600)
				require.NoError(t, err)
				t.Cleanup(func() { os.Remove(path) })
			}
			t.Run("sql-server can run in root", func(t *testing.T) {
				Setup(t)
				RunServerUntilEndOfTest(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_one"}}`},
					DynamicPort: "server_one",
				}, &ports)
			})
			t.Run("sql-server can run in db_one", func(t *testing.T) {
				Setup(t)
				RunServerUntilEndOfTest(t, dbOne, &driver.Server{
					Args: []string{"--port", `{{get_port "server_one"}}`},
					DynamicPort: "server_one",
				}, &ports)
			})
			t.Run("sql can run in root", func(t *testing.T) {
				Setup(t)
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("sql can run in db_one", func(t *testing.T) {
				Setup(t)
				cmd := dbOne.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
			})
		})
		t.Run("With malformed sql-server.info file in root", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			Setup := func(t *testing.T) {
				path := filepath.Join(rs.Dir, ".dolt/sql-server.info")
				err := os.WriteFile(path, []byte("1:3306:this_is_not_a_real_secret:extra:fields:make:it:fail"), 0600)
				require.NoError(t, err)
				t.Cleanup(func() { os.Remove(path) })
			}
			t.Run("sql-server can run in root", func(t *testing.T) {
				Setup(t)
				RunServerUntilEndOfTest(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_one"}}`},
					DynamicPort: "server_one",
				}, &ports)
			})
			t.Run("sql-server can run in db_one", func(t *testing.T) {
				Setup(t)
				RunServerUntilEndOfTest(t, rs, &driver.Server{
					Args: []string{"--port", `{{get_port "server_one"}}`},
					DynamicPort: "server_one",
				}, &ports)
			})
			t.Run("sql can run in root", func(t *testing.T) {
				Setup(t)
				cmd := rs.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
				require.Regexp(t, "db_two", outstr)
			})
			t.Run("sql can run in db_one", func(t *testing.T) {
				Setup(t)
				cmd := dbOne.DoltCmd("sql", "-q", "show databases")
				output, err := cmd.CombinedOutput()
				outstr := string(output)
				require.NoError(t, err)
				require.Regexp(t, "db_one", outstr)
			})
			t.Run("with dolt sql running in db_one", func(t *testing.T) {
				RunDoltSQLUntilEndOfTest(t, dbOne)

				t.Run("dolt sql -q fails in db_one", func(t *testing.T) {
					Setup(t)
					cmd := dbOne.DoltCmd("sql", "-q", "show databases")
					_, err = cmd.CombinedOutput()
					require.Error(t, err)
				})
				t.Run("dolt sql -q succeeds in root", func(t *testing.T) {
					Setup(t)
					cmd := rs.DoltCmd("sql", "-q", "show databases")
					output, err := cmd.CombinedOutput()
					require.NoError(t, err)
					outstr := string(output)
					require.Regexp(t, "db_one", outstr)
					require.Regexp(t, "db_two", outstr)
				})
			})
		})
	})
	t.Run("With Empty RepoStore", func(t *testing.T) {
		u, err := driver.NewDoltUser()
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})

		rs, err := u.MakeRepoStore()
		require.NoError(t, err)

		// TODO: This is strange behavior which the current implementation allows.
		// If the top-level directory is empty when both servers are
		// started, they can both run against it. Only one of their
		// credential files will win the write.
		t.Run("Can Run Two Servers At Once", func(t *testing.T) {
			var ports DynamicResources
			ports.global = &GlobalPorts
			ports.t = t
			RunServerUntilEndOfTest(t, rs, &driver.Server{
				Args: []string{"--port", `{{get_port "server_one"}}`},
				DynamicPort: "server_one",
			}, &ports)
			RunServerUntilEndOfTest(t, rs, &driver.Server{
				Args: []string{"--port", `{{get_port "server_two"}}`},
				DynamicPort: "server_two",
			}, &ports)
		})
	})
}

func RunDoltSQLUntilEndOfTest(t *testing.T, dc driver.DoltCmdable) {
	// Spawn `dolt sql`, capturing output.
	sqlCmd := dc.DoltCmd("sql")
	in, err := sqlCmd.StdinPipe()
	require.NoError(t, err)
	out, err := sqlCmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, sqlCmd.Start())

	// Send a query so we know it is fully initialized.
	go func() {
		io.WriteString(in, "show databases;\n")
	}()

	// Read lines of the response until we see an empty line.
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}
	}

	// Register cleanup. We close stdin at the of the function and assert
	// that `dolt sql` exited with exit code 0.
	go func() {
		io.Copy(io.Discard, out)
	}()
	t.Cleanup(func() {
		assert.NoError(t, in.Close())
		assert.NoError(t, sqlCmd.Wait())
	})
}

// Run a server until the end of the test. Because we do not return the server
// for doing things like making connections to it, this is only useful for
// for asserting the behavior of other dolt commands which interact with the
// server.
func RunServerUntilEndOfTest(t *testing.T, dc driver.DoltCmdable, s *driver.Server, ports *DynamicResources) {
	server := MakeServer(t, dc, s, ports)
	require.NotNil(t, server)
	db, err := server.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
