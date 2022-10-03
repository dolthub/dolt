// Copyright 2022 Dolthub, Inc.
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
	"os"
	"testing"
	"time"

	"database/sql"
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TestDef is the top-level definition of tests to run.
type TestDef struct {
	Tests []Test `yaml:"tests"`
}

// Test is a single test to run. The Repos and MultiRepos will be created, and
// any Servers defined within them will be started. The interactions and
// assertions defined in Conns will be run.
type Test struct {
	Name       string       `yaml:"name"`
	Repos      []TestRepo   `yaml:"repos"`
	MultiRepos []MultiRepo  `yaml:"multi_repos"`
	Conns      []Connection `yaml:"connections"`
}

// |Connection| represents a single connection to a sql-server instance defined
// in the test. The connection will be established and every |Query| in
// |Queries| will be run against it. At the end, the connection will be torn down.
// If |RestartServer| is non-nil, the server which the connection targets will
// be restarted after the connection is terminated.
type Connection struct {
	On            string       `yaml:"on"`
	Queries       []Query      `yaml:"queries"`
	RestartServer *RestartArgs `yaml:"restart_server"`
}

// |RestartArgs| are possible arguments, to change the arguments which are
// provided to the sql-server process when it is restarted. This is used, for
// example, to change server config on a restart.
type RestartArgs struct {
	Args *[]string `yaml:"args"`
}

// |TestRepo| represents an init'd dolt repository that is available to a
// server instance. It can be created with some files and with remotes defined.
// |Name| can include path components separated by `/`, which will create the
// repository in a subdirectory.
type TestRepo struct {
	Name        string       `yaml:"name"`
	WithFiles   []WithFile   `yaml:"with_files"`
	WithRemotes []WithRemote `yaml:"with_remotes"`

	// Only valid on Test.Repos, not in Test.MultiRepos.Repos. If set, a
	// sql-server process will be run against this TestRepo. It will be
	// available as TestRepo.Name.
	Server *Server `yaml:"server"`
}

// |MultiRepo| is a subdirectory where many |TestRepo|s can be defined. You can
// start a sql-server on a |MultiRepo|, in which case there will be no default
// database to connect to.
type MultiRepo struct {
	Name      string     `yaml:"name"`
	Repos     []TestRepo `yaml:"repos"`
	WithFiles []WithFile `yaml:"with_files"`

	// If set, a sql-server process will be run against this TestRepo. It
	// will be available as MultiRepo.Name.
	Server *Server `yaml:"server"`
}

// |WithRemote| defines remotes which should be defined on the repository
// before the sql-server is started.
type WithRemote struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// |WithFile| defines a file and its contents to be created in a |Repo| or
// |MultiRepo| before the servers are started.
type WithFile struct {
	Name     string `yaml:"name"`
	Contents string `yaml:"contents"`
}

// |Server| defines a sql-server process to start. |Name| must match the
// top-level |Name| of a |TestRepo| or |MultiRepo|.
type Server struct {
	Name string   `yaml:"name"`
	Args []string `yaml:"args"`

	// The |Port| which the server will be running on. For now, it is up to
	// the |Args| to make sure this is true. Defaults to 3308.
	Port int `yaml:"port"`

	// Assertions to be run against the log output of the server process
	// after the server process successfully terminates.
	LogMatches []string `yaml:"log_matches"`

	// Assertions to be run against the log output of the server process
	// after the server process exits with an error. If |ErrorMatches| is
	// defined, then the server process must exit with a non-0 exit code
	// after it is launched. This will be asserted before any |Connections|
	// interactions are performed.
	ErrorMatches []string `yaml:"error_matches"`
}

// The primary interaction of a |Connection|. Either |Query| or |Exec| should
// be set, not both.
type Query struct {
	// Run a query against the connection.
	Query string `yaml:"query"`

	// Run a command against the connection.
	Exec string `yaml:"exec"`

	// Args to be passed as query parameters to either Query or Exec.
	Args []string `yaml:"args"`

	// This can only be non-empty for a |Query|. Asserts the results of the
	// |Query|.
	Result QueryResult `yaml:"result"`

	// If this is non-empty, asserts the the |Query| or the |Exec|
	// generates an error that matches this string.
	ErrorMatch string `yaml:"error_match"`

	// If this is non-zero, it represents the number of times to try the
	// |Query| or the |Exec| and to check its assertions before we fail the
	// test as a result of failed assertions. When interacting with queries
	// that introspect things like replication state, this can be used to
	// wait for quiescence in an inherently racey process. Interactions
	// will be delayed slightly between each failure.
	RetryAttempts int `yaml:"retry_attempts"`
}

// |QueryResult| specifies assertions on the results of a |Query|. This must be
// specified for a |Query| and the query results must fully match. All
// assertions here are string equality.
type QueryResult struct {
	Columns []string   `yaml:"columns"`
	Rows    [][]string `yaml:"rows"`
}

func ParseTestsFile(path string) (TestDef, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return TestDef{}, err
	}
	var res TestDef
	err = yaml.UnmarshalStrict(contents, &res)
	return res, err
}

func MakeRepo(t *testing.T, rs RepoStore, r TestRepo) Repo {
	repo, err := rs.MakeRepo(r.Name)
	require.NoError(t, err)
	for _, f := range r.WithFiles {
		require.NoError(t, repo.WriteFile(f.Name, f.Contents))
	}
	for _, remote := range r.WithRemotes {
		require.NoError(t, repo.CreateRemote(remote.Name, remote.URL))
	}
	return repo
}

func MakeServer(t *testing.T, dc DoltCmdable, s *Server) (*SqlServer, func()) {
	if s == nil {
		return nil, nil
	}
	opts := []SqlServerOpt{WithArgs(s.Args...)}
	if s.Port != 0 {
		opts = append(opts, WithPort(s.Port))
	}
	server, err := StartSqlServer(dc, opts...)
	require.NoError(t, err)
	if len(s.ErrorMatches) > 0 {
		err := server.ErrorStop()
		require.Error(t, err)
		output := string(server.Output.Bytes())
		for _, a := range s.ErrorMatches {
			require.Regexp(t, a, output)
		}
		return nil, nil
	} else {
		return server, func() {
			err := server.GracefulStop()
			require.NoError(t, err)
			output := string(server.Output.Bytes())
			for _, a := range s.LogMatches {
				require.Regexp(t, a, output)
			}
		}
	}
}

func RunTestsFile(t *testing.T, path string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	for _, test := range def.Tests {
		t.Run(test.Name, func(t *testing.T) {
			u, err := NewDoltUser()
			require.NoError(t, err)
			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			doltlocs := make(map[string]DoltCmdable)
			servers := make(map[string]*SqlServer)
			for _, r := range test.Repos {
				repo := MakeRepo(t, rs, r)
				doltlocs[r.Name] = repo

				server, close := MakeServer(t, repo, r.Server)
				if server != nil {
					server.DBName = r.Name
					servers[r.Name] = server
					defer close()
				}
			}
			for _, mr := range test.MultiRepos {
				// Each MultiRepo gets its own dolt config --global.
				u, err := NewDoltUser()
				require.NoError(t, err)
				rs, err = u.MakeRepoStore()
				require.NoError(t, err)
				for _, r := range mr.Repos {
					MakeRepo(t, rs, r)
				}
				for _, f := range mr.WithFiles {
					require.NoError(t, rs.WriteFile(f.Name, f.Contents))
				}
				doltlocs[mr.Name] = rs

				server, close := MakeServer(t, rs, mr.Server)
				if server != nil {
					servers[mr.Name] = server
					defer close()
				}
			}

			dbs := make(map[string]*sql.DB)
			defer func() {
				for _, db := range dbs {
					db.Close()
				}
			}()
			for n, s := range servers {
				db, err := s.DB()
				require.NoError(t, err)
				dbs[n] = db
			}

			for i, c := range test.Conns {
				db := dbs[c.On]
				require.NotNilf(t, db, "error in test spec: could not find database %s for connection %d", c.On, i)
				conn, err := db.Conn(context.Background())
				require.NoError(t, err)
				func() {
					// Do not return this connection to the connection pool.
					defer conn.Raw(func(any) error {
						return driver.ErrBadConn
					})
					for _, q := range c.Queries {
						RunQuery(t, conn, q)
					}
				}()
				if c.RestartServer != nil {
					olddb := dbs[c.On]
					olddb.Close()
					require.NotNilf(t, olddb, "error in test spec: could not find database %s for connection %d", c.On, i)
					s := servers[c.On]
					require.NotNilf(t, s, "error in test spec: could not find server %s for connection %d", c.On, i)
					err := s.Restart(c.RestartServer.Args)
					require.NoError(t, err)
					db, err := s.DB()
					require.NoError(t, err)
					dbs[c.On] = db
				}
			}
		})
	}
}

type retryTestingT struct {
	errorfStrings []string
	errorfArgs    [][]interface{}
	failNow       bool
}

func (r *retryTestingT) Errorf(format string, args ...interface{}) {
	r.errorfStrings = append(r.errorfStrings, format)
	r.errorfArgs = append(r.errorfArgs, args)
}

func (r *retryTestingT) FailNow() {
	r.failNow = true
	panic(r)
}

func RetryTestRun(t require.TestingT, attempts int, test func(require.TestingT)) {
	if attempts == 0 {
		attempts = 1
	}
	var rtt *retryTestingT
	for i := 0; i < attempts; i++ {
		if i != 0 {
			time.Sleep(50 * time.Millisecond)
		}
		rtt = new(retryTestingT)
		func() {
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(*retryTestingT); ok {
					} else {
						panic(r)
					}
				}
			}()
			test(rtt)
		}()
		if !rtt.failNow && len(rtt.errorfStrings) == 0 {
			return
		}
	}
	for i := range(rtt.errorfStrings) {
		t.Errorf(rtt.errorfStrings[i], rtt.errorfArgs[i]...)
	}
	if rtt.failNow {
		t.FailNow()
	}
}

func RunQuery(t require.TestingT, conn *sql.Conn, q Query) {
	RetryTestRun(t, q.RetryAttempts, func(t require.TestingT) {
		RunQueryAttempt(t, conn, q)
	})
}

func RunQueryAttempt(t require.TestingT, conn *sql.Conn, q Query) {
	args := make([]any, len(q.Args))
	for i := range q.Args {
		args[i] = q.Args[i]
	}
	if q.Query != "" {
		rows, err := conn.QueryContext(context.Background(), q.Query, args...)
		if q.ErrorMatch != "" {
			require.Error(t, err)
			require.Regexp(t, q.ErrorMatch, err.Error())
			return
		}
		require.NoError(t, err)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, q.Result.Columns, cols)
		for _, r := range q.Result.Rows {
			require.True(t, rows.Next())
			scanned := make([]any, len(r))
			for j := range scanned {
				scanned[j] = new(sql.NullString)
			}
			require.NoError(t, rows.Scan(scanned...))
			printed := make([]string, len(r))
			for j := range scanned {
				s := scanned[j].(*sql.NullString)
				if !s.Valid {
					printed[j] = "NULL"
				} else {
					printed[j] = s.String
				}
			}
			require.Equal(t, r, printed)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	} else if q.Exec != "" {
		_, err := conn.ExecContext(context.Background(), q.Exec, args...)
		if q.ErrorMatch == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Regexp(t, q.ErrorMatch, err.Error())
		}
	}
}
