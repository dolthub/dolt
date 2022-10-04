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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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

	// Skip the entire test with this reason.
	Skip string `yaml:"skip"`
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

	// Rarely needed, allows the entire connection assertion to be retried
	// on an assertion failure. Use this is only for idempotent connection
	// interactions and only if the sql-server is prone to tear down the
	// connection based on things that are happening, such as cluster role
	// transitions.
	RetryAttempts int `yaml:"retry_attempts"`
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
	Name string `yaml:"name"`

	// The contents of the file, provided inline in the YAML.
	Contents string `yaml:"contents"`

	// A source file path to copy to |Name|. Mutually exclusive with
	// Contents.
	SourcePath string `yaml:"source_path"`
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

// |QueryResult| specifies assertions on the results of a |Query|. Columns must
// be specified for a |Query| and the query results must fully match. If Rows
// are ommited, anything is allowed as long as all rows are read successfully.
// All assertions here are string equality.
type QueryResult struct {
	Columns []string   `yaml:"columns"`
	Rows    ResultRows `yaml:"rows"`
}

type ResultRows struct {
	Or *[][][]string
}

func (r *ResultRows) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		res := make([][][]string, 1)
		r.Or = &res
		return value.Decode(&(*r.Or)[0])
	}
	var or struct {
		Or *[][][]string `yaml:"or"`
	}
	err := value.Decode(&or)
	if err != nil {
		return err
	}
	r.Or = or.Or
	return nil
}

func ParseTestsFile(path string) (TestDef, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return TestDef{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	var res TestDef
	err = dec.Decode(&res)
	return res, err
}

func (f WithFile) WriteAtDir(dir string) error {
	path := filepath.Join(dir, f.Name)
	d := filepath.Dir(path)
	err := os.MkdirAll(d, 0750)
	if err != nil {
		return err
	}
	if f.SourcePath != "" {
		source, err := os.Open(f.SourcePath)
		if err != nil {
			return err
		}
		defer source.Close()
		dest, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0550)
		if err != nil {
			return err
		}
		_, err = io.Copy(dest, source)
		return err
	} else {
		return os.WriteFile(path, []byte(f.Contents), 0550)
	}
}

func MakeRepo(t *testing.T, rs RepoStore, r TestRepo) Repo {
	repo, err := rs.MakeRepo(r.Name)
	require.NoError(t, err)
	for _, f := range r.WithFiles {
		require.NoError(t, f.WriteAtDir(repo.dir))
	}
	for _, remote := range r.WithRemotes {
		require.NoError(t, repo.CreateRemote(remote.Name, remote.URL))
	}
	return repo
}

func MakeServer(t *testing.T, dc DoltCmdable, s *Server) *SqlServer {
	if s == nil {
		return nil
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
		return nil
	} else {
		t.Cleanup(func() {
			// We use assert, not require here, since FailNow() in
			// a Cleanup does not make sense.
			err := server.GracefulStop()
			if assert.NoError(t, err) {
				output := string(server.Output.Bytes())
				for _, a := range s.LogMatches {
					assert.Regexp(t, a, output)
				}
			}
		})

		return server
	}
}

func (test Test) Run(t *testing.T) {
	if test.Skip != "" {
		t.Skip(test.Skip)
	}

	u, err := NewDoltUser()
	require.NoError(t, err)
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	servers := make(map[string]*SqlServer)

	for _, r := range test.Repos {
		repo := MakeRepo(t, rs, r)

		server := MakeServer(t, repo, r.Server)
		if server != nil {
			server.DBName = r.Name
			servers[r.Name] = server
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
			require.NoError(t, f.WriteAtDir(rs.dir))
		}

		server := MakeServer(t, rs, mr.Server)
		if server != nil {
			servers[mr.Name] = server
		}
	}

	for i, c := range test.Conns {
		server := servers[c.On]
		require.NotNilf(t, server, "error in test spec: could not find server %s for connection %d", c.On, i)
		if c.RetryAttempts > 1 {
			RetryTestRun(t, c.RetryAttempts, func(t require.TestingT) {
				db, err := server.DB()
				require.NoError(t, err)
				defer db.Close()

				conn, err := db.Conn(context.Background())
				require.NoError(t, err)
				defer conn.Close()

				for _, q := range c.Queries {
					RunQueryAttempt(t, conn, q)
				}
			})
		} else {
			func() {
				db, err := server.DB()
				require.NoError(t, err)
				defer db.Close()

				conn, err := db.Conn(context.Background())
				require.NoError(t, err)
				defer conn.Close()

				for _, q := range c.Queries {
					RunQuery(t, conn, q)
				}
			}()
		}
		if c.RestartServer != nil {
			err := server.Restart(c.RestartServer.Args)
			require.NoError(t, err)
		}
	}
}

func RunTestsFile(t *testing.T, path string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	for _, test := range def.Tests {
		t.Run(test.Name, test.Run)
	}
}

type retryTestingT struct {
	*testing.T
	errorfStrings []string
	errorfArgs    [][]interface{}
	failNow       bool
}

func (r *retryTestingT) Errorf(format string, args ...interface{}) {
	r.T.Helper()
	r.errorfStrings = append(r.errorfStrings, format)
	r.errorfArgs = append(r.errorfArgs, args)
}

func (r *retryTestingT) FailNow() {
	r.T.Helper()
	r.failNow = true
	panic(r)
}

func (r *retryTestingT) try(attempts int, test func(require.TestingT)) {
	for i := 0; i < attempts; i++ {
		r.errorfStrings = nil
		r.errorfArgs = nil
		r.failNow = false
		if i != 0 {
			time.Sleep(RetrySleepDuration)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(*retryTestingT); ok {
					} else {
						panic(r)
					}
				}
			}()
			test(r)
		}()
		if !r.failNow && len(r.errorfStrings) == 0 {
			return
		}
	}
	for i := range r.errorfStrings {
		r.T.Errorf(r.errorfStrings[i], r.errorfArgs[i]...)
	}
	if r.failNow {
		r.T.FailNow()
	}
}

func RetryTestRun(t *testing.T, attempts int, test func(require.TestingT)) {
	if attempts == 0 {
		attempts = 1
	}
	rtt := &retryTestingT{T: t}
	rtt.try(attempts, test)
}

func RunQuery(t *testing.T, conn *sql.Conn, q Query) {
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
		if err == nil {
			defer rows.Close()
		}
		if q.ErrorMatch != "" {
			require.Error(t, err)
			require.Regexp(t, q.ErrorMatch, err.Error())
			return
		}
		require.NoError(t, err)

		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, q.Result.Columns, cols)

		rowstrings, err := RowsToStrings(len(cols), rows)
		require.NoError(t, err)
		if q.Result.Rows.Or != nil {
			require.Contains(t, *q.Result.Rows.Or, rowstrings)
		}
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

func RowsToStrings(cols int, rows *sql.Rows) ([][]string, error) {
	ret := make([][]string, 0)
	for rows.Next() {
		scanned := make([]any, cols)
		for j := range scanned {
			scanned[j] = new(sql.NullString)
		}
		err := rows.Scan(scanned...)
		if err != nil {
			return nil, err
		}
		printed := make([]string, cols)
		for j := range scanned {
			s := scanned[j].(*sql.NullString)
			if !s.Valid {
				printed[j] = "NULL"
			} else {
				printed[j] = s.String
			}
		}
		ret = append(ret, printed)
	}
	return ret, rows.Err()
}
