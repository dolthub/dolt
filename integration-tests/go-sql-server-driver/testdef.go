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
	"os"
	"testing"
	"time"

	"database/sql"
	
	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

type TestDef struct {
	Tests []Test `yaml:"tests"`
}

// Test is a single test to run. The Repos and MultiRepos will be created, and
// any Servers defined within them will be started. The interactions and
// assertions defined in Conns will be run.
type Test struct {
	Name        string              `yaml:"name"`
	Repos       []driver.TestRepo   `yaml:"repos"`
	MultiRepos  []driver.MultiRepo  `yaml:"multi_repos"`
	Conns       []driver.Connection `yaml:"connections"`

	// Skip the entire test with this reason.
	Skip string `yaml:"skip"`
}

// Set this environment variable to effectively disable timeouts for debugging.
const debugEnvKey = "DOLT_SQL_SERVER_TEST_DEBUG"
var timeout = 20 * time.Second

func init() {
	_, ok := os.LookupEnv(debugEnvKey)
	if ok {
		timeout = 1000 * time.Hour
	}
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

func MakeRepo(t *testing.T, rs driver.RepoStore, r driver.TestRepo) driver.Repo {
	repo, err := rs.MakeRepo(r.Name)
	require.NoError(t, err)
	for _, f := range r.WithFiles {
		require.NoError(t, f.WriteAtDir(repo.Dir))
	}
	for _, remote := range r.WithRemotes {
		require.NoError(t, repo.CreateRemote(remote.Name, remote.URL))
	}
	return repo
}

func MakeServer(t *testing.T, dc driver.DoltCmdable, s *driver.Server) *driver.SqlServer {
	if s == nil {
		return nil
	}
	opts := []driver.SqlServerOpt{driver.WithArgs(s.Args...), driver.WithEnvs(s.Envs...), driver.WithName(s.Name)}
	if s.Port != 0 {
		opts = append(opts, driver.WithPort(s.Port))
	}

	var server *driver.SqlServer
	var err error
	if s.DebugPort != 0 {
		server, err = driver.DebugSqlServer(dc, s.DebugPort, opts...)	
	} else {
		server, err = driver.StartSqlServer(dc, opts...)
	}
	
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

	u, err := driver.NewDoltUser(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	servers := make(map[string]*driver.SqlServer)

	for _, r := range test.Repos {
		repo := MakeRepo(t, rs, r)

		if r.Server.Name == "" {
			r.Server.Name = r.Name
		}
		server := MakeServer(t, repo, r.Server)
		if server != nil {
			server.DBName = r.Name
			servers[r.Name] = server
		}
	}
	for _, mr := range test.MultiRepos {
		// Each MultiRepo gets its own dolt config --global.
		u, err := driver.NewDoltUser(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})
		rs, err = u.MakeRepoStore()
		require.NoError(t, err)
		for _, r := range mr.Repos {
			MakeRepo(t, rs, r)
		}
		for _, f := range mr.WithFiles {
			require.NoError(t, f.WriteAtDir(rs.Dir))
		}

		if mr.Server.Name == "" {
			mr.Server.Name = mr.Name
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
				db, err := server.DB(c)
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
				db, err := server.DB(c)
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
			err := server.Restart(c.RestartServer.Args, c.RestartServer.Envs)
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

func RunSingleTest(t *testing.T, path string, testName string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	for _, test := range def.Tests {
		if test.Name == testName {
			t.Run(test.Name, test.Run)
		}
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
			time.Sleep(driver.RetrySleepDuration)
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

func RunQuery(t *testing.T, conn *sql.Conn, q driver.Query) {
	RetryTestRun(t, q.RetryAttempts, func(t require.TestingT) {
		RunQueryAttempt(t, conn, q)
	})
}

func RunQueryAttempt(t require.TestingT, conn *sql.Conn, q driver.Query) {
	args := make([]any, len(q.Args))
	for i := range q.Args {
		args[i] = q.Args[i]
	}
	if q.Query != "" {
		ctx, c := context.WithTimeout(context.Background(), timeout)
		defer c()
		rows, err := conn.QueryContext(ctx, q.Query, args...)
		if err == nil {
			defer rows.Close()
		}
		if q.ErrorMatch != "" {
			require.Error(t, err, "expected error running query %s", q.Query)
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
		ctx, c := context.WithTimeout(context.Background(), timeout)
		defer c()
		_, err := conn.ExecContext(ctx, q.Exec, args...)
		if q.ErrorMatch == "" {
			require.NoError(t, err, "error running query %s: %v", q.Exec, err)
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
