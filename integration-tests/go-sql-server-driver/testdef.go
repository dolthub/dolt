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
	"sync"
	"testing"
	"text/template"
	"time"

	"database/sql"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

var GlobalPorts GlobalDynamicPorts

type TestDef struct {
	Tests []Test `yaml:"tests"`

	// If true, RunTestfile will run each subtest in parallel.
	Parallel bool `yaml:"parallel"`
}

// Test is a single test to run. The Repos and MultiRepos will be created, and
// any Servers defined within them will be started. The interactions and
// assertions defined in Conns will be run.
type Test struct {
	Name       string              `yaml:"name"`
	Repos      []driver.TestRepo   `yaml:"repos"`
	MultiRepos []driver.MultiRepo  `yaml:"multi_repos"`
	Conns      []driver.Connection `yaml:"connections"`

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

func MakeRepo(t *testing.T, rs driver.RepoStore, r driver.TestRepo, ports *DynamicPorts) driver.Repo {
	repo, err := rs.MakeRepo(r.Name)
	require.NoError(t, err)
	for _, f := range r.WithFiles {
		f.Template = func(s string) string {
			return ports.ApplyTemplate(s)
		}
		require.NoError(t, f.WriteAtDir(repo.Dir))
	}
	for _, remote := range r.WithRemotes {
		url := remote.URL
		url = ports.ApplyTemplate(url)
		require.NoError(t, repo.CreateRemote(remote.Name, url))
	}
	return repo
}

// Simple interface for wrapping *testing.T. Used for retryingT.
type TestingT interface {
	Fatal(...any)

	FailNow()

	Errorf(string, ...any)

	Cleanup(func())
}

// Globally available dynamic ports, backs every instance of
// DynamicPorts and hands them out in a thread-safe manner.
//
// XXX: This structure and its initialization does not currently look
// for "available" ports on the running host. It simply avoids handing
// out the same port to two separate tests that are running at the
// same time. It recycles ports as tests complete.
type GlobalDynamicPorts struct {
	mu        sync.Mutex
	available []int
}

func (g *GlobalDynamicPorts) Get(t TestingT) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.available) == 0 {
		t.Fatal("cannot get a port; we are all out.")
	}
	next := g.available[len(g.available)-1]
	g.available = g.available[:len(g.available)-1]
	return next
}

func (g *GlobalDynamicPorts) Return(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.available = append(g.available, n)
}

// Tracks dynamic ports available for expansion in `{{get_port ...}}`
// templates for server args and config files.
type DynamicPorts struct {
	// Where we go when we need a new one.
	global *GlobalDynamicPorts

	t TestingT

	// Where we put allocated ports. For a given test, the first
	// use will get a new unused port from
	// GlobalDynamicPorts. Then that same port will be returned
	// from here for all uses. When the test finishes, its Cleanup
	// returns the port to GlobalDynamicPorts.
	allocated map[string]int
}

func (d *DynamicPorts) Get(name string) (int, bool) {
	if d.allocated != nil {
		v, ok := d.allocated[name]
		return v, ok
	} else {
		return 0, false
	}
}

func (d *DynamicPorts) GetOrAllocate(name string) int {
	v, ok := d.Get(name)
	if ok {
		return v
	}
	v = d.global.Get(d.t)
	if d.allocated == nil {
		d.allocated = make(map[string]int)
		// We register one cleanup function for the entire
		// DynamicPorts and we return them all at once.
		//
		// In cases where there are two dependent servers, we
		// want to return all ports after both servers have
		// been shut down. If we return them as we allocated
		// them, it's possible that we allocated them to
		// render the entire config for the first server, some
		// referring to the second server, for example. If
		// testing.T runs cleanups in FIFO order, and the
		// Cleanup for running the second server is
		// responsible for shutting it down, it is possible we
		// would return the second server's ports before it is
		// shut down if we didn't return them all at once.
		d.t.Cleanup(func() {
			for _, p := range d.allocated {
				d.global.Return(p)
			}
		})
	}
	d.allocated[name] = v
	return v
}

func (d *DynamicPorts) ApplyTemplate(s string) string {
	tmpl, err := template.New("sql").Funcs(map[string]any{
		"get_port": d.GetOrAllocate,
	}).Parse(s)
	require.NoError(d.t, err)
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, nil)
	require.NoError(d.t, err)
	return buf.String()
}

func MakeServer(t *testing.T, dc driver.DoltCmdable, s *driver.Server, dynPorts *DynamicPorts) *driver.SqlServer {
	if s == nil {
		return nil
	}
	args := make([]string, len(s.Args))
	for i := range args {
		args[i] = dynPorts.ApplyTemplate(s.Args[i])
	}
	opts := []driver.SqlServerOpt{
		driver.WithArgs(args...),
		driver.WithEnvs(append([]string{"DOLT_CONTEXT_VALIDATION_ENABLED=true"}, s.Envs...)...),
		driver.WithName(s.Name),
	}
	if s.Port != 0 {
		t.Fatal("cannot specify s.Port on these tests; please use {{get_port ...}} and dynamic_port: to specify a dynamic port.")
	}
	if s.DynamicPort == "" {
		t.Fatal("you must specify s.DynamicPort on these tests; please use {{get_port ...}} and dynamic_port: to specify a dynamic port.")
	}
	port, ok := dynPorts.Get(s.DynamicPort)
	if !ok {
		t.Fatalf("cannot find dynamic port %s after expanding server config, requested as dynamic server port", s.DynamicPort)
	}
	opts = append(opts, driver.WithPort(port))

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

// Runs the defined test, applying its asserts.
//
// Supports {{get_port ...}} dynamic port assignment in the following
// places:
// * Repo with_files contents
// * MultiRepo with_files contents
// * Server args
// * RestartServer args
// * Query exec string literals. Currently not args.
// * Query result row assert string literals.
func (test Test) Run(t *testing.T) {
	if test.Skip != "" {
		t.Skip(test.Skip)
	}

	var ports DynamicPorts
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})
	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	servers := make(map[string]*driver.SqlServer)

	for _, r := range test.Repos {
		repo := MakeRepo(t, rs, r, &ports)

		if r.Server.Name == "" {
			r.Server.Name = r.Name
		}
		server := MakeServer(t, repo, r.Server, &ports)
		if server != nil {
			server.DBName = r.Name
			servers[r.Name] = server
		}
	}

	for _, mr := range test.MultiRepos {
		// Each MultiRepo gets its own dolt config --global.
		u, err := driver.NewDoltUser()
		require.NoError(t, err)
		t.Cleanup(func() {
			u.Cleanup()
		})
		rs, err = u.MakeRepoStore()
		require.NoError(t, err)
		for _, r := range mr.Repos {
			MakeRepo(t, rs, r, &ports)
		}
		for _, f := range mr.WithFiles {
			f.Template = func(s string) string {
				return ports.ApplyTemplate(s)
			}
			require.NoError(t, f.WriteAtDir(rs.Dir))
		}
		if mr.Server.Name == "" {
			mr.Server.Name = mr.Name
		}
		server := MakeServer(t, rs, mr.Server, &ports)
		if server != nil {
			servers[mr.Name] = server
		}
	}

	for i, c := range test.Conns {
		server := servers[c.On]
		require.NotNilf(t, server, "error in test spec: could not find server %s for connection %d", c.On, i)
		if c.RetryAttempts > 1 {
			RetryTestRun(t, c.RetryAttempts, func(t TestingT) {
				db, err := server.DB(c)
				require.NoError(t, err)
				defer db.Close()

				conn, err := db.Conn(context.Background())
				require.NoError(t, err)
				defer conn.Close()

				for _, q := range c.Queries {
					RunQueryAttempt(t, conn, q, &ports)
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
					RunQuery(t, conn, q, &ports)
				}
			}()
		}
		if c.RestartServer != nil {
			args := c.RestartServer.Args
			if args != nil {
				tmplArgs := make([]string, len(*args))
				for i := range tmplArgs {
					tmplArgs[i] = ports.ApplyTemplate((*args)[i])
				}
				args = &tmplArgs
			}
			err := server.Restart(args, c.RestartServer.Envs)
			require.NoError(t, err)
		}
	}
}

func RunTestsFile(t *testing.T, path string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	parallel := def.Parallel
	for _, test := range def.Tests {
		t.Run(test.Name, func(t *testing.T) {
			if parallel {
				t.Parallel()
			}
			test.Run(t)
		})
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

func (r *retryTestingT) try(attempts int, test func(TestingT)) {
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

func RetryTestRun(t *testing.T, attempts int, test func(TestingT)) {
	if attempts == 0 {
		attempts = 1
	}
	rtt := &retryTestingT{T: t}
	rtt.try(attempts, test)
}

func RunQuery(t *testing.T, conn *sql.Conn, q driver.Query, ports *DynamicPorts) {
	RetryTestRun(t, q.RetryAttempts, func(t TestingT) {
		RunQueryAttempt(t, conn, q, ports)
	})
}

func RunQueryAttempt(t TestingT, conn *sql.Conn, q driver.Query, ports *DynamicPorts) {
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
			match := *q.Result.Rows.Or
			for i := range match {
				for j := range match[i] {
					for k := range match[i][j] {
						match[i][j][k] = ports.ApplyTemplate(match[i][j][k])
					}
				}
			}
			require.Contains(t, match, rowstrings)
		}
	} else if q.Exec != "" {
		ctx, c := context.WithTimeout(context.Background(), timeout)
		defer c()
		exec := q.Exec
		exec = ports.ApplyTemplate(exec)
		_, err := conn.ExecContext(ctx, exec, args...)
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
