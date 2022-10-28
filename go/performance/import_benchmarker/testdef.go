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

package import_benchmarker

import (
	"bytes"
	"context"
	"fmt"
	"github.com/creasty/defaults"
	"github.com/dolthub/vitess/go/sqltypes"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"database/sql"
	sql2 "github.com/dolthub/go-mysql-server/sql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestDef is the top-level definition of tests to run.
type TestDef struct {
	Tests []ImportTest `yaml:"tests"`
	Opts  *Opts        `yaml:"opts"`
}

type Opts struct {
	Seed int `yaml:"seed"`
}

// ImportTest is a single test to run. The Repos and MultiRepos will be created, and
// any Servers defined within them will be started. The interactions and
// assertions defined in Conns will be run.
type ImportTest struct {
	Name   string     `yaml:"name"`
	Repos  []TestRepo `yaml:"repos"`
	Tables []Table    `yaml:"tables"`

	// Skip the entire test with this reason.
	Skip string `yaml:"skip"`
}

type Table struct {
	Name    string `yaml:"name"`
	Schema  string `yaml:"schema"`
	Rows    int    `default:"400000" yaml:"rows"`
	Fmt     string `default:"csv" yaml:"fmt"`
	Shuffle bool   `default:"true" yaml:"shuffle"'`
}

func (s *Table) UnmarshalYAML(unmarshal func(interface{}) error) error {
	defaults.Set(s)

	type plain Table
	if err := unmarshal((*plain)(s)); err != nil {
		return err
	}

	return nil
}

// |TestRepo| represents an init'd dolt repository that is available to a
// server instance. It can be created with some files and with remotes defined.
// |Name| can include path components separated by `/`, which will create the
// repository in a subdirectory.
type TestRepo struct {
	Name string `yaml:"name"`

	// Only valid on ImportTest.Repos, not in Test.MultiRepos.Repos. If set, a
	// sql-server process will be run against this TestRepo. It will be
	// available as TestRepo.Name.
	Server         *Server         `yaml:"server"`
	ExternalServer *ExternalServer `yaml:"external-server"`
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

type ExternalServer struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// The |Port| which the server will be running on. For now, it is up to
	// the |Args| to make sure this is true. Defaults to 3308.
	Port int `yaml:"port"`
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

func MakeRepo(rs RepoStore, r TestRepo) (Repo, error) {
	repo, err := rs.MakeRepo(r.Name)
	if err != nil {
		return Repo{}, err
	}
	return repo, nil
}

func MakeServer(dc DoltCmdable, s *Server) (*SqlServer, error) {
	if s == nil {
		return nil, nil
	}
	opts := []SqlServerOpt{WithArgs(s.Args...)}
	if s.Port != 0 {
		opts = append(opts, WithPort(s.Port))
	}
	server, err := StartSqlServer(dc, opts...)
	if err != nil {
		return nil, err
	}
	//t.Cleanup(func() {
	//	// We use assert, not require here, since FailNow() in
	//	// a Cleanup does not make sense.
	//	err := server.GracefulStop()
	//	if assert.NoError(t, err) {
	//		output := string(server.Output.Bytes())
	//		for _, a := range s.LogMatches {
	//			assert.Regexp(t, a, output)
	//		}
	//	}
	//})

	return server, nil
}

type ImportResult struct {
	table string
	repo  string
	test  string
	time  float64
}

func (r ImportResult) String() string {
	return fmt.Sprintf("- %s/%s/%s: %.2fs\n", r.test, r.repo, r.table, r.time)
}

type ImportResults struct {
	res []ImportResult
}

func (r *ImportResults) append(ir ImportResult) {
	r.res = append(r.res, ir)
}

func (r *ImportResults) String() string {
	b := strings.Builder{}
	b.WriteString("results:\n")
	for _, x := range r.res {
		b.WriteString(x.String())
	}
	return b.String()
}

func (test ImportTest) Run(t *testing.T) {
	if test.Skip != "" {
		t.Skip(test.Skip)
	}

	results := new(ImportResults)
	for _, r := range test.Repos {
		if r.ExternalServer != nil {
			// mysql conn
			db, err := ConnectDB(r.ExternalServer.User, r.ExternalServer.Password, r.ExternalServer.Name, r.ExternalServer.Host, r.ExternalServer.Port)
			require.NoError(t, err)
			err = test.RunServerTests(r.Name, db, results)
			require.NoError(t, err)
		} else if r.Server != nil {
			u, err := NewDoltUser()
			require.NoError(t, err)
			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			// start dolt server
			repo, err := MakeRepo(rs, r)
			r.Server.Args = append(r.Server.Args, "")
			server, err := MakeServer(repo, r.Server)
			if server != nil {
				server.DBName = r.Name
			}
			defer server.GracefulStop()

			db, err := server.DB()
			require.NoError(t, err)

			_, err = db.Exec("SET GLOBAL local_infile=1 ")
			require.NoError(t, err)

			err = test.RunServerTests(r.Name, db, results)
			require.NoError(t, err)
		} else {
			u, err := NewDoltUser()
			require.NoError(t, err)
			rs, err := u.MakeRepoStore()
			require.NoError(t, err)

			// cli only access
			repo, err := MakeRepo(rs, r)
			require.NoError(t, err)

			err = test.RunCliTests(r.Name, repo, results)
			require.NoError(t, err)
		}
	}
	fmt.Println(results.String())
}

func (test ImportTest) RunServerTests(name string, db *sql.DB, results *ImportResults) error {
	return IterImportTables(test.Tables, func(tab Table, f *os.File) error {
		// start timer
		conn, err := db.Conn(context.Background())
		if err != nil {
			return err
		}
		defer conn.Close()

		rows, err := conn.QueryContext(context.Background(), tab.Schema)
		if err == nil {
			rows.Close()
		} else {
			return err
		}

		start := time.Now()

		q := fmt.Sprintf(`
LOAD DATA LOCAL INFILE '%s' INTO TABLE xy
FIELDS TERMINATED BY ',' ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES;`, f.Name())

		rows, err = conn.QueryContext(context.Background(), q)
		if err == nil {
			rows.Close()
		} else {
			return err
		}

		stop := time.Now()

		// end timer, append result
		results.append(ImportResult{
			test:  test.Name,
			repo:  name,
			table: tab.Name,
			time:  stop.Sub(start).Seconds(),
		})

		rows, err = conn.QueryContext(context.Background(), "drop table xy;")
		if err == nil {
			rows.Close()
		} else {
			return err
		}

		return nil
	})
}

func (test ImportTest) RunCliTests(name string, repo Repo, results *ImportResults) error {
	return IterImportTables(test.Tables, func(tab Table, f *os.File) error {
		var err error

		err = repo.DoltExec("sql", "-q", tab.Schema)
		if err != nil {
			return err
		}

		// start timer
		start := time.Now()

		cmd := repo.DoltCmd("table", "import", "-r", "--file-type", "csv", "xy", f.Name())
		_, err = cmd.StdoutPipe()
		if err != nil {
			return err
		}
		cmd.Stderr = cmd.Stdout
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("%w: %s", err, cmd.Stderr)
		}

		// end timer, append result
		stop := time.Now()

		results.append(ImportResult{
			test:  test.Name,
			repo:  name,
			table: tab.Name,
			time:  stop.Sub(start).Seconds(),
		})

		// reset repo at end
		return repo.DoltExec("sql", "-q", "drop table xy;")
	})
}

func IterImportTables(tables []Table, cb func(t Table, f *os.File) error) error {
	tmpdir, err := os.MkdirTemp("", "repo-store-")
	if err != nil {
		return err
	}

	for _, t := range tables {
		// parse schema to get data types
		names, types := getSchemaColsTypes(t.Schema)
		// generate n rows for types
		f, err := os.CreateTemp(tmpdir, "import-data-")
		if err != nil {
			return err
		}

		switch t.Fmt {
		case "csv":
			// header
			f.WriteString(strings.Join(names, ","))
			f.WriteString("\n")
		case "dump":
		default:
			panic(fmt.Sprintf("unknown format: %s", t.Fmt))
		}
		genRows(types, t.Rows, func(r []string) {
			switch t.Fmt {
			case "csv":
				f.WriteString(strings.Join(r, ","))
				f.WriteString("\n")
			case "dump":
			default:
				panic(fmt.Sprintf("unknown format: %s", t.Fmt))
			}
		})

		err = cb(t, f)
		if err != nil {
			return err
		}
	}
	return nil
}

func getSchemaColsTypes(q string) ([]string, []sql2.Type) {
	stmt, _, err := ast.ParseOne(q)
	if err != nil {
		panic(fmt.Sprintf("invalid query: %s; %s", q, err))
	}
	var types []sql2.Type
	var names []string
	switch n := stmt.(type) {
	case *ast.DDL:
		for _, col := range n.TableSpec.Columns {
			names = append(names, col.Name.String())
			typ, err := sql2.ColumnTypeToType(&col.Type)
			if err != nil {
				panic(fmt.Sprintf("unexpected error reading type: %s", err))
			}
			types = append(types, typ)
		}
	default:
		panic(fmt.Sprintf("expected CREATE TABLE, found: %s", q))
	}
	return names, types
}

func genRows(types []sql2.Type, n int, cb func(r []string)) {
	// generate |n| rows with column types
	for i := 0; i < n; i++ {
		row := make([]string, len(types))
		for j, t := range types {
			row[j] = genValue(i, t)
		}
		cb(row)
	}
}

func genValue(i int, typ sql2.Type) string {
	switch typ.Type() {
	case sqltypes.Blob:
		return fmt.Sprintf("blob %d", i)
	case sqltypes.VarChar:
		return fmt.Sprintf("varchar %d", i)
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return strconv.Itoa(i)
	case sqltypes.Float32, sqltypes.Float64:
		return strconv.FormatFloat(float64(i), 'E', -1, 32)
	case sqltypes.Bit:
		return strconv.Itoa(i)
	case sqltypes.Geometry:
		return `{"type": "Point", "coordinates": [1,2]}`
	case sqltypes.Timestamp:
		return "2019-12-31T12:00:00Z"
	case sqltypes.Date:
		return "2019-12-31T00:00:00Z"
	default:
		panic(fmt.Sprintf("expected type, found: %s", typ))
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
