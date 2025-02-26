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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/creasty/defaults"
	sql2 "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

const defaultBatchSize = 500

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
	Name   string            `yaml:"name"`
	Repos  []driver.TestRepo `yaml:"repos"`
	Tables []Table           `yaml:"tables"`

	// Skip the entire test with this reason.
	Skip string `yaml:"skip"`

	Results *ImportResults
	files   map[uint64]*os.File
	tmpdir  string
}

type Table struct {
	Name        string `yaml:"name"`
	Schema      string `yaml:"schema"`
	Rows        int    `default:"200000" yaml:"rows"`
	Fmt         string `default:"csv" yaml:"fmt"`
	Shuffle     bool   `default:"false" yaml:"shuffle"`
	Batch       bool   `default:"false" yaml:"batch"`
	TargetTable string
}

func (s *Table) UnmarshalYAML(unmarshal func(interface{}) error) error {
	defaults.Set(s)

	type plain Table
	if err := unmarshal((*plain)(s)); err != nil {
		return err
	}

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

func MakeRepo(rs driver.RepoStore, r driver.TestRepo) (driver.Repo, error) {
	repo, err := rs.MakeRepo(r.Name)
	if err != nil {
		return driver.Repo{}, err
	}
	return repo, nil
}

func MakeServer(dc driver.DoltCmdable, s *driver.Server) (*driver.SqlServer, error) {
	if s == nil {
		return nil, nil
	}
	opts := []driver.SqlServerOpt{driver.WithArgs(s.Args...)}
	if s.Port != 0 {
		opts = append(opts, driver.WithPort(s.Port))
	}
	server, err := driver.StartSqlServer(dc, opts...)
	if err != nil {
		return nil, err
	}

	return server, nil
}

type ImportResult struct {
	detail string
	server string
	test   string
	time   float64
	rows   int
	fmt    string
	sorted bool
	batch  bool
}

func (r ImportResult) String() string {
	return fmt.Sprintf("- %s/%s/%s: %.2fs\n", r.test, r.server, r.detail, r.time)
}

type ImportResults struct {
	res []ImportResult
}

func (r *ImportResults) append(ir ImportResult) {
	r.res = append(r.res, ir)
}

func (r *ImportResults) String() string {
	b := strings.Builder{}
	b.WriteString("Results:\n")
	for _, x := range r.res {
		b.WriteString(x.String())
	}
	return b.String()
}

func (r *ImportResults) SqlDump() string {
	b := strings.Builder{}
	b.WriteString(`CREATE TABLE IF NOT EXISTS import_perf_results (
  test_name varchar(64),
  server varchar(64),
  detail varchar(64),
  row_cnt int,
  time double,
  file_format varchar(8),
  sorted bool,
  batch bool,
  primary key (test_name, detail, server)
);
`)

	b.WriteString("insert into import_perf_results values\n")
	for i, r := range r.res {
		if i > 0 {
			b.WriteString(",\n  ")
		}
		var sorted int
		if r.sorted {
			sorted = 1
		}
		var batch int
		if r.batch {
			batch = 1
		}
		b.WriteString(fmt.Sprintf(
			"('%s', '%s', '%s', %d, %.2f, '%s', %b, %b)",
			r.test, r.server, r.detail, r.rows, r.time, r.fmt, sorted, batch))
	}
	b.WriteString(";\n")

	return b.String()
}

func (test *ImportTest) InitWithTmpDir(s string) {
	test.tmpdir = s
	test.files = make(map[uint64]*os.File)
}

// Run executes an import configuration. Test parallelism makes
// runtimes resulting from this method unsuitable for reporting.
func (test *ImportTest) Run(t *testing.T) {
	if test.Skip != "" {
		t.Skip(test.Skip)
	}
	var err error
	if test.Results == nil {
		test.Results = new(ImportResults)
		tmp, err := os.MkdirTemp("", "repo-store-")
		if err != nil {
			require.NoError(t, err)
		}
		test.InitWithTmpDir(tmp)
	}

	u, err := driver.NewDoltUser()
	for _, r := range test.Repos {
		if r.ExternalServer != nil {
			err := test.RunExternalServerTests(r.Name, r.ExternalServer)
			require.NoError(t, err)
		} else if r.Server != nil {
			err = test.RunSqlServerTests(r, u)
			require.NoError(t, err)
		} else {
			err = test.RunCliTests(r, u)
			require.NoError(t, err)
		}
	}
	fmt.Println(test.Results.String())
}

// RunExternalServerTests connects to a single externally provided server to run every test
func (test *ImportTest) RunExternalServerTests(repoName string, s *driver.ExternalServer) error {
	return test.IterImportTables(test.Tables, func(tab Table, f *os.File) error {
		db, err := driver.ConnectDB(s.User, s.Password, s.Name, s.Host, s.Port, nil)
		if err != nil {
			return err
		}
		defer db.Close()
		switch tab.Fmt {
		case "csv":
			return test.benchLoadData(repoName, db, tab, f)
		case "sql":
			return test.benchSql(repoName, db, tab, f)
		default:
			return fmt.Errorf("unexpected table import format: %s", tab.Fmt)
		}
	})
}

// RunSqlServerTests creates a new repo and server for every import test.
func (test *ImportTest) RunSqlServerTests(repo driver.TestRepo, user driver.DoltUser) error {
	return test.IterImportTables(test.Tables, func(tab Table, f *os.File) error {
		//make a new server for every test
		server, err := newServer(user, repo)
		if err != nil {
			return err
		}
		defer server.GracefulStop()

		db, err := server.DB(driver.Connection{User: "root", Pass: ""})
		if err != nil {
			return err
		}
		err = modifyServerForImport(db)
		if err != nil {
			return err
		}

		switch tab.Fmt {
		case "csv":
			return test.benchLoadData(repo.Name, db, tab, f)
		case "sql":
			return test.benchSql(repo.Name, db, tab, f)
		default:
			return fmt.Errorf("unexpected table import format: %s", tab.Fmt)
		}
	})
}

func newServer(u driver.DoltUser, r driver.TestRepo) (*driver.SqlServer, error) {
	rs, err := u.MakeRepoStore()
	if err != nil {
		return nil, err
	}
	// start dolt server
	repo, err := MakeRepo(rs, r)
	if err != nil {
		return nil, err
	}
	server, err := MakeServer(repo, r.Server)
	if err != nil {
		return nil, err
	}
	if server != nil {
		server.DBName = r.Name
	}
	return server, nil
}

func modifyServerForImport(db *sql.DB) error {
	_, err := db.Exec("SET GLOBAL local_infile=1 ")
	if err != nil {
		return err
	}
	return nil
}

func (test *ImportTest) benchLoadData(repoName string, db *sql.DB, tab Table, f *os.File) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, tab.Schema)
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

	rows, err = conn.QueryContext(ctx, q)
	if err == nil {
		rows.Close()
	} else {
		return err
	}

	runtime := time.Since(start)

	test.Results.append(ImportResult{
		test:   test.Name,
		server: repoName,
		detail: tab.Name,
		time:   runtime.Seconds(),
		rows:   tab.Rows,
		fmt:    tab.Fmt,
		sorted: !tab.Shuffle,
		batch:  tab.Batch,
	})

	rows, err = conn.QueryContext(
		ctx,
		fmt.Sprintf("drop table %s;", tab.TargetTable),
	)
	if err == nil {
		rows.Close()
	} else {
		return err
	}

	return nil
}

func (test *ImportTest) benchSql(repoName string, db *sql.DB, tab Table, f *os.File) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, tab.Schema)
	if err == nil {
		rows.Close()
	} else {
		return err
	}

	defer conn.ExecContext(
		ctx,
		fmt.Sprintf("drop table %s;", tab.TargetTable),
	)

	f.Seek(0, 0)
	s := bufio.NewScanner(f)
	s.Split(ScanQueries)
	start := time.Now()

	for lineno := 1; s.Scan(); lineno++ {
		line := s.Text()
		var br bool
		switch {
		case line == "":
			return fmt.Errorf("unexpected blank line, line number: %d", lineno)
		case line == "\n":
			br = true
		default:
		}
		if br {
			break
		}

		if err := s.Err(); err != nil {
			return fmt.Errorf("%s:%d: %v", f.Name(), lineno, err)
		}

		_, err := conn.ExecContext(ctx, line)
		if err != nil {
			return err
		}

	}

	runtime := time.Since(start)

	test.Results.append(ImportResult{
		test:   test.Name,
		server: repoName,
		detail: tab.Name,
		time:   runtime.Seconds(),
		rows:   tab.Rows,
		fmt:    tab.Fmt,
		sorted: !tab.Shuffle,
		batch:  tab.Batch,
	})

	if err == nil {
		rows.Close()
	} else {
		return err
	}

	return nil
}

func ScanQueries(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ';'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

// RunCliTests runs each import test on a new dolt repo to avoid accumulated
// startup costs over time between tests.
func (test *ImportTest) RunCliTests(r driver.TestRepo, user driver.DoltUser) error {
	return test.IterImportTables(test.Tables, func(tab Table, f *os.File) error {
		var err error

		rs, err := user.MakeRepoStore()
		if err != nil {
			return err
		}

		repo, err := MakeRepo(rs, r)
		if err != nil {
			return err
		}

		err = repo.DoltExec("sql", "-q", tab.Schema)
		if err != nil {
			return err
		}

		// start timer
		start := time.Now()

		cmd := repo.DoltCmd("table", "import", "-r", "--file-type", tab.Fmt, tab.TargetTable, f.Name())
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
		runtime := time.Since(start)

		test.Results.append(ImportResult{
			test:   test.Name,
			server: r.Name,
			detail: tab.Name,
			time:   runtime.Seconds(),
			rows:   tab.Rows,
			fmt:    tab.Fmt,
			sorted: !tab.Shuffle,
			batch:  tab.Batch,
		})

		// reset repo at end
		return repo.DoltExec("sql", "-q", fmt.Sprintf("drop table %s", tab.TargetTable))
	})
}

func (test *ImportTest) IterImportTables(tables []Table, cb func(t Table, f *os.File) error) error {
	for _, t := range tables {
		key, err := tableKey(t)
		if err != nil {
			return err
		}
		table, names, types := parseTableAndSchema(t.Schema)
		t.TargetTable = table

		if f, ok := test.files[key]; ok {
			// short circuit if we've already made file for schema/row count
			err = cb(t, f)
			if err != nil {
				return err
			}
			continue
		}

		rows := make([]string, 0, t.Rows)
		genRows(types, t.Rows, t.Fmt, func(r []string) {
			switch t.Fmt {
			case "csv":
				rows = append(rows, strings.Join(r, ","))
			case "sql":
				rows = append(rows, fmt.Sprintf("(%s)", strings.Join(r, ", ")))
			default:
				panic(fmt.Sprintf("unknown format: %s", t.Fmt))
			}
		})

		if t.Shuffle {
			rand.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		}

		f, err := os.CreateTemp(test.tmpdir, "import-data-")
		if err != nil {
			return err
		}

		switch t.Fmt {
		case "csv":
			fmt.Fprintf(f, "%s\n", strings.Join(names, ","))
			for _, r := range rows {
				fmt.Fprintf(f, "%s\n", r)
			}
		case "sql":
			if t.Batch {
				batchSize := defaultBatchSize
				var i int
				for i+batchSize < len(rows) {
					fmt.Fprint(f, newBatch(t.TargetTable, rows[i:i+batchSize]))
					i += batchSize
				}
				if i < len(rows) {
					fmt.Fprint(f, newBatch(t.TargetTable, rows[i:]))
				}
			} else {
				for _, r := range rows {
					fmt.Fprintf(f, "INSERT INTO %s VALUES %s;\n", t.TargetTable, r)
				}
			}
		default:
			panic(fmt.Sprintf("unknown format: %s", t.Fmt))
		}

		// cache file for schema and row count
		test.files[key] = f

		err = cb(t, f)
		if err != nil {
			return err
		}
	}
	return nil
}

func newBatch(name string, rows []string) string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("INSERT INTO %s VALUES\n", name))
	for _, r := range rows[:len(rows)-1] {
		b.WriteString("  ")
		b.WriteString(r)
		b.WriteString(",\n")
	}
	b.WriteString("  ")
	b.WriteString(rows[len(rows)-1])
	b.WriteString(";\n")

	return b.String()
}

func tableKey(t Table) (uint64, error) {
	hash := xxhash.New()
	_, err := hash.Write([]byte(t.Schema))
	if err != nil {
		return 0, err
	}
	if _, err := hash.Write([]byte(fmt.Sprintf("%#v,", t.Rows))); err != nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	_, err = hash.Write([]byte(t.Fmt))
	if err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

func parseTableAndSchema(q string) (string, []string, []sql2.Type) {
	stmt, _, err := ast.ParseOne(context.Background(), q)
	if err != nil {
		panic(fmt.Sprintf("invalid query: %s; %s", q, err))
	}
	var types []sql2.Type
	var names []string
	var table string
	switch n := stmt.(type) {
	case *ast.DDL:
		table = n.Table.String()
		for _, col := range n.TableSpec.Columns {
			names = append(names, col.Name.String())
			typ, err := gmstypes.ColumnTypeToType(&col.Type)
			if err != nil {
				panic(fmt.Sprintf("unexpected error reading type: %s", err))
			}
			types = append(types, typ)
		}
	default:
		panic(fmt.Sprintf("expected CREATE TABLE, found: %s", q))
	}
	return table, names, types
}

func genRows(types []sql2.Type, n int, fmt string, cb func(r []string)) {
	// generate |n| rows with column types
	for i := 0; i < n; i++ {
		row := make([]string, len(types))
		for j, t := range types {
			switch fmt {
			case "sql":
				switch t.Type() {
				case sqltypes.Blob, sqltypes.VarChar, sqltypes.Timestamp, sqltypes.Date:
					row[j] = "'" + genValue(i, t) + "'"
				default:
					row[j] = genValue(i, t)
				}
			default:
				row[j] = genValue(i, t)
			}
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
