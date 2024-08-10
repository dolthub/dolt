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

package sysbench

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/creasty/defaults"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestDef is the top-level definition of tests to run.
type TestDef struct {
	Tests []Script `yaml:"tests"`
}

// Config specifies default sysbench script arguments
type Config struct {
	DbDriver  string `yaml:"dbDriver"`
	Host      string `yaml:"host"`
	Port      string `yaml:"port"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	RandType  string `yaml:"randType"`
	EventCnt  int    `yaml:"eventCnt"`
	Time      int    `yaml:"time"`
	Seed      int    `yaml:"seed"`
	TableSize int    `yaml:"tableSize"`
	Histogram bool   `yaml:"histogram"`
	ScriptDir string `yaml:"scriptDir"`
	Verbose   bool   `yaml:"verbose"`
	Prepared  bool   `yaml:"prepared"`
}

func (c Config) WithScriptDir(dir string) Config {
	c.ScriptDir = dir
	return c
}

func (c Config) WithVerbose(v bool) Config {
	c.Verbose = v
	return c
}

func (c Config) WithPrepared(v bool) Config {
	c.Prepared = v
	return c
}

func (c Config) AsOpts() []string {
	var ret []string
	if c.DbDriver != "" {
		ret = append(ret, fmt.Sprintf("--db-driver=%s", c.DbDriver))
	}
	if c.Host != "" {
		ret = append(ret, fmt.Sprintf("--mysql-host=%s", c.Host))
	}
	if c.User != "" {
		ret = append(ret, fmt.Sprintf("--mysql-user=%s", c.User))
	}
	if c.Port != "" {
		ret = append(ret, fmt.Sprintf("--mysql-port=%s", c.Port))
	}
	if c.Password != "" {
		ret = append(ret, fmt.Sprintf("--mysql-password=%s", c.Password))
	}
	if c.RandType != "" {
		ret = append(ret, fmt.Sprintf("--rand-type=%s", c.RandType))
	}
	if c.EventCnt > 0 {
		ret = append(ret, fmt.Sprintf("--events=%s", strconv.Itoa(c.EventCnt)))
	}
	if c.Time > 0 {
		ret = append(ret, fmt.Sprintf("--time=%s", strconv.Itoa(c.Time)))
	}
	if c.Prepared {
		ret = append(ret, fmt.Sprint("--db-ps-mode=auto"))
	}
	ret = append(ret,
		fmt.Sprintf("--rand-seed=%s", strconv.Itoa(c.Seed)),
		fmt.Sprintf("--table-size=%s", strconv.Itoa(c.TableSize)),
		fmt.Sprintf("--histogram=%s", strconv.FormatBool(c.Histogram)))

	return ret
}

// Script is a single test to run. The Repos and MultiRepos will be created, and
// any Servers defined within them will be started. The interactions and
// assertions defined in Conns will be run.
type Script struct {
	Name    string            `yaml:"name"`
	Repos   []driver.TestRepo `yaml:"repos"`
	Scripts []string          `yaml:"scripts"`

	// Skip the entire test with this reason.
	Skip string `yaml:"skip"`

	Results   *Results
	tmpdir    string
	scriptDir string
}

func (s *Script) UnmarshalYAML(unmarshal func(interface{}) error) error {
	defaults.Set(s)

	type plain Script
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

func ParseConfig(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	var res Config
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

type Hist struct {
	bins []float64
	cnts []int
	sum  float64
	cnt  int
	mn   float64
	md   float64
	v    float64
}

func newHist() *Hist {
	return &Hist{bins: make([]float64, 0), cnts: make([]int, 0)}
}

// add assumes bins are passed in increasing order before
// any statistics are evaluated
func (h *Hist) add(bin float64, cnt int) {
	if h.mn != 0 || h.v != 0 || h.md != 0 {
		panic("tried to edit finished histogram")
	}
	h.sum += bin * float64(cnt)
	h.cnt += cnt
	h.bins = append(h.bins, bin)
	h.cnts = append(h.cnts, cnt)
}

// mean is a lossy mean based on histogram bucket sizes
func (h *Hist) mean() float64 {
	if h.mn == 0 {
		h.mn = math.Round(h.sum*1e3/(float64(h.cnt))) / 1e3
	}
	return h.mn

}

// median is a lossy median based on histogram bucket sizes
func (h *Hist) median() float64 {
	if h.md == 0 {
		mid := h.cnt / 2
		var i int
		var cnt int
		for cnt < mid {
			cnt += h.cnts[i]
			i++
		}
		h.md = h.bins[i]
	}
	return h.md

}

// variance is a lossy sum of least squares
func (h *Hist) variance() float64 {
	if h.v == 0 {
		ss := 0.0
		for i := range h.cnts {
			ss += float64(h.cnts[i]) * math.Pow(h.bins[i]-h.mean(), 2)
		}
		h.v = math.Round(ss*1e3/(float64(h.cnt-1))) / 1e3
	}
	return h.v
}

var histRe = regexp.MustCompile(`([0-9]+\.+[0-9]+)\s+.+\s+([1-9][0-9]*)\n`)

func (h *Hist) populate(buf []byte) error {
	res := histRe.FindAllSubmatch(buf, -1)
	var bin float64
	var cnt int
	var err error
	if len(res) == 0 {
		return fmt.Errorf("histogram not found")
	}
	for _, r := range res {
		bin, err = strconv.ParseFloat(string(r[1]), 0)
		if err != nil {
			return fmt.Errorf("failed to parse bin: %s -> '%s' - '%s'; %s", r[0], r[1], r[2], err)
		}
		cnt, err = strconv.Atoi(string(r[2]))
		if err != nil {
			return fmt.Errorf("failed to parse cnt: %s -> '%s' - '%s'; %s", r[0], r[1], r[2], err)
		}
		h.add(bin, cnt)
	}
	return nil
}

func (r *Result) populateHistogram(buf []byte) error {
	r.hist = newHist()
	r.hist.populate(buf)

	r.stddev = math.Sqrt(r.hist.variance())
	r.median = r.hist.median()

	var err error
	{
		timer := regexp.MustCompile(`total time:\s+([0-9][0-9]*\.[0-9]+)s\n`)
		res := timer.FindSubmatch(buf)
		if len(res) == 0 {
			return fmt.Errorf("time not found")
		}
		time, err := strconv.ParseFloat(string(res[1]), 0)
		if err != nil {
			return fmt.Errorf("failed to parse time: %s -> '%s' ; %s", res[0], res[1], err)
		}
		r.time = math.Round(time*1e3) / 1e3
	}
	{
		itersRe := regexp.MustCompile(`total number of events:\s+([1-9][0-9]*)\n`)
		res := itersRe.FindSubmatch(buf)
		if len(res) == 0 {
			return fmt.Errorf("time not found")
		}
		r.iters, err = strconv.Atoi(string(res[1]))
		if err != nil {
			return fmt.Errorf("failed to parse bin: %s -> '%s'; %s", res[0], res[1], err)
		}
	}
	{
		avgRe := regexp.MustCompile(`avg:\s+([0-9][0-9]*\.[0-9]+)\n`)
		res := avgRe.FindSubmatch(buf)
		if len(res) == 0 {
			return fmt.Errorf("avg not found")
		}
		r.avg, err = strconv.ParseFloat(string(res[1]), 0)
		if err != nil {
			return fmt.Errorf("failed to parse avg: %s -> '%s'; %s", res[0], res[1], err)
		}
	}
	return nil
}

func (r *Result) populateAvg(buf []byte) error {
	panic("TODO")
}

type Result struct {
	server string
	detail string
	test   string

	time  float64
	iters int

	hist   *Hist
	avg    float64
	median float64
	stddev float64
}

func newResult(server, test, detail string) *Result {
	return &Result{
		server: server,
		detail: detail,
		test:   test,
	}
}

func (r *Result) String() string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "result:\n")
	b.WriteString("result:\n")
	if r.test != "" {
		fmt.Fprintf(b, "- test: '%s'\n", r.test)
	}
	if r.detail != "" {
		fmt.Fprintf(b, "- detail: '%s'\n", r.detail)
	}
	if r.server != "" {
		fmt.Fprintf(b, "- server: '%s'\n", r.server)
	}
	fmt.Fprintf(b, "- time: %.3f\n", r.time)
	fmt.Fprintf(b, "- iters: %d\n", r.iters)
	fmt.Fprintf(b, "- mean: %.3f\n", r.hist.mean())
	fmt.Fprintf(b, "- median: %.3f\n", r.median)
	fmt.Fprintf(b, "- stddev: %.3f\n", r.stddev)
	return b.String()
}

type Results struct {
	Res []*Result
}

func (r *Results) Append(ir ...*Result) {
	r.Res = append(r.Res, ir...)
}

func (r *Results) String() string {
	b := strings.Builder{}
	b.WriteString("Results:\n")
	for _, x := range r.Res {
		b.WriteString(x.String())
	}
	return b.String()
}

func (r *Results) SqlDump() string {
	b := strings.Builder{}
	b.WriteString(`CREATE TABLE IF NOT EXISTS sysbench_results (
  test_name varchar(64),
  detail varchar(64),
  server varchar(64),
  time double,
  iters int,
  avg double,
  median double,
  stdd double,
  primary key (test_name, detail, server)
);
`)

	b.WriteString("insert into sysbench_results values\n")
	for i, r := range r.Res {
		if i > 0 {
			b.WriteString(",\n  ")
		}
		b.WriteString(fmt.Sprintf(
			"('%s', '%s', '%s', %.3f, %d, %.3f, %.3f, %.3f)",
			r.test, r.detail, r.server, r.time, r.iters, r.avg, r.median, r.stddev))
	}
	b.WriteString(";\n")

	return b.String()
}

func (test *Script) InitWithTmpDir(s string) {
	test.tmpdir = s
	test.Results = new(Results)

}

// Run executes an import configuration. Test parallelism makes
// runtimes resulting from this method unsuitable for reporting.
func (test *Script) Run(t *testing.T) {
	if test.Skip != "" {
		t.Skip(test.Skip)
	}

	conf, err := ParseConfig("testdata/default-config.yaml")
	if err != nil {
		require.NoError(t, err)
	}

	if _, err = os.Stat(test.scriptDir); err != nil {
		require.NoError(t, err)
	}

	conf = conf.WithScriptDir(test.scriptDir)

	tmpdir, err := os.MkdirTemp("", "repo-store-")
	if err != nil {
		require.NoError(t, err)
	}

	results := new(Results)
	u, err := driver.NewDoltUser()
	test.Results = results
	test.InitWithTmpDir(tmpdir)
	for _, r := range test.Repos {
		var err error
		switch {
		case r.ExternalServer != nil:
			panic("unsupported")
		case r.Server != nil:
			err = test.RunSqlServerTests(r, u, conf)
		default:
			panic("unsupported")
		}
		if err != nil {
			require.NoError(t, err)
		}
		results.Append(test.Results.Res...)
	}

	fmt.Println(test.Results)
}

func modifyServerForImport(db *sql.DB) error {
	_, err := db.Exec("CREATE DATABASE sbtest")
	if err != nil {
		return err
	}
	return nil
}

// RunExternalServerTests connects to a single externally provided server to run every test
func (test *Script) RunExternalServerTests(repoName string, s *driver.ExternalServer, conf Config) error {
	conf.Port = strconv.Itoa(s.Port)
	conf.Password = s.Password
	return test.IterSysbenchScripts(conf, test.Scripts, func(script string, prep, run, clean *exec.Cmd) error {
		log.Printf("starting script: %s", script)

		db, err := driver.ConnectDB(s.User, s.Password, s.Name, s.Host, s.Port, nil)
		if err != nil {
			return err
		}
		defer db.Close()
		defer clean.Run()

		buf := new(bytes.Buffer)
		prep.Stdout = buf
		if err := prep.Run(); err != nil {
			log.Println(buf)
			return err
		}

		buf = new(bytes.Buffer)
		run.Stdout = buf
		err = run.Run()
		if err != nil {
			log.Println(buf)
			return err
		}

		// TODO scrape histogram data
		r := newResult(repoName, script, test.Name)
		if conf.Histogram {
			r.populateHistogram(buf.Bytes())
		} else {
			r.populateAvg(buf.Bytes())
		}
		test.Results.Append(r)

		return clean.Run()
	})
}

// RunSqlServerTests creates a new repo and server for every import test.
func (test *Script) RunSqlServerTests(repo driver.TestRepo, user driver.DoltUser, conf Config) error {
	return test.IterSysbenchScripts(conf, test.Scripts, func(script string, prep, run, clean *exec.Cmd) error {
		log.Printf("starting script: %s", script)
		//make a new server for every test
		server, err := newServer(user, repo, conf)
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

		if err := prep.Run(); err != nil {
			return err
		}

		buf := new(bytes.Buffer)
		run.Stdout = buf
		err = run.Run()
		if err != nil {
			log.Println(buf)
			return err
		}

		// TODO scrape histogram data
		r := newResult(repo.Name, script, test.Name)
		if conf.Histogram {
			r.populateHistogram(buf.Bytes())
		} else {
			r.populateAvg(buf.Bytes())
		}
		test.Results.Append(r)

		//if conf.Verbose {
		//	return nil
		//}
		return clean.Run()
	})
}

func newServer(u driver.DoltUser, r driver.TestRepo, conf Config) (*driver.SqlServer, error) {
	rs, err := u.MakeRepoStore()
	if err != nil {
		return nil, err
	}
	// start dolt server
	repo, err := MakeRepo(rs, r)
	if err != nil {
		return nil, err
	}
	if conf.Verbose {
		log.Printf("database at: '%s'", repo.Dir)
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

const luaExt = ".lua"

// IterSysbenchScripts returns 3 executable commands for the given script path: prepare, run, cleanup
func (test *Script) IterSysbenchScripts(conf Config, scripts []string, cb func(name string, prep, run, clean *exec.Cmd) error) error {
	newCmd := func(command, script string) *exec.Cmd {
		cmd := exec.Command("sysbench")
		cmd.Args = append(cmd.Args, conf.AsOpts()...)
		cmd.Args = append(cmd.Args, script, command)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd
	}

	for _, script := range scripts {
		p := script
		if strings.HasSuffix(script, luaExt) {
			p = path.Join(conf.ScriptDir, script)
			if _, err := os.Stat(p); err != nil {
				return fmt.Errorf("failed to run script: '%s'", err)
			}
		}
		prep := newCmd("prepare", p)
		run := newCmd("run", p)
		clean := newCmd("cleanup", p)
		if err := cb(script, prep, run, clean); err != nil {
			return err
		}
	}
	return nil
}

func RunTestsFile(t *testing.T, path, scriptDir string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	for _, test := range def.Tests {
		test.scriptDir = scriptDir
		t.Run(test.Name, test.Run)
	}
}
