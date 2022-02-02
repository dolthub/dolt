// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package suite implements a performance test suite for Noms, intended for
// measuring and reporting long running tests.
//
// Usage is similar to testify's suite:
//  1. Define a test suite struct which inherits from suite.PerfSuite.
//  2. Define methods on that struct that start with the word "Test", optionally
//     followed by digits, then followed a non-empty capitalized string.
//  3. Call suite.Run with an instance of that struct.
//  4. Run go test with the -perf <path to noms db> flag.
//
// Flags:
//  -perf.mem      Backs the database by a memory store, instead of nbs.
//  -perf.prefix   Gives the dataset IDs for test results a prefix.
//  -perf.repeat   Sets how many times tests are repeated ("reps").
//  -perf.run      Only run tests that match a regex (case insensitive).
//  -perf.testdata Sets a custom path to the Noms testdata directory.
//
// PerfSuite also supports testify/suite style Setup/TearDown methods:
//  Setup/TearDownSuite is called exactly once.
//  Setup/TearDownRep   is called for each repetition of the test runs, i.e. -perf.repeat times.
//  Setup/TearDownTest  is called for every test.
//
// Test results are written to Noms, along with a dump of the environment they were recorded in.
//
// Test names are derived from that "non-empty capitalized string": "Test" is omitted because it's
// redundant, and leading digits are omitted to allow for manual test ordering. For example:
//
//  > cat ./samples/go/csv/csv-import/perf_test.go
//  type perfSuite {
//    suite.PerfSuite
//  }
//
//  func (s *perfSuite) TestFoo() { ... }
//  func (s *perfSuite) TestZoo() { ... }
//  func (s *perfSuite) Test01Qux() { ... }
//  func (s *perfSuite) Test02Bar() { ... }
//
//  func TestPerf(t *testing.T) {
//    suite.Run("csv-import", t, &perfSuite{})
//  }
//
//  > noms serve &
//  > go test -v ./samples/go/csv/... -perf http://localhost:8000 -perf.repeat 3
//  (perf) RUN(1/3) Test01Qux (recorded as "Qux")
//  (perf) PASS:    Test01Qux (5s, paused 15s, total 20s)
//  (perf) RUN(1/3) Test02Bar (recorded as "Bar")
//  (perf) PASS:    Test02Bar (15s, paused 2s, total 17s)
//  (perf) RUN(1/3) TestFoo (recorded as "Foo")
//  (perf) PASS:    TestFoo (10s, paused 1s, total 11s)
//  (perf) RUN(1/3) TestZoo (recorded as "Zoo")
//  (perf) PASS:    TestZoo (1s, paused 42s, total 43s)
//  ...
//
//  > noms show http://localhost:8000::csv-import
//  {
//    environment: ...
//    tests: [{
//      "Bar": {elapsed: 15s, paused: 2s,  total: 17s},
//      "Foo": {elapsed: 10s, paused: 1s,  total: 11s},
//      "Qux": {elapsed: 5s,  paused: 15s, total: 20s},
//      "Zoo": {elapsed: 1s,  paused: 42s, total: 43s},
//    }, ...]
//    ...
//  }
package suite

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testifySuite "github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
)

var (
	perfFlag         = flag.String("perf", "", "The database to write perf tests to. If this isn't specified, perf tests are skipped. If you want a dry run, use \"mem\" as a database")
	perfMemFlag      = flag.Bool("perf.mem", false, "Back the test database by a memory store, not nbs. This will affect test timing, but it's provided in case you're low on disk space")
	perfPrefixFlag   = flag.String("perf.prefix", "", `Prefix for the dataset IDs where results are written. For example, a prefix of "foo/" will write test datasets like "foo/csv-import" instead of just "csv-import"`)
	perfRepeatFlag   = flag.Int("perf.repeat", 1, "The number of times to repeat each perf test")
	perfRunFlag      = flag.String("perf.run", "", "Only run perf tests that match a regular expression")
	perfTestdataFlag = flag.String("perf.testdata", "", "Path to the noms testdata directory. By default this is ../testdata relative to the noms directory")
	testNamePattern  = regexp.MustCompile("^Test[0-9]*([A-Z].*$)")
)

// PerfSuite is the core of the perf testing suite. See package documentation for details.
type PerfSuite struct {
	// T is the testing.T instance set when the suite is passed into Run.
	T *testing.T

	// W is the io.Writer to write test output, which only outputs if the verbose flag is set.
	W io.Writer

	// AtticLabs is the path to the attic-labs directory (e.g. /path/to/go/src/github.com/attic-labs).
	AtticLabs string

	// Testdata is the path to the testdata directory - typically /path/to/go/src/github.com/attic-labs, but it can be overridden with the -perf.testdata flag.
	Testdata string

	// Database is a Noms database that tests can use for reading and writing. State is persisted across a single Run of a suite.
	Database datas.Database

	VS *types.ValueStore

	// DatabaseSpec is the Noms spec of Database (typically a localhost URL).
	DatabaseSpec string

	tempFiles []*os.File
	tempDirs  []string
	paused    time.Duration
	datasetID string
}

// SetupRepSuite has a SetupRep method, which runs every repetition of the test, i.e. -perf.repeat times in total.
type SetupRepSuite interface {
	SetupRep()
}

// TearDownRepSuite has a TearDownRep method, which runs every repetition of the test, i.e. -perf.repeat times in total.
type TearDownRepSuite interface {
	TearDownRep()
}

type perfSuiteT interface {
	Suite() *PerfSuite
}

type environment struct {
	DiskUsages map[string]disk.UsageStat
	Cpus       map[int]cpu.InfoStat
	Mem        mem.VirtualMemoryStat
	Host       host.InfoStat
	Partitions map[string]disk.PartitionStat
}

type timeInfo struct {
	elapsed, paused, total time.Duration
}

type testRep map[string]timeInfo

type nopWriter struct{}

func (r nopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// Run runs suiteT and writes results to dataset datasetID in the database given by the -perf command line flag.
func Run(datasetID string, t *testing.T, suiteT perfSuiteT) {
	t.Skip()
	assert := assert.New(t)

	if !assert.NotEqual("", datasetID) {
		return
	}

	// Piggy-back off the go test -v flag.
	verboseFlag := flag.Lookup("test.v")
	assert.NotNil(verboseFlag)
	verbose := verboseFlag.Value.(flag.Getter).Get().(bool)

	if *perfFlag == "" {
		if verbose {
			fmt.Printf("(perf) Skipping %s, -perf flag not set\n", datasetID)
		}
		return
	}

	suite := suiteT.Suite()
	suite.T = t
	if verbose {
		suite.W = os.Stdout
	} else {
		suite.W = nopWriter{}
	}

	id, _ := uuid.NewUUID()
	suite.AtticLabs = filepath.Join(os.TempDir(), "attic-labs", "noms", "suite", id.String())
	suite.Testdata = *perfTestdataFlag
	if suite.Testdata == "" {
		suite.Testdata = filepath.Join(suite.AtticLabs, "testdata")
	}

	// Clean up temporary directories/files last.
	defer func() {
		for _, f := range suite.tempFiles {
			f.Close()
			file.Remove(f.Name())
		}
		for _, d := range suite.tempDirs {
			file.RemoveAll(d)
		}
	}()

	suite.datasetID = datasetID

	// This is the database the perf test results are written to.
	sp, err := spec.ForDatabase(*perfFlag)
	if !assert.NoError(err) {
		return
	}
	defer sp.Close()

	// List of test runs, each a map of test name => timing info.
	testReps := make([]testRep, *perfRepeatFlag)

	// Note: the default value of perfRunFlag is "", which is actually a valid
	// regular expression that matches everything.
	perfRunRe, err := regexp.Compile("(?i)" + *perfRunFlag)
	if !assert.NoError(err, `Invalid regular expression "%s"`, *perfRunFlag) {
		return
	}

	defer func() {
		db := sp.GetDatabase(context.Background())
		vrw := sp.GetVRW(context.Background())

		reps := make([]types.Value, *perfRepeatFlag)
		for i, rep := range testReps {
			timesSlice := types.ValueSlice{}
			for name, info := range rep {
				st, err := types.NewStruct(vrw.Format(), "", types.StructData{
					"elapsed": types.Float(info.elapsed.Nanoseconds()),
					"paused":  types.Float(info.paused.Nanoseconds()),
					"total":   types.Float(info.total.Nanoseconds()),
				})

				require.NoError(t, err)
				timesSlice = append(timesSlice, types.String(name), st)
			}
			reps[i], err = types.NewMap(context.Background(), vrw, timesSlice...)
		}

		l, err := types.NewList(context.Background(), vrw, reps...)
		require.NoError(t, err)
		record, err := types.NewStruct(vrw.Format(), "", map[string]types.Value{
			"environment":      suite.getEnvironment(vrw),
			"nomsRevision":     types.String(suite.getGitHead(path.Join(suite.AtticLabs, "noms"))),
			"testdataRevision": types.String(suite.getGitHead(suite.Testdata)),
			"reps":             l,
		})
		require.NoError(t, err)

		ds, err := db.GetDataset(context.Background(), *perfPrefixFlag+datasetID)
		require.NoError(t, err)
		_, err = datas.CommitValue(context.Background(), db, ds, record)
		require.NoError(t, err)
	}()

	if t, ok := suiteT.(testifySuite.SetupAllSuite); ok {
		t.SetupSuite()
	}

	for repIdx := 0; repIdx < *perfRepeatFlag; repIdx++ {
		testReps[repIdx] = testRep{}

		storage := &chunks.MemoryStorage{}
		memCS := storage.NewView()
		suite.DatabaseSpec = "mem://"
		suite.VS = types.NewValueStore(memCS)
		suite.Database = datas.NewTypesDatabase(suite.VS)
		defer suite.Database.Close()

		if t, ok := suiteT.(SetupRepSuite); ok {
			t.SetupRep()
		}

		for t, mIdx := reflect.TypeOf(suiteT), 0; mIdx < t.NumMethod(); mIdx++ {
			m := t.Method(mIdx)

			parts := testNamePattern.FindStringSubmatch(m.Name)
			if parts == nil {
				continue
			}

			recordName := parts[1]
			if !perfRunRe.MatchString(recordName) && !perfRunRe.MatchString(m.Name) {
				continue
			}

			if _, ok := testReps[repIdx][recordName]; ok {
				assert.Fail(`Multiple tests are named "%s"`, recordName)
				continue
			}

			if verbose {
				fmt.Printf("(perf) RUN(%d/%d) %s (as \"%s\")\n", repIdx+1, *perfRepeatFlag, m.Name, recordName)
			}

			if t, ok := suiteT.(testifySuite.SetupTestSuite); ok {
				t.SetupTest()
			}

			start := time.Now()
			suite.paused = 0

			err := callSafe(m.Name, m.Func, suiteT)

			total := time.Since(start)
			elapsed := total - suite.paused

			if verbose && err == nil {
				fmt.Printf("(perf) PASS:    %s (%s, paused for %s, total %s)\n", m.Name, elapsed, suite.paused, total)
			} else if err != nil {
				fmt.Printf("(perf) FAIL:    %s (%s, paused for %s, total %s)\n", m.Name, elapsed, suite.paused, total)
				fmt.Println(err)
			}

			if osutil.IsWindows && elapsed == 0 {
				elapsed = 1
				total = 1
			}
			testReps[repIdx][recordName] = timeInfo{elapsed, suite.paused, total}

			if t, ok := suiteT.(testifySuite.TearDownTestSuite); ok {
				t.TearDownTest()
			}
		}

		if t, ok := suiteT.(TearDownRepSuite); ok {
			t.TearDownRep()
		}
	}

	if t, ok := suiteT.(testifySuite.TearDownAllSuite); ok {
		t.TearDownSuite()
	}
}

func (suite *PerfSuite) Suite() *PerfSuite {
	return suite
}

// NewAssert returns the assert.Assertions instance for this test.
func (suite *PerfSuite) NewAssert() *assert.Assertions {
	return assert.New(suite.T)
}

// TempFile creates a temporary file, which will be automatically cleaned up by
// the perf test suite. Files will be prefixed with the test's dataset ID
func (suite *PerfSuite) TempFile() *os.File {
	f, err := os.CreateTemp("", suite.tempPrefix())
	require.NoError(suite.T, err)
	suite.tempFiles = append(suite.tempFiles, f)
	return f
}

// TempDir creates a temporary directory, which will be automatically cleaned
// up by the perf test suite. Directories will be prefixed with the test's
// dataset ID.
func (suite *PerfSuite) TempDir() string {
	d, err := os.MkdirTemp("", suite.tempPrefix())
	require.NoError(suite.T, err)
	suite.tempDirs = append(suite.tempDirs, d)
	return d
}

func (suite *PerfSuite) tempPrefix() string {
	sep := fmt.Sprintf("%c", os.PathSeparator)
	return strings.Replace(fmt.Sprintf("perf.%s.", suite.datasetID), sep, ".", -1)
}

// Pause pauses the test timer while fn is executing. Useful for omitting long setup code (e.g. copying files) from the test elapsed time.
func (suite *PerfSuite) Pause(fn func()) {
	start := time.Now()
	fn()
	suite.paused += time.Since(start)
}

// OpenGlob opens the concatenation of all files that match pattern, returned
// as []io.Reader so it can be used immediately with io.MultiReader.
//
// Large CSV files in testdata are broken up into foo.a, foo.b, etc to get
// around GitHub file size restrictions.
func (suite *PerfSuite) OpenGlob(pattern ...string) []io.Reader {
	glob, err := filepath.Glob(path.Join(pattern...))
	require.NoError(suite.T, err)

	files := make([]io.Reader, len(glob))
	for i, m := range glob {
		f, err := os.Open(m)
		require.NoError(suite.T, err)
		files[i] = f
	}

	return files
}

// CloseGlob closes all of the files, designed to be used with OpenGlob.
func (suite *PerfSuite) CloseGlob(files []io.Reader) {
	for _, f := range files {
		require.NoError(suite.T, f.(*os.File).Close())
	}
}

func callSafe(name string, fun reflect.Value, args ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	funArgs := make([]reflect.Value, len(args))
	for i, arg := range args {
		funArgs[i] = reflect.ValueOf(arg)
	}

	fun.Call(funArgs)
	return
}

func (suite *PerfSuite) getEnvironment(vrw types.ValueReadWriter) types.Value {
	env := environment{
		DiskUsages: map[string]disk.UsageStat{},
		Cpus:       map[int]cpu.InfoStat{},
		Partitions: map[string]disk.PartitionStat{},
	}

	partitions, err := disk.Partitions(false)
	require.NoError(suite.T, err)
	for _, p := range partitions {
		usage, err := disk.Usage(p.Mountpoint)
		require.NoError(suite.T, err)
		env.DiskUsages[p.Mountpoint] = *usage
		env.Partitions[p.Device] = p
	}

	cpus, err := cpu.Info()
	require.NoError(suite.T, err)
	for i, c := range cpus {
		env.Cpus[i] = c
	}

	mem, err := mem.VirtualMemory()
	require.NoError(suite.T, err)
	env.Mem = *mem

	hostInfo, err := host.Info()
	require.NoError(suite.T, err)
	env.Host = *hostInfo

	envStruct, err := marshal.Marshal(context.Background(), vrw, env)
	require.NoError(suite.T, err)
	return envStruct
}

func (suite *PerfSuite) getGitHead(dir string) string {
	stdout := &bytes.Buffer{}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Stdout = stdout
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		return ""
	}
	return strings.TrimSpace(stdout.String())
}
