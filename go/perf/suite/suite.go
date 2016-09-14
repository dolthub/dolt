// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package suite implements a performance test suite for Noms, intended for measuring and reporting
// long running tests.
//
// Usage is similar to testify's suite:
//  1. Define a test suite struct which inherits from suite.PerfSuite.
//  2. Define methods on that struct that start with the word "Test", optionally followed by
//     digits, then followed a non-empty capitalized string.
//  3. Call suite.Run with an instance of that struct.
//  4. Run go test with the -perf <path to noms db> flag.
//
// Flags:
//  -perf.mem      backs the database by a memory store, instead of a (temporary) leveldb.
//  -perf.prefix   gives the dataset IDs for test results a prefix
//  -perf.repeat   sets how many times tests are repeated ("reps").
//  -perf.testdata sets a custom path to the Noms testdata directory.
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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
	testifySuite "github.com/attic-labs/testify/suite"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

var (
	perfFlag         = flag.String("perf", "", "The database to write perf tests to. If this isn't specified, perf tests are skipped. If you want a dry run, use \"mem\" as a database")
	perfMemFlag      = flag.Bool("perf.mem", false, "Back the test database by a memory store, not leveldb. This will affect test timing, but it's provided in case you're low on disk space")
	perfPrefixFlag   = flag.String("perf.prefix", "", `Prefix for the dataset IDs where results are written. For example, a prefix of "foo/" will write test datasets like "foo/csv-import" instead of just "csv-import"`)
	perfRepeatFlag   = flag.Int("perf.repeat", 1, "The number of times to repeat each perf test")
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

	// DatabaseSpec is the Noms spec of Database (typically a localhost URL).
	DatabaseSpec string

	tempFiles []*os.File
	tempDirs  []string
	paused    time.Duration
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
	assert := assert.New(t)

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

	gopath := os.Getenv("GOPATH")
	assert.NotEmpty(gopath)
	suite.AtticLabs = path.Join(gopath, "src", "github.com", "attic-labs")
	suite.Testdata = *perfTestdataFlag
	if suite.Testdata == "" {
		suite.Testdata = path.Join(suite.AtticLabs, "testdata")
	}

	// Clean up temporary directories/files last.
	defer func() {
		for _, f := range suite.tempFiles {
			os.Remove(f.Name())
		}
		for _, d := range suite.tempDirs {
			os.RemoveAll(d)
		}
	}()

	// This is the database the perf test results are written to.
	db, err := spec.GetDatabase(*perfFlag)
	assert.NoError(err)

	// List of test runs, each a map of test name => timing info.
	testReps := make([]testRep, *perfRepeatFlag)

	defer func() {
		reps := make([]types.Value, *perfRepeatFlag)
		for i, rep := range testReps {
			timesSlice := types.ValueSlice{}
			for name, info := range rep {
				timesSlice = append(timesSlice, types.String(name), types.NewStruct("", types.StructData{
					"elapsed": types.Number(info.elapsed.Nanoseconds()),
					"paused":  types.Number(info.paused.Nanoseconds()),
					"total":   types.Number(info.total.Nanoseconds()),
				}))
			}
			reps[i] = types.NewMap(timesSlice...)
		}

		record := types.NewStruct("", map[string]types.Value{
			"environment":      suite.getEnvironment(),
			"nomsRevision":     types.String(suite.getGitHead(path.Join(suite.AtticLabs, "noms"))),
			"testdataRevision": types.String(suite.getGitHead(suite.Testdata)),
			"reps":             types.NewList(reps...),
		})

		ds := dataset.NewDataset(db, *perfPrefixFlag+datasetID)
		var err error
		ds, err = ds.CommitValue(record)
		assert.NoError(err)
		assert.NoError(db.Close())
	}()

	if t, ok := suiteT.(testifySuite.SetupAllSuite); ok {
		t.SetupSuite()
	}

	for repIdx := 0; repIdx < *perfRepeatFlag; repIdx++ {
		testReps[repIdx] = testRep{}

		serverHost, stopServerFn := suite.startServer()
		suite.DatabaseSpec = serverHost
		suite.Database = datas.NewRemoteDatabase(serverHost, "")

		if t, ok := suiteT.(SetupRepSuite); ok {
			t.SetupRep()
		}

		for t, mIdx := reflect.TypeOf(suiteT), 0; mIdx < t.NumMethod(); mIdx++ {
			m := t.Method(mIdx)

			parts := testNamePattern.FindStringSubmatch(m.Name)
			if parts == nil {
				continue
			}

			if t, ok := suiteT.(testifySuite.SetupTestSuite); ok {
				t.SetupTest()
			}

			recordName := parts[1]
			if verbose {
				fmt.Printf("(perf) RUN(%d/%d) %s (as \"%s\")\n", repIdx+1, *perfRepeatFlag, m.Name, recordName)
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

			testReps[repIdx][recordName] = timeInfo{elapsed, suite.paused, total}

			if t, ok := suiteT.(testifySuite.TearDownTestSuite); ok {
				t.TearDownTest()
			}
		}

		if t, ok := suiteT.(TearDownRepSuite); ok {
			t.TearDownRep()
		}

		stopServerFn()
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

// TempFile creates a temporary file, which will be automatically cleaned up by the perf test suite.
func (suite *PerfSuite) TempFile(prefix string) *os.File {
	f, err := ioutil.TempFile("", prefix)
	assert.NoError(suite.T, err)
	suite.tempFiles = append(suite.tempFiles, f)
	return f
}

// TempDir creates a temporary directory, which will be automatically cleaned up by the perf test suite.
func (suite *PerfSuite) TempDir(prefix string) string {
	d, err := ioutil.TempDir("", prefix)
	assert.NoError(suite.T, err)
	suite.tempDirs = append(suite.tempDirs, d)
	return d
}

// Pause pauses the test timer while fn is executing. Useful for omitting long setup code (e.g. copying files) from the test elapsed time.
func (suite *PerfSuite) Pause(fn func()) {
	start := time.Now()
	fn()
	suite.paused += time.Since(start)
}

func callSafe(name string, fun reflect.Value, args ...interface{}) error {
	funArgs := make([]reflect.Value, len(args))
	for i, arg := range args {
		funArgs[i] = reflect.ValueOf(arg)
	}
	return d.Try(func() {
		fun.Call(funArgs)
	})
}

func (suite *PerfSuite) getEnvironment() types.Value {
	assert := suite.NewAssert()

	env := environment{
		DiskUsages: map[string]disk.UsageStat{},
		Cpus:       map[int]cpu.InfoStat{},
		Partitions: map[string]disk.PartitionStat{},
	}

	partitions, err := disk.Partitions(false)
	assert.NoError(err)
	for _, p := range partitions {
		usage, err := disk.Usage(p.Mountpoint)
		assert.NoError(err)
		env.DiskUsages[p.Mountpoint] = *usage
		env.Partitions[p.Device] = p
	}

	cpus, err := cpu.Info()
	assert.NoError(err)
	for i, c := range cpus {
		env.Cpus[i] = c
	}

	mem, err := mem.VirtualMemory()
	assert.NoError(err)
	env.Mem = *mem

	hostInfo, err := host.Info()
	assert.NoError(err)
	env.Host = *hostInfo

	envStruct, err := marshal.Marshal(env)
	assert.NoError(err)
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

func (suite *PerfSuite) startServer() (host string, stopFn func()) {
	// This is the temporary database for tests to use.
	//
	// * Why not use a local database + memory store?
	// Firstly, because the spec would be "mem", and the spec library doesn't know how to reuse stores.
	// Secondly, because it's an unrealistic performance measurement.
	//
	// * Why use a remote (HTTP) database?
	// It's more realistic to exercise the HTTP stack, even if it's just talking over localhost.
	//
	// * Why provide an option for leveldb vs memory underlying store?
	// Again, leveldb is more realistic than memory, and in common cases disk space > memory space.
	// However, on this developer's laptop, there is actually very little disk space, and a lot of memory;
	// plus making the test run a little bit faster locally is nice.
	var chunkStore chunks.ChunkStore
	if *perfMemFlag {
		chunkStore = chunks.NewMemoryStore()
	} else {
		ldbDir := suite.TempDir("suite.suite")
		chunkStore = chunks.NewLevelDBStoreUseFlags(ldbDir, "")
	}

	server := datas.NewRemoteDatabaseServer(chunkStore, 0)
	portChan := make(chan int)
	server.Ready = func() { portChan <- server.Port() }
	go server.Run()

	port := <-portChan
	host = fmt.Sprintf("http://localhost:%d", port)
	stopFn = func() { server.Stop() }
	return
}
