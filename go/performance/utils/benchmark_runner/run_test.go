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

package benchmark_runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var runTests = os.Getenv("RUN_BENCHMARK_RUNNER_TESTS")
var doltExec = os.Getenv("BENCHMARK_RUNNER_DOLT_EXEC")
var doltVersion = os.Getenv("BENCHMARK_RUNNER_DOLT_VERSION")
var mysqlExec = os.Getenv("BENCHMARK_RUNNER_MYSQL_EXEC")
var mysqlProtocol = os.Getenv("BENCHMARK_RUNNER_MYSQL_PROTOCOL")
var mysqlSocket = os.Getenv("BENCHMARK_RUNNER_MYSQL_SOCKET")
var mysqlVersion = os.Getenv("BENCHMARK_RUNNER_MYSQL_VERSION")
var doltgresExec = os.Getenv("BENCHMARK_RUNNER_DOLTGRES_EXEC")
var doltgresVersion = os.Getenv("BENCHMARK_RUNNER_DOLTGRES_VERSION")
var doltgresConfigFilePath = os.Getenv("BENCHMARK_RUNNER_DOLTGRES_CONFIG_FILE_PATH")
var postgresExec = os.Getenv("BENCHMARK_RUNNER_POSTGRES_EXEC")
var postgresInitExec = os.Getenv("BENCHMARK_RUNNER_POSTGRES_INIT_EXEC")
var postgresVersion = os.Getenv("BENCHMARK_RUNNER_POSTGRES_VERSION")
var sysbenchLuaScripts = os.Getenv("BENCHMARK_RUNNER_SYSBENCH_LUA_SCRIPTS")
var tpccLuaScripts = os.Getenv("BENCHMARK_RUNNER_TPCC_LUA_SCRIPTS")

func TestRunner(t *testing.T) {
	if runTests == "" {
		t.Skip()
	}
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &sysbenchRunnerConfigImpl{
		Tests: []TestConfig{
			NewTestConfig("oltp_read_only", nil, false),
			//NewTestConfig("oltp_update_index", nil, false),
			//NewTestConfig("oltp_delete_insert", nil, true),
		},
		//Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_update_non_index", "oltp_insert", "bulk_insert", "oltp_write_only", "oltp_delete"),
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test",
				Version:       doltVersion,
				ResultsFormat: CsvFormat,
				ServerExec:    doltExec,
			},
		},
		ScriptDir: sysbenchLuaScripts,
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=30",
			"--rand-type=uniform",
			"--time=30",
			"--percentile=50",
		},
		InitBigRepo: true,
	}

	err = Run(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}

func selectTests(names ...string) []TestConfig {
	tests := make([]TestConfig, len(names))
	for i := range names {
		tests[i] = &testConfigImpl{Name: names[i], FromScript: false}
	}
	return tests
}

func TestDoltMysqlSysbenchRunner(t *testing.T) {
	if runTests == "" {
		t.Skip()
	}
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &sysbenchRunnerConfigImpl{
		Tests: []TestConfig{
			NewTestConfig("oltp_read_only", nil, false),
			//NewTestConfig("oltp_update_index", nil, false),
			//NewTestConfig("oltp_delete_insert", nil, true),
		},
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test-dolt",
				Version:       doltVersion,
				ResultsFormat: CsvFormat,
				ServerExec:    doltExec,
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql",
				Port:               3606,
				Version:            mysqlVersion,
				ResultsFormat:      CsvFormat,
				ServerExec:         mysqlExec,
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: mysqlProtocol,
				Socket:             mysqlSocket,
			},
		},
		ScriptDir: sysbenchLuaScripts,
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=30",
			"--rand-type=uniform",
			"--time=30",
			"--percentile=50",
		},
		InitBigRepo: true,
	}

	err = Run(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDoltgresPostgresSysbenchRunner(t *testing.T) {
	if runTests == "" {
		t.Skip()
	}
	if doltgresConfigFilePath == "" {
		t.Skip("skipping doltgres/postgres benchmark runner tests, no config file specified")
	}

	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &sysbenchRunnerConfigImpl{
		Tests: []TestConfig{
			NewTestConfig("oltp_read_only", nil, false),
			//NewTestConfig("oltp_update_index", nil, false),
		},
		Servers: []ServerConfig{
			&postgresServerConfigImpl{
				Id:            "test-postgres",
				Host:          "127.0.0.1",
				Version:       postgresVersion,
				ResultsFormat: CsvFormat,
				ServerExec:    postgresExec,
				InitExec:      postgresInitExec,
				ServerUser:    "root",
			},
			&doltgresServerConfigImpl{
				Id:             "test-doltgres",
				Port:           4433,
				Host:           "127.0.0.1",
				ConfigFilePath: doltgresConfigFilePath,
				Version:        doltgresVersion,
				ResultsFormat:  CsvFormat,
				ServerExec:     doltgresExec,
			},
		},
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=30",
			"--rand-type=uniform",
			"--time=30",
			"--percentile=50",
		},
	}

	err = Run(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDoltProfiler(t *testing.T) {
	if runTests == "" {
		t.Skip()
	}
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	id := "test-dolt-profile"
	conf := &sysbenchRunnerConfigImpl{
		Tests: []TestConfig{
			NewTestConfig("oltp_read_only", nil, false),
		},
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            id,
				Version:       doltVersion,
				ResultsFormat: CsvFormat,
				ServerExec:    doltExec,
				ServerProfile: CpuServerProfile,
				ProfilePath:   dir,
			},
		},
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=30",
			"--rand-type=uniform",
			"--time=30",
			"--percentile=50",
		},
	}

	err = Run(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}

	expectedProfile := filepath.Join(dir, fmt.Sprintf("%s_%s", id, cpuProfileFilename))
	if _, err := os.Stat(expectedProfile); errors.Is(err, os.ErrNotExist) {
		log.Fatal("failed to create dolt cpu profile")
	}
}

func TestDoltMysqlTpccRunner(t *testing.T) {
	t.Skip() // skip for now since this is kinda slow for pr ci
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &tpccConfigImpl{
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test-dolt-tpcc",
				Version:       doltVersion,
				ResultsFormat: CsvFormat,
				ServerExec:    doltExec,
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql-tpcc",
				Port:               3606,
				Version:            mysqlVersion,
				ResultsFormat:      CsvFormat,
				ServerExec:         mysqlExec,
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: mysqlProtocol,
				Socket:             mysqlSocket,
			},
		},
		ScriptDir: tpccLuaScripts,
	}

	err = RunTpcc(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}
