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
			NewTestConfig("oltp_read_write", nil, false),
			NewTestConfig("oltp_update_index", nil, false),
			NewTestConfig("oltp_delete_insert", nil, true),
		},
		//Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_update_non_index", "oltp_insert", "bulk_insert", "oltp_write_only", "oltp_delete"),
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test",
				Version:       "HEAD",
				ResultsFormat: CsvFormat,
				ServerExec:    "/usr/local/bin/dolt",
			},
		},
		ScriptDir: "/sysbench-lua-scripts",
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
			NewTestConfig("oltp_read_write", nil, false),
			NewTestConfig("oltp_update_index", nil, false),
			NewTestConfig("oltp_delete_insert", nil, true),
		},
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test-dolt",
				Version:       "HEAD",
				ResultsFormat: CsvFormat,
				ServerExec:    "/usr/local/bin/dolt",
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql",
				Port:               3606,
				Version:            "8.0.35",
				ResultsFormat:      CsvFormat,
				ServerExec:         "/usr/sbin/mysqld",
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: "unix",
			},
		},
		ScriptDir: "/sysbench-lua-scripts",
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
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &sysbenchRunnerConfigImpl{
		Tests: []TestConfig{
			NewTestConfig("oltp_read_write", nil, false),
			NewTestConfig("oltp_update_index", nil, false),
		},
		Servers: []ServerConfig{
			&postgresServerConfigImpl{
				Id:            "test-postgres",
				Host:          "127.0.0.1",
				Version:       "15.5",
				ResultsFormat: CsvFormat,
				ServerExec:    "/usr/lib/postgresql/15/bin/postgres",
				InitExec:      "/usr/lib/postgresql/15/bin/initdb",
				ServerUser:    "root",
			},
			&doltgresServerConfigImpl{
				Id:            "test-doltgres",
				Port:          4433,
				Host:          "127.0.0.1",
				Version:       "b139dfb",
				ResultsFormat: CsvFormat,
				ServerExec:    "doltgres",
			},
		},
		ScriptDir: "/sysbench-lua-scripts",
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
			NewTestConfig("oltp_read_write", nil, false),
		},
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            id,
				Version:       "HEAD",
				ResultsFormat: CsvFormat,
				ServerExec:    "/usr/local/bin/dolt",
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
	if runTests == "" {
		t.Skip()
	}
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
				Version:       "HEAD",
				ResultsFormat: CsvFormat,
				ServerExec:    "/usr/local/bin/dolt",
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql-tpcc",
				Port:               3606,
				Version:            "8.0.35",
				ResultsFormat:      CsvFormat,
				ServerExec:         "/usr/sbin/mysqld",
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: "unix",
			},
		},
		ScriptDir: "/sysbench-tpcc",
	}

	err = RunTpcc(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}
