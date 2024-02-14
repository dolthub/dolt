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

func TestRunner(t *testing.T) {
	t.Skip()
	dir := t.TempDir()
	log.Println(dir)
	err := os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &sysbenchRunnerConfigImpl{
		Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_delete_insert"),
		//Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_update_non_index", "oltp_insert", "bulk_insert", "oltp_write_only", "oltp_delete"),
		Servers: []ServerConfig{
			&doltServerConfigImpl{
				Id:            "test",
				Version:       "0.39.2",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/max-hoffman/go/bin/dolt",
			},
		},
		ScriptDir: "/Users/max-hoffman/Documents/dolthub/sysbench-lua-scripts",
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=10000",
			"--rand-type=uniform",
			"--time=120",
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
	t.Skip()
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
				Version:       "1.33.0",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/dustin/go/bin/dolt",
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql",
				Host:               "127.0.0.1",
				Port:               3606,
				Version:            "8.0.35",
				ResultsFormat:      CsvFormat,
				ServerExec:         "/opt/homebrew/bin/mysqld",
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: "tcp",
			},
		},
		ScriptDir: "/Users/dustin/src/sysbench-lua-scripts",
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=10000",
			"--rand-type=uniform",
			"--time=120",
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
	t.Skip()
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
				ServerExec:    "/opt/homebrew/opt/postgresql@15/bin/postgres",
				InitExec:      "/opt/homebrew/opt/postgresql@15/bin/initdb",
				ServerUser:    "root",
			},
			&doltgresServerConfigImpl{
				Id:            "test-doltgres",
				Port:          4433,
				Host:          "127.0.0.1",
				Version:       "b139dfb",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/dustin/go/bin/doltgres",
			},
		},
		ScriptDir: "/Users/dustin/src/sysbench-lua-scripts",
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=10000",
			"--rand-type=uniform",
			"--time=120",
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
	t.Skip()
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
				Version:       "1.33.0",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/dustin/go/bin/dolt",
				ServerProfile: CpuServerProfile,
				ProfilePath:   dir,
			},
		},
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=10000",
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
	t.Skip()
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
				Version:       "1.33.0",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/dustin/go/bin/dolt",
			},
			&mysqlServerConfigImpl{
				Id:                 "test-mysql-tpcc",
				Host:               "127.0.0.1",
				Port:               3606,
				Version:            "8.0.35",
				ResultsFormat:      CsvFormat,
				ServerExec:         "/opt/homebrew/bin/mysqld",
				ServerUser:         "root",
				SkipLogBin:         true,
				ConnectionProtocol: "tcp",
			},
		},
		ScriptDir: "/Users/dustin/src/sysbench-tpcc",
	}

	err = RunTpcc(context.Background(), conf)
	if err != nil {
		log.Fatal(err)
	}
}
