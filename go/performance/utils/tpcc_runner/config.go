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

package tpcc_runner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	defaultHost = "127.0.0.1"
	defaultUser = "root"

	tpccUserLocal = "FILL_IN"
	tpccPassLocal = "FILL"

	defaultSocket = "/var/run/mysqld/mysqld.sock"
	tcpProtocol   = "tcp"
	unixProtocol  = "unix"
)

var defaultTpccParams = []string{
	fmt.Sprintf("--mysql-db=%s", dbName),
	"--db-driver=mysql",
}

// TpccBenchmarkConfig represents a configuration for an execution of the TPCC Benchmark. It executes a series of tests
// against different ServerConfigurations.
type TpccBenchmarkConfig struct {
	// RuntimeOS is the platform the benchmarks ran on
	RuntimeOS string

	// RuntimeGoArch is the runtime architecture
	RuntimeGoArch string

	// ScriptDir represents the location of the TPCC tests
	ScriptDir string

	// Servers are the servers to benchmark.
	Servers []*sysbench_runner.ServerConfig

	//// Tests are the tests to run. If no tests are provided,
	//// the default tests will be used. We run tests against each type of Server (i.e Dolt and MySQL).
	//Tests []*TpccTest

	// ScaleFactors represent the scale at which to run each TpccBenchmark at.
	ScaleFactors []int
}

func NeeTpccConfig() *TpccBenchmarkConfig {
	return &TpccBenchmarkConfig{
		Servers:      make([]*sysbench_runner.ServerConfig, 0),
		ScaleFactors: make([]int, 0),
	}
}

// FromFileConfig returns a validated Config based on the config file at the configPath
func FromFileConfig(configPath string) (*TpccBenchmarkConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := NeeTpccConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// TpccTest encapsulates an End to End prepare, run, cleanup test case.
type TpccTest struct {
	Id string

	Name string

	Params *TpccTestParams
}

type TpccTestParams struct {
	// NumThreads represents the number of threads running queries concurrently.
	NumThreads int

	// ScaleFactor represents the number of warehouse to test this at scale.
	ScaleFactor int

	// Tables represents the number of tables to create. TODO: Need to be more specific here
	Tables int

	// TrxLevel represents what transaction level to use
	TrxLevel string

	// ReportCSV determines whether to report output as a csv.
	ReportCSV bool

	// ReportInterval defines how often the tpcc benchmark outputs performance stats.
	ReportInterval int

	// Time represents how long
	Time int
}

func NewDefaultTpccParams() *TpccTestParams {
	// TODO: Should these params be abstracted to the config file? Probably I guess :)
	return &TpccTestParams{
		NumThreads:     1,
		ScaleFactor:    1,
		Tables:         1,
		TrxLevel:       "RR", // TODO: Not actually uyses
		ReportCSV:      true,
		ReportInterval: 1,
		Time:           30,
	}
}

func NewTpccTest(name string, params *TpccTestParams) *TpccTest {
	return &TpccTest{
		Id:     uuid.New().String(),
		Name:   name,
		Params: params,
	}
}

// getArgs returns a test's args for all TPCC steps
func (t *TpccTest) getArgs(serverConfig *sysbench_runner.ServerConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultTpccParams...)

	params = append(params, fmt.Sprintf("--mysql-host=%s", serverConfig.Host))

	// TODO: Handle MySQL Socket case

	// handle sysbench user for local mysql server
	if serverConfig.Server == sysbench_runner.MySql && serverConfig.Host == defaultHost {
		params = append(params, "--mysql-socket=/tmp/mysql.sock")
		params = append(params, fmt.Sprintf("--mysql-password=%s", tpccPassLocal))
		params = append(params, fmt.Sprintf("--mysql-user=%s", tpccUserLocal))
	} else {
		params = append(params, fmt.Sprintf("--mysql-port=%d", serverConfig.Port))
		params = append(params, fmt.Sprintf("--mysql-user=%s", defaultUser))
	}

	params = append(params, fmt.Sprintf("--time=%d", t.Params.Time))
	params = append(params, fmt.Sprintf("--threads=%d", t.Params.NumThreads))
	params = append(params, fmt.Sprintf("--report_interval=%d", t.Params.ReportInterval))
	params = append(params, fmt.Sprintf("--tables=%d", t.Params.Tables))
	params = append(params, fmt.Sprintf("--scale=%d", t.Params.ScaleFactor))

	return params
}

func (t *TpccTest) getPrepareArgs(serverConfig *sysbench_runner.ServerConfig) []string {
	return append(t.getArgs(serverConfig), "prepare")
}

// getRunArgs returns a test's args for the TPCC run step.
func (t *TpccTest) getRunArgs(serverConfig *sysbench_runner.ServerConfig) []string {
	return append(t.getArgs(serverConfig), "run")
}

// getCleanupArgs returns a test's args for the TPCC cleanup step.
func (t *TpccTest) getCleanupArgs(serverConfig *sysbench_runner.ServerConfig) []string {
	return append(t.getArgs(serverConfig), "cleanup")
}

// TODO: Refactor all of this
func (t *TpccTest) TpccPrepare(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, scriptDir+"/tpcc.lua", t.getPrepareArgs(serverConfig)...)
	lp := filepath.Join(scriptDir, "?.lua")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))

	return cmd
}

func (t *TpccTest) TpccRun(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, scriptDir+"/tpcc.lua", t.getRunArgs(serverConfig)...)
	lp := filepath.Join(scriptDir, "?.lua")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))

	return cmd
}

func (t *TpccTest) TpccCleanup(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, scriptDir+"/tpcc.lua", t.getCleanupArgs(serverConfig)...)
	lp := filepath.Join(scriptDir, "?.lua")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))

	return cmd
}
