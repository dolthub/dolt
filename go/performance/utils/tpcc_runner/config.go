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
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

const (
	defaultHost = "127.0.0.1"
	defaultUser = "root"

	// Note this is built for the SysbenchDocker file. If you want to run locally you'll need to override these variables
	// for your local MySQL setup.
	tpccUserLocal = "'sysbench'@'localhost'"
	tpccPassLocal = "sysbenchpass"
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

	// ScaleFactors represent the scale at which to run each TpccBenchmark at.
	ScaleFactors []int
}

func NewTpccConfig() *TpccBenchmarkConfig {
	return &TpccBenchmarkConfig{
		Servers:      make([]*sysbench_runner.ServerConfig, 0),
		ScaleFactors: make([]int, 0),
	}
}

func (c *TpccBenchmarkConfig) updateDefaults() error {
	if len(c.Servers) < 1 {
		return sysbench_runner.ErrNoServersDefined
	}

	// TODO: Eventually we need to support scale factors all the way to 10
	if len(c.ScaleFactors) == 0 {
		c.ScaleFactors = append(c.ScaleFactors, 1)
	}

	if c.RuntimeOS == "" {
		c.RuntimeOS = runtime.GOOS
	}
	if c.RuntimeGoArch == "" {
		c.RuntimeGoArch = runtime.GOARCH
	}

	return c.validateServerConfigs()
}

// validateServerConfigs ensures the ServerConfigs are valid
func (c *TpccBenchmarkConfig) validateServerConfigs() error {
	portMap := make(map[int]sysbench_runner.ServerType)
	for _, s := range c.Servers {
		if s.Server != sysbench_runner.Dolt && s.Server != sysbench_runner.MySql {
			return fmt.Errorf("unsupported server type: %s", s.Server)
		}

		err := sysbench_runner.ValidateRequiredFields(string(s.Server), s.Version, s.ResultsFormat)
		if err != nil {
			return err
		}

		if s.Server == sysbench_runner.MySql {
			err = sysbench_runner.CheckProtocol(s.ConnectionProtocol)
			if err != nil {
				return err
			}
		}

		if s.Host == "" {
			s.Host = defaultHost
		}

		portMap, err = sysbench_runner.CheckUpdatePortMap(s, portMap)
		if err != nil {
			return err
		}

		err = sysbench_runner.CheckExec(s)
		if err != nil {
			return err
		}
	}
	return nil
}

// FromFileConfig returns a validated Config based on the config file at the configPath
func FromFileConfig(configPath string) (*TpccBenchmarkConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := NewTpccConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// TpccTest encapsulates an End to End prepare, run, cleanup test case.
type TpccTest struct {
	// Id represents a unique test id
	Id string

	// Name represents the name of the test case
	Name string

	// Params are associated parameters this test runs with
	Params *TpccTestParams
}

type TpccTestParams struct {
	// NumThreads represents the number of threads running queries concurrently.
	NumThreads int

	// ScaleFactor represents the number of warehouse to test this at scale.
	ScaleFactor int

	// Tables represents the number of tables created per warehouse.
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

// NewDefaultTpccParams returns default TpccTestParams.
func NewDefaultTpccParams() *TpccTestParams {
	return &TpccTestParams{
		NumThreads:     2, // TODO: When ready, expose as command line argument.
		ScaleFactor:    1,
		Tables:         1,
		TrxLevel:       "RR",
		ReportCSV:      true,
		ReportInterval: 1,
		Time:           30,
	}
}

// NewTpccTest instantiates and returns a TPCC test.
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

	// handle sysbench user for local mysql server
	if serverConfig.Server == sysbench_runner.MySql && serverConfig.Host == defaultHost {
		params = append(params, fmt.Sprintf("--mysql-user=%s", "sysbench"))
		params = append(params, fmt.Sprintf("--mysql-password=%s", tpccPassLocal))
	} else {
		params = append(params, fmt.Sprintf("--mysql-port=%d", serverConfig.Port))
		params = append(params, fmt.Sprintf("--mysql-user=%s", defaultUser))
	}

	params = append(params, fmt.Sprintf("--time=%d", t.Params.Time))
	params = append(params, fmt.Sprintf("--threads=%d", t.Params.NumThreads))
	params = append(params, fmt.Sprintf("--report_interval=%d", t.Params.ReportInterval))
	params = append(params, fmt.Sprintf("--tables=%d", t.Params.Tables))
	params = append(params, fmt.Sprintf("--scale=%d", t.Params.ScaleFactor))
	params = append(params, fmt.Sprintf("--trx_level=%s", t.Params.TrxLevel))

	return params
}

// TpccPrepare prepares the command executable for the Prepare step.
func (t *TpccTest) TpccPrepare(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, scriptDir+"/tpcc.lua", append(t.getArgs(serverConfig), "prepare")...)
	return addParamsToCmd(cmd, scriptDir)
}

// TpccRun prepares the command executable for the Run step.
func (t *TpccTest) TpccRun(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, scriptDir+"/tpcc.lua", append(t.getArgs(serverConfig), "run")...)
	return addParamsToCmd(cmd, scriptDir)
}

// TpccCleanup prepares the cleanup executable for the Cleanup step.
func (t *TpccTest) TpccCleanup(ctx context.Context, serverConfig *sysbench_runner.ServerConfig, scriptDir string) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, scriptDir+"/tpcc.lua", append(t.getArgs(serverConfig), "cleanup")...)
	return addParamsToCmd(cmd, scriptDir)
}

func addParamsToCmd(cmd *exec.Cmd, scriptDir string) *exec.Cmd {
	lp := filepath.Join(scriptDir, "?.lua")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	cmd.Env = append(cmd.Env, "DOLT_TRANSACTION_MERGE_STOMP=1")

	return cmd
}
