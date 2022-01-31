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
	"fmt"
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	defaultHost = "127.0.0.1"
	defaultUser = "root"

	tpccPassLocal = "tpccpass"
)

var defaultTpccParams = []string{
	fmt.Sprintf("--mysql-user=%s", defaultUser),
	fmt.Sprintf("--mysql-db=%s", dbName),
	"--db-driver=mysql",
}

// TpccConfig represents a configuration a full TPCC test of prepare, run, and clean.
type TpccConfig struct {
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

	// ScriptDir represents the location of the TPCC tests
	ScriptDir string
}

// TpccTest encapsulates an End to End prepare, run, cleanup test case.
type TpccTest struct {
	Id string

	Name string

	TpccConfig *TpccConfig

	ServerConfig *sysbench_runner.ServerConfig

	// FromScript indicates if this test is from a lua script
	FromScript bool
}

func NewTpccTest(name string, tpccConfig *TpccConfig, serverConfig *sysbench_runner.ServerConfig, fromScript bool) *TpccTest {
	return &TpccTest{
		Id:           uuid.New().String(),
		Name:         name,
		TpccConfig:   tpccConfig,
		ServerConfig: serverConfig,
		FromScript:   fromScript,
	}
}

// getArgs returns a test's args for all TPCC steps
func (t *TpccTest) getArgs() []string {
	params := make([]string, 0)
	params = append(params, defaultTpccParams...)

	params = append(params, fmt.Sprintf("--mysql-host=%s", t.ServerConfig.Host))

	// TODO: Handle MySQL Socket case

	// handle sysbench user for local mysql server
	if t.ServerConfig.Server == sysbench_runner.MySql && t.ServerConfig.Host == defaultHost {
		params = append(params, "--mysql-socket=/tmp/mysql.sock")
		params = append(params, fmt.Sprintf("--mysql-password=%s", tpccPassLocal))
		params = append(params, "--mysql-user=tpcc")
	} else {
		params = append(params, fmt.Sprintf("--mysql-port=%d", t.ServerConfig.Port))
		params = append(params, "--mysql-user=root")
	}

	params = append(params, fmt.Sprintf("--time=%d", t.TpccConfig.Time))
	params = append(params, fmt.Sprintf("--threads=%d", t.TpccConfig.NumThreads))
	params = append(params, fmt.Sprintf("--report_interval=%d", t.TpccConfig.ReportInterval))
	params = append(params, fmt.Sprintf("--tables=%d", t.TpccConfig.Tables))
	params = append(params, fmt.Sprintf("--scale=%d", t.TpccConfig.ScaleFactor))

	return params
}

func (t *TpccTest) getPrepareArgs() []string {
	return append(t.getArgs(), "prepare")
}

// getRunArgs returns a test's args for the TPCC run step.
func (t *TpccTest) getRunArgs() []string {
	return append(t.getArgs(), "run")
}

// getCleanupArgs returns a test's args for the TPCC cleanup step.
func (t *TpccTest) getCleanupArgs() []string {
	return append(t.getArgs(), "cleanup")
}

func (t *TpccTest) TpccPrepare(ctx context.Context) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, t.TpccConfig.ScriptDir+"/tpcc.lua", t.getPrepareArgs()...)
	if t.FromScript {
		lp := filepath.Join(t.TpccConfig.ScriptDir, "?.lua")
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}

	return cmd
}

func (t *TpccTest) TpccRun(ctx context.Context) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, t.TpccConfig.ScriptDir+"/tpcc.lua", t.getRunArgs()...)
	if t.FromScript {
		lp := filepath.Join(t.TpccConfig.ScriptDir, "?.lua")
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}

	return cmd
}

func (t *TpccTest) TpccCleanup(ctx context.Context) *exec.Cmd {
	cmd := sysbench_runner.ExecCommand(ctx, t.TpccConfig.ScriptDir+"/tpcc.lua", t.getCleanupArgs()...)
	if t.FromScript {
		lp := filepath.Join(t.TpccConfig.ScriptDir, "?.lua")
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}

	return cmd
}
