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
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

const (
	dbName = "sbt"
)

// BenchmarkDolt executes a set of tpcc tests against a dolt server.
func BenchmarkDolt(ctx context.Context, tppcConfig *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig) (sysbench_runner.Results, error) {
	serverParams := serverConfig.GetServerArgs()

	err := sysbench_runner.UpdateDoltConfig(ctx, serverConfig)
	if err != nil {
		return nil, err
	}

	testRepo, err := initDoltRepo(ctx, serverConfig)
	if err != nil {
		return nil, err
	}

	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := getDoltServer(serverCtx, serverConfig, testRepo, serverParams)

	// handle user interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-quit
		defer wg.Done()
		signal.Stop(quit)
		cancel()
	}()

	// launch the dolt server
	gServer.Go(func() error {
		return server.Run()
	})

	// sleep to allow the server to start
	time.Sleep(5 * time.Second)

	// GetTests and Benchmarks
	tests := getTests(tppcConfig)
	results := make(sysbench_runner.Results, 0)

	for _, test := range tests {
		result, err := benchmark(ctx, test, serverConfig, tppcConfig)
		if err != nil {
			close(quit)
			wg.Wait()
			return nil, err
		}

		results = append(results, result)
	}

	// send signal to dolt server
	quit <- syscall.SIGTERM

	err = gServer.Wait()
	if err != nil {
		// we expect a kill error
		// we only exit in error if this is not the
		// error
		if err.Error() != "signal: killed" {
			close(quit)
			wg.Wait()
			return nil, err
		}
	}

	close(quit)
	wg.Wait()

	return results, os.RemoveAll(testRepo)
}

// initDoltRepo initializes a dolt repo and returns the repo path
func initDoltRepo(ctx context.Context, config *sysbench_runner.ServerConfig) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	testRepo := filepath.Join(cwd, dbName)
	err = os.MkdirAll(testRepo, os.ModePerm)
	if err != nil {
		return "", err
	}

	err = os.RemoveAll(filepath.Join(testRepo, ".dolt"))
	if err != nil {
		return "", err
	}

	doltInit := sysbench_runner.ExecCommand(ctx, config.ServerExec, "init")
	doltInit.Dir = testRepo
	err = doltInit.Run()
	if err != nil {
		return "", err
	}

	return testRepo, nil
}

// getDoltServer returns a exec.Cmd for a dolt server
func getDoltServer(ctx context.Context, config *sysbench_runner.ServerConfig, testRepo string, params []string) *exec.Cmd {
	server := sysbench_runner.ExecCommand(ctx, config.ServerExec, params...)
	server.Dir = testRepo
	return server
}

// getTests creates a set of tests that the server needs to be executed on.
func getTests(config *TpccBenchmarkConfig) []*TpccTest {
	tests := make([]*TpccTest, 0)
	for _, sf := range config.ScaleFactors {
		params := NewDefaultTpccParams()
		params.ScaleFactor = sf
		test := NewTpccTest(fmt.Sprintf("tpcc-scale-factor-%d", sf), params)
		tests = append(tests, test)
	}

	return tests
}

// benchmark runs the relevant tpcc test against a server with a config.
func benchmark(ctx context.Context, test *TpccTest, serverConfig *sysbench_runner.ServerConfig, config *TpccBenchmarkConfig) (*sysbench_runner.Result, error) {
	prepare := test.TpccPrepare(ctx, serverConfig, config.ScriptDir)
	run := test.TpccRun(ctx, serverConfig, config.ScriptDir)
	cleanup := test.TpccCleanup(ctx, serverConfig, config.ScriptDir)

	err := prepare.Run()
	if err != nil {
		return nil, err
	}

	if run.Stdout != nil {
		run.Stdout = nil
	}

	out, err := run.Output()
	if err != nil {
		fmt.Print(string(out))
		return nil, err
	}

	fmt.Print(string(out))

	result, err := FromOutputResult(out, config, serverConfig, test, "tpcc", nil)
	if err != nil {
		return nil, err
	}

	err = cleanup.Run()
	if err != nil {
		return nil, err
	}

	return result, nil
}
