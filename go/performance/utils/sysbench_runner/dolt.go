// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

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
)

const (
	dbName  = "test"
	luaPath = "?.lua"
)

var stampFunc = func() string { return time.Now().UTC().Format(stampFormat) }

// BenchmarkDolt benchmarks dolt based on the provided configurations
func BenchmarkDolt(ctx context.Context, config *Config, serverConfig *ServerConfig) (Results, error) {
	serverParams := serverConfig.GetServerArgs()

	err := doltVersion(ctx, serverConfig)
	if err != nil {
		return nil, err
	}

	err = UpdateDoltConfig(ctx, serverConfig)
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

	tests, err := GetTests(config, serverConfig, nil)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	for i := 0; i < config.Runs; i++ {
		for _, test := range tests {
			r, err := benchmark(withKeyCtx, test, config, serverConfig, stampFunc, serverConfig.GetId())
			if err != nil {
				close(quit)
				wg.Wait()
				return nil, err
			}
			results = append(results, r)
		}
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

	err = os.RemoveAll(testRepo)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// doltVersion ensures the dolt binary can run
func doltVersion(ctx context.Context, config *ServerConfig) error {
	doltVersion := ExecCommand(ctx, config.ServerExec, "version")
	return doltVersion.Run()
}

// initDoltRepo initializes a dolt repo and returns the repo path
func initDoltRepo(ctx context.Context, config *ServerConfig) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	testRepo := filepath.Join(cwd, dbName)
	err = os.MkdirAll(testRepo, os.ModePerm)
	if err != nil {
		return "", err
	}

	doltInit := ExecCommand(ctx, config.ServerExec, "init")
	doltInit.Dir = testRepo
	err = doltInit.Run()
	if err != nil {
		return "", err
	}

	return testRepo, nil
}

// UpdateDoltConfig updates the dolt config if necessary
func UpdateDoltConfig(ctx context.Context, config *ServerConfig) error {
	err := checkSetDoltConfig(ctx, config, "user.name", "benchmark")
	if err != nil {
		return err
	}
	return checkSetDoltConfig(ctx, config, "user.email", "benchmark@dolthub.com")
}

// checkSetDoltConfig checks the output of `dolt config --global --get` and sets the key, val if necessary
func checkSetDoltConfig(ctx context.Context, config *ServerConfig, key, val string) error {
	check := ExecCommand(ctx, config.ServerExec, "config", "--global", "--get", key)
	err := check.Run()
	if err != nil {
		// config get calls exit with 1 if not set
		if err.Error() != "exit status 1" {
			return err
		}

		set := ExecCommand(ctx, config.ServerExec, "config", "--global", "--add", key, val)
		err := set.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

// getDoltServer returns a exec.Cmd for a dolt server
func getDoltServer(ctx context.Context, config *ServerConfig, testRepo string, params []string) *exec.Cmd {
	server := ExecCommand(ctx, config.ServerExec, params...)
	server.Dir = testRepo
	return server
}

// sysbenchPrepare returns a exec.Cmd for running the sysbench prepare step
func sysbenchPrepare(ctx context.Context, test *Test, scriptDir string) *exec.Cmd {
	cmd := ExecCommand(ctx, "sysbench", test.Prepare()...)
	if test.FromScript {
		lp := filepath.Join(scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}
	return cmd
}

// sysbenchRun returns a exec.Cmd for running the sysbench run step
func sysbenchRun(ctx context.Context, test *Test, scriptDir string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "sysbench", test.Run()...)
	if test.FromScript {
		lp := filepath.Join(scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}
	return cmd
}

// sysbenchPrepare returns a exec.Cmd for running the sysbench cleanup step
func sysbenchCleanup(ctx context.Context, test *Test, scriptDir string) *exec.Cmd {
	cmd := ExecCommand(ctx, "sysbench", test.Cleanup()...)
	if test.FromScript {
		lp := filepath.Join(scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf("LUA_PATH=%s", lp))
	}
	return cmd
}

// benchmark runs a sysbench benchmark against a server calling prepare, run, cleanup
func benchmark(
	ctx context.Context,
	test *Test,
	config *Config,
	serverConfig *ServerConfig,
	stampFunc func() string,
	suiteId string,
) (*Result, error) {
	prepare := sysbenchPrepare(ctx, test, config.ScriptDir)
	run := sysbenchRun(ctx, test, config.ScriptDir)
	cleanup := sysbenchCleanup(ctx, test, config.ScriptDir)

	err := prepare.Run()
	if err != nil {
		return nil, err
	}

	out, err := run.Output()
	if err != nil {
		fmt.Print(string(out))
		return nil, err
	}

	if Debug == true {
		fmt.Print(string(out))
	}

	r, err := FromOutputResult(out, config, serverConfig, test, suiteId, nil)
	if err != nil {
		return nil, err
	}

	r.Stamp(stampFunc)

	return r, cleanup.Run()
}

// fromChannelResults collects all Results from the given channel and returns them
func fromChannelResults(rc chan *Result) Results {
	results := make(Results, 0)
	for r := range rc {
		if r != nil {
			results = append(results, r)
		}
	}
	return results
}
