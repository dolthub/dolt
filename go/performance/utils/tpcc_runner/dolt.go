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
	"golang.org/x/sync/errgroup"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	dbName = "sbt"
)

func BenchmarkDolt(ctx context.Context, tppcConfig *TpccConfig, serverConfig *sysbench_runner.ServerConfig) error {
	// TODO: Need to implement username and password configs for docker deployment
	serverParams := serverConfig.GetServerArgs()

	testRepo, err := initDoltRepo(ctx, serverConfig)
	if err != nil {
		return err
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
	test := NewTpccTest(uuid.New().String(), tppcConfig, serverConfig, true)
	err = benchmark(ctx, test)
	if err != nil {
		close(quit)
		wg.Wait()
		return err
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
			return err
		}
	}

	close(quit)
	wg.Wait()

	return os.RemoveAll(testRepo)
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

	// TODO: Ignore an error if init already exists
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

func benchmark(ctx context.Context, test *TpccTest) error {
	prepare := test.TpccPrepare(ctx)
	run := test.TpccRun(ctx)
	cleanup := test.TpccCleanup(ctx)

	out, err := prepare.Output()
	if err != nil {
		fmt.Print(string(out))
		return err
	}

	out, err = run.Output()
	if err != nil {
		fmt.Print(string(out))
		return err
	}
	
	out, err = cleanup.Output()
	if err != nil {
		fmt.Print(string(out))
		return err
	}

	return nil
}
