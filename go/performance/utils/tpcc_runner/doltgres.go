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
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// BenchmarkDoltgres executes a set of tpcc tests against a doltgres server.
func BenchmarkDoltgres(ctx context.Context, tppcConfig *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig) (sysbench_runner.Results, error) {
	serverParams := serverConfig.GetServerArgs()

	err := sysbench_runner.DoltVersion(ctx, serverConfig.ServerExec)
	if err != nil {
		return nil, err
	}

	serverDir, err := sysbench_runner.CreateDoltgresServerDir(dbName)
	if err != nil {
		return nil, err
	}
	defer func() {
		sysbench_runner.CleanupDoltgresServerDir(serverDir)
	}()

	serverParams = append(serverParams, fmt.Sprintf("--data-dir=%s", serverDir))

	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := getDoltServer(serverCtx, serverConfig, serverDir, serverParams)

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
		server.Stdout = os.Stdout
		server.Stderr = os.Stderr
		return server.Run()
	})

	// sleep to allow the server to start
	time.Sleep(10 * time.Second)

	// create the db against the running server
	err = sysbench_runner.CreateDoltgresDb(ctx, serverConfig.Host, fmt.Sprintf("%d", serverConfig.Port), "doltgres", dbName)
	if err != nil {
		close(quit)
		wg.Wait()
		return nil, err
	}

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

	return results, nil
}
