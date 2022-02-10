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
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// BenchmarkMysql benchmarks a mysql server based on the provided configurations.
func BenchmarkMysql(ctx context.Context, config *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig) (sysbench_runner.Results, error) {
	withKeyCtx, cancel := context.WithCancel(ctx)

	var localServer bool
	var gServer *errgroup.Group
	var serverCtx context.Context
	var server *exec.Cmd
	if serverConfig.Host == defaultHost {
		localServer = true
		gServer, serverCtx = errgroup.WithContext(withKeyCtx)
		serverParams := serverConfig.GetServerArgs()
		server = getMysqlServer(serverCtx, serverConfig, serverParams)

		// launch the mysql server
		gServer.Go(func() error {
			return server.Run()
		})

		// sleep to allow the server to start
		time.Sleep(10 * time.Second)

		// setup mysqldb
		err := sysbench_runner.SetupDB(ctx, serverConfig, dbName)
		if err != nil {
			cancel()
			return nil, err
		}
	}

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

	tests := getTests(config, serverConfig.Server)

	results := make(sysbench_runner.Results, 0)

	for _, test := range tests {
		r, err := benchmark(withKeyCtx, test, serverConfig, config)
		if err != nil {
			close(quit)
			wg.Wait()
			return nil, err
		}
		results = append(results, r)
	}

	// stop local mysql server
	if localServer {
		// send signal to server
		quit <- syscall.SIGTERM

		err := gServer.Wait()
		if err != nil {
			// we expect a kill error
			// we only exit in error if this is not the
			// error
			if err.Error() != "signal: killed" && err.Error() != "exit status 1" {
				close(quit)
				wg.Wait()
				return nil, err
			}
		}
	}

	close(quit)
	wg.Wait()

	return results, nil
}

// getMysqlServer returns a exec.Cmd for a dolt server
func getMysqlServer(ctx context.Context, config *sysbench_runner.ServerConfig, params []string) *exec.Cmd {
	return sysbench_runner.ExecCommand(ctx, config.ServerExec, params...)
}
