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
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

// BenchmarkMysql benchmarks mysql based on the provided configurations
func BenchmarkMysql(ctx context.Context, config *Config, serverConfig *ServerConfig) (Results, error) {
	withKeyCtx, cancel := context.WithCancel(ctx)

	var localServer bool
	var gServer *errgroup.Group
	var serverCtx context.Context
	var server *exec.Cmd
	if serverConfig.Host == defaultHost {
		fmt.Println("Launching the default server")
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
		err := SetupDB(ctx, serverConfig, dbName)
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

	// stop local mysql server
	if localServer {
		// send signal to server
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
	}

	fmt.Println("Successfully killed server")
	close(quit)
	wg.Wait()

	return results, nil
}

// getMysqlServer returns a exec.Cmd for a dolt server
func getMysqlServer(ctx context.Context, config *ServerConfig, params []string) *exec.Cmd {
	return ExecCommand(ctx, config.ServerExec, params...)
}

func SetupDB(ctx context.Context, serverConfig *ServerConfig, databaseName string) (err error) {
	dsn, err := formatDSN(serverConfig)
	if err != nil {
		return err
	}

	// TODO make sure this can work on windows
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()
	err = db.Ping()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", databaseName))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", databaseName))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", sysbenchUserLocal))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH mysql_native_password BY '%s'", sysbenchUserLocal, sysbenchPassLocal))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("GRANT ALL ON %s.* to %s", databaseName, sysbenchUserLocal))
	if err != nil {
		return err
	}

	// Required for running groupby_scan.lua without error
	_, err = db.ExecContext(ctx, "SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
	if err != nil {
		return err
	}

	return
}

func formatDSN(serverConfig *ServerConfig) (string, error) {
	var socketPath string
	if serverConfig.Socket != "" {
		socketPath = serverConfig.Socket
	} else {
		socketPath = defaultSocket
	}
	if serverConfig.ConnectionProtocol == tcpProtocol {
		return fmt.Sprintf("root@tcp(%s:%d)/", defaultHost, serverConfig.Port), nil
	} else if serverConfig.ConnectionProtocol == unixProtocol {
		return fmt.Sprintf("root@unix(%s)/", socketPath), nil
	} else {
		return "", ErrUnsupportedConnectionProtocol
	}
}
