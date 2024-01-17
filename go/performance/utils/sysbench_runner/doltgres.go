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
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"database/sql"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

// BenchmarkDoltgres benchmarks doltgres based on the provided configurations
func BenchmarkDoltgres(ctx context.Context, config *Config, serverConfig *ServerConfig) (Results, error) {
	serverParams := serverConfig.GetServerArgs()

	err := DoltVersion(ctx, serverConfig.ServerExec)
	if err != nil {
		return nil, err
	}

	serverDir, err := createServerDir(dbName)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanupDoltgresServerDir(serverDir)
	}()

	serverParams = append(serverParams, fmt.Sprintf("--data-dir=%s", serverDir))

	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := getServer(serverCtx, serverConfig, serverDir, serverParams)

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

	// create the db against the running server
	err = createDb(ctx, serverConfig.Host, fmt.Sprintf("%d", serverConfig.Port), "doltgres", dbName)
	if err != nil {
		close(quit)
		wg.Wait()
		return nil, err
	}

	tests, err := GetTests(config, serverConfig, nil)
	if err != nil {
		close(quit)
		wg.Wait()
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
			fmt.Println(err)
			close(quit)
			wg.Wait()
			return nil, err
		}
	}

	fmt.Println("Successfully killed server")
	close(quit)
	wg.Wait()

	return results, nil
}

func createDb(ctx context.Context, host, port, user, dbname string) error {
	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, "", dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	// close database
	defer db.Close()

	// check db
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("create database %s;", dbname))
	return err
}

// createServerDir creates a server directory
func createServerDir(dbName string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	serverDir := filepath.Join(cwd, dbName)
	err = os.MkdirAll(serverDir, os.ModePerm)
	if err != nil {
		return "", err
	}

	return serverDir, nil
}

// cleanupDoltgresServerDir cleans up the doltgres assets in the provided dir
func cleanupDoltgresServerDir(dir string) error {
	dataDir := filepath.Join(dir, ".dolt")
	defaultDir := filepath.Join(dir, "doltgres")
	testDir := filepath.Join(dir, dbName)
	for _, d := range []string{dataDir, defaultDir, testDir} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			err = os.RemoveAll(d)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
