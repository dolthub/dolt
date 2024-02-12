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

//import (
//	_ "github.com/lib/pq"
//)

//type PostgresConfig struct {
//	Socket             string
//	ConnectionProtocol string
//	Port               int
//	Host               string
//}

//// BenchmarkPostgres benchmarks postgres based on the provided configurations
//func BenchmarkPostgres(ctx context.Context, config *Config, serverConfig *ServerConfig) (Results, error) {
//	withKeyCtx, cancel := context.WithCancel(ctx)
//
//	var serverDir string
//	defer func() {
//		if serverDir != "" {
//			os.RemoveAll(serverDir)
//		}
//	}()
//
//	var localServer bool
//	var gServer *errgroup.Group
//	var serverCtx context.Context
//	var server *exec.Cmd
//	var err error
//	if serverConfig.Host == defaultHost {
//		log.Println("Launching the default server")
//		localServer = true
//
//		serverDir, err = initPostgresDataDir(ctx, serverConfig)
//		if err != nil {
//			cancel()
//			return nil, err
//		}
//		gServer, serverCtx = errgroup.WithContext(withKeyCtx)
//		var serverParams []string
//		serverParams, err = serverConfig.GetServerArgs()
//		if err != nil {
//			cancel()
//			return nil, err
//		}
//		serverParams = append(serverParams, "-D", serverDir)
//		server = getMysqlServer(serverCtx, serverConfig, serverParams)
//		server.Env = append(server.Env, "LC_ALL=C")
//
//		// launch the postgres server
//		gServer.Go(func() error {
//			return server.Run()
//		})
//
//		// sleep to allow the server to start
//		time.Sleep(10 * time.Second)
//
//		// setup postgres db
//		err := setupPostgresDB(ctx, serverConfig.Host, fmt.Sprintf("%d", serverConfig.Port), "postgres", dbName)
//		if err != nil {
//			cancel()
//			return nil, err
//		}
//
//		log.Println("Successfully set up the Postgres database")
//	}
//
//	// handle user interrupt
//	quit := make(chan os.Signal, 1)
//	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//		<-quit
//		defer wg.Done()
//		signal.Stop(quit)
//		cancel()
//	}()
//
//	tests, err := GetTests(config, serverConfig, nil)
//	if err != nil {
//		return nil, err
//	}
//
//	results := make(Results, 0)
//	for i := 0; i < config.Runs; i++ {
//		for _, test := range tests {
//			r, err := benchmark(withKeyCtx, test, config, serverConfig, stampFunc, serverConfig.GetId())
//			if err != nil {
//				close(quit)
//				wg.Wait()
//				return nil, err
//			}
//			results = append(results, r)
//		}
//	}
//
//	// stop local mysql server
//	if localServer {
//		// send signal to server
//		quit <- syscall.SIGTERM
//
//		err = gServer.Wait()
//		if err != nil {
//			// we expect a kill error
//			// we only exit in error if this is not the
//			// error
//			if err.Error() != "signal: killed" {
//				close(quit)
//				wg.Wait()
//				return nil, err
//			}
//		}
//	}
//
//	fmt.Println("Successfully killed server")
//	close(quit)
//	wg.Wait()
//
//	return results, nil
//}

//// initPostgresDataDir initializes a postgres data dir and returns the path
//func initPostgresDataDir(ctx context.Context, config *ServerConfig) (string, error) {
//	serverDir, err := CreateServerDir(dbName)
//	if err != nil {
//		return "", err
//	}
//
//	pgInit := ExecCommand(ctx, config.InitExec, fmt.Sprintf("--pgdata=%s", serverDir), "--username=postgres")
//	err = pgInit.Run()
//	if err != nil {
//		return "", err
//	}
//
//	return serverDir, nil
//}

//func setupPostgresDB(ctx context.Context, host, port, user, dbname string) (err error) {
//	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, "", dbname)
//
//	db, err := sql.Open("postgres", psqlconn)
//	if err != nil {
//		return err
//	}
//	defer func() {
//		rerr := db.Close()
//		if err == nil {
//			err = rerr
//		}
//	}()
//	err = db.PingContext(ctx)
//	if err != nil {
//		return err
//	}
//	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
//	if err != nil {
//		return err
//	}
//	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", sysbenchUsername))
//	if err != nil {
//		return err
//	}
//	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", sysbenchUsername, sysbenchPassLocal))
//	if err != nil {
//		return err
//	}
//	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s WITH OWNER %s", dbname, sysbenchUsername))
//	if err != nil {
//		return err
//	}
//	return
//}
