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

package import_benchmarker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = 3306

	defaultSocket = "/var/run/mysqld/mysqld.sock"
	dbName        = "test"
)

func BenchmarkMySQLImportJobs(jobs []*ImportBenchmarkJob, mConfig sysbench_runner.MysqlConfig) ([]result, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	ctx := context.Background()
	withCancelCtx, cancel := context.WithCancel(ctx)

	gServer, serverCtx := errgroup.WithContext(withCancelCtx)
	var serverErr bytes.Buffer

	// Assume first server is okay
	server := getMysqlServer(serverCtx, jobs[0].ExecPath, getServersArgs())
	server.Stderr = &serverErr

	// launch the mysql server
	gServer.Go(func() error {
		err := server.Run()
		if err != nil {
			return err
		}
		if len(strings.TrimSpace(serverErr.String())) > 0 {
			return errors.New(fmt.Sprintf("server produced stderr output: %s", serverErr.String()))
		}
		return nil
	})

	// sleep to allow the server to start
	time.Sleep(5 * time.Second)

	// set up the relevant testing database and permissions
	err := sysbench_runner.SetupDB(ctx, mConfig, dbName)
	if err != nil {
		cancel()
		return nil, err
	}

	log.Println("successfully setup the database")

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

	results := make([]result, len(jobs))

	for i, job := range jobs {
		// benchmark the actual job
		var err error
		br := testing.Benchmark(func(b *testing.B) {
			err = benchmarkLoadData(ctx, b, mConfig, job)
		})
		if err != nil {
			return nil, err
		}

		results[i] = result{
			name:        job.Name,
			format:      job.Format,
			rows:        job.NumRows,
			columns:     len(genSampleCols()),
			sizeOnDisk:  -1, // TODO: Think about how to collect MySQL table size
			br:          br,
			doltVersion: job.Version,
			program:     "mysql",
		}
	}

	return results, nil
}

func benchmarkLoadData(ctx context.Context, b *testing.B, mConfig sysbench_runner.MysqlConfig, job *ImportBenchmarkJob) (err error) {
	var dsn string
	dsn, err = sysbench_runner.FormatDsn(mConfig)
	if err != nil {
		return
	}

	var db *sql.DB
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()

	err = db.Ping()
	if err != nil {
		return
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("USE %s", dbName))
	if err != nil {
		return
	}

	// Load the schema for the test table. This assumes the table has the same name as testTable
	var data []byte
	data, err = ioutil.ReadFile(job.SchemaPath)
	if err != nil {
		return
	}

	// Register the local file as per https://github.com/go-sql-driver/mysql#load-data-local-infile-support
	mysql.RegisterLocalFile(job.Filepath)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Since dolt also creates the table on import we'll add dropping and creating the table to the benchmark
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
		if err != nil {
			return
		}

		// Run the CREATE TABLE command stored in the schema file
		// TODO: This schema file must have the same name as testTable.
		_, err = db.ExecContext(ctx, string(data))
		if err != nil {
			return
		}

		// Run LOAD DATA on the csv file
		_, err = db.ExecContext(ctx, fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES`, job.Filepath, testTable))
		if err != nil {
			return
		}

		log.Printf("MySQL server loaded file %s \n", job.Filepath)
	}

	return
}

// getServerArgs returns the arguments that run the mysql server
func getServersArgs() []string {
	return []string{"--user=mysql", fmt.Sprintf("--port=%d", defaultPort), "--local-infile=ON"}
}

// getMysqlServer returns a exec.Cmd for a dolt server
func getMysqlServer(ctx context.Context, serverExec string, params []string) *exec.Cmd {
	return execCommand(ctx, serverExec, params...)
}
