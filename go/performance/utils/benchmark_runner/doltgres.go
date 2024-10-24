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

package benchmark_runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type doltgresBenchmarkerImpl struct {
	dir          string // cwd
	config       SysbenchConfig
	serverConfig ServerConfig
}

var _ Benchmarker = &doltgresBenchmarkerImpl{}

func NewDoltgresBenchmarker(dir string, config SysbenchConfig, serverConfig ServerConfig) *doltgresBenchmarkerImpl {
	return &doltgresBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltgresBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.GetServerExec(), doltgresVersionCommand)
	return version.Run()
}

func (b *doltgresBenchmarkerImpl) createServerDir() (string, error) {
	return CreateServerDir(dbName)
}

func (b *doltgresBenchmarkerImpl) cleanupServerDir(dir string) error {
	dataDir := filepath.Join(dir, doltDataDir)
	defaultDir := filepath.Join(dir, doltgresUser)
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

func (b *doltgresBenchmarkerImpl) createTestingDb(ctx context.Context) error {
	psqlconn := fmt.Sprintf(psqlDsnTemplate, b.serverConfig.GetHost(), b.serverConfig.GetPort(), doltgresUser, "", dbName)

	// open database
	db, err := sql.Open(postgresDriver, psqlconn)
	if err != nil {
		return err
	}

	// close database
	defer db.Close()

	// todo: currently db.Ping is busted in doltgres,
	// results in error
	// check db
	//err = db.PingContext(ctx)
	//if err != nil {
	//	return err
	//}

	_, err = db.ExecContext(ctx, fmt.Sprintf(createDatabaseTemplate, dbName))
	return err
}

func (b *doltgresBenchmarkerImpl) Benchmark(ctx context.Context) (results Results, err error) {
	err = b.checkInstallation(ctx)
	if err != nil {
		return
	}

	var serverDir string
	serverDir, err = CreateServerDir(dbName)
	if err != nil {
		return
	}
	defer func() {
		rerr := b.cleanupServerDir(serverDir)
		if err == nil {
			err = rerr
		}
	}()

	var serverParams []string
	serverParams, err = b.serverConfig.GetServerArgs()
	if err != nil {
		return
	}

	serverParams = append(serverParams, fmt.Sprintf("%s=%s", doltgresDataDirFlag, serverDir))

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
	if err != nil {
		return
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return
	}

	var tests []Test
	tests, err = GetTests(b.config, b.serverConfig)
	if err != nil {
		return
	}

	testsFailed := make([]string, 0)
	results = make(Results, 0)
	runs := b.config.GetRuns()
	for i := 0; i < runs; i++ {
		for _, test := range tests {
			t, ok := test.(SysbenchTest)
			if !ok {
				return nil, ErrNotSysbenchTest
			}
			tester := NewSysbenchTester(b.config, b.serverConfig, t, serverParams, stampFunc)
			var r *Result
			r, err = tester.Test(ctx)
			if err != nil {
				testsFailed = append(testsFailed, t.GetName())
				// get new server for the next test
				server.Stop()
				server = NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
				err = server.Start()
				if err != nil {
					return
				}
				continue
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return
	}

	if len(testsFailed) > 0 {
		fmt.Printf("Failed test files that were skipped: %s\n", strings.Join(testsFailed, ", "))
	}

	return
}

// CreateServerDir creates a server directory
func CreateServerDir(dbName string) (string, error) {
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
