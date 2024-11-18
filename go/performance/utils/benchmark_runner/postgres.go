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
	"syscall"

	_ "github.com/lib/pq"
)

type postgresBenchmarkerImpl struct {
	dir          string // cwd
	config       SysbenchConfig
	serverConfig InitServerConfig
}

var _ Benchmarker = &postgresBenchmarkerImpl{}

func NewPostgresBenchmarker(dir string, config SysbenchConfig, serverConfig InitServerConfig) *postgresBenchmarkerImpl {
	return &postgresBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *postgresBenchmarkerImpl) initDataDir(ctx context.Context) (string, error) {
	serverDir, err := CreateServerDir(dbName)
	if err != nil {
		return "", err
	}

	pgInit := ExecCommand(ctx, b.serverConfig.GetInitDbExec(), fmt.Sprintf("%s=%s", postgresInitDbDataDirFlag, serverDir), fmt.Sprintf("%s=%s", postgresUsernameFlag, postgresUsername))
	err = pgInit.Run()
	if err != nil {
		return "", err
	}

	return serverDir, nil
}

func (b *postgresBenchmarkerImpl) createTestingDb(ctx context.Context) (err error) {
	psqlconn := fmt.Sprintf(psqlDsnTemplate, b.serverConfig.GetHost(), b.serverConfig.GetPort(), postgresUsername, "", dbName)

	var db *sql.DB
	db, err = sql.Open(postgresDriver, psqlconn)
	if err != nil {
		return
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()
	err = db.PingContext(ctx)
	if err != nil {
		return
	}

	stmts := []string{
		fmt.Sprintf(postgresDropDatabaseSqlTemplate, dbName),
		fmt.Sprintf(postgresDropUserSqlTemplate, sysbenchUsername),
		fmt.Sprintf(postgresCreateUserSqlTemplate, sysbenchUsername, sysbenchPassLocal),
		fmt.Sprintf(postgresCreateDatabaseSqlTemplate, dbName, sysbenchUsername),
	}

	for _, s := range stmts {
		_, err = db.ExecContext(ctx, s)
		if err != nil {
			return
		}
	}

	return
}

func (b *postgresBenchmarkerImpl) Benchmark(ctx context.Context) (results Results, err error) {
	var serverDir string
	serverDir, err = b.initDataDir(ctx)
	if err != nil {
		return
	}
	defer func() {
		rerr := os.RemoveAll(serverDir)
		if err == nil {
			err = rerr
		}
	}()

	var serverParams []string
	serverParams, err = b.serverConfig.GetServerArgs()
	if err != nil {
		return
	}

	serverParams = append(serverParams, postgresDataDirFlag, serverDir)

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	server.WithEnv(postgresLcAllEnvVarKey, postgresLcAllEnvVarValue)

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
				server.Stop()
				return
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return
	}

	return
}
