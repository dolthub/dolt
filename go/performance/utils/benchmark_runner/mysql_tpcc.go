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
	"fmt"
	"os"
	"syscall"
)

type mysqlTpccBenchmarkerImpl struct {
	dir          string // cwd
	config       TpccConfig
	serverConfig ProtocolServerConfig
}

var _ Benchmarker = &mysqlTpccBenchmarkerImpl{}

func NewMysqlTpccBenchmarker(dir string, config TpccConfig, serverConfig ProtocolServerConfig) *mysqlTpccBenchmarkerImpl {
	return &mysqlTpccBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *mysqlTpccBenchmarkerImpl) getDsn() (string, error) {
	return GetMysqlDsn(b.serverConfig.GetHost(), b.serverConfig.GetSocket(), b.serverConfig.GetConnectionProtocol(), b.serverConfig.GetPort())
}

func (b *mysqlTpccBenchmarkerImpl) createTestingDb(ctx context.Context) error {
	dsn, err := b.getDsn()
	if err != nil {
		return err
	}
	return CreateMysqlTestingDb(ctx, dsn, tpccDbName)
}

func (b *mysqlTpccBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	serverDir, err := InitMysqlDataDir(ctx, b.serverConfig.GetServerExec(), tpccDbName)
	if err != nil {
		return nil, err
	}

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}
	serverParams = append(serverParams, fmt.Sprintf("%s=%s", MysqlDataDirFlag, serverDir))

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
	if err != nil {
		return nil, err
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return nil, err
	}

	tests := GetTpccTests(b.config)

	results := make(Results, 0)
	for _, test := range tests {
		tester := NewTpccTester(b.config, b.serverConfig, test, serverParams, stampFunc)
		r, err := tester.Test(ctx)
		if err != nil {
			server.Stop()
			return nil, err
		}
		results = append(results, r)
	}

	err = server.Stop()
	if err != nil {
		return nil, err
	}

	return results, os.RemoveAll(serverDir)
}
