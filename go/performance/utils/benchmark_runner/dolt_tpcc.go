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

type doltTpccBenchmarkerImpl struct {
	dir          string // cwd
	config       TpccConfig
	serverConfig ServerConfig
}

var _ Benchmarker = &doltTpccBenchmarkerImpl{}

func NewDoltTpccBenchmarker(dir string, config TpccConfig, serverConfig ServerConfig) *doltTpccBenchmarkerImpl {
	return &doltTpccBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltTpccBenchmarkerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, b.serverConfig.GetServerExec(), doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, b.serverConfig.GetServerExec(), doltConfigEmailKey, doltBenchmarkEmail)
}

func (b *doltTpccBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.GetServerExec(), doltVersionCommand)
	return version.Run()
}

func (b *doltTpccBenchmarkerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, b.dir, b.serverConfig.GetServerExec(), b.config.GetNomsBinFormat(), tpccDbName)
}

func (b *doltTpccBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	err := b.checkInstallation(ctx)
	if err != nil {
		return nil, err
	}

	err = b.updateGlobalConfig(ctx)
	if err != nil {
		return nil, err
	}

	testRepo, err := b.initDoltRepo(ctx)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(testRepo)

	if err := configureServer(ctx, b.serverConfig.GetServerExec(), testRepo); err != nil {
		return nil, err
	}

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}

	server := NewServer(ctx, testRepo, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
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

	return results, nil
}

// GetTpccTests creates a set of tests that the server needs to be executed on.
func GetTpccTests(config TpccConfig) []Test {
	tests := make([]Test, 0)
	for _, sf := range config.GetScaleFactors() {
		params := NewDefaultTpccParams()
		params.ScaleFactor = sf
		test := NewTpccTest(fmt.Sprintf(tpccScaleFactorTemplate, sf), params)
		tests = append(tests, test)
	}
	return tests
}

func configureServer(ctx context.Context, doltPath, dbPath string) error {
	queries := []string{
		"set @@PERSIST.dolt_stats_auto_refresh_enabled = 1;",
		"set @@PERSIST.dolt_stats_auto_refresh_interval = 2;",
		"set @@PERSIST.dolt_stats_auto_refresh_threshold = 1.0;",
	}
	for _, q := range queries {
		q := ExecCommand(ctx, doltPath, "sql", "-q", q)
		q.Dir = dbPath
		if err := q.Run(); err != nil {
			return err
		}
	}
	return nil
}
