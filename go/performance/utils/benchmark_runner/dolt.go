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
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dolthub/dolt/go/store/types"
)

var ErrNotSysbenchTest = errors.New("sysbench test is required")

var stampFunc = func() string { return time.Now().UTC().Format(stampFormat) }

type doltBenchmarkerImpl struct {
	dir          string // cwd
	config       SysbenchConfig
	serverConfig ServerConfig
}

var _ Benchmarker = &doltBenchmarkerImpl{}

func NewDoltBenchmarker(dir string, config SysbenchConfig, serverConfig ServerConfig) *doltBenchmarkerImpl {
	return &doltBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltBenchmarkerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, b.serverConfig.GetServerExec(), doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, b.serverConfig.GetServerExec(), doltConfigEmailKey, doltBenchmarkEmail)
}

func (b *doltBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.GetServerExec(), doltVersionCommand)
	return version.Run()
}

func (b *doltBenchmarkerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, b.dir, b.serverConfig.GetServerExec(), b.config.GetNomsBinFormat(), dbName)
}

func (b *doltBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	err := b.checkInstallation(ctx)
	if err != nil {
		return nil, err
	}

	err = b.updateGlobalConfig(ctx)
	if err != nil {
		return nil, err
	}

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}

	tests, err := GetTests(b.config, b.serverConfig)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	runs := b.config.GetRuns()
	for i := 0; i < runs; i++ {
		for _, test := range tests {
			t, ok := test.(SysbenchTest)
			if !ok {
				return nil, ErrNotSysbenchTest
			}

			testRepo, err := b.initDoltRepo(ctx)
			if err != nil {
				return nil, err
			}

			server := NewServer(ctx, testRepo, b.serverConfig, syscall.SIGTERM, serverParams)
			err = server.Start()
			if err != nil {
				return nil, err
			}

			tester := NewSysbenchTester(b.config, b.serverConfig, t, serverParams, stampFunc)

			var res *Result
			res, err = tester.Test(ctx)
			if err != nil {
				server.Stop()
				return nil, err
			}
			results = append(results, res)
		}
	}

	return results, nil
}

// InitDoltRepo initializes a dolt database and returns its path
func InitDoltRepo(ctx context.Context, dir, serverExec, nomsBinFormat, dbName string) (string, error) {
	testRepo := filepath.Join(dir, dbName)
	if nomsBinFormat == types.Format_LD_1.VersionString() {
		err := ExecCommand(ctx, serverExec, doltCloneCommand, bigEmptyRepo, dbName).Run()
		if err != nil {
			return "", err
		}
		return testRepo, nil
	}

	err := os.MkdirAll(testRepo, os.ModePerm)
	if err != nil {
		return "", err
	}

	if nomsBinFormat != "" {
		if err = os.Setenv(nbfEnvVar, nomsBinFormat); err != nil {
			return "", err
		}
	}

	doltInit := ExecCommand(ctx, serverExec, doltInitCommand)
	doltInit.Dir = testRepo
	err = doltInit.Run()
	if err != nil {
		return "", err
	}

	return testRepo, nil
}

// CheckSetDoltConfig checks the output of `dolt config --global --get` and sets the key, val if necessary
func CheckSetDoltConfig(ctx context.Context, serverExec, key, val string) error {
	check := ExecCommand(ctx, serverExec, doltConfigCommand, doltConfigGlobalFlag, doltConfigGetFlag, key)
	err := check.Run()
	if err != nil {
		// config get calls exit with 1 if not set
		if err.Error() != "exit status 1" {
			return err
		}
		set := ExecCommand(ctx, serverExec, doltConfigCommand, doltConfigGlobalFlag, doltConfigAddFlag, key, val)
		err := set.Run()
		if err != nil {
			return err
		}
	}
	return nil
}
