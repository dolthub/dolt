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
	"path/filepath"
	"syscall"
)

type Profiler interface {
	Profile(ctx context.Context) error
}

type doltProfilerImpl struct {
	dir          string // cwd
	config       SysbenchConfig
	serverConfig ProfilingServerConfig
}

var _ Profiler = &doltProfilerImpl{}

func NewDoltProfiler(dir string, config SysbenchConfig, serverConfig ProfilingServerConfig) *doltProfilerImpl {
	return &doltProfilerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (p *doltProfilerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, p.serverConfig.GetServerExec(), doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, p.serverConfig.GetServerExec(), doltConfigEmailKey, doltBenchmarkEmail)
}

func (p *doltProfilerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, p.serverConfig.GetServerExec(), doltVersionCommand)
	return version.Run()
}

func (p *doltProfilerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, p.dir, p.serverConfig.GetServerExec(), p.config.GetNomsBinFormat(), dbName)
}

func (p *doltProfilerImpl) Profile(ctx context.Context) error {
	err := p.checkInstallation(ctx)
	if err != nil {
		return err
	}

	err = p.updateGlobalConfig(ctx)
	if err != nil {
		return err
	}

	testRepo, err := p.initDoltRepo(ctx)
	if err != nil {
		return err
	}
	defer os.RemoveAll(testRepo)

	serverParams, err := p.serverConfig.GetServerArgs()
	if err != nil {
		return err
	}

	profilePath, err := os.MkdirTemp("", "dolt_profile_path_*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(profilePath)

	tempProfile := filepath.Join(profilePath, cpuProfileFilename)
	profileParams := make([]string, 0)
	profileParams = append(profileParams, profileFlag, cpuProfile, profilePathFlag, profilePath)
	profileParams = append(profileParams, serverParams...)

	server := NewServer(ctx, testRepo, p.serverConfig, syscall.SIGTERM, profileParams)
	err = server.Start()
	if err != nil {
		return err
	}

	tests, err := GetTests(p.config, p.serverConfig)
	if err != nil {
		return err
	}

	results := make(Results, 0)
	runs := p.config.GetRuns()
	for i := 0; i < runs; i++ {
		for _, test := range tests {
			t, ok := test.(SysbenchTest)
			if !ok {
				return ErrNotSysbenchTest
			}
			tester := NewSysbenchTester(p.config, p.serverConfig, t, profileParams, stampFunc)
			r, err := tester.Test(ctx)
			if err != nil {
				server.Stop()
				return err
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return err
	}

	info, err := os.Stat(tempProfile)
	if err != nil {
		return err
	}

	if info.Size() < 1 {
		return fmt.Errorf("failed to create profile: file was empty")
	}

	finalProfile := filepath.Join(p.serverConfig.GetProfilePath(), fmt.Sprintf("%s_%s", p.serverConfig.GetId(), cpuProfileFilename))
	return os.Rename(tempProfile, finalProfile)
}
