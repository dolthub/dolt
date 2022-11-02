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

package tpcc_runner

import (
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// FromConfigsNewResult returns a new result with some fields set based on the provided configs
func FromConfigsNewResult(config *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig, test *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	serverParams := serverConfig.GetServerArgs()

	var getId func() string
	if idFunc == nil {
		getId = func() string {
			return uuid.New().String()
		}
	} else {
		getId = idFunc
	}

	var name string
	base := filepath.Base(test.Name)
	ext := filepath.Ext(base)
	name = strings.TrimSuffix(base, ext)

	return &sysbench_runner.Result{
		Id:            getId(),
		SuiteId:       suiteId,
		TestId:        test.Id,
		RuntimeOS:     config.RuntimeOS,
		RuntimeGoArch: config.RuntimeGoArch,
		ServerName:    string(serverConfig.Server),
		ServerVersion: serverConfig.Version,
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(test.getArgs(serverConfig), " "),
	}, nil
}

// FromOutputResult accepts raw sysbench run output and returns the Result
func FromOutputResult(output []byte, config *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig, test *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	result, err := FromConfigsNewResult(config, serverConfig, test, suiteId, idFunc)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(output), "\n")
	var process bool
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, sysbench_runner.SqlStatsPrefix) {
			process = true
			continue
		}
		if process {
			err := sysbench_runner.UpdateResult(result, trimmed)
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
