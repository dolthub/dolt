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
	"path/filepath"
)

// Run runs sysbench runner
func Run(ctx context.Context, config *sysbenchRunnerConfigImpl) error {
	err := config.Validate()
	if err != nil {
		return err
	}

	err = sysbenchVersion(ctx)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	for _, serverConfig := range config.Servers {
		var results Results
		var b Benchmarker
		switch serverConfig.Server {
		case Dolt:
			// handle a profiling run
			if serverConfig.ServerProfile != "" {
				fmt.Println("Profiling dolt while running sysbench tests")
				p := NewDoltProfiler(cwd, config, serverConfig)
				return p.Profile(ctx)
			}
			fmt.Println("Running dolt sysbench tests")
			b = NewDoltBenchmarker(cwd, config, serverConfig)
		case Doltgres:
			fmt.Println("Running doltgres sysbench tests")
			b = NewDoltgresBenchmarker(cwd, config, serverConfig)
		case MySql:
			fmt.Println("Running mysql sysbench tests")
			b = NewMysqlBenchmarker(cwd, config, serverConfig)
		case Postgres:
			fmt.Println("Running postgres sysbench tests")
			b = NewPostgresBenchmarker(cwd, config, serverConfig)
		default:
			panic(fmt.Sprintf("unexpected server type: %s", serverConfig.Server))
		}

		results, err = b.Benchmark(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("Successfuly finished %s\n", serverConfig.Server)

		err = WriteResults(serverConfig, results)
		if err != nil {
			return err
		}

		fmt.Printf("Successfuly wrote results for %s\n", serverConfig.Server)
	}
	return nil
}

func sysbenchVersion(ctx context.Context) error {
	sysbenchVersion := ExecCommand(ctx, "sysbench", "--version")
	return sysbenchVersion.Run()
}

func WriteResults(serverConfig *doltServerConfigImpl, results Results) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	var writePath string
	switch serverConfig.ResultsFormat {
	case CsvFormat, CsvExt:
		writePath = filepath.Join(
			cwd,
			"results",
			string(serverConfig.Server),
			serverConfig.Version,
			serverConfig.GetId(),
			fmt.Sprintf(ResultFileTemplate, serverConfig.GetId(), serverConfig.Server, serverConfig.Version, CsvExt))
		return WriteResultsCsv(writePath, results)
	case JsonFormat, JsonExt:
		writePath = filepath.Join(
			cwd,
			"results",
			string(serverConfig.Server),
			serverConfig.Version,
			serverConfig.GetId(),
			fmt.Sprintf(ResultFileTemplate, serverConfig.GetId(), serverConfig.Server, serverConfig.Version, JsonExt))
		return WriteResultsJson(writePath, results)
	default:
	}
	return fmt.Errorf("unsupported results format: %s", serverConfig.ResultsFormat)
}
