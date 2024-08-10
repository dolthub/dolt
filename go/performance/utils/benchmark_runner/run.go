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
	"fmt"
	"os"
	"path/filepath"
)

var ErrNotProtocolServerConfig = errors.New("protocol server config required")
var ErrNotInitDbServerConfig = errors.New("init db server config required")

// Run runs sysbench runner
func Run(ctx context.Context, config SysbenchConfig) error {
	err := config.Validate(ctx)
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

	svs := config.GetServerConfigs()
	for _, serverConfig := range svs {
		var results Results
		var b Benchmarker
		st := serverConfig.GetServerType()
		switch st {
		case Dolt:
			// handle a profiling run
			sc, ok := serverConfig.(ProfilingServerConfig)
			if ok {
				if string(sc.GetServerProfile()) != "" {
					fmt.Println("Profiling dolt while running sysbench tests")
					p := NewDoltProfiler(cwd, config, sc)
					return p.Profile(ctx)
				}
			}

			fmt.Println("Running dolt sysbench tests")
			b = NewDoltBenchmarker(cwd, config, serverConfig)
		case Doltgres:
			fmt.Println("Running doltgres sysbench tests")
			b = NewDoltgresBenchmarker(cwd, config, serverConfig)
		case MySql:
			sc, ok := serverConfig.(ProtocolServerConfig)
			if !ok {
				return ErrNotProtocolServerConfig
			}
			fmt.Println("Running mysql sysbench tests")
			b = NewMysqlBenchmarker(cwd, config, sc)
		case Postgres:
			sc, ok := serverConfig.(InitServerConfig)
			if !ok {
				return ErrNotInitDbServerConfig
			}
			fmt.Println("Running postgres sysbench tests")
			b = NewPostgresBenchmarker(cwd, config, sc)
		default:
			panic(fmt.Sprintf("unexpectededed server type: %s", st))
		}

		results, err = b.Benchmark(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully finished %s\n", st)

		err = WriteResults(serverConfig, results)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully wrote results for %s\n", st)
	}
	return nil
}

func sysbenchVersion(ctx context.Context) error {
	version := ExecCommand(ctx, sysbenchCommand, sysbenchVersionFlag)
	return version.Run()
}

func WriteResults(serverConfig ServerConfig, results Results) error {
	st := serverConfig.GetServerType()
	version := serverConfig.GetVersion()
	id := serverConfig.GetId()
	format := serverConfig.GetResultsFormat()

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	//var writePath string
	switch format {
	case CsvFormat, CsvExt:
		writePath := filepath.Join(
			cwd,
			resultsDirname,
			string(st),
			version,
			id,
			fmt.Sprintf(ResultFileTemplate, id, st, version, CsvExt))
		return WriteResultsCsv(writePath, results)
	case JsonFormat, JsonExt:
		writePath := filepath.Join(
			cwd,
			resultsDirname,
			string(st),
			version,
			id,
			fmt.Sprintf(ResultFileTemplate, id, st, version, JsonExt))
		return WriteResultsJson(writePath, results)
	default:
		return fmt.Errorf("unsupported results format: %s", format)
	}
}
