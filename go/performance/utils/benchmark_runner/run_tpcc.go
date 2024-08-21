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

package benchmark_runner

import (
	"context"
	"fmt"
	"os"
)

func RunTpcc(ctx context.Context, config TpccConfig) error {
	err := config.Validate(ctx)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	svs := config.GetServerConfigs()
	for _, serverConfig := range svs {
		var b Benchmarker
		var results Results
		st := serverConfig.GetServerType()
		switch st {
		case Dolt:
			fmt.Println("Running dolt tpcc benchmarks")
			b = NewDoltTpccBenchmarker(cwd, config, serverConfig)
		case MySql:
			sc, ok := serverConfig.(ProtocolServerConfig)
			if !ok {
				return ErrNotProtocolServerConfig
			}

			fmt.Println("Running mysql tpcc benchmarks")
			b = NewMysqlTpccBenchmarker(cwd, config, sc)
		default:
			panic(fmt.Sprintf("unexpected server type: %s", st))
		}

		results, err = b.Benchmark(ctx)
		if err != nil {
			return err
		}

		err = WriteResults(serverConfig, results)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully wrote results for %s\n", st)
	}

	return nil
}
