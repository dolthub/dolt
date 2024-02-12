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

package sysbench_runner

import (
	"context"
	"fmt"
	"os"
)

func RunTpcc(ctx context.Context, config *TpccBenchmarkConfig) error {
	err := config.updateDefaults()
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	for _, serverConfig := range config.Servers {
		var b Benchmarker
		var results Results
		switch serverConfig.Server {
		case Dolt:
			fmt.Println("Running dolt tpcc benchmarks")
			b = NewDoltTpccBenchmarker(cwd, config, serverConfig)
		case MySql:
			fmt.Println("Running mysql tpcc benchmarks")
			b = NewMysqlTpccBenchmarker(cwd, config, serverConfig)
		default:
			panic(fmt.Sprintf("unexpected server type: %s", serverConfig.Server))
		}

		results, err = b.Benchmark(ctx)
		if err != nil {
			return err
		}

		err = WriteResults(serverConfig, results)
		if err != nil {
			return err
		}

		fmt.Printf("Successfuly wrote results for %s\n", serverConfig.Server)
	}

	return nil
}
