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
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

func Run(config *TpccBenchmarkConfig) error {
	err := config.updateDefaults()
	if err != nil {
		return err
	}

	ctx := context.Background()

	for _, serverConfig := range config.Servers {
		var results sysbench_runner.Results
		var err error
		switch serverConfig.Server {
		case sysbench_runner.Dolt:
			fmt.Println("Running Dolt Benchmark")
			results, err = BenchmarkDolt(ctx, config, serverConfig)
			if err != nil {
				return err
			}
		case sysbench_runner.MySql:
			fmt.Println("Running MySQL benchmark")
			results, err = BenchmarkMysql(ctx, config, serverConfig)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unexpected server type: %s", serverConfig.Server))
		}
		if err != nil {
			return err
		}

		err = sysbench_runner.WriteResults(serverConfig, results)
		if err != nil {
			return err
		}
	}

	return nil
}
