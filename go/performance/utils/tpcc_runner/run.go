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
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// TODO: Revalidate the config here
func Run() error {
	tpccConfig := &TpccConfig{
		NumThreads:     1,
		ScaleFactor:    1,
		Tables:         1,
		TrxLevel:       "RR", // TODO: Not actually uyses
		ReportCSV:      true,
		ReportInterval: 1,
		Time:           30,
		ScriptDir:      "/Users/vinairachakonda/go/src/dolthub/sysbench-tpcc",
	}

	serverConfig := &sysbench_runner.ServerConfig{
		Id:            "id",
		Host:          "127.0.0.1",
		Port:          3307,
		Server:        "dolt",
		ServerExec:    "/Users/vinairachakonda/go/bin/dolt",
		ResultsFormat: sysbench_runner.CsvFormat,
	}

	ctx := context.Background()

	results, err := BenchmarkDolt(ctx, tpccConfig, serverConfig)
	if err != nil {
		return err
	}

	err = sysbench_runner.WriteResults(serverConfig, results)
	if err != nil {
		return err
	}

	return nil
}
