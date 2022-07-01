// Copyright 2019 Dolthub, Inc.
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

package main

import (
	"flag"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	smallSet         = 100000
	mediumSet        = 1000000
	largeSet         = 10000000
	resultsTableName = "results"
)

var configPath = flag.String("config", "", "the path to a config file")

func main() {
	flag.Parse()

	config := NewDefaultImportBenchmarkConfig()
	var err error
	if *configPath != "" {
		config, err = FromFileConfig(*configPath)
	}

	if err != nil {
		panic(err.Error())
	}

	tests := NewImportBenchmarkTests(config)
	results := make([]result, 0)

	for i, test := range tests {
		benchmarkFunc := BenchmarkDoltImport(test)
		br := testing.Benchmark(benchmarkFunc)
		res := result{
			name:             config.Jobs[i].Name,
			format:           config.Jobs[i].Format,
			rows:             config.Jobs[i].NumRows,
			columns:          len(genSampleCols()),
			garbageGenerated: getAmountOfGarbageGenerated(),
			br:               br,
		}
		results = append(results, res)
	}

	// write results data
	serializeResults(results, getWorkingDir(), resultsTableName, csvExt)

	// cleanup temp dolt data dir
	removeTempDoltDataDir(filesys.LocalFS)

	os.Exit(0)
}
