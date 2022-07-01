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
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"os"
	"testing"
)

const (
	smallSet         = 100000
	mediumSet        = 1000
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

	test := NewImportBenchmarkTest(config)

	benchmarkFunc := BenchmarkDoltImport(test)
	br := testing.Benchmark(benchmarkFunc)
	res := result{
		name:    config.Name,
		format:  config.Format,
		rows:    config.NumRows,
		columns: len(genSampleCols()),
		br:      br,
	}
	results := make([]result, 0)
	results = append(results, res)

	// write results data
	serializeResults(results, getWorkingDir(), resultsTableName, csvExt)

	// cleanup temp dolt data dir
	removeTempDoltDataDir(filesys.LocalFS)

	os.Exit(0)
}
