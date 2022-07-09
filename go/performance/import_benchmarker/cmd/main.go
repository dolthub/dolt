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
	"log"
	"os"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/performance/import_benchmarker"
)

const (
	resultsTableName = "results"
)

var configPath = flag.String("config", "", "the path to a config file")

func main() {
	flag.Parse()

	// Construct a config
	config := import_benchmarker.NewDefaultImportBenchmarkConfig()
	var err error
	if *configPath != "" {
		config, err = import_benchmarker.FromFileConfig(*configPath)
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	// Get the working directory the tests will be executing in
	wd := import_benchmarker.GetWorkingDir()

	// Delete any .dolt directories even in the case of an error created by the benchmarker.
	defer import_benchmarker.RemoveTempDoltDataDir(filesys.LocalFS, wd)

	// Generate the tests and the benchmarker.
	tests := import_benchmarker.NewImportBenchmarkTests(config)
	results := import_benchmarker.RunBenchmarkTests(config, tests)

	import_benchmarker.SerializeResults(results, wd, resultsTableName, "csv")

	os.Exit(0)
}
