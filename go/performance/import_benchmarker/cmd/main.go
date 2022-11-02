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

	"github.com/dolthub/dolt/go/performance/import_benchmarker"
)

const (
	resultsTableName = "results"
)

var configPath = flag.String("config", "", "the path to a config file")

func main() {
	flag.Parse()

	// Construct a config
	config, err := import_benchmarker.NewDefaultImportBenchmarkConfig()
	if *configPath != "" {
		config, err = import_benchmarker.FromFileConfig(*configPath)
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	// Get the working directory the tests will be executing in
	wd := import_benchmarker.GetWorkingDir()

	// Generate the tests and the benchmarker.
	results, err := import_benchmarker.RunBenchmarkTests(config, wd)
	if err != nil {
		log.Fatal(err)
	}

	import_benchmarker.SerializeResults(results, wd, resultsTableName, "csv")

	os.Exit(0)
}
