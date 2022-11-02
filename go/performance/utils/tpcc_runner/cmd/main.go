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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	tpcc_runner "github.com/dolthub/dolt/go/performance/utils/tpcc_runner"
)

var configFile = flag.String("config", "", "path to config file q")

func main() {
	fmt.Println("Running the TPCC benchmark.")

	flag.Parse()
	if *configFile == "" {
		log.Fatal("Must supply config")
	}

	configPath, err := filepath.Abs(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = os.Stat(configPath); os.IsNotExist(err) {
		log.Fatal(err)
	}

	tpccBenchmarkConfig, err := tpcc_runner.FromFileConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}

	// Run the TPCC test
	err = tpcc_runner.Run(tpccBenchmarkConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Exit(0)
}
