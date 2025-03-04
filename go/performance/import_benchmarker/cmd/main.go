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
	"fmt"
	"log"
	"os"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	ib "github.com/dolthub/dolt/go/performance/import_benchmarker"
)

const (
	resultsTableName = "results"
)

var path = flag.String("test", "", "the path to a test file")
var out = flag.String("out", "", "result output path")

func main() {
	flag.Parse()
	def, err := ib.ParseTestsFile(*path)
	if err != nil {
		log.Fatalln(err)
	}

	tmpdir, err := os.MkdirTemp("", "repo-store-")
	if err != nil {
		log.Fatalln(err)
	}

	results := new(ib.ImportResults)
	u, err := driver.NewDoltUser()
	for _, test := range def.Tests {
		test.Results = results
		test.InitWithTmpDir(tmpdir)

		for _, r := range test.Repos {
			var err error
			switch {
			case r.ExternalServer != nil:
				err = test.RunExternalServerTests(r.Name, r.ExternalServer)
			case r.Server != nil:
				err = test.RunSqlServerTests(r, u)
			default:
				err = test.RunCliTests(r, u)
			}
			if err != nil {
				log.Fatalln(err)
			}
		}
	}
	if *out != "" {
		of, err := os.Create(*out)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Fprint(of, results.SqlDump())
	} else {
		fmt.Println(results.SqlDump())
	}
	os.Exit(0)
}
