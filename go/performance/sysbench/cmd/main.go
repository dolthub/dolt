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

	"github.com/dolthub/dolt/go/performance/sysbench"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

var run = flag.String("run", "", "the path to a test file")
var scriptDir = flag.String("script-dir", "", "the path to the script directory")
var config = flag.String("config", "", "the path to a config file")
var out = flag.String("out", "", "result output path")
var verbose = flag.Bool("verbose", true, "verbose output")

func main() {
	flag.Parse()
	defs, err := sysbench.ParseTestsFile(*run)
	if err != nil {
		log.Fatalln(err)
	}

	conf, err := sysbench.ParseConfig(*config)
	if err != nil {
		log.Fatalln(err)
	}

	conf = conf.WithScriptDir(*scriptDir).WithVerbose(*verbose)
	if err := os.Chdir(*scriptDir); err != nil {
		log.Fatalf("failed to 'cd %s'", *scriptDir)
	}

	tmpdir, err := os.MkdirTemp("", "repo-store-")
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(tmpdir)

	userdir, err := os.MkdirTemp("", "sysbench-user-dir_")
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(userdir)

	results := new(sysbench.Results)
	u, err := driver.NewDoltUser(userdir)
	for _, test := range defs.Tests {
		test.InitWithTmpDir(tmpdir)

		for _, r := range test.Repos {
			var err error
			switch {
			case r.ExternalServer != nil:
				err = test.RunExternalServerTests(r.Name, r.ExternalServer, conf)
			case r.Server != nil:
				err = test.RunSqlServerTests(r, u, conf)
			default:
				panic("unsupported")
			}
			if err != nil {
				log.Fatalln(err)
			}
		}
		results.Append(test.Results.Res...)
	}
	if *out != "" {
		of, err := os.Create(*out)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Fprintf(of, results.SqlDump())
	} else {
		fmt.Println(results.SqlDump())
	}
}
