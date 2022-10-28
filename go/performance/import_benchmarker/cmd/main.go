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
	ib "github.com/dolthub/dolt/go/performance/import_benchmarker"
	"log"
	"os"
)

const (
	resultsTableName = "results"
)

var path = flag.String("testse", "", "the path to a test file")

func main() {
	flag.Parse()
	def, err := ib.ParseTestsFile(*path)
	if err != nil {
		log.Fatalln(err)
	}
	results := new(ib.ImportResults)
	for _, test := range def.Tests {
		for _, r := range test.Repos {
			if r.ExternalServer != nil {
				// mysql conn
				db, err := ib.ConnectDB(r.ExternalServer.User, r.ExternalServer.Password, r.ExternalServer.Name, r.ExternalServer.Host, r.ExternalServer.Port)
				if err != nil {
					log.Fatalln(err)
				}
				err = test.RunServerTests(r.Name, db, results)
				if err != nil {
					log.Fatalln(err)
				}
			} else if r.Server != nil {
				u, err := ib.NewDoltUser()
				if err != nil {
					log.Fatalln(err)
				}
				rs, err := u.MakeRepoStore()
				if err != nil {
					log.Fatalln(err)
				}

				// start dolt server
				repo, err := ib.MakeRepo(rs, r)
				r.Server.Args = append(r.Server.Args, "")
				server, err := ib.MakeServer(repo, r.Server)
				if server != nil {
					server.DBName = r.Name
				}
				defer server.GracefulStop()

				db, err := server.DB()
				if err != nil {
					log.Fatalln(err)
				}

				_, err = db.Exec("SET GLOBAL local_infile=1 ")
				if err != nil {
					log.Fatalln(err)
				}

				err = test.RunServerTests(r.Name, db, results)
				if err != nil {
					log.Fatalln(err)
				}
			} else {
				u, err := ib.NewDoltUser()
				if err != nil {
					log.Fatalln(err)
				}
				rs, err := u.MakeRepoStore()
				if err != nil {
					log.Fatalln(err)
				}

				// cli only access
				repo, err := ib.MakeRepo(rs, r)
				if err != nil {
					log.Fatalln(err)
				}
				err = test.RunCliTests(r.Name, repo, results)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}
	}
	fmt.Println(results.String())
	os.Exit(0)
}
