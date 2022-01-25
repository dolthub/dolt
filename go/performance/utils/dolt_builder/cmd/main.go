// Copyright 2019-2022 Dolthub, Inc.
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
	"fmt"
	"log"
	"os"

	builder "github.com/dolthub/dolt/go/performance/utils/dolt_builder"
)

func main() {
	commitList := os.Args[1:]
	if len(commitList) < 1 {
		helpStr := "dolt-builder takes Dolt commit shas or tags as arguments\n" +
			"and builds corresponding binaries to a path specified\n" +
			"by DOLT_BIN\n" +
			"If DOLT_BIN is not set, ./doltBin will be used\n" +
			"usage: dolt-builder dccba46 4bad226 ...\n" +
			"usage: dolt-builder v0.19.0 v0.22.6 ...\n" +
			"set DEBUG=1 to run in debug mode\n"
		fmt.Print(helpStr)
		os.Exit(2)
	}

	err := builder.Run(commitList)
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}
