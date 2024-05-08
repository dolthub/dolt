// Copyright 2024 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/minver"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: genminver_validation <outfile>")
	}

	outFile := os.Args[1]

	err := minver.GenValidationFile(&sqlserver.YAMLConfig{}, outFile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("'%s' written successfully", outFile)
}
