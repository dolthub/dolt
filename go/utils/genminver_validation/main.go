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
	"reflect"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/minver"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/structwalk"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: genminver_validation <outfile>")
	}

	outFile := os.Args[1]

	lines := []string{
		"# file automatically updated by the release process.",
		"# if you are getting an error with this file it's likely you",
		"# have added a new minver tag with a value other than TBD",
	}

	err := structwalk.Walk(&sqlserver.YAMLConfig{}, func(field reflect.StructField, depth int) error {
		fi := minver.FieldInfoFromStructField(field, depth)
		lines = append(lines, fi.String())
		return nil
	})

	if err != nil {
		log.Fatal("Error generating data for "+outFile+":", err)
	}

	fileContents := strings.Join(lines, "\n")

	fmt.Printf("New contents of '%s'\n%s\n", outFile, fileContents)

	err = os.WriteFile(outFile, []byte(fileContents), 0644)

	if err != nil {
		log.Fatal("Error writing "+outFile+":", err)
	}

	fmt.Printf("'%s' written successfully", outFile)
}
