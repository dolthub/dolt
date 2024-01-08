package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

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
		fi := sqlserver.MinVerFieldInfoFromStructField(field, depth)
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
