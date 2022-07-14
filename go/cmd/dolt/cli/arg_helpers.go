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

package cli

import (
	"errors"
	"os"

	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var ErrEmptyDefTuple = errors.New("empty definition tuple")

type UsagePrinter func()

// ParseArgs is used for Dolt SQL functions that are run on the server and should not exit
func ParseArgs(ap *argparser.ArgParser, args []string, usagePrinter UsagePrinter) (*argparser.ArgParseResults, error) {
	apr, err := ap.Parse(args)

	if err != nil {
		// --help param
		if usagePrinter != nil {
			usagePrinter()
		}

		return nil, err
	}

	return apr, nil
}

// ParseArgsOrDie is used for CLI command that should exit after erroring.
func ParseArgsOrDie(ap *argparser.ArgParser, args []string, usagePrinter UsagePrinter) *argparser.ArgParseResults {
	apr, err := ap.Parse(args)

	if err != nil {
		if err != argparser.ErrHelp {
			PrintErrln(err.Error())

			if usagePrinter != nil {
				usagePrinter()
			}

			os.Exit(1)
		}

		// --help param
		if usagePrinter != nil {
			usagePrinter()
		}
		os.Exit(0)
	}

	return apr
}

func HelpAndUsagePrinters(cmdDoc *CommandDocumentation) (UsagePrinter, UsagePrinter) {
	// TODO handle error states
	longDesc, _ := cmdDoc.GetLongDesc(CliFormat)
	synopsis, _ := cmdDoc.GetSynopsis(CliFormat)

	return func() {
			PrintHelpText(cmdDoc.CommandStr, cmdDoc.GetShortDesc(), longDesc, synopsis, cmdDoc.ArgParser)
		}, func() {
			PrintUsage(cmdDoc.CommandStr, synopsis, cmdDoc.ArgParser)
		}
}
