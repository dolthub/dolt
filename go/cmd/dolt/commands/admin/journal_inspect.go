// Copyright 2025 Dolthub, Inc.
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

package admin

import (
	"context"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/nbs"
)

type JournalInspectCmd struct {
}

func (cmd JournalInspectCmd) Name() string {
	return "journal-inspect"
}

func (cmd JournalInspectCmd) Description() string {
	return "Inspect a Dolt journal file and display information about it."
}

func (cmd JournalInspectCmd) RequiresRepo() bool {
	return false
}

func (cmd JournalInspectCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		ShortDesc: "Inspect a Dolt journal file and display information about it",
		LongDesc: `This tool is intented for debugging Dolt journal files. Since it is intended to debug potentially
corrupted files, it is best run from a location which doesn't attempt to load databases. Ie, go to /tmp, and run
dolt admin journal-inspect /path/to/journal/file`,
		Synopsis: []string{
			"<journal-path>",
		},
	}
}

func (cmd JournalInspectCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.SupportsFlag("roots", "r", "Display root hashes found in the journal")
	ap.SupportsFlag("chunks", "c", "Display chunk hashes found in the journal")
	ap.SupportsFlag("verbose", "v", "Display verbose output during inspection (same as -r -c")
	return ap
}

func (cmd JournalInspectCmd) Exec(_ context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, usage)

	var journalPath string
	if apr.NArg() == 1 {
		journalPath = apr.Arg(0)
	} else {
		usage()
		return 1
	}
	seeRoots := apr.Contains("roots")
	seeChunks := apr.Contains("chunks")
	if apr.Contains("verbose") {
		seeRoots = true
		seeChunks = true
	}

	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		cli.PrintErrln("Error: Journal file does not exist:", journalPath)
		return 1
	}

	absPath, err := filepath.Abs(journalPath)
	if err != nil {
		cli.PrintErrln("Error getting absolute path:", err.Error())
		return 1
	}

	// JournalInspect returns an exit code. It's entire purpose it to print errors, after all.
	return nbs.JournalInspect(absPath, seeRoots, seeChunks)
}
