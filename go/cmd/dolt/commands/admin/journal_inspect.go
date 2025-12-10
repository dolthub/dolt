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

	"github.com/sirupsen/logrus"

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
dolt admin journal-inspect /path/to/journal/file

When using the --filter-roots or --filter-chunks options, a new journal file will be created next to the original 
file with the .filtered extension. This new journal will be identical to the original except that it will not contain 
records of the specified type with any of the specified hashes. Multiple hashes can be provided as a comma-separated 
list. The two filter options can be used together to filter both root and chunk records.`,
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
	ap.SupportsFlag("crc-scan", "", "Scan invalid sections for valid CRCs. SLOW.")
	ap.SupportsFlag("snappy-scan", "", "Scan invalid sections for snappy tags and content headers.")
	ap.SupportsString("filter-roots", "", "hashcode1,hashcode2,...", "Create filtered copy of journal excluding the specified root hashes (comma-separated)")
	ap.SupportsString("filter-chunks", "", "hashcode1,hashcode2,...", "Create filtered copy of journal excluding the specified chunk hashes (comma-separated)")
	return ap
}

func (cmd JournalInspectCmd) Exec(_ context.Context, commandStr string, args []string, _ *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, usage)

	// All output is to logrus, which prints to stderr by default. We want it to print to stdout, so we can pipe to `less` etc.
	logrus.SetOutput(cli.CliOut)
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:      true,
		DisableTimestamp: false,
		FullTimestamp:    false, // keeps the "INFO[0000]" short style
	})

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

	crcScan := apr.Contains("crc-scan")
	snappScan := apr.Contains("snappy-scan")
	filterRootsStr := apr.GetValueOrDefault("filter-roots", "")
	filterChunksStr := apr.GetValueOrDefault("filter-chunks", "")

	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		cli.PrintErrln("Error: Journal file does not exist:", journalPath)
		return 1
	}

	absPath, err := filepath.Abs(journalPath)
	if err != nil {
		cli.PrintErrln("Error getting absolute path:", err.Error())
		return 1
	}

	// Handle filter mode
	if filterRootsStr != "" || filterChunksStr != "" {
		return nbs.JournalFilter(absPath, filterRootsStr, filterChunksStr)
	}

	// JournalInspect returns an exit code. It's entire purpose it to print errors, after all.
	return nbs.JournalInspect(absPath, seeRoots, seeChunks, crcScan, snappScan)
}
