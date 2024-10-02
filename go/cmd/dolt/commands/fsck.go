// Copyright 2023 Dolthub, Inc.
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

package commands

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type FsckCmd struct{}

var _ cli.Command = FsckCmd{}

func (cmd FsckCmd) Description() string {
	return "Verifies the contents of the database are not corrupted."
}

var fsckDocs = cli.CommandDocumentationContent{
	ShortDesc: "Verifies the contents of the database are not corrupted.",
	LongDesc:  "Verifies the contents of the database are not corrupted.",
	Synopsis: []string{
		"[--quiet]",
		"--threads 16",
	},
}

func (cmd FsckCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(fsckDocs, cmd.ArgParser())
}

func (cmd FsckCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsInt(cli.ThreadsFlag, "", "thread_count", "Number of threads to use for fsck. Defaults to 8.")
	ap.SupportsFlag(cli.QuietFlag, "", "Don't show progress. Just print final report.")

	return ap
}

func (cmd FsckCmd) Name() string {
	return "fsck"
}

func (cmd FsckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, fsckDocs)
	if terminate {
		return status
	}

	quiet := apr.Contains(cli.QuietFlag)
	progress := make(chan interface{}, 32)
	defer close(progress)
	fsckHandleProgress(progress, quiet)

	threads, haveThreads := apr.GetInt(cli.ThreadsFlag)
	if !haveThreads {
		threads = 8
	}
	if threads <= 0 {
		threads = 1
	}

	report, err := dEnv.DoltDB.FSCK(ctx, threads, progress)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	cli.Printf("Chunks Scanned: %d\n", report.ChunkCount)
	if len(report.Problems) == 0 {
		cli.Println("No problems found.")
		return 0
	} else {
		for _, e := range report.Problems {
			cli.PrintErrln(e.Error())
		}

		return 1
	}
}

func fsckHandleProgress(progress chan interface{}, quiet bool) {
	go func() {
		for item := range progress {
			if !quiet {
				cli.Println(item)
			}
		}
	}()
}
