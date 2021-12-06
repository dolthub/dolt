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

package commands

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var statusDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show the working status",
	LongDesc:  `Displays working tables that differ from the current HEAD commit, tables that differ from the staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would commit by running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}; the second and third are what you could commit by running {{.EmphasisLeft}}dolt add .{{.EmphasisRight}} before running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,
	Synopsis:  []string{""},
}

type StatusCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StatusCmd) Name() string {
	return "status"
}

// Description returns a description of the command
func (cmd StatusCmd) Description() string {
	return "Show the working tree status."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd StatusCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, statusDocs, ap))
}

func (cmd StatusCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, statusDocs, ap))
	cli.ParseArgsOrDie(ap, args, help)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, roots)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	workingTblsWithViolations, _, _, err := merge.GetTablesWithConstraintViolations(ctx, roots)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	docsOnDisk, err := dEnv.DocsReadWriter().GetDocsOnDisk()
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	stagedDocDiffs, notStagedDocDiffs, err := diff.GetDocDiffs(ctx, roots, docsOnDisk)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	err = PrintStatus(ctx, dEnv, staged, notStaged, workingTblsInConflict, workingTblsWithViolations, stagedDocDiffs, notStagedDocDiffs)
	if err != nil {
		cli.PrintErrln(toStatusVErr(err).Verbose())
		return 1
	}

	return 0
}

func toStatusVErr(err error) errhand.VerboseError {
	return errhand.VerboseErrorFromError(err)
}
