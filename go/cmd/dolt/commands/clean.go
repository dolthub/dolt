// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	DryrunCleanParam = "dry-run"
)

var cleanDocContent = cli.CommandDocumentationContent{
	ShortDesc: "Deletes untracked working tables",
	LongDesc: "{{.EmphasisLeft}}dolt clean [--dry-run]{{.EmphasisRight}}\n\n" +
		"The default (parameterless) form clears the values for all untracked working {{.LessThan}}tables{{.GreaterThan}} ." +
		"This command permanently deletes unstaged or uncommitted tables.\n\n" +
		"The {{.EmphasisLeft}}--dry-run{{.EmphasisRight}} flag can be used to test whether the clean can succeed without " +
		"deleting any tables from the current working set.\n\n" +
		"{{.EmphasisLeft}}dolt clean [--dry-run] {{.LessThan}}tables{{.GreaterThan}}...{{.EmphasisRight}}\n\n" +
		"If {{.LessThan}}tables{{.GreaterThan}} is specified, only those table names are considered for deleting.\n\n",
	Synopsis: []string{
		"[--dry-run]",
		"[--dry-run] {{.LessThan}}tables{{.GreaterThan}}...",
	},
}

type CleanCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CleanCmd) Name() string {
	return "clean"
}

// Description returns a description of the command
func (cmd CleanCmd) Description() string {
	return "Remove untracked tables from working set."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CleanCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cli.CreateCleanArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, cleanDocContent, ap))
}

func (cmd CleanCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCleanArgParser()
}

// Exec executes the command
func (cmd CleanCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateCleanArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cleanDocContent, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	roots, err = actions.CleanUntracked(ctx, roots, apr.Args, apr.Contains(DryrunCleanParam))
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge())
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return handleResetError(err, usage)
}
