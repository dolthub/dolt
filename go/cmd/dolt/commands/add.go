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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var addDocs = cli.CommandDocumentationContent{
	ShortDesc: `Add table contents to the list of staged tables`,
	LongDesc: `
This command updates the list of tables using the current content found in the working root, to prepare the content staged for the next commit. It adds the current content of existing tables as a whole or remove tables that do not exist in the working root anymore.

This command can be performed multiple times before a commit. It only adds the content of the specified table(s) at the time the add command is run; if you want subsequent changes included in the next commit, then you must run dolt add again to add the new content to the index.

The dolt status command can be used to obtain a summary of which tables have changes that are staged for the next commit.`,
	Synopsis: []string{
		`[{{.LessThan}}table{{.GreaterThan}}...]`,
	},
}

type AddCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd AddCmd) Name() string {
	return "add"
}

// Description returns a description of the command
func (cmd AddCmd) Description() string {
	return "Add table changes to the list of staged table changes."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd AddCmd) CreateMarkdown(writer io.Writer, commandStr string) error {
	ap := cli.CreateAddArgParser()
	return CreateMarkdown(writer, cli.GetCommandDocumentation(commandStr, addDocs, ap))
}

func (cmd AddCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateAddArgParser()
}

// Exec executes the command
func (cmd AddCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateAddArgParser()
	helpPr, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, addDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, helpPr)

	if apr.ContainsArg(doltdb.DocTableName) {
		// Only allow adding the dolt_docs table if it has a conflict to resolve
		hasConflicts, _ := docCnfsOnWorkingRoot(ctx, dEnv)
		if !hasConflicts {
			return HandleDocTableVErrAndExitCode()
		}
	}

	allFlag := apr.Contains(cli.AllFlag)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return handleStageError(err)
	}

	if apr.NArg() == 0 && !allFlag {
		cli.Println("Nothing specified, nothing added.\n Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		roots, err = actions.StageAllTables(ctx, roots, dEnv.Docs)
		if err != nil {
			return handleStageError(err)
		}
	} else {
		tables, docs, err := actions.GetTablesOrDocs(dEnv.DocsReadWriter(), apr.Args)
		if err != nil {
			return handleStageError(err)
		}

		roots, err = actions.StageTables(ctx, roots, docs, tables)
		if err != nil {
			return handleStageError(err)
		}
	}

	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		return handleStageError(err)
	}

	return 0
}

func handleStageError(err error) int {
	cli.PrintErrln(toAddVErr(err).Verbose())
	return 1
}

func toAddVErr(err error) errhand.VerboseError {
	switch {
	case doltdb.IsRootValUnreachable(err):
		rt := doltdb.GetUnreachableRootType(err)
		bdr := errhand.BuildDError("Unable to read %s.", rt.String())
		bdr.AddCause(doltdb.GetUnreachableRootCause(err))
		return bdr.Build()

	case actions.IsTblNotExist(err):
		tbls := actions.GetTablesForError(err)
		bdr := errhand.BuildDError("Some of the specified tables or docs were not found")
		bdr.AddDetails("Unknown tables or docs: %v", tbls)

		return bdr.Build()

	case actions.IsTblInConflict(err) || actions.IsTblViolatesConstraints(err):
		tbls := actions.GetTablesForError(err)
		bdr := errhand.BuildDError("error: not all tables merged")

		for _, tbl := range tbls {
			bdr.AddDetails("  %s", tbl)
		}

		return bdr.Build()

	default:
		return errhand.BuildDError("Unknown error").AddCause(err).Build()
	}
}

func HandleDocTableVErrAndExitCode() int {
	return HandleVErrAndExitCode(errhand.BuildDError("'%s' is not a valid table name", doltdb.DocTableName).Build(), nil)
}
