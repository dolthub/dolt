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

	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
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
func (cmd AddCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cli.CreateAddArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, addDocs, ap))
}

// Exec executes the command
func (cmd AddCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateAddArgParser()
	helpPr, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, addDocs, ap))
	apr := cli.ParseArgs(ap, args, helpPr)

	if apr.ContainsArg(doltdb.DocTableName) {
		// Only allow adding the dolt_docs table if it has a conflict to resolve
		hasConflicts, _ := docCnfsOnWorkingRoot(ctx, dEnv)
		if !hasConflicts {
			return HandleDocTableVErrAndExitCode()
		}
	}

	allFlag := apr.Contains(cli.AllFlag)

	var err error
	if apr.NArg() == 0 && !allFlag {
		cli.Println("Nothing specified, nothing added.\n Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		err = actions.StageAllTables(ctx, dEnv.DbData())
	} else {
		err = actions.StageTables(ctx, dEnv.DbData(), apr.Args())
	}

	if err != nil {
		cli.PrintErrln(toAddVErr(err).Verbose())
		return 1
	}

	return 0
}

func toAddVErr(err error) errhand.VerboseError {
	switch {
	case actions.IsRootValUnreachable(err):
		rt := actions.GetUnreachableRootType(err)
		bdr := errhand.BuildDError("Unable to read %s.", rt.String())
		bdr.AddCause(actions.GetUnreachableRootCause(err))
		return bdr.Build()

	case actions.IsTblNotExist(err):
		tbls := actions.GetTablesForError(err)
		bdr := errhand.BuildDError("Some of the specified tables or docs were not found")
		bdr.AddDetails("Unknown tables or docs: %v", tbls)

		return bdr.Build()

	case actions.IsTblInConflict(err):
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
