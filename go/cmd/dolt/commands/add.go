// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

const allParam = "all"

var addShortDesc = `Add table contents to the list of staged tables`
var addLongDesc = `This command updates the list of tables using the current content found in the working root, to prepare the content staged for the next commit. It adds the current content of existing tables as a whole or remove tables that do not exist in the working root anymore.

This command can be performed multiple times before a commit. It only adds the content of the specified table(s) at the time the add command is run; if you want subsequent changes included in the next commit, then you must run dolt add again to add the new content to the index.

The dolt status command can be used to obtain a summary of which tables have changes that are staged for the next commit.`
var addSynopsis = []string{
	`[<table>...]`,
}

func Add(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "Working table(s) to add to the list tables staged to be committed. The abbreviation '.' can be used to add all tables."
	ap.SupportsFlag(allParam, "a", "Stages any and all changes (adds, deletes, and modifications).")
	helpPr, _ := cli.HelpAndUsagePrinters(commandStr, addShortDesc, addLongDesc, addSynopsis, ap)
	apr := cli.ParseArgs(ap, args, helpPr)

	allFlag := apr.Contains(allParam)

	var err error
	if apr.NArg() == 0 && !allFlag {
		cli.Println("Nothing specified, nothing added.\n Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		err = actions.StageAllTables(context.Background(), dEnv, false)
	} else {
		err = actions.StageTables(context.Background(), dEnv, apr.Args(), false)
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
		bdr := errhand.BuildDError("Some of the specified tables were not found")
		bdr.AddDetails("Unknown tables: %v", tbls)

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
