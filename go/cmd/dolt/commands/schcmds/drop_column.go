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

package schcmds

import (
	"context"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var schDropColShortDesc = "Removes a column of the specified table"
var schDropColLongDesc = ""
var schDropColSynopsis = []string{
	"<table> <column>",
}

type DropColumnCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DropColumnCmd) Name() string {
	return "drop-column"
}

// Description returns a description of the command
func (cmd DropColumnCmd) Description() string {
	return "Removes a column of the specified table."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd DropColumnCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, schDropColShortDesc, schDropColLongDesc, schDropColSynopsis, ap)
}

func (cmd DropColumnCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose schema is being displayed."})
	return ap
}

// EventType returns the type of the event to log
func (cmd DropColumnCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

// Exec executes the command
func (cmd DropColumnCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, schDropColShortDesc, schDropColLongDesc, schDropColSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.ContainsArg(doltdb.DocTableName) {
		return commands.HandleDocTableVErrAndExitCode()
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = removeColumn(ctx, apr, root, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func removeColumn(ctx context.Context, apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Table name and column to be removed must be specified.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if has, err := root.HasTable(ctx, tblName); err != nil {
		return errhand.BuildDError("error: failed to read tables from database.").Build()
	} else if !has {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
	}

	colName := apr.Arg(1)

	newTbl, err := alterschema.DropColumn(ctx, tbl, colName)

	if err != nil {
		return errToVerboseErr(colName, "", err)
	}

	root, err = root.PutTable(ctx, tblName, newTbl)

	if err != nil {
		return errhand.BuildDError("error: failed to write table back to database").AddCause(err).Build()
	}

	return commands.UpdateWorkingWithVErr(dEnv, root)
}
