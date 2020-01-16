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

var schRenameColShortDesc = "Renames a column of the specified table."
var schRenameColLongDesc = "<b>dolt schema rename-column</b> will change the display name of a column. Changing the name" +
	"of the column will only modify the schema and no data will change."
var schRenameColSynopsis = []string{
	"<table> <old> <new>",
}

type RenameColumnCmd struct{}

func (cmd RenameColumnCmd) Name() string {
	return "rename-column"
}

func (cmd RenameColumnCmd) Description() string {
	return "Renames a column of the specified table."
}

func (cmd RenameColumnCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, schRenameColShortDesc, schRenameColLongDesc, schRenameColSynopsis, ap)
}

func (cmd RenameColumnCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table being modified."})
	return ap
}

func (cmd RenameColumnCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

func (cmd RenameColumnCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, schRenameColShortDesc, schRenameColLongDesc, schRenameColSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = renameColumn(ctx, apr, root, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func renameColumn(ctx context.Context, apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Table name, current column name, and new column name are needed to rename column.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if has, err := root.HasTable(ctx, tblName); err != nil {
		return errhand.BuildDError("error: failed to read tables from database").AddCause(err).Build()
	} else if !has {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
	}

	oldColName := apr.Arg(1)
	newColName := apr.Arg(2)

	newTbl, err := alterschema.RenameColumn(ctx, dEnv.DoltDB, tbl, oldColName, newColName)
	if err != nil {
		return errToVerboseErr(oldColName, newColName, err)
	}

	root, err = root.PutTable(ctx, tblName, newTbl)

	if err != nil {
		return errhand.BuildDError("error: failed to write table back to database").Build()
	}

	return commands.UpdateWorkingWithVErr(dEnv, root)
}
