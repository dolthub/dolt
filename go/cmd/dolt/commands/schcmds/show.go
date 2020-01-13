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

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

const ()

var tblSchemaShortDesc = "Shows the schema of one or more tables."
var tblSchemaLongDesc = "dolt table schema displays the schema of tables at a given commit.  If no commit is provided the working set will be used." +
	"\n" +
	"A list of tables can optionally be provided.  If it is omitted all table schemas will be shown."

var tblSchemaSynopsis = []string{
	"[<commit>] [<table>...]",
}

var bold = color.New(color.Bold)

type ShowCmd struct{}

func (cmd ShowCmd) Name() string {
	return "show"
}

func (cmd ShowCmd) Description() string {
	return "Shows the schema of one or more tables."
}

func (cmd ShowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table(s) whose schema is being displayed."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."

	help, usage := cli.HelpAndUsagePrinters(commandStr, tblSchemaShortDesc, tblSchemaLongDesc, tblSchemaSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	verr := printSchemas(ctx, apr, dEnv)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func printSchemas(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	cmStr := "working"
	args := apr.Args()

	var root *doltdb.RootValue
	var verr errhand.VerboseError
	var cm *doltdb.Commit

	if apr.NArg() == 0 {
		cm, verr = nil, nil
	} else {
		cm, verr = commands.MaybeGetCommitWithVErr(dEnv, args[0])
	}

	if verr == nil {
		if cm != nil {
			cmStr = args[0]
			args = args[1:]

			var err error
			root, err = cm.GetRootValue()

			if err != nil {
				verr = errhand.BuildDError("unable to get root value").AddCause(err).Build()
			}
		} else {
			root, verr = commands.GetWorkingWithVErr(dEnv)
		}
	}

	if verr == nil {
		tables := args

		// If the user hasn't specified table names, try to grab them all;
		// show usage and error out if there aren't any
		if len(tables) == 0 {
			var err error
			tables, err = root.GetTableNames(ctx)

			if err != nil {
				return errhand.BuildDError("unable to get table names.").AddCause(err).Build()
			}

			if len(tables) == 0 {
				return errhand.BuildDError("").SetPrintUsage().Build()
			}
		}

		var notFound []string
		for _, tblName := range tables {
			tbl, ok, err := root.GetTable(ctx, tblName)

			if err != nil {
				return errhand.BuildDError("unable to get table '%s'", tblName).AddCause(err).Build()
			}

			if !ok {
				notFound = append(notFound, tblName)
			} else {
				verr = printTblSchema(ctx, cmStr, tblName, tbl)
				cli.Println()
			}
		}

		for _, tblName := range notFound {
			cli.PrintErrln(color.YellowString("%s not found", tblName))
		}
	}

	return verr
}

func printTblSchema(ctx context.Context, cmStr string, tblName string, tbl *doltdb.Table) errhand.VerboseError {
	cli.Println(bold.Sprint(tblName), "@", cmStr)
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return errhand.BuildDError("unable to get schema").AddCause(err).Build()
	}

	cli.Println(sql.SchemaAsCreateStmt(tblName, sch))
	return nil
}

func errToVerboseErr(oldName, newName string, err error) errhand.VerboseError {
	switch err {
	case schema.ErrColNameCollision:
		return errhand.BuildDError("error: A column already exists with the name %s", newName).Build()

	case schema.ErrColNotFound:
		return errhand.BuildDError("error: Column %s unknown", oldName).Build()

	default:
		return errhand.BuildDError("error: Failed to alter schema").AddCause(err).Build()
	}
}
