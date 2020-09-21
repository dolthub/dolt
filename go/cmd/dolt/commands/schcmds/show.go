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

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var tblSchemaDocs = cli.CommandDocumentationContent{
	ShortDesc: "Shows the schema of one or more tables.",
	LongDesc: `{{.EmphasisLeft}}dolt schema show{{.EmphasisRight}} displays the schema of tables at a given commit.  If no commit is provided the working set will be used.

A list of tables can optionally be provided.  If it is omitted all table schemas will be shown.`,
	Synopsis: []string{
		"[{{.LessThan}}commit{{.GreaterThan}}] [{{.LessThan}}table{{.GreaterThan}}...]",
	},
}

var bold = color.New(color.Bold)

type ShowCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ShowCmd) Name() string {
	return "show"
}

// Description returns a description of the command
func (cmd ShowCmd) Description() string {
	return "Shows the schema of one or more tables."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ShowCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, tblSchemaDocs, ap))
}

func (cmd ShowCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose schema is being displayed."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"commit", "commit at which point the schema will be displayed."})
	return ap
}

// EventType returns the type of the event to log
func (cmd ShowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

// Exec executes the command
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblSchemaDocs, ap))
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

			tables = commands.RemoveDocsTbl(tables)
			if len(tables) == 0 {
				cli.Println("No tables in working set")
				return nil
			}
		}

		sqlCtx, engine, _ := dsqle.PrepareCreateTableStmt(ctx, root)

		var notFound []string
		for _, tblName := range tables {
			ok, err := root.HasTable(ctx, tblName)
			if err != nil {
				return errhand.BuildDError("unable to get table '%s'", tblName).AddCause(err).Build()
			}

			if !ok {
				notFound = append(notFound, tblName)
			} else {
				cli.Println(bold.Sprint(tblName), "@", cmStr)
				stmt, err := dsqle.GetCreateTableStmt(sqlCtx, engine, tblName)
				if err != nil {
					return errhand.VerboseErrorFromError(err)
				}
				cli.Println(stmt)
				cli.Println()
			}
		}

		for _, tblName := range notFound {
			cli.PrintErrln(color.YellowString("%s not found", tblName))
		}
	}

	return verr
}
