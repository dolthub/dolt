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

package schcmds

import (
	"context"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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

func (cmd ShowCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(tblSchemaDocs, ap)
}

func (cmd ShowCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose schema is being displayed."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"commit", "commit at which point the schema will be displayed."})
	return ap
}

// EventType returns the type of the event to log
func (cmd ShowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

// Exec executes the command
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tblSchemaDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	verr := printSchemas(ctx, apr, dEnv)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func printSchemas(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	cmStr := "working"
	args := apr.Args

	var root doltdb.RootValue
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
			root, err = cm.GetRootValue(ctx)

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
			tables, err = root.GetTableNames(ctx, doltdb.DefaultSchemaName)

			if err != nil {
				return errhand.BuildDError("unable to get table names.").AddCause(err).Build()
			}

			tables = actions.RemoveDocsTable(tables)
			if len(tables) == 0 {
				cli.Println("No tables in working set")
				return nil
			}
		}

		tmpDir, err := dEnv.TempTableFilesDir()
		if err != nil {
			return errhand.BuildDError("error: ").AddCause(err).Build()
		}
		opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
		sqlCtx, engine, _ := dsqle.PrepareCreateTableStmt(ctx, dsqle.NewUserSpaceDatabase(root, opts))

		var notFound []string
		for _, tblName := range tables {
			if doltdb.IsFullTextTable(tblName) {
				continue
			}
			ok, err := root.HasTable(ctx, doltdb.TableName{Name: tblName})
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
