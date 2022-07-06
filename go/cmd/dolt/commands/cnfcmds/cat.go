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

package cnfcmds

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var catDocs = cli.CommandDocumentationContent{
	ShortDesc: "print conflicts",
	LongDesc:  `The dolt conflicts cat command reads table conflicts and writes them to the standard output.`,
	Synopsis: []string{
		"[{{.LessThan}}commit{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}}...",
	},
}

type CatCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CatCmd) Name() string {
	return "cat"
}

// Description returns a description of the command
func (cmd CatCmd) Description() string {
	return "Writes out the table conflicts."
}

func (cmd CatCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(catDocs, ap)
}

// EventType returns the type of the event to log
func (cmd CatCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CONF_CAT
}

func (cmd CatCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be printed. '.' can be used to print conflicts for all tables."})

	return ap
}

// Exec executes the command
func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, catDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	args = apr.Args

	if len(args) == 0 {
		cli.Println("No tables specified")
		cli.Println(" Maybe you wanted to say 'dolt conflicts cat .'?")
		usage()
		return 1
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return exitWithVerr(verr)
	}

	cm, verr := commands.MaybeGetCommitWithVErr(dEnv, args[0])
	if verr != nil {
		return exitWithVerr(verr)
	}

	// If no commit was resolved from the first argument, assume the args are all table names and print the conflicts
	if cm == nil {
		if verr := printConflicts(ctx, dEnv, root, args); verr != nil {
			return exitWithVerr(verr)
		}

		return 0
	}

	tblNames := args[1:]
	if len(tblNames) == 0 {
		cli.Println("No tables specified")
		usage()
		return 1
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return exitWithVerr(errhand.BuildDError("unable to get the root value").AddCause(err).Build())
	}

	if verr = printConflicts(ctx, dEnv, root, tblNames); verr != nil {
		return exitWithVerr(verr)
	}

	return 0
}

func exitWithVerr(verr errhand.VerboseError) int {
	cli.PrintErrln(verr.Verbose())
	return 1
}

func printConflicts(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, tblNames []string) errhand.VerboseError {
	if len(tblNames) == 1 && tblNames[0] == "." {
		var err error
		tblNames, err = root.GetTableNames(ctx)
		if err != nil {
			return errhand.BuildDError("unable to read tables").AddCause(err).Build()
		}
	}

	eng, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	for _, tblName := range tblNames {
		verr := func() errhand.VerboseError {
			if has, err := root.HasTable(ctx, tblName); err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			} else if !has {
				return errhand.BuildDError("error: unknown table '%s'", tblName).Build()
			}

			tbl, _, err := root.GetTable(ctx, tblName)
			if err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			}

			has, err := tbl.HasConflicts(ctx)
			if err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			}
			if !has {
				return nil
			}

			baseSch, sch, mergeSch, err := tbl.GetConflictSchemas(ctx, tblName)
			if err != nil {
				return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
			}
			unionSch, err := untyped.UntypedSchemaUnion(baseSch, sch, mergeSch)
			if err != nil {
				return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
			}
			sqlUnionSch, err := sqlutil.FromDoltSchema(tblName, unionSch)
			if err != nil {
				return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
			}

			sqlCtx, err := engine.NewLocalSqlContext(ctx, eng)
			if err != nil {
				return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
			}

			confSqlSch, rowItr, err := eng.Query(sqlCtx, buildConflictQuery(baseSch, sch, mergeSch, tblName))
			if err != nil {
				return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
			}

			tw := tabular.NewFixedWidthConflictTableWriter(sqlUnionSch.Schema, iohelp.NopWrCloser(cli.CliOut), 100)
			err = writeConflictResults(sqlCtx, confSqlSch, sqlUnionSch.Schema, rowItr, tw)
			if err != nil {
				return errhand.BuildDError("failed to print conflicts").AddCause(err).Build()
			}

			return nil
		}()

		if verr != nil {
			return verr
		}
	}

	return nil
}

func writeConflictResults(
	ctx *sql.Context,
	resultSch sql.Schema,
	targetSch sql.Schema,
	iter sql.RowIter,
	writer *tabular.FixedWidthConflictTableWriter) (err error) {

	cs, err := newConflictSplitter(resultSch, targetSch)
	if err != nil {
		return err
	}

	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			return writer.Close(ctx)
		} else if err != nil {
			return err
		}

		conflictRows, err := cs.splitConflictRow(r)
		if err != nil {
			return err
		}

		for _, cR := range conflictRows {
			err := writer.WriteRow(ctx, cR.version, cR.row, cR.diffType)
			if err != nil {
				return err
			}
		}
	}
}

func buildConflictQuery(base, sch, mergeSch schema.Schema, tblName string) string {
	cols := castColumnWithPrefix(base.GetAllCols().GetColumnNames(), "base_")
	cols = append(cols, castColumnWithPrefix(sch.GetAllCols().GetColumnNames(), "our_")...)
	cols = append(cols, castColumnWithPrefix(mergeSch.GetAllCols().GetColumnNames(), "their_")...)
	colNames := strings.Join(cols, ", ")
	query := fmt.Sprintf("SELECT %s, our_diff_type, their_diff_type from dolt_conflicts_%s", colNames, tblName)
	return query
}

func castColumnWithPrefix(arr []string, prefix string) []string {
	out := make([]string, len(arr))
	for i := range arr {
		n := prefix + arr[i]
		out[i] = fmt.Sprintf("cast (%s as char) as `%s`", n, n)
	}
	return out
}
