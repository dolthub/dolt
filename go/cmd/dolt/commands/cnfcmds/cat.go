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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
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
	LongDesc:  `The dolt conflicts cat command reads table conflicts from the working set and writes them to the standard output.`,
	Synopsis: []string{
		"{{.LessThan}}table{{.GreaterThan}}...",
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
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be printed. '.' can be used to print conflicts for all tables."})

	return ap
}

// Exec executes the command
func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
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

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return exitWithVerr(errhand.VerboseErrorFromError(err))
	}

	tblNames := args
	if len(tblNames) == 0 {
		cli.Println("No tables specified")
		usage()
		return 1
	} else if len(tblNames) == 1 && tblNames[0] == "." {
		tblNames, err = ws.WorkingRoot().GetTableNames(ctx)
		if err != nil {
			return exitWithVerr(errhand.VerboseErrorFromError(err))
		}
	}

	if verr := printConflicts(ctx, dEnv, ws, tblNames); verr != nil {
		return exitWithVerr(errhand.VerboseErrorFromError(err))
	}
	return 0
}

func exitWithVerr(verr errhand.VerboseError) int {
	cli.PrintErrln(verr.Verbose())
	return 1
}

func printConflicts(ctx context.Context, dEnv *env.DoltEnv, ws *doltdb.WorkingSet, tblNames []string) error {
	eng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return err
	}
	stdOut := iohelp.NopWrCloser(cli.CliOut)

	// first print schema conflicts
	if ws.MergeActive() && ws.MergeState().HasSchemaConflicts() {
		sqlCtx, err := eng.NewLocalContext(ctx)
		if err != nil {
			return err
		}
		sqlCtx.SetCurrentDatabase(dbName)

		for _, table := range tblNames {
			if err = printSchemaConflicts(sqlCtx, stdOut, eng, table); err != nil {
				return err
			}
		}
	}

	// next print data conflicts
	root := ws.WorkingRoot()
	for _, tblName := range tblNames {
		if has, err := root.HasTable(ctx, tblName); err != nil {
			return err
		} else if !has {
			return fmt.Errorf("error: unknown table '%s'", tblName)
		}

		tbl, _, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}

		has, err := tbl.HasConflicts(ctx)
		if err != nil {
			return err
		} else if !has {
			continue
		}

		base, sch, mergeSch, err := tbl.GetConflictSchemas(ctx, tblName)
		if err != nil {
			return err
		}

		sqlCtx, err := eng.NewLocalContext(ctx)
		if err != nil {
			return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
		}
		sqlCtx.SetCurrentDatabase(dbName)

		confSqlSch, rowItr, err := eng.Query(sqlCtx, buildDataConflictQuery(base, sch, mergeSch, tblName))
		if err != nil {
			return err
		}

		unionSch, err := untyped.UntypedSchemaUnion(base, sch, mergeSch)
		if err != nil {
			return err
		}

		sqlUnionSch, err := sqlutil.FromDoltSchema(tblName, unionSch)
		if err != nil {
			return err
		}

		tw := tabular.NewFixedWidthConflictTableWriter(sqlUnionSch.Schema, stdOut, 100)

		err = writeConflictResults(sqlCtx, confSqlSch, sqlUnionSch.Schema, rowItr, tw)
		if err != nil {
			return err
		}
	}

	return nil
}

func printSchemaConflicts(sqlCtx *sql.Context, wrCloser io.WriteCloser, eng *engine.SqlEngine, table string) error {

	sqlSch, rowItr, err := eng.Query(sqlCtx, buildSchemaConflictQuery(table))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := rowItr.Close(sqlCtx); err == nil {
			err = cerr
		}
	}()

	tw := tabular.NewFixedWidthTableWriter(sqlSch, wrCloser, 100)
	defer func() {
		if cerr := tw.Close(sqlCtx); err == nil {
			err = cerr
		}
	}()

	for {
		r, err := rowItr.Next(sqlCtx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if err = tw.WriteSqlRow(sqlCtx, r); err != nil {
			return err
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

func buildSchemaConflictQuery(table string) string {
	return fmt.Sprintf("select our_schema, their_schema, base_schema, description "+
		"from dolt_schema_conflicts where table_name = '%s'", table)
}

func buildDataConflictQuery(base, sch, mergeSch schema.Schema, tblName string) string {
	cols := quoteWithPrefix(base.GetAllCols().GetColumnNames(), "base_")
	cols = append(cols, quoteWithPrefix(sch.GetAllCols().GetColumnNames(), "our_")...)
	cols = append(cols, quoteWithPrefix(mergeSch.GetAllCols().GetColumnNames(), "their_")...)
	colNames := strings.Join(cols, ", ")
	query := fmt.Sprintf("SELECT %s, our_diff_type, their_diff_type from `dolt_conflicts_%s`", colNames, tblName)
	return query
}
