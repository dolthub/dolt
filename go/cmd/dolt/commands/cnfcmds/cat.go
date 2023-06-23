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
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

type mergeStatus struct {
	isMerging      bool
	source         string
	sourceCommit   string
	target         string
	unmergedTables []string
}

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

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	tblNames := args
	if len(tblNames) == 0 {
		cli.Println("No tables specified")
		usage()
		return 1
	}

	if err := printConflicts(queryist, sqlCtx, tblNames); err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}

func printConflicts(queryist cli.Queryist, sqlCtx *sql.Context, tblNames []string) error {
	stdOut := iohelp.NopWrCloser(cli.CliOut)

	mergeStatus, err := getMergeStatus(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("error: failed to get merge status: %w", err)
	}
	schemaConflictsExist, err := getSchemaConflictsExist(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("error: failed to determine if schema conflicts exist: %w", err)
	}
	allTableNames, err := getTableNames(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("error: failed to get all table names: %w", err)
	}

	// if no tables were specified, set tblNames to all table names
	if len(tblNames) == 1 && tblNames[0] == "." {
		tblNames = []string{}
		for tableName := range allTableNames {
			tblNames = append(tblNames, tableName)
		}
	}

	// first print schema conflicts
	if mergeStatus.isMerging && schemaConflictsExist {
		for _, table := range tblNames {
			err = printSchemaConflicts(queryist, sqlCtx, stdOut, table)
			if err != nil {
				return fmt.Errorf("error: failed to print schema conflicts for table '%s': %w", table, err)
			}
		}
	}

	//// next print data conflicts
	//for _, tblName := range tblNames {
	//	_, tableExists := allTableNames[tblName]
	//	if !tableExists {
	//		return fmt.Errorf("error: unknown table '%s'", tblName)
	//	}
	//
	//	dataConflictsExist, err := getTableDataConflictsExist(queryist, sqlCtx, tblName)
	//	if err != nil {
	//		return err
	//	} else if !dataConflictsExist {
	//		continue
	//	}
	//
	//	base, sch, mergeSch, err := tbl.GetConflictSchemas(ctx, tblName)
	//	if err != nil {
	//		return err
	//	}
	//
	//	sqlCtx, err := eng.NewLocalContext(ctx)
	//	if err != nil {
	//		return errhand.BuildDError("failed to fetch conflicts").AddCause(err).Build()
	//	}
	//	sqlCtx.SetCurrentDatabase(dbName)
	//
	//	confSqlSch, rowItr, err := eng.Query(sqlCtx, buildDataConflictQuery(base, sch, mergeSch, tblName))
	//	if err != nil {
	//		return err
	//	}
	//
	//	unionSch, err := untyped.UntypedSchemaUnion(base, sch, mergeSch)
	//	if err != nil {
	//		return err
	//	}
	//
	//	sqlUnionSch, err := sqlutil.FromDoltSchema(tblName, unionSch)
	//	if err != nil {
	//		return err
	//	}
	//
	//	tw := tabular.NewFixedWidthConflictTableWriter(sqlUnionSch.Schema, stdOut, 100)
	//
	//	err = writeConflictResults(sqlCtx, confSqlSch, sqlUnionSch.Schema, rowItr, tw)
	//	if err != nil {
	//		return err
	//	}
	//}

	return nil
}

func printSchemaConflicts(queryist cli.Queryist, sqlCtx *sql.Context, wrCloser io.WriteCloser, table string) error {
	q := buildSchemaConflictQuery(table)
	sqlSch, rowItr, err := queryist.Query(sqlCtx, q)
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

func getTableNames(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]bool, error) {
	q := "show full tables"
	rows, err := commands.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	tableNames := make(map[string]bool)
	for _, row := range rows {
		tableName := row[0].(string)
		tableType := row[1].(string)
		isTable := tableType == "BASE TABLE"
		if isTable {
			tableNames[tableName] = true
		}
	}

	return tableNames, nil
}

func getMergeStatus(queryist cli.Queryist, sqlCtx *sql.Context) (mergeStatus, error) {
	ms := mergeStatus{}
	q := "select * from dolt_merge_status;"
	rows, err := commands.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return ms, err
	}

	if len(rows) > 1 {
		return ms, fmt.Errorf("error: multiple rows in dolt_merge_status")
	}

	if len(rows) == 0 {
		ms.isMerging = false
		return ms, nil
	}

	row := rows[0]
	ms.isMerging = true
	ms.source = row[1].(string)
	ms.sourceCommit = row[2].(string)
	ms.target = row[3].(string)
	unmergedTables := row[4].(string)
	ms.unmergedTables = strings.Split(unmergedTables, ", ")

	return ms, nil
}

func getSchemaConflictsExist(queryist cli.Queryist, sqlCtx *sql.Context) (bool, error) {
	q := "select count(*) from dolt_schema_conflicts;"
	rows, err := commands.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return false, err
	}

	row := rows[0]
	count := row[0].(int64)
	conflictsExist := count > 0
	return conflictsExist, nil
}

func getTableDataConflictsExist(queryist cli.Queryist, sqlCtx *sql.Context, tableName string) (bool, error) {
	q := fmt.Sprintf("select count(*) from dolt_conflicts_%s;", tableName)
	rows, err := commands.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return false, err
	}

	row := rows[0]
	count := row[0].(int64)
	conflictsExist := count > 0
	return conflictsExist, nil
}
