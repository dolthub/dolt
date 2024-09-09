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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/ishell"
	"github.com/fatih/color"
	"golang.org/x/exp/slices"
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

var _ cli.RepoNotRequiredCommand = AddCmd{}

func (cmd AddCmd) RequiresRepo() bool {
	return false
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd AddCmd) Name() string {
	return "add"
}

// Description returns a description of the command
func (cmd AddCmd) Description() string {
	return "Add table changes to the list of staged table changes."
}

func (cmd AddCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateAddArgParser()
	return cli.NewCommandDocumentation(addDocs, ap)
}

func (cmd AddCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateAddArgParser()
}

// generateAddSql returns the query that will call the `DOLT_ADD` stored proceudre.
// This function assumes that the inputs are validated table names, which cannot contain quotes.
func generateAddSql(apr *argparser.ArgParseResults) string {
	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_ADD(")

	write := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'")
		buffer.WriteString(s)
		buffer.WriteString("'")
		first = false
	}

	if apr.Contains(cli.AllFlag) {
		write("-A")
	}
	if apr.Contains(cli.ForceFlag) {
		write("-f")
	}
	for _, arg := range apr.Args {
		write(arg)
	}
	buffer.WriteString(")")
	return buffer.String()
}

// Exec executes the command
func (cmd AddCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateAddArgParser()

	// This flag is only supported in a CLI context, not the in the dolt procedure.
	ap.SupportsFlag("patch", "p", "Interactively select changes to add to the staged set.")

	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, addDocs)
	if terminate {
		return status
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// Allow staging tables with merge conflicts.
	_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_force_transaction_commit=1;")
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	if apr.Contains("patch") {
		return patchWorkflow(sqlCtx, queryist, apr.Args)
	} else {
		for _, tableName := range apr.Args {
			if tableName != "." && !doltdb.IsValidTableName(tableName) {
				return HandleVErrAndExitCode(errhand.BuildDError("'%s' is not a valid table name", tableName).Build(), nil)
			}
		}

		_, rowIter, _, err := queryist.Query(sqlCtx, generateAddSql(apr))
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}

		_, err = sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
	}

	return 0
}

func patchWorkflow(sqlCtx *sql.Context, queryist cli.Queryist, tables []string) int {
	if len(tables) == 0 {
		// Get the list of tables to patch
		_, rowIter, _, err := queryist.Query(sqlCtx, "select table_name from dolt_status where not staged")
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}

		rows, err := sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}

		for _, r := range rows {
			tbl := r[0].(string)
			tables = append(tables, tbl)
		}
	}
	sort.Strings(tables)
	runAddPatchShell(sqlCtx, queryist, tables)

	return 0
}

// tablePatchInfo is a struct that holds the summary of details for a single table's unstaged changes.
// This includes the number of added, modified, and removed rows, and the ID of the first and last rows where
// staged == false.
type tablePatchInfo struct {
	add      int
	modifies int
	removes  int
	firstId  int
	lastId   int
	schema   sql.Schema
}

func (tpi *tablePatchInfo) total() int {
	return tpi.add + tpi.modifies + tpi.removes
}

func runAddPatchShell(sqlCtx *sql.Context, queryist cli.Queryist, tables []string) int {
	state, err := newState(sqlCtx, queryist, tables)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	shell := ishell.New()
	shell.AutoHelp(false)

	shell.AddCmd(&ishell.Cmd{
		Name: "?",
		Help: "show this help",
		Func: opHelp,
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "y",
		Help: "stage the current change",
		Func: state.opYes,
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "n",
		Help: "do not stage the current change",
		Func: state.opNo,
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "q",
		Help: "quit",
		Func: func(c *ishell.Context) {
			c.Stop()
		},
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "a",
		Help: "add all changes in this table",
		Func: state.opAddAllOfTable,
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "d",
		Help: "do not stage any further changes in this table",
		Func: state.opSkipTable,
	})
	shell.AddCmd(&ishell.Cmd{
		Name: "s",
		Help: "show summary of unstaged changes and start over",
		Func: state.reset,
	})

	prompt := "Stage this row [y,n,q,a,d,s,?]? "
	prompt = color.HiGreenString(prompt)
	shell.SetPrompt(prompt)

	// run shell. This blocks until The stop() function is called on the ishell context.
	shell.Run()

	if state.err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(state.err))
		return 1
	}
	return 0
}

// queryForUnstagedChanges queries the dolt_workspace_* tables for the add/modify/remove counts for each table.
func queryForUnstagedChanges(sqlCtx *sql.Context, queryist cli.Queryist, tables []string) (map[string]*tablePatchInfo, error) {
	changeCounts := make(map[string]*tablePatchInfo)
	for _, tableName := range tables {
		qry := fmt.Sprintf("SELECT * FROM dolt_workspace_%s LIMIT 1", tableName)
		tableSchema, _, _, err := queryist.Query(sqlCtx, qry)
		if err != nil {
			return nil, err
		}

		// Now get the add/drop/modify counts. This query is hand crafted which seems like a bad idea, but it's only the
		// table name that is inserted, and that comes from our data, not user input.
		qry = fmt.Sprintf("SELECT diff_type,count(*) AS count FROM dolt_workspace_%s WHERE NOT staged GROUP BY diff_type", tableName)
		_, rowIter, _, err := queryist.Query(sqlCtx, qry)
		if err != nil {
			return nil, err
		}
		rows, err := sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			return nil, err
		}

		changeCounts[tableName] = &tablePatchInfo{}
		for _, row := range rows {
			diffType := row[0].(string)
			count, err := coerceToInt(row[1])
			if err != nil {
				return nil, err
			}
			switch diffType {
			case "added":
				changeCounts[tableName].add = count
			case "modified":
				changeCounts[tableName].modifies = count
			case "removed":
				changeCounts[tableName].removes = count
			default:
				return nil, errors.New("Unexpected diff type: " + diffType)
			}
		}

		// Kind of lame to do another query, but so be it.
		qry = fmt.Sprintf("SELECT min(id) as first_id, max(id) as last_id FROM dolt_workspace_%s WHERE NOT staged", tableName)
		_, rowIter, _, err = queryist.Query(sqlCtx, qry)
		if err != nil {
			return nil, err
		}
		rows, err = sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, errors.New("Expected one row")
		}
		firstId, err := coerceToInt(rows[0][0])
		if err != nil {
			return nil, err
		}
		changeCounts[tableName].firstId = firstId
		lastId, err := coerceToInt(rows[0][1])
		if err != nil {
			return nil, err
		}
		changeCounts[tableName].lastId = lastId

		reconstructedSchema, err := reconstructSchema(tableSchema)
		if err != nil {
			return nil, err
		}
		changeCounts[tableName].schema = reconstructedSchema
	}

	return changeCounts, nil
}

func coerceToInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, errors.New("Expected int, int64 or string")
	}
}

func queryForSingleChange(sqlCtx *sql.Context, queryist cli.Queryist, tableName string, rowId int) (sql.Row, error) {
	qry := fmt.Sprintf("SELECT * FROM dolt_workspace_%s WHERE ID = %d LIMIT 1", tableName, rowId)
	_, rowIter, _, err := queryist.Query(sqlCtx, qry)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows) > 1 {
		return nil, errors.New("Expected exactly one row")
	}
	return rows[0], nil
}

func opHelp(_ *ishell.Context) {
	help := `y - stage the current change
n - do not stage the current change
q - quit
a - add all changes in this table
d - do not stage any further changes in this table
s - show summary of unstaged changes and start over
? - show this help`
	help = color.CyanString(help)
	cli.Println(help)
}

type patchState struct {
	sqlCtx                *sql.Context
	queryist              cli.Queryist
	tables                []string
	changeCounts          map[string]*tablePatchInfo
	currentTable          string
	currentTableSchema    sql.Schema
	currentTableLastRowId int
	currentRowId          int
	currentRow            sql.Row
	err                   error
}

func (ps *patchState) opYes(c *ishell.Context) {
	qry := fmt.Sprintf("UPDATE dolt_workspace_%s SET staged = TRUE WHERE id = %d", ps.currentTable, ps.currentRowId)
	_, iter, _, err := ps.queryist.Query(ps.sqlCtx, qry)
	if err != nil {
		ps.err = err
		c.Stop()
		return
	}

	for {
		_, err = iter.Next(ps.sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			ps.err = err
			c.Stop()
			return
		}
	}
	err = iter.Close(ps.sqlCtx)
	if err != nil {
		ps.err = err
		c.Stop()
		return
	}

	ps.currentRowId++
	if ps.currentRowId <= ps.currentTableLastRowId {
		ps.setCurrentRowState(c)
	} else {
		ps.nextTable(c)
	}
}

func (ps *patchState) opNo(c *ishell.Context) {
	ps.currentRowId++
	if ps.currentRowId <= ps.currentTableLastRowId {
		ps.setCurrentRowState(c)
	} else {
		ps.nextTable(c)
	}
}

func (ps *patchState) opSkipTable(c *ishell.Context) {
	ps.nextTable(c)
}

func (ps *patchState) opAddAllOfTable(c *ishell.Context) {
	// grab the row id.
	id, err := coerceToInt(ps.currentRow[0])
	if err != nil {
		ps.err = err
		c.Stop()
		return
	}

	qry := fmt.Sprintf("UPDATE dolt_workspace_%s SET staged = TRUE WHERE id >= %d", ps.currentTable, id)
	_, iter, _, err := ps.queryist.Query(ps.sqlCtx, qry)
	if err != nil {
		ps.err = err
		c.Stop()
		return
	}

	for {
		_, err = iter.Next(ps.sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			ps.err = err
			c.Stop()
			return
		}
	}
	err = iter.Close(ps.sqlCtx)
	if err != nil {
		ps.err = err
		c.Stop()
		return
	}
	ps.nextTable(c)
}

func (ps *patchState) reset(c *ishell.Context) {
	changeCounts, err := queryForUnstagedChanges(ps.sqlCtx, ps.queryist, ps.tables)
	if err != nil {
		ps.err = err
		if c != nil {
			c.Stop()
		}
	}
	ps.changeCounts = changeCounts

	printTableSummary(ps.tables, changeCounts)
	ps.currentTable = ""
	ps.nextTable(c)
}

func (ps *patchState) setCurrentRowState(c *ishell.Context) {
	if ps.currentRowId > ps.currentTableLastRowId {
		ps.nextTable(c)
		return
	}

	newRow, err := queryForSingleChange(ps.sqlCtx, ps.queryist, ps.currentTable, ps.currentRowId)
	if err != nil {
		ps.err = err
		if c != nil {
			c.Stop()
		}
		return
	}

	ps.currentRow = newRow

	err = printSingleChange(ps.sqlCtx, ps.currentRow, ps.currentTableSchema)
	if err != nil {
		ps.err = err
		if c != nil {
			c.Stop()
		}
	}
}

// Move the state to work on the next table. Print the header for the table. If there are no more tables false is returned.
func (ps *patchState) nextTable(c *ishell.Context) {
	tblIdx := -1
	if ps.currentTable != "" {
		// The currentTable is always in the tables slice. No need to check if == -1.
		tblIdx = slices.Index(ps.tables, ps.currentTable)
	}
	tblIdx++

	if tblIdx < len(ps.tables) {
		nextTbl := ps.tables[tblIdx]
		if _, ok := ps.changeCounts[nextTbl]; !ok {
			// It's possible that a table has no more change if the user restarted the workflow.
			ps.nextTable(c)
			return
		}
		ps.currentTable = nextTbl
		changes := ps.changeCounts[ps.currentTable]
		ps.currentRowId = changes.firstId
		ps.currentTableLastRowId = changes.lastId
		ps.currentTableSchema = changes.schema

		cli.Printf("=============\n")
		cli.Printf("Table: %s\n", ps.currentTable)

		ps.setCurrentRowState(c)
	} else {
		c.Stop()
	}
}

func newState(sqlCtx *sql.Context, queryist cli.Queryist, tables []string) (*patchState, error) {
	ans := &patchState{sqlCtx: sqlCtx, queryist: queryist, tables: tables}
	ans.reset(nil)

	if ans.err != nil {
		return nil, ans.err
	}
	return ans, nil
}

func printSingleChange(sqlCtx *sql.Context, workspaceRow sql.Row, schema sql.Schema) (err error) {
	writer := tabular.NewFixedWidthDiffTableWriter(schema, iohelp.NopWrCloser(cli.CliOut), len(workspaceRow)/2)
	defer writer.Close(sqlCtx.Context)

	toRow := workspaceRow[3:5]
	fromRow := workspaceRow[5:7]

	diffType := workspaceRow[2].(string)
	switch diffType {
	case "added":
		err = writer.WriteRow(sqlCtx.Context, toRow, diff.Added, colDiffType(diff.Added, len(toRow)))
	case "modified":
		err = writer.WriteCombinedRow(sqlCtx.Context, fromRow, toRow, diff.ModeContext)
	case "removed":
		err = writer.WriteRow(sqlCtx.Context, fromRow, diff.Removed, colDiffType(diff.Removed, len(fromRow)))
	default:
		err = errors.New(fmt.Sprintf("Unexpected diff type: %s", diffType))
	}

	return err
}

func reconstructSchema(workspaceSchema sql.Schema) (sql.Schema, error) {
	toSchema := workspaceSchema[3:(3 + ((len(workspaceSchema) - 3) / 2))]

	// This column names _should_ all be prefixed with "to_". A bug if not.
	for _, col := range toSchema {
		if strings.HasPrefix(col.Name, "to_") {
			col.Name = strings.TrimPrefix(col.Name, "to_")
		} else {
			return nil, errors.New("Unexpected column name: " + col.Name)
		}
	}

	return toSchema, nil
}

func colDiffType(t diff.ChangeType, n int) []diff.ChangeType {
	ans := make([]diff.ChangeType, n)
	for i := range ans {
		ans[i] = t
	}
	return ans
}

// printTableSummary prints a summary of the changes in the tables. tables slice should be the table names in alphabetical order.
// counts map should be the change counts for each table.
func printTableSummary(tables []string, counts map[string]*tablePatchInfo) {
	header := "Table                              Added / Modified / Removed\n"
	header += "=====                              =====   ========   =======\n"
	header = color.YellowString(header)

	cli.Printf(header)

	totalChgCount := 0

	// Print each entry with aligned columns
	for _, tbl := range tables {
		c := counts[tbl]
		addStr := color.GreenString("%-7d", c.add)
		modifiesStr := color.YellowString("%-10d", c.modifies)
		removesStr := color.RedString("%d", c.removes)

		cli.Printf("%-34s %s %s %s\n", tbl, addStr, modifiesStr, removesStr)

		totalChgCount += c.total()
	}

	if totalChgCount > 25 {
		warning := `You have %d changes in total. Consider updating dolt_workspace_* tables directly as
'add --patch' requires you to individually evaluate each changed row.
`
		warning = color.YellowString(warning)
		cli.Printf(warning, totalChgCount)
	}
}
