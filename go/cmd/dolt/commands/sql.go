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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/abiosoft/readline"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"github.com/liquidata-inc/ishell"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// An environment variable to set to get indexed join behavior, currently experimental
const UseIndexedJoinsEnv = "DOLT_USE_INDEXED_JOINS"

var sqlShortDesc = "Runs a SQL query"
var sqlLongDesc = `Runs a SQL query you specify. By default, begins an interactive shell to run queries and view the
results. With the -q option, runs the given query and prints any results, then exits.

THIS FUNCTIONALITY IS EXPERIMENTAL and being intensively developed. Feedback is welcome: 
dolt-interest@liquidata.co

Reasonably well supported functionality:
* SELECT statements, including most kinds of joins
* CREATE TABLE statements
* ALTER TABLE / DROP TABLE statements
* UPDATE and DELETE statements
* Table and column aliases
* Column functions, e.g. CONCAT
* ORDER BY and LIMIT clauses
* GROUP BY
* Aggregate functions, e.g. SUM 

Known limitations:
* Some expressions in SELECT statements
* Subqueries
* Non-primary indexes
* Foreign keys
* Column constraints besides NOT NULL
* VARCHAR columns are unlimited length; FLOAT, INTEGER columns are 64 bit
* Performance is very bad for many SELECT statements, especially JOINs
`
var sqlSynopsis = []string{
	"",
	"-q <query>",
}

const (
	queryFlag  = "query"
	welcomeMsg = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`
)

func Sql(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(queryFlag, "q", "SQL query to run", "Runs a single query and exits")
	help, usage := cli.HelpAndUsagePrinters(commandStr, sqlShortDesc, sqlLongDesc, sqlSynopsis, ap)

	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	// run a single command and exit
	if query, ok := apr.GetValue(queryFlag); ok {
		if newRoot, err := processQuery(ctx, query, dEnv, root); err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		} else if newRoot != nil {
			return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, newRoot), usage)
		} else {
			return 0
		}
	}

	// Run in either batch mode for piped input, or shell mode for interactive
	fi, err := os.Stdin.Stat()
	// Windows has a bug where STDIN can't be statted in some cases, see https://github.com/golang/go/issues/33570
	if (err != nil && osutil.IsWindows) || (fi.Mode()&os.ModeCharDevice) == 0 {
		root, err = runBatchMode(ctx, dEnv, root)
		if err != nil {
			return 1
		}
	} else if err != nil {
		HandleVErrAndExitCode(errhand.BuildDError("Couldn't stat STDIN. This is a bug.").Build(), usage)
	} else {
		var err error
		root, err = runShell(ctx, dEnv, root)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("unable to start shell").AddCause(err).Build(), usage)
		}
	}

	// If the SQL session wrote a new root value, update the working set with it
	if root != nil {
		return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, root), usage)
	}

	return 0
}

// ScanStatements is a split function for a Scanner that returns each SQL statement in the input as a token. It doesn't
// work for strings that contain semi-colons. Supporting that requires implementing a state machine.
func scanStatements(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ';'); i >= 0 {
		// We have a full ;-terminated line.
		return i + 1, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// runBatchMode processes queries until EOF and returns the resulting root value
func runBatchMode(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	scanner := bufio.NewScanner(os.Stdin)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	scanner.Split(scanStatements)

	batcher := dsql.NewSqlBatcher(dEnv.DoltDB, root)

	var query string
	for scanner.Scan() {
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		if !batchInsertEarlySemicolon(query) {
			query += ";"
			continue
		}
		if newRoot, err := processBatchQuery(ctx, query, dEnv, root, batcher); newRoot != nil {
			root = newRoot
		} else if err != nil {
			_, _ = fmt.Fprintf(cli.CliErr, "Error processing query '%s': %s\n", query, err.Error())
			return nil, err
		}
		query = ""
	}

	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	if newRoot, _ := batcher.Commit(ctx); newRoot != nil {
		root = newRoot
	}

	return root, nil
}

// batchInsertEarlySemicolon loops through a string to check if Scan stopped too early on a semicolon
func batchInsertEarlySemicolon(query string) bool {
	quotes := []uint8{'\'', '"'}
	midQuote := false
	queryLength := len(query)
	for i := 0; i < queryLength; i++ {
		for _, quote := range quotes {
			if query[i] == quote {
				i++
				midQuote = true
				inEscapeMode := false
				for ; i < queryLength; i++ {
					if inEscapeMode {
						inEscapeMode = false
					} else {
						if query[i] == quote {
							midQuote = false
							break
						} else if query[i] == '\\' {
							inEscapeMode = true
						}
					}
				}
				break
			}
		}
	}
	return !midQuote
}

// runShell starts a SQL shell. Returns when the user exits the shell with the root value resulting from any queries.
func runShell(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)

	// start the doltsql shell
	historyFile := filepath.Join(dEnv.GetDoltDir(), ".sqlhistory")
	rlConf := readline.Config{
		Prompt:                 "doltsql> ",
		Stdout:                 cli.CliOut,
		Stderr:                 cli.CliOut,
		HistoryFile:            historyFile,
		HistoryLimit:           500,
		HistorySearchFold:      true,
		DisableAutoSaveHistory: true,
	}
	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string{
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator: ";",
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt("      -> ")
	// TODO: update completer on create / drop / alter statements
	completer, err := newCompleter(ctx, dEnv)

	if err != nil {
		return nil, err
	}

	shell.CustomCompleter(completer)

	shell.EOF(func(c *ishell.Context) {
		c.Stop()
	})

	shell.Interrupt(func(c *ishell.Context, count int, input string) {
		if count > 1 {
			c.Stop()
		} else {
			c.Println("Received SIGINT. Interrupt again to exit, or use ^D, quit, or exit")
		}
	})

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		if len(strings.TrimSpace(query)) == 0 {
			return
		}

		if newRoot, err := processQuery(ctx, query, dEnv, root); err != nil {
			shell.Println(color.RedString(err.Error()))
		} else if newRoot != nil {
			root = newRoot
		}

		// TODO: there's a bug in the readline library when editing multi-line history entries.
		// Longer term we need to switch to a new readline library, like in this bug:
		// https://github.com/cockroachdb/cockroach/issues/15460
		// For now, we store all history entries as single-line strings to avoid the issue.
		singleLine := strings.ReplaceAll(query, "\n", " ")
		if err := shell.AddHistory(singleLine); err != nil {
			// TODO: handle better, like by turning off history writing for the rest of the session
			shell.Println(color.RedString(err.Error()))
		}
	})

	shell.Run()
	_ = iohelp.WriteLine(cli.CliOut, "Bye")

	return root, nil
}

// Returns a new auto completer with table names, column names, and SQL keywords.
func newCompleter(ctx context.Context, dEnv *env.DoltEnv) (*sqlCompleter, error) {
	var completionWords []string

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return &sqlCompleter{}, nil
	}

	tableNames, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	completionWords = append(completionWords, tableNames...)
	var columnNames []string
	for _, tableName := range tableNames {
		tbl, _, err := root.GetTable(ctx, tableName)

		if err != nil {
			return nil, err
		}

		sch, err := tbl.GetSchema(ctx)

		if err != nil {
			return nil, err
		}

		err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			completionWords = append(completionWords, col.Name)
			columnNames = append(columnNames, col.Name)
			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	completionWords = append(completionWords, dsql.CommonKeywords...)

	return &sqlCompleter{
		allWords:    completionWords,
		columnNames: columnNames,
	}, nil
}

type sqlCompleter struct {
	allWords    []string
	columnNames []string
}

// Do function for autocompletion, defined by the Readline library. Mostly stolen from ishell.
func (c *sqlCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	var words []string
	if w, err := shlex.Split(string(line)); err == nil {
		words = w
	} else {
		// fall back
		words = strings.Fields(string(line))
	}

	var cWords []string
	prefix := ""
	lastWord := ""
	if len(words) > 0 && pos > 0 && line[pos-1] != ' ' {
		lastWord = words[len(words)-1]
		prefix = strings.ToLower(lastWord)
	} else if len(words) > 0 {
		lastWord = words[len(words)-1]
	}

	cWords = c.getWords(lastWord)

	var suggestions [][]rune
	for _, w := range cWords {
		lowered := strings.ToLower(w)
		if strings.HasPrefix(lowered, prefix) {
			suggestions = append(suggestions, []rune(strings.TrimPrefix(lowered, prefix)))
		}
	}
	if len(suggestions) == 1 && prefix != "" && string(suggestions[0]) == "" {
		suggestions = [][]rune{[]rune(" ")}
	}

	return suggestions, len(prefix)
}

// Simple suggestion function. Returns column name suggestions if the last word in the input has exactly one '.' in it,
// otherwise returns all tables, columns, and reserved words.
func (c *sqlCompleter) getWords(lastWord string) (s []string) {
	lastDot := strings.LastIndex(lastWord, ".")
	if lastDot > 0 && strings.Count(lastWord, ".") == 1 {
		alias := lastWord[:lastDot]
		return prepend(alias+".", c.columnNames)
	}

	return c.allWords
}

func prepend(s string, ss []string) []string {
	newSs := make([]string, len(ss))
	for i := range ss {
		newSs[i] = s + ss[i]
	}
	return newSs
}

// Processes a single query and returns the new root value of the DB, or an error encountered.
func processQuery(ctx context.Context, query string, dEnv *env.DoltEnv, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Show:
		return nil, sqlShow(ctx, root, s)
	case *sqlparser.Select, *sqlparser.OtherRead:
		_, sqlSch, rowIter, err := sqlNewEngine(query, root, dEnv)
		if err == nil {
			err = prettyPrintResults(ctx, root.VRW().Format(), sqlSch, rowIter)
		}
		return nil, err
	case *sqlparser.Insert, *sqlparser.Update:
		newRoot, sqlSch, rowIter, err := sqlNewEngine(query, root, dEnv)
		if err == nil {
			err = prettyPrintResults(ctx, newRoot.VRW().Format(), sqlSch, rowIter)
		}
		return newRoot, err
	case *sqlparser.Delete:
		newRoot, ok := sqlCheckThenDeleteAllRows(ctx, root, s)
		if ok {
			return newRoot, nil
		}
		newRoot, sqlSch, rowIter, err := sqlNewEngine(query, root, dEnv)
		if err == nil {
			err = prettyPrintResults(ctx, newRoot.VRW().Format(), sqlSch, rowIter)
		}
		return newRoot, err
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return nil, fmt.Errorf("Error parsing DDL: %v.", err.Error())
		}
		return sqlDDL(ctx, dEnv, root, s, query)
	default:
		return nil, fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
}

// Processes a single query in batch mode and returns the result. The RootValue may or may not be changed.
func processBatchQuery(ctx context.Context, query string, dEnv *env.DoltEnv, root *doltdb.RootValue, batcher *dsql.SqlBatcher) (*doltdb.RootValue, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Insert:
		return sqlInsertBatch(ctx, dEnv, root, s, batcher)
	default:
		// For any other kind of statement, we need to commit whatever batch edit we've accumulated so far before executing
		// the query
		newRoot, err := batcher.Commit(ctx)
		if err != nil {
			return nil, err
		}
		newRoot, err = processQuery(ctx, query, dEnv, newRoot)
		if err != nil {
			return nil, err
		}
		if newRoot != nil {
			root = newRoot
			if err := batcher.UpdateRoot(root); err != nil {
				return nil, err
			}
		}

		return root, nil
	}
}

// Executes a SQL statement of either SHOW or SELECT and returns values for printing if applicable.
func sqlNewEngine(query string, root *doltdb.RootValue, dEnv *env.DoltEnv) (*doltdb.RootValue, sql.Schema, sql.RowIter, error) {
	db := dsqle.NewDatabase("dolt", root, dEnv)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	ctx := sql.NewEmptyContext()

	// Indexes are not well tested enough to use in production yet.
	if _, ok := os.LookupEnv(UseIndexedJoinsEnv); ok {
		engine.Catalog.RegisterIndexDriver(dsqle.NewDoltIndexDriver(db))
		err := engine.Init()
		if err != nil {
			return nil, nil, nil, err
		}
	}

	sqlSch, rowIter, err := engine.Query(ctx, query)
	return db.Root(), sqlSch, rowIter, err
}

// Executes a SQL show statement and prints the result to the CLI.
func sqlShow(ctx context.Context, root *doltdb.RootValue, show *sqlparser.Show) error {
	p, sch, err := dsql.BuildShowPipeline(ctx, root, show)
	if err != nil {
		return err
	}

	return runPrintingPipeline(ctx, root.VRW().Format(), p, sch)
}

// Pretty prints the output of the new SQL engine
func prettyPrintResults(ctx context.Context, nbf *types.NomsBinFormat, sqlSch sql.Schema, rowIter sql.RowIter) error {
	var chanErr error
	doltSch, err := dsqle.SqlSchemaToDoltResultSchema(sqlSch)
	if err != nil {
		return err
	}

	untypedSch, err := untyped.UntypeUnkeySchema(doltSch)
	if err != nil {
		return err
	}

	rowChannel := make(chan row.Row)
	p := pipeline.NewPartialPipeline(pipeline.InFuncForChannel(rowChannel))

	go func() {
		defer close(rowChannel)
		var sqlRow sql.Row
		for sqlRow, chanErr = rowIter.Next(); chanErr == nil; sqlRow, chanErr = rowIter.Next() {
			taggedVals := make(row.TaggedValues)
			for i, col := range sqlRow {
				if col != nil {
					taggedVals[uint64(i)] = types.String(fmt.Sprintf("%v", col))
				}
			}

			var r row.Row
			r, chanErr = row.New(nbf, untypedSch, taggedVals)

			if chanErr == nil {
				rowChannel <- r
			}
		}
	}()

	nullPrinter := nullprinter.NewNullPrinter(untypedSch)
	p.AddStage(pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow))

	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{Name: fwtStageName, Func: autoSizeTransform.TransformToFWT})

	// Redirect output to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	wr, err := tabular.NewTextTableWriter(cliWr, untypedSch)

	if err != nil {
		return err
	}

	p.RunAfter(func() { wr.Close(ctx) })

	cliSink := pipeline.ProcFuncForWriter(ctx, wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, untypedSch)))
		return true
	})

	colNames, err := schema.ExtractAllColNames(untypedSch)

	if err != nil {
		return err
	}

	r, err := untyped.NewRowFromTaggedStrings(nbf, untypedSch, colNames)

	if err != nil {
		return err
	}

	// Insert the table header row at the appropriate stage
	p.InjectRow(fwtStageName, r)

	p.Start()
	if err := p.Wait(); err != nil {
		return fmt.Errorf("error processing results: %v", err)
	}

	if chanErr != io.EOF {
		return fmt.Errorf("error processing results: %v", chanErr)
	}

	return nil
}

// Adds some print-handling stages to the pipeline given and runs it, returning any error.
// Adds null-printing and fixed-width transformers. The schema given is assumed to be untyped (string-typed).
func runPrintingPipeline(ctx context.Context, nbf *types.NomsBinFormat, p *pipeline.Pipeline, untypedSch schema.Schema) error {
	nullPrinter := nullprinter.NewNullPrinter(untypedSch)
	p.AddStage(pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow))

	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{Name: fwtStageName, Func: autoSizeTransform.TransformToFWT})

	// Redirect output to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	wr, err := tabular.NewTextTableWriter(cliWr, untypedSch)

	if err != nil {
		return err
	}

	p.RunAfter(func() { wr.Close(ctx) })

	cliSink := pipeline.ProcFuncForWriter(ctx, wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, untypedSch)))
		return true
	})

	colNames, err := schema.ExtractAllColNames(untypedSch)

	if err != nil {
		return err
	}

	r, err := untyped.NewRowFromTaggedStrings(nbf, untypedSch, colNames)

	if err != nil {
		return err
	}

	// Insert the table header row at the appropriate stage
	p.InjectRow(fwtStageName, r)

	p.Start()
	if err := p.Wait(); err != nil {
		return fmt.Errorf("error processing results: %v", err)
	}

	return nil
}

type stats struct {
	numRowsInserted  int
	numRowsUpdated   int
	numErrorsIgnored int
}

var batchEditStats stats
var displayStrLen int

// Executes a SQL insert statement in batch mode and returns the new root value (which is usually unchanged) or an
// error. No output is written to the console in batch mode.
func sqlInsertBatch(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, stmt *sqlparser.Insert, batcher *dsql.SqlBatcher) (*doltdb.RootValue, error) {
	result, err := dsql.ExecuteBatchInsert(ctx, root, stmt, batcher)
	if err != nil {
		return nil, fmt.Errorf("Error inserting rows: %v", err.Error())
	}
	mergeResultIntoStats(result, &batchEditStats)

	displayStr := fmt.Sprintf("Rows inserted: %d, Updated: %d, Errors: %d",
		batchEditStats.numRowsInserted, batchEditStats.numRowsUpdated, batchEditStats.numErrorsIgnored)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)

	if result.Root != nil {
		root = result.Root
	}

	return root, nil
}

func mergeResultIntoStats(result *dsql.InsertResult, stats *stats) {
	stats.numRowsInserted += result.NumRowsInserted
	stats.numRowsUpdated += result.NumRowsUpdated
	stats.numErrorsIgnored += result.NumErrorsIgnored
}

// Checks if the query is a naked delete and then deletes all rows if so
func sqlCheckThenDeleteAllRows(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Delete) (*doltdb.RootValue, bool) {
	if s.Where == nil && s.Limit == nil && s.Partitions == nil && len(s.TableExprs) == 1 {
		if ate, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
			if ste, ok := ate.Expr.(sqlparser.TableName); ok {
				tName := ste.Name.String()
				table, ok, err := root.GetTable(ctx, tName)
				if err == nil && ok {
					rowData, err := table.GetRowData(ctx)
					if err != nil {
						return nil, false
					}
					printRowIter := sql.RowsToRowIter(sql.NewRow(rowData.Len()))
					emptyMap, err := types.NewMap(ctx, root.VRW())
					if err != nil {
						return nil, false
					}
					newTable, err := table.UpdateRows(ctx, emptyMap)
					if err != nil {
						return nil, false
					}
					newRoot, err := doltdb.PutTable(ctx, root, root.VRW(), tName, newTable)
					if err != nil {
						return nil, false
					}
					_ = prettyPrintResults(ctx, root.VRW().Format(), sql.Schema{{Name: "updated", Type: sql.Uint64}}, printRowIter)
					return newRoot, true
				}
			}
		}
	}
	return nil, false
}

// Executes a SQL DDL statement (create, update, etc.). Returns the new root value to be written as appropriate.
func sqlDDL(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	switch ddl.Action {
	case sqlparser.CreateStr, sqlparser.DropStr:
		newRoot, _, _, err := sqlNewEngine(query, root, dEnv)
		return newRoot, err
	case sqlparser.AlterStr, sqlparser.RenameStr:
		newRoot, err := dsql.ExecuteAlter(ctx, dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, fmt.Errorf("Error altering table: %v", err)
		}
		return newRoot, nil
	case sqlparser.TruncateStr:
		return nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	default:
		return nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}
