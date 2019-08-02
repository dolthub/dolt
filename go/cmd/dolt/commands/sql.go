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

func Sql(commandStr string, args []string, dEnv *env.DoltEnv) int {
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
		if newRoot, err := processQuery(query, dEnv, root); err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		} else if newRoot != nil {
			return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, newRoot), usage)
		} else {
			return 0
		}
	}

	// Run in either batch mode for piped input, or shell mode for interactive
	fi, _ := os.Stdin.Stat()
	if (fi.Mode() & os.ModeCharDevice) == 0 {
		root = runBatchMode(dEnv, root)
	} else {
		root = runShell(dEnv, root)
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
func runBatchMode(dEnv *env.DoltEnv, root *doltdb.RootValue) *doltdb.RootValue {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(scanStatements)

	batcher := dsql.NewSqlBatcher(dEnv.DoltDB, root)

	for scanner.Scan() {
		query := scanner.Text()
		if newRoot, err := processBatchQuery(query, dEnv, root, batcher); newRoot != nil {
			root = newRoot
		} else if err != nil {
			cli.Println(fmt.Sprintf("Error processing query '%s': %s", query, err.Error()))
		}
	}

	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	if newRoot, _ := batcher.Commit(context.Background()); newRoot != nil {
		root = newRoot
	}

	return root
}

// runShell starts a SQL shell. Returns when the user exits the shell with the root value resulting from any queries.
func runShell(dEnv *env.DoltEnv, root *doltdb.RootValue) *doltdb.RootValue {
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
	shell.CustomCompleter(newCompleter(dEnv))

	batcher := dsql.NewSqlBatcher(dEnv.DoltDB, root)

	cleanup := func() {
		if newRoot, _ := batcher.Commit(context.Background()); newRoot != nil {
			root = newRoot
		}
	}

	shell.EOF(func(c *ishell.Context) {
		c.Stop()
		cleanup()
	})

	shell.Interrupt(func(c *ishell.Context, count int, input string) {
		if count > 1 {
			c.Stop()
			cleanup()
		} else {
			c.Println("Received SIGINT. Interrupt again to exit, or use ^D, quit, or exit")
		}
	})

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		if len(strings.TrimSpace(query)) == 0 {
			return
		}

		if newRoot, err := processBatchQuery(query, dEnv, root, batcher); err != nil {
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

	return root
}

// Returns a new auto completer with table names, column names, and SQL keywords.
func newCompleter(dEnv *env.DoltEnv) *sqlCompleter {
	var completionWords []string

	root, err := dEnv.WorkingRoot(context.TODO())
	if err != nil {
		return &sqlCompleter{}
	}

	tableNames := root.GetTableNames(context.TODO())
	completionWords = append(completionWords, tableNames...)
	var columnNames []string
	for _, tableName := range tableNames {
		tbl, _ := root.GetTable(context.TODO(), tableName)
		sch := tbl.GetSchema(context.TODO())
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
			completionWords = append(completionWords, col.Name)
			columnNames = append(columnNames, col.Name)
			return false
		})
	}

	completionWords = append(completionWords, dsql.CommonKeywords...)

	return &sqlCompleter{
		allWords:    completionWords,
		columnNames: columnNames,
	}
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
func processQuery(query string, dEnv *env.DoltEnv, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Show:
		return nil, sqlShow(root, s)
	case *sqlparser.Select, *sqlparser.OtherRead:
		sqlSch, rowIter, err := sqlNewEngine(query, root)
		if err == nil {
			err = prettyPrintResults(root.VRW().Format(), sqlSch, rowIter)
		}
		return nil, err
	case *sqlparser.Insert:
		return sqlInsert(dEnv, root, s)
	case *sqlparser.Update:
		return sqlUpdate(dEnv, root, s, query)
	case *sqlparser.Delete:
		return sqlDelete(dEnv, root, s, query)
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return nil, fmt.Errorf("Error parsing DDL: %v.", err.Error())
		}
		return sqlDDL(dEnv, root, s, query)
	default:
		return nil, fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
}

// Processes a single query in batch mode and returns the result. The RootValue may or may not be changed.
func processBatchQuery(query string, dEnv *env.DoltEnv, root *doltdb.RootValue, batcher *dsql.SqlBatcher) (*doltdb.RootValue, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Insert:
		return sqlInsertBatch(dEnv, root, s, batcher)
	default:
		// For any other kind of statement, we need to commit whatever batch edit we've accumulated so far before executing
		// the query
		newRoot, err := batcher.Commit(context.Background())
		if err != nil {
			return nil, err
		}
		newRoot, err = processQuery(query, dEnv, newRoot)
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
func sqlNewEngine(query string, root *doltdb.RootValue) (sql.Schema, sql.RowIter, error) {
	db := dsqle.NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	ctx := sql.NewEmptyContext()

	// Indexes are not well tested enough to use in production yet.
	if _, ok := os.LookupEnv(UseIndexedJoinsEnv); ok {
		engine.Catalog.RegisterIndexDriver(dsqle.NewDoltIndexDriver(db))
		err := engine.Init()
		if err != nil {
			return nil, nil, err
		}
	}

	return engine.Query(ctx, query)
}

// Executes a SQL show statement and prints the result to the CLI.
func sqlShow(root *doltdb.RootValue, show *sqlparser.Show) error {
	p, sch, err := dsql.BuildShowPipeline(context.TODO(), root, show)
	if err != nil {
		return err
	}

	return runPrintingPipeline(root.VRW().Format(), p, sch)
}

// Pretty prints the output of the new SQL engine
func prettyPrintResults(nbf *types.NomsBinFormat, sqlSch sql.Schema, rowIter sql.RowIter) error {
	var chanErr error
	doltSch := dsqle.SqlSchemaToDoltSchema(sqlSch)
	untypedSch := untyped.UntypeUnkeySchema(doltSch)

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
			rowChannel <- row.New(nbf, untypedSch, taggedVals)
		}
	}()

	nullPrinter := nullprinter.NewNullPrinter(untypedSch)
	p.AddStage(pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow))

	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{Name: fwtStageName, Func: autoSizeTransform.TransformToFWT})

	// Redirect output to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	wr := tabular.NewTextTableWriter(cliWr, untypedSch)
	p.RunAfter(func() { wr.Close(context.TODO()) })

	cliSink := pipeline.ProcFuncForWriter(context.TODO(), wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(context.Background(), tff.Row, untypedSch)))
		return true
	})

	// Insert the table header row at the appropriate stage
	p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(nbf, untypedSch, schema.ExtractAllColNames(untypedSch)))

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
func runPrintingPipeline(nbf *types.NomsBinFormat, p *pipeline.Pipeline, untypedSch schema.Schema) error {
	nullPrinter := nullprinter.NewNullPrinter(untypedSch)
	p.AddStage(pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow))

	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{Name: fwtStageName, Func: autoSizeTransform.TransformToFWT})

	// Redirect output to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	wr := tabular.NewTextTableWriter(cliWr, untypedSch)
	p.RunAfter(func() { wr.Close(context.TODO()) })

	cliSink := pipeline.ProcFuncForWriter(context.TODO(), wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(context.Background(), tff.Row, untypedSch)))
		return true
	})

	// Insert the table header row at the appropriate stage
	p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(nbf, untypedSch, schema.ExtractAllColNames(untypedSch)))

	p.Start()
	if err := p.Wait(); err != nil {
		return fmt.Errorf("error processing results: %v", err)
	}

	return nil
}

// Executes a SQL insert statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlInsert(dEnv *env.DoltEnv, root *doltdb.RootValue, stmt *sqlparser.Insert) (*doltdb.RootValue, error) {
	result, err := dsql.ExecuteInsert(context.Background(), dEnv.DoltDB, root, stmt)
	if err != nil {
		return nil, fmt.Errorf("Error inserting rows: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows inserted: %v", result.NumRowsInserted))
	if result.NumRowsUpdated > 0 {
		cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
	}
	if result.NumErrorsIgnored > 0 {
		cli.Println(fmt.Sprintf("Errors ignored: %v", result.NumErrorsIgnored))
	}

	return result.Root, nil
}

// Executes a SQL insert statement in batch mode and returns the new root value (which is usually unchanged) or an
// error. No output is written to the console in batch mode.
func sqlInsertBatch(dEnv *env.DoltEnv, root *doltdb.RootValue, stmt *sqlparser.Insert, batcher *dsql.SqlBatcher) (*doltdb.RootValue, error) {
	result, err := dsql.ExecuteBatchInsert(context.Background(), root, stmt, batcher)
	if err != nil {
		return nil, fmt.Errorf("Error inserting rows: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows inserted: %v", result.NumRowsInserted))
	if result.NumRowsUpdated > 0 {
		cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
	}
	if result.NumErrorsIgnored > 0 {
		cli.Println(fmt.Sprintf("Errors ignored: %v", result.NumErrorsIgnored))
	}

	if result.Root != nil {
		root = result.Root
	}

	return root, nil
}

// Executes a SQL update statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlUpdate(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Update, query string) (*doltdb.RootValue, error) {
	result, err := dsql.ExecuteUpdate(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return nil, fmt.Errorf("Error during update: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
	if result.NumRowsUnchanged > 0 {
		cli.Println(fmt.Sprintf("Rows matched but unchanged: %v", result.NumRowsUnchanged))
	}

	return result.Root, nil
}

// Executes a SQL delete statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlDelete(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Delete, query string) (*doltdb.RootValue, error) {
	result, err := dsql.ExecuteDelete(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return nil, fmt.Errorf("Error during update: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows deleted: %v", result.NumRowsDeleted))

	return result.Root, nil
}

// Executes a SQL DDL statement (create, update, etc.). Returns the new root value to be written as appropriate.
func sqlDDL(dEnv *env.DoltEnv, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	switch ddl.Action {
	case sqlparser.CreateStr:
		newRoot, _, err := dsql.ExecuteCreate(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, fmt.Errorf("Error creating table: %v", err)
		}
		return newRoot, nil
	case sqlparser.AlterStr, sqlparser.RenameStr:
		newRoot, err := dsql.ExecuteAlter(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, fmt.Errorf("Error altering table: %v", err)
		}
		return newRoot, nil
	case sqlparser.DropStr:
		newRoot, err := dsql.ExecuteDrop(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, fmt.Errorf("Error dropping table: %v", err)
		}
		return newRoot, nil
	case sqlparser.TruncateStr:
		return nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	default:
		return nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}