// Copyright 2019-2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
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
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
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
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var sqlShortDesc = "Runs a SQL query"
var sqlLongDesc = `Runs a SQL query you specify. By default, begins an interactive shell to run queries and view the
results. With the -q option, runs the given query and prints any results, then exits.

Known limitations:
* No support for creating indexes
* No support for foreign keys
* No support for column constraints besides NOT NULL
* No support for default values
* Column types aren't always preserved accurately from SQL create table statements. VARCHAR columns are unlimited 
    length; FLOAT, INTEGER columns are 64 bit
* Joins can only use indexes for two table joins. Three or more tables in a join query will use a non-indexed
    join, which is very slow.
`
var sqlSynopsis = []string{
	"",
	"-q <query>",
	"-q <query> -r <result format>",
}

const (
	queryFlag  = "query"
	formatFlag = "result-format"
	welcomeMsg = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`
)

type SqlCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SqlCmd) Name() string {
	return "sql"
}

// Description returns a description of the command
func (cmd SqlCmd) Description() string {
	return "Run a SQL query against tables in repository."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd SqlCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, sqlShortDesc, sqlLongDesc, sqlSynopsis, ap)
}

func (cmd SqlCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(queryFlag, "q", "SQL query to run", "Runs a single query and exits")
	ap.SupportsString(formatFlag, "r", "Result output format", "How to format result output. Valid values are tabular, csv. Defaults to tabular. ")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// Exec executes the command
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, sqlShortDesc, sqlLongDesc, sqlSynopsis, ap)

	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	format := formatTabular
	if formatSr, ok := apr.GetValue(formatFlag); ok {
		format, verr = getFormat(formatSr)
		if verr != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(verr), usage)
		}
	}

	origRoot := root

	// run a single command and exit
	if query, ok := apr.GetValue(queryFlag); ok {
		se, err := newSqlEngine(ctx, dEnv, dsqle.NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState), format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		if err := processQuery(ctx, query, se); err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		} else if se.sdb.Root() != origRoot {
			return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, se.sdb.Root()), usage)
		} else {
			return 0
		}
	} else if len(args) > 0 {
		return HandleVErrAndExitCode(errhand.BuildDError("Invalid Argument: use --query or -q to pass inline SQL queries").Build(), usage)
	}

	// Run in either batch mode for piped input, or shell mode for interactive
	fi, err := os.Stdin.Stat()
	var se *sqlEngine
	// Windows has a bug where STDIN can't be statted in some cases, see https://github.com/golang/go/issues/33570
	if (err != nil && osutil.IsWindows) || (fi.Mode()&os.ModeCharDevice) == 0 {
		se, err = newSqlEngine(ctx, dEnv, dsqle.NewBatchedDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState), format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		err = runBatchMode(ctx, se)
		if err != nil {
			return 1
		}
	} else if err != nil {
		HandleVErrAndExitCode(errhand.BuildDError("Couldn't stat STDIN. This is a bug.").Build(), usage)
	} else {
		se, err = newSqlEngine(ctx, dEnv, dsqle.NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState), format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		err = runShell(ctx, se, dEnv)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("unable to start shell").AddCause(err).Build(), usage)
		}
	}

	// If the SQL session wrote a new root value, update the working set with it
	if se.sdb.Root() != origRoot {
		return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, se.sdb.Root()), usage)
	}

	return 0
}

func getFormat(format string) (resultFormat, errhand.VerboseError) {
	switch strings.ToLower(format) {
	case "tabular":
		return formatTabular, nil
	case "csv":
		return formatCsv, nil
	default:
		return formatTabular,  errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular,csv").Build()
	}
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

// runBatchMode processes queries until EOF. The Root of the sqlEngine may be updated.
func runBatchMode(ctx context.Context, se *sqlEngine) error {
	scanner := bufio.NewScanner(os.Stdin)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	scanner.Split(scanStatements)

	var query string
	for scanner.Scan() {
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		if !batchInsertEarlySemicolon(query) {
			query += ";"
			// TODO: We should fix this problem by properly implementing a state machine for scanStatements
			continue
		}
		if err := processBatchQuery(ctx, query, se); err != nil {
			_, _ = fmt.Fprintf(cli.CliErr, "Error processing query '%s': %s\n", query, err.Error())
			return err
		}
		query = ""
	}

	updateBatchInsertOutput()

	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	if err := se.sdb.Flush(ctx); err != nil {
		return err
	}

	return nil
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

// runShell starts a SQL shell. Returns when the user exits the shell. The Root of the sqlEngine may
// be updated by any queries which were processed.
func runShell(ctx context.Context, se *sqlEngine, dEnv *env.DoltEnv) error {
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
		return err
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

		if err := processQuery(ctx, query, se); err != nil {
			shell.Println(color.RedString(err.Error()))
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

	return nil
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

// Processes a single query. The Root of the sqlEngine will be updated if necessary.
func processQuery(ctx context.Context, query string, se *sqlEngine) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Select, *sqlparser.Insert, *sqlparser.Update, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain:
		sqlSch, rowIter, err := se.query(ctx, query)
		if err == nil {
			err = se.prettyPrintResults(ctx, se.ddb.ValueReadWriter().Format(), sqlSch, rowIter)
		}
		return err
	case *sqlparser.Delete:
		ok := se.checkThenDeleteAllRows(ctx, s)
		if ok {
			return nil
		}
		sqlSch, rowIter, err := se.query(ctx, query)
		if err == nil {
			err = se.prettyPrintResults(ctx, se.ddb.Format(), sqlSch, rowIter)
		}
		return err
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return fmt.Errorf("Error parsing DDL: %v.", err.Error())
		}
		return se.ddl(ctx, s, query)
	default:
		return fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
}

type stats struct {
	numRowsInserted  int
	numRowsUpdated   int
	numErrorsIgnored int
}

var batchEditStats stats
var displayStrLen int

const maxBatchSize = 50000
const updateInterval = 500

// Processes a single query in batch mode. The Root of the sqlEngine may or may not be changed.
func processBatchQuery(ctx context.Context, query string, se *sqlEngine) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch sqlStatement.(type) {
	case *sqlparser.Insert:
		_, rowIter, err := se.query(ctx, query)
		if err != nil {
			return fmt.Errorf("Error inserting rows: %v", err.Error())
		}

		err = mergeInsertResultIntoStats(rowIter, &batchEditStats)
		if err != nil {
			return fmt.Errorf("Error inserting rows: %v", err.Error())
		}

		if batchEditStats.numRowsInserted%maxBatchSize == 0 {
			err := se.sdb.Flush(ctx)
			if err != nil {
				return err
			}
		}

		if batchEditStats.numRowsInserted%updateInterval == 0 {
			updateBatchInsertOutput()
		}

		return nil
	default:
		// For any other kind of statement, we need to commit whatever batch edit we've accumulated so far before executing
		// the query
		err := se.sdb.Flush(ctx)
		if err != nil {
			return err
		}

		err = processQuery(ctx, query, se)
		if err != nil {
			return err
		}

		return nil
	}
}

func updateBatchInsertOutput() {
	displayStr := fmt.Sprintf("Rows inserted: %d", batchEditStats.numRowsInserted)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

// Updates the batch insert stats with the results of an insert operation.
func mergeInsertResultIntoStats(rowIter sql.RowIter, s *stats) error {
	for {
		row, err := rowIter.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else {
			updated := row[0].(int64)
			s.numRowsInserted += int(updated)
		}
	}
}

type resultFormat byte
const (
	formatTabular resultFormat = iota
	formatCsv
)

type sqlEngine struct {
	sdb          *dsqle.Database
	ddb          *doltdb.DoltDB
	engine       *sqle.Engine
	resultFormat resultFormat
}

// sqlEngine packages up the context necessary to run sql queries against sqle.
func newSqlEngine(ctx context.Context, dEnv *env.DoltEnv, db *dsqle.Database, format resultFormat) (*sqlEngine, error) {
	engine := sqle.NewDefault()
	engine.AddDatabase(db)

	engine.Catalog.RegisterIndexDriver(dsqle.NewDoltIndexDriver(db))
	err := engine.Init()
	if err != nil {
		return nil, err
	}

	err = dsqle.RegisterSchemaFragments(sql.NewContext(ctx), engine.Catalog, db)
	if err != nil {
		return nil, err
	}

	return &sqlEngine{db, dEnv.DoltDB, engine, format}, nil
}

// Execute a SQL statement and return values for printing.
func (se *sqlEngine) query(ctx context.Context, query string) (sql.Schema, sql.RowIter, error) {
	sqlCtx := sql.NewContext(ctx)
	return se.engine.Query(sqlCtx, query)
}

// Pretty prints the output of the new SQL engine
func (se *sqlEngine) prettyPrintResults(ctx context.Context, nbf *types.NomsBinFormat, sqlSch sql.Schema, rowIter sql.RowIter) error {
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

	if se.resultFormat == formatTabular {
		autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
		p.AddStage(pipeline.NamedTransform{Name: fwtStageName, Func: autoSizeTransform.TransformToFWT})
	}

	// Redirect output to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	var wr table.TableWriteCloser

	switch se.resultFormat {
	case formatTabular:
		wr, err = tabular.NewTextTableWriter(cliWr, untypedSch)
	case formatCsv:
		wr, err = csv.NewCSVWriter(cliWr, untypedSch, csv.NewCSVInfo())
	default:
		panic("unimplemented output format type")
	}

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
	if se.resultFormat == formatTabular {
		p.InjectRow(fwtStageName, r)
	}

	p.Start()
	if err := p.Wait(); err != nil {
		return fmt.Errorf("error processing results: %v", err)
	}

	if chanErr != io.EOF {
		return fmt.Errorf("error processing results: %v", chanErr)
	}

	return nil
}

// Checks if the query is a naked delete and then deletes all rows if so. Returns true if it did so, false otherwise.
func (se *sqlEngine) checkThenDeleteAllRows(ctx context.Context, s *sqlparser.Delete) bool {
	if s.Where == nil && s.Limit == nil && s.Partitions == nil && len(s.TableExprs) == 1 {
		if ate, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
			if ste, ok := ate.Expr.(sqlparser.TableName); ok {
				root := se.sdb.Root()
				tName := ste.Name.String()
				table, ok, err := root.GetTable(ctx, tName)
				if err == nil && ok {
					rowData, err := table.GetRowData(ctx)
					if err != nil {
						return false
					}
					printRowIter := sql.RowsToRowIter(sql.NewRow(rowData.Len()))
					emptyMap, err := types.NewMap(ctx, root.VRW())
					if err != nil {
						return false
					}
					newTable, err := table.UpdateRows(ctx, emptyMap)
					if err != nil {
						return false
					}
					newRoot, err := doltdb.PutTable(ctx, root, root.VRW(), tName, newTable)
					if err != nil {
						return false
					}
					_ = se.prettyPrintResults(ctx, root.VRW().Format(), sql.Schema{{Name: "updated", Type: sql.Uint64}}, printRowIter)
					se.sdb.SetRoot(newRoot)
					return true
				}
			}
		}
	}
	return false
}

// Executes a SQL DDL statement (create, update, etc.). Updates the new root value in
// the sqlEngine if necessary.
func (se *sqlEngine) ddl(ctx context.Context, ddl *sqlparser.DDL, query string) error {
	switch ddl.Action {
	case sqlparser.CreateStr, sqlparser.DropStr, sqlparser.AlterStr, sqlparser.RenameStr:
		_, ri, err := se.query(ctx, query)
		if err == nil {
			ri.Close()
		}
		return err
	default:
		return fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}
