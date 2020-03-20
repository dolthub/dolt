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
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var sqlDocs = cli.CommandDocumentationContent{
	ShortDesc: "Runs a SQL query",
	LongDesc: "Runs a SQL query you specify. With no arguments, begins an interactive shell to run queries and view " +
		"the results. With the {{.EmphasisLeft}}-q{{.EmphasisRight}} option, runs the given query and prints any " +
		"results, then exits.\n" +
		"\n" +
		"By default, {{.EmphasisLeft}}-q{{.EmphasisRight}} executes a single statement. To execute multiple SQL " +
		"statements separated by semicolons, use {{.EmphasisLeft}}-b{{.EmphasisRight}} to enable batch mode. Queries can" +
		"be saved with {{.EmphasisLeft}}-s{{.EmphasisRight}}.\n" +
		"\n" +
		"Alternatively {{.EmpahasisLeft}}-x{{.EmphasisRight}} can be used to execute a saved query by name.\n" +
		"\n" +
		"Pipe SQL statements to dolt sql (no {{.EmphasisLeft}}-q{{.EmphasisRight}}) to execute a SQL import or update " +
		"script.\n" +
		"\n" +
		"Known limitations:\n" +
		"* No support for creating indexes\n" +
		"* No support for foreign keys\n" +
		"* No support for column constraints besides NOT NULL\n" +
		"* No support for default values\n" +
		"* Joins can only use indexes for two table joins. Three or more tables in a join query will use a non-indexed " +
		"join, which is very slow.",

	Synopsis: []string{
		"",
		"-q {{.LessThan}}query{{.GreaterThan}}",
		"-q {{.LessThan}}query;query{{.GreaterThan}} -b",
		"-q {{.LessThan}}query{{.GreaterThan}} -r {{.LessThan}}result format{{.GreaterThan}}",
		"-q {{.LessThan}}query{{.GreaterThan}} -s {{.LessThan}}name{{.GreaterThan}} -m {{.LessThan}}message{{.GreaterThan}}",
		"-x {{.LessThan}}name{{.GreaterThan}}",
		"--list-saved",
	},
}

const (
	queryFlag     = "query"
	formatFlag    = "result-format"
	saveFlag      = "save"
	executeFlag   = "execute"
	listSavedFlag = "list-saved"
	messageFlag   = "message"
	batchFlag     = "batch"
	welcomeMsg    = `# Welcome to the DoltSQL shell.
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
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, sqlDocs, ap))
}

func (cmd SqlCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(queryFlag, "q", "SQL query to run", "Runs a single query and exits")
	ap.SupportsString(formatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv. Defaults to tabular. ")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name")
	ap.SupportsFlag(listSavedFlag, "l", "Lists all saved queries")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also --name")
	ap.SupportsFlag(batchFlag, "b", "batch mode, to run more than one query with --query, separated by ';'. Piping input to sql with no arguments also uses batch mode")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// Exec executes the command
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlDocs, ap))

	apr := cli.ParseArgs(ap, args, help)
	err := validateSqlArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

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

	_, forceBatchMode := apr.GetValue(batchFlag)

	if query, queryOK := apr.GetValue(queryFlag); queryOK {
		saveMessage := apr.GetValueOrDefault(messageFlag, "")
		saveName := apr.GetValueOrDefault(saveFlag, "")

		return execQuery(ctx, dEnv, root, query, saveName, saveMessage, forceBatchMode, format, usage)
	}

	if savedQueryName, exOk := apr.GetValue(executeFlag); exOk {
		sq, err := dsqle.RetrieveFromQueryCatalog(ctx, root, savedQueryName)

		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		cli.PrintErrf("Executing saved query '%s':\n%s\n", savedQueryName, sq.Query)
		return execQuery(ctx, dEnv, root, sq.Query, "", "", forceBatchMode, format, usage)
	}

	if apr.Contains(listSavedFlag) {
		hasQC, err := root.HasTable(ctx, doltdb.DoltQueryCatalogTableName)

		if err != nil {
			verr := errhand.BuildDError("error: Failed to read from repository.").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}

		if !hasQC {
			return 0
		}

		query := "SELECT * FROM " + doltdb.DoltQueryCatalogTableName
		return execQuery(ctx, dEnv, root, query, "", "", forceBatchMode, format, usage)
	}

	return execQuery(ctx, dEnv, root, "", "", "", forceBatchMode, format, usage)
}

func execQuery(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query, saveName, saveMessage string, forceBatchMode bool, format resultFormat, usage cli.UsagePrinter) int {
	var se *sqlEngine
	origRoot := root
	queryOK := query != ""

	// run a single command and exit
	if !forceBatchMode && queryOK {
		se, err := newSqlEngine(ctx, dEnv, dsqle.NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState), format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		sqlSch, rowIter, err := processQuery(ctx, query, se)
		if err != nil {
			verr := formatQueryError(query, err)
			return HandleVErrAndExitCode(verr, usage)
		}

		if rowIter != nil {
			defer rowIter.Close()
			err = se.prettyPrintResults(ctx, se.ddb.ValueReadWriter().Format(), sqlSch, rowIter)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
		}

		if se.sdb.Root() != origRoot {
			return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, se.sdb.Root()), usage)
		}

		if saveName != "" {
			return HandleVErrAndExitCode(saveQuery(ctx, origRoot, dEnv, query, saveName, saveMessage), usage)
		}

		return 0
	}

	// Run in either batch mode for piped input, or shell mode for interactive
	fi, err := os.Stdin.Stat()
	// Windows has a bug where STDIN can't be statted in some cases, see https://github.com/golang/go/issues/33570
	if (err != nil && osutil.IsWindows) || (fi.Mode()&os.ModeCharDevice) == 0 || forceBatchMode {
		var batchInput io.Reader = os.Stdin
		if forceBatchMode {
			batchInput = strings.NewReader(query)
		}

		se, err = newSqlEngine(ctx, dEnv, dsqle.NewBatchedDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState), format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		err = runBatchMode(ctx, se, batchInput)
		if err != nil {
			cli.PrintErrln(color.RedString("Error processing batch"))
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

func formatQueryError(query string, err error) errhand.VerboseError {
	const (
		maxStatementLen     = 128
		maxPosWhenTruncated = 64
	)

	if se, ok := vterrors.AsSyntaxError(err); ok {
		verrBuilder := errhand.BuildDError("Error parsing SQL")
		verrBuilder.AddDetails(se.Message)

		statement := se.Statement
		position := se.Position

		prevLines := ""
		for {
			idxNewline := strings.IndexRune(statement, '\n')

			if idxNewline == -1 {
				break
			} else if idxNewline < position {
				position -= idxNewline + 1
				prevLines += statement[:idxNewline+1]
				statement = statement[idxNewline+1:]
			} else {
				statement = statement[:idxNewline]
				break
			}
		}

		if len(statement) > maxStatementLen {
			if position > maxPosWhenTruncated {
				statement = statement[position-maxPosWhenTruncated:]
				position = maxPosWhenTruncated
			}

			if len(statement) > maxStatementLen {
				statement = statement[:maxStatementLen]
			}
		}

		verrBuilder.AddDetails(prevLines + statement)

		marker := make([]rune, position+1)
		for i := 0; i < position; i++ {
			marker[i] = ' '
		}

		marker[position] = '^'
		verrBuilder.AddDetails(string(marker))

		return verrBuilder.Build()
	} else {
		return errhand.VerboseErrorFromError(err)
	}
}

func getFormat(format string) (resultFormat, errhand.VerboseError) {
	switch strings.ToLower(format) {
	case "tabular":
		return formatTabular, nil
	case "csv":
		return formatCsv, nil
	default:
		return formatTabular, errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular,csv").Build()
	}
}

func validateSqlArgs(apr *argparser.ArgParseResults) error {
	_, query := apr.GetValue(queryFlag)
	_, save := apr.GetValue(saveFlag)
	_, msg := apr.GetValue(messageFlag)
	_, batch := apr.GetValue(batchFlag)
	_, list := apr.GetValue(listSavedFlag)
	_, execute := apr.GetValue(executeFlag)

	if len(apr.Args()) > 0 && !query {
		return errhand.BuildDError("Invalid Argument: use --query or -q to pass inline SQL queries").Build()
	}

	if execute {
		if list {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --list-saved").Build()
		} else if query {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --query|-q").Build()
		} else if msg {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --message|-m").Build()
		} else if save {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --save|-s").Build()
		}
	}

	if list {
		if execute {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --executed|x").Build()
		} else if query {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --query|-q").Build()
		} else if msg {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --message|-m").Build()
		} else if save {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --save|-s").Build()
		}
	}

	if batch {
		if !query {
			return errhand.BuildDError("Invalid Argument: --batch|-b must be used with --query|-q").Build()
		}
		if save || msg {
			return errhand.BuildDError("Invalid Argument: --batch|-b is not compatible with --save|-s or --message|-m").Build()
		}
	}

	if query {
		if !save && msg {
			return errhand.BuildDError("Invalid Argument: --message|-m is only used with --query|-q and --save|-s").Build()
		}
	} else {
		if save {
			return errhand.BuildDError("Invalid Argument: --save|-s is only used with --query|-q").Build()
		}
		if msg {
			return errhand.BuildDError("Invalid Argument: --message|-m is only used with --query|-q and --save|-s").Build()
		}
	}

	return nil
}

// Saves the query given to the catalog with the name and message given.
func saveQuery(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, query string, name string, message string) errhand.VerboseError {
	_, newRoot, err := dsqle.NewQueryCatalogEntryWithNameAsID(ctx, root, name, query, message)
	if err != nil {
		return errhand.BuildDError("Couldn't save query").AddCause(err).Build()
	}

	return UpdateWorkingWithVErr(dEnv, newRoot)
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
func runBatchMode(ctx context.Context, se *sqlEngine, input io.Reader) error {
	scanner := bufio.NewScanner(input)
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
			verr := formatQueryError(query, err)
			cli.PrintErrln(verr.Verbose())
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

		if sqlSch, rowIter, err := processQuery(ctx, query, se); err != nil {
			verr := formatQueryError(query, err)
			shell.Println(verr.Verbose())
		} else if rowIter != nil {
			defer rowIter.Close()
			err = se.prettyPrintResults(ctx, se.ddb.ValueReadWriter().Format(), sqlSch, rowIter)
			if err != nil {
				shell.Println(color.RedString(err.Error()))
			}
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
// Returns the schema and the row iterator for the results, which may be nil, and an error if one occurs.
func processQuery(ctx context.Context, query string, se *sqlEngine) (sql.Schema, sql.RowIter, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Select, *sqlparser.Insert, *sqlparser.Update, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
		return se.query(ctx, query)
	case *sqlparser.Delete:
		ok := se.checkThenDeleteAllRows(ctx, s)
		if ok {
			return nil, nil, err
		}
		return se.query(ctx, query)
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			if se, ok := vterrors.AsSyntaxError(err); ok {
				return nil, nil, vterrors.SyntaxError{Message: "While Parsing DDL: " + se.Message, Position: se.Position, Statement: se.Statement}
			} else {
				return nil, nil, fmt.Errorf("Error parsing DDL: %v.", err.Error())
			}
		}
		return se.ddl(ctx, s, query)
	default:
		return nil, nil, fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
}

type stats struct {
	rowsInserted   int
	rowsUpdated    int
	rowsDeleted    int
	unflushedEdits int
	unprintedEdits int
}

var batchEditStats = &stats{}
var displayStrLen int

const maxBatchSize = 200000
const updateInterval = 1000

func (s *stats) numUpdates() int {
	return s.rowsUpdated + s.rowsDeleted + s.rowsInserted
}

func (s *stats) shouldUpdateBatchModeOutput() bool {
	return s.unprintedEdits >= updateInterval
}

func (s *stats) shouldFlush() bool {
	return s.unflushedEdits >= maxBatchSize
}

func flushBatchedEdits(ctx context.Context, se *sqlEngine) error {
	err := se.sdb.Flush(ctx)
	if err != nil {
		return err
	}

	batchEditStats.unflushedEdits = 0
	return nil
}

// Processes a single query in batch mode. The Root of the sqlEngine may or may not be changed.
func processBatchQuery(ctx context.Context, query string, se *sqlEngine) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	if canProcessAsBatchInsert(sqlStatement) {
		err = processBatchInsert(se, ctx, query, sqlStatement)
		if err != nil {
			return err
		}
	} else {
		err := processNonInsertBatchQuery(ctx, se, query, sqlStatement)
		if err != nil {
			return err
		}
	}

	if batchEditStats.shouldUpdateBatchModeOutput() {
		updateBatchInsertOutput()
	}

	return nil
}

func processNonInsertBatchQuery(ctx context.Context, se *sqlEngine, query string, sqlStatement sqlparser.Statement) error {
	// We need to commit whatever batch edits we've accumulated so far before executing the query
	err := flushBatchedEdits(ctx, se)
	if err != nil {
		return err
	}

	sqlSch, rowIter, err := processQuery(ctx, query, se)
	if err != nil {
		return err
	}

	if rowIter != nil {
		defer rowIter.Close()
		err = mergeResultIntoStats(sqlStatement, rowIter, batchEditStats)
		if err != nil {
			return fmt.Errorf("error executing statement: %v", err.Error())
		}

		// Some statement types should print results, even in batch mode.
		switch sqlStatement.(type) {
		case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
			if displayStrLen > 0 {
				// If we've been printing in batch mode, print a newline to put the regular output on its own line
				cli.Print("\n")
				displayStrLen = 0
			}
			err = se.prettyPrintResults(ctx, se.ddb.ValueReadWriter().Format(), sqlSch, rowIter)
			if err != nil {
				return err
			}
		}
	}

	// And flush again afterwards, to make sure any following insert statements have the latest data
	return flushBatchedEdits(ctx, se)
}

func processBatchInsert(se *sqlEngine, ctx context.Context, query string, sqlStatement sqlparser.Statement) error {
	_, rowIter, err := se.query(ctx, query)
	if err != nil {
		return fmt.Errorf("Error inserting rows: %v", err.Error())
	}

	if rowIter != nil {
		defer rowIter.Close()
		err = mergeResultIntoStats(sqlStatement, rowIter, batchEditStats)
		if err != nil {
			return fmt.Errorf("Error inserting rows: %v", err.Error())
		}
	}

	if batchEditStats.shouldFlush() {
		return flushBatchedEdits(ctx, se)
	}

	return nil
}

// canProcessBatchInsert returns whether the given statement can be processed as a batch insert. Only simple inserts
// (inserting a list of values) can be processed in this way. Other kinds of insert (notably INSERT INTO SELECT AS) need
// a flushed root and can't benefit from batch optimizations.
func canProcessAsBatchInsert(sqlStatement sqlparser.Statement) bool {
	switch s := sqlStatement.(type) {
	case *sqlparser.Insert:
		if _, ok := s.Rows.(sqlparser.Values); ok {
			return true
		}
		return false
	default:
		return false
	}
}

func updateBatchInsertOutput() {
	displayStr := fmt.Sprintf("Rows inserted: %d Rows updated: %d Rows deleted: %d",
		batchEditStats.rowsInserted, batchEditStats.rowsUpdated, batchEditStats.rowsDeleted)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
	batchEditStats.unprintedEdits = 0
}

// Updates the batch insert stats with the results of an INSERT, UPDATE, or DELETE statement.
func mergeResultIntoStats(statement sqlparser.Statement, rowIter sql.RowIter, s *stats) error {
	switch statement.(type) {
	case *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
		break
	default:
		return nil
	}

	for {
		row, err := rowIter.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else {
			numRowsUpdated := row[0].(int64)
			s.unflushedEdits += int(numRowsUpdated)
			s.unprintedEdits += int(numRowsUpdated)
			switch statement.(type) {
			case *sqlparser.Insert:
				s.rowsInserted += int(numRowsUpdated)
			case *sqlparser.Delete:
				s.rowsDeleted += int(numRowsUpdated)
			case *sqlparser.Update:
				s.rowsUpdated += int(numRowsUpdated)
			}
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
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))

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

					// Let the SQL engine handle system table deletes to avoid duplicating business logic here
					if doltdb.HasDoltPrefix(tName) {
						return false
					}

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
func (se *sqlEngine) ddl(ctx context.Context, ddl *sqlparser.DDL, query string) (sql.Schema, sql.RowIter, error) {
	switch ddl.Action {
	case sqlparser.CreateStr, sqlparser.DropStr, sqlparser.AlterStr, sqlparser.RenameStr:
		_, ri, err := se.query(ctx, query)
		if err == nil {
			ri.Close()
		}
		return nil, nil, err
	default:
		return nil, nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}
