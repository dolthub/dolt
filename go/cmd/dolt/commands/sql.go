package commands

import (
	"context"
	"errors"
	"fmt"
	"github.com/abiosoft/readline"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"github.com/liquidata-inc/ishell"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/xwb1989/sqlparser"
	"path/filepath"
	"strings"
)

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
* ORDER BY and LIMIT clauses

Known limitations:
* Some expressions in SELECT statements
* GROUP BY or aggregate functions
* Subqueries
* Column functions, e.g. CONCAT
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
	queryFlag = "query"
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

	// start an interactive shell
	root = runShell(dEnv, root)

	// If the SQL session wrote a new root value, update the working set with it
	if root != nil {
		return HandleVErrAndExitCode(UpdateWorkingWithVErr(dEnv, root), usage)
	}

	return 0
}

// runShell starts a SQL shell. Returns when the user exits the shell with the root value resulting from any queries.
func runShell(dEnv *env.DoltEnv, root *doltdb.RootValue) *doltdb.RootValue {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)

	// start the doltsql shell
	historyFile := filepath.Join(dEnv.GetDoltDir(), ".sqlhistory")
	rlConf := readline.Config{
		Prompt: "doltsql> ",
		Stdout: cli.CliOut,
		Stderr: cli.CliOut,
		HistoryFile: historyFile,
		HistoryLimit: 500,
		HistorySearchFold: true,
		DisableAutoSaveHistory: true,
	}
	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string {
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator: ";",
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt( "      -> ")
	// TODO: update completer on create / drop / alter statements
	shell.CustomCompleter(newCompleter(dEnv))

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

		if newRoot, err := processQuery(query, dEnv, root); err != nil {
			shell.Println(color.RedString(err.Error()))
		} else if newRoot != nil {
			root = newRoot
		}

		// TODO: there's a bug in the readline library when editing multi-line history entries.
		// Longer term we need to switch to a new readline library, like in this bug:
		// https://github.com/cockroachdb/cockroach/issues/15460
		// For now, we store all history entries as single-line strings to avoid the issue.
		// TODO: only store history if it's a tty
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

	completionWords = append(completionWords, sql.CommonKeywords...)

	return &sqlCompleter{
		allWords: completionWords,
		columnNames: columnNames,
	}
}

type sqlCompleter struct {
	allWords []string
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
		return prepend(alias + ".", c.columnNames)
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
		return nil, errFmt("Error parsing SQL: %v.", err.Error())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Show:
		return nil, sqlShow(root, s)
	case *sqlparser.Select:
		return nil, sqlSelect(root, s)
	case *sqlparser.Insert:
		return sqlInsert(dEnv, root, s, query)
	case *sqlparser.Update:
		return sqlUpdate(dEnv, root, s, query)
	case *sqlparser.Delete:
		return sqlDelete(dEnv, root, s, query)
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return nil, errFmt("Error parsing DDL: %v.", err.Error())
		}
		return sqlDDL(dEnv, root, s, query)
	default:
		return nil, errFmt("Unsupported SQL statement: '%v'.", query)
	}
}

// Executes a SQL show statement and prints the result to the CLI.
func sqlShow(root *doltdb.RootValue, show *sqlparser.Show) error {
	p, sch, err := sql.BuildShowPipeline(context.TODO(), root, show)
	if err != nil {
		return err
	}

	return runPrintingPipeline(p, sch)
}

// Executes a SQL select statement and prints the result to the CLI.
func sqlSelect(root *doltdb.RootValue, s *sqlparser.Select) error {

	p, statement, err := sql.BuildSelectQueryPipeline(context.TODO(), root, s)
	if err != nil {
		return err
	}

	// Now that we have the output schema, we add three additional steps to the pipeline:
	// 1) Coerce all the values in each row into strings
	// 2) Convert null values to printed values
	// 3) Run them through a fixed width transformer to make them print pretty
	resultSchema := statement.ResultSetSchema
	untypedSch, untypingTransform := newUntypingTransformer(resultSchema)
	p.AddStage(untypingTransform)

	return runPrintingPipeline(p, untypedSch)
}

// Adds some print-handling stages to the pipeline given and runs it, returning any error.
// Adds null-printing and fixed-width transformers. The schema given is assumed to be untyped (string-typed).
func runPrintingPipeline(p *pipeline.Pipeline, untypedSch schema.Schema) error {
	nullPrinter := nullprinter.NewNullPrinter(untypedSch)
	p.AddStage(pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow))

	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{fwtStageName, autoSizeTransform.TransformToFWT})

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
	p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(untypedSch, schema.ExtractAllColNames(untypedSch)))

	p.Start()
	if err := p.Wait(); err != nil {
		return errFmt("error processing results: %v", err)
	}

	return nil
}

// Executes a SQL insert statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlInsert(dEnv *env.DoltEnv, root *doltdb.RootValue, stmt *sqlparser.Insert, query string) (*doltdb.RootValue, error) {
	result, err := sql.ExecuteInsert(context.Background(), dEnv.DoltDB, root, stmt, query)
	if err != nil {
		return nil, errFmt("Error inserting rows: %v", err.Error())
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

// Executes a SQL update statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlUpdate(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Update, query string) (*doltdb.RootValue, error) {
	result, err := sql.ExecuteUpdate(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return nil, errFmt("Error during update: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
	if result.NumRowsUnchanged > 0 {
		cli.Println(fmt.Sprintf("Rows matched but unchanged: %v", result.NumRowsUnchanged))
	}

	return result.Root, nil
}

// Executes a SQL delete statement and prints the result to the CLI. Returns the new root value to be written as appropriate.
func sqlDelete(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Delete, query string) (*doltdb.RootValue, error) {
	result, err := sql.ExecuteDelete(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return nil, errFmt("Error during update: %v", err.Error())
	}

	cli.Println(fmt.Sprintf("Rows deleted: %v", result.NumRowsDeleted))

	return result.Root, nil
}

// Executes a SQL DDL statement (create, update, etc.). Returns the new root value to be written as appropriate.
func sqlDDL(dEnv *env.DoltEnv, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	switch ddl.Action {
	case sqlparser.CreateStr:
		newRoot, _, err := sql.ExecuteCreate(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, errFmt("Error creating table: %v", err)
		}
		return newRoot, nil
	case sqlparser.AlterStr, sqlparser.RenameStr:
		newRoot, err := sql.ExecuteAlter(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, errFmt("Error altering table: %v", err)
		}
		return newRoot, nil
	case sqlparser.DropStr:
		newRoot, err := sql.ExecuteDrop(context.Background(), dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return nil, errFmt("Error dropping table: %v", err)
		}
		return newRoot, nil
	case sqlparser.TruncateStr:
		return nil, errFmt("Unhandled DDL action %v in query %v", ddl.Action, query)
	default:
		return nil, errFmt("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}

func errFmt(fmtMsg string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(fmtMsg, args...))
}

// Returns a new untyping transformer for the schema given.
// TODO: move this somewhere more appropriate. Import cycles currently make this difficult.
func newUntypingTransformer(sch schema.Schema) (schema.Schema, pipeline.NamedTransform) {
	untypedSch := untyped.UntypeUnkeySchema(sch)
	mapping, err := rowconv.TagMapping(sch, untypedSch)

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	return untypedSch, pipeline.NewNamedTransform("untype", rowconv.GetRowConvTransformFunc(rConv))
}
