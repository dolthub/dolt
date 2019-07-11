package sqle

import (
	"context"
	"fmt"
	"github.com/abiosoft/readline"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"github.com/liquidata-inc/ishell"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	dsql "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	dsqle "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"path/filepath"
	"strings"
)

var sqlShortDesc = "Runs a SQL query"
var sqlLongDesc = `Runs a SQL query you specify. By default, begins an interactive shell to run queries and view the
results. With the -q option, runs the given query and prints any results, then exits. With the -s
option, a MySQL-compatible server is started which can be connected to by MySQL clients.

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
	"-s [-a <host>] [-p <port>] [-u <user>] [-w <password>] [-t <timeout>] [-l <loglevel>] [-r]",
}

const (
	queryFlag = "query"
	serveFlag = "serve"
	hostFlag  = "host"
	portFlag  = "port"
	userFlag = "user"
	passwordFlag = "password"
	timeoutFlag = "timeout"
	readonlyFlag = "readonly"
	logLevelFlag = "loglevel"
	welcomeMsg = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`
)

func Sql(commandStr string, args []string, dEnv *env.DoltEnv) int {
	return sqlImpl(commandStr, args, dEnv, nil)
}

func sqlImpl(commandStr string, args []string, dEnv *env.DoltEnv, serverController *ServerController) int {
	serverConfig := DefaultServerConfig()

	ap := argparser.NewArgParser()
	ap.SupportsString(queryFlag, "q", "SQL query to run", "Runs a single query and exits")
	ap.SupportsFlag(serveFlag, "s", "Start a SQL server")
	ap.SupportsString(hostFlag, "a", "Host address", fmt.Sprintf("Defines the host address that the server will run on (default `%v`)", serverConfig.Host))
	ap.SupportsUint(portFlag, "p", "Port", fmt.Sprintf("Defines the port that the server will run on (default `%v`)", serverConfig.Port))
	ap.SupportsString(userFlag, "u", "User", fmt.Sprintf("Defines the server user (default `%v`)", serverConfig.User))
	ap.SupportsString(passwordFlag, "w", "Password", fmt.Sprintf("Defines the server password (default `%v`)", serverConfig.Password))
	ap.SupportsInt(timeoutFlag, "t", "Connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections (default `%v`)", serverConfig.Timeout))
	ap.SupportsFlag(readonlyFlag, "r", "Disables modification of the database")
	ap.SupportsString(logLevelFlag, "l", "Log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `debug`, `info`, `warning`, `error`, `fatal` (default `%v`)", serverConfig.LogLevel))
	help, usage := cli.HelpAndUsagePrinters(commandStr, sqlShortDesc, sqlLongDesc, sqlSynopsis, ap)

	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if _, ok := apr.GetValue(serveFlag); ok {
		if host, ok := apr.GetValue(hostFlag); ok {
			serverConfig.Host = host
		}
		if port, ok := apr.GetInt(portFlag); ok {
			serverConfig.Port = port
		}
		if user, ok := apr.GetValue(userFlag); ok {
			serverConfig.User = user
		}
		if password, ok := apr.GetValue(passwordFlag); ok {
			serverConfig.Password = password
		}
		if timeout, ok := apr.GetInt(timeoutFlag); ok {
			serverConfig.Timeout = timeout
		}
		if _, ok := apr.GetValue(readonlyFlag); ok {
			serverConfig.ReadOnly = true
		}
		if logLevel, ok := apr.GetValue(logLevelFlag); ok {
			serverConfig.LogLevel = LogLevel(logLevel)
		}
		if startError, closeError := serve(serverConfig, root, serverController); startError != nil || closeError != nil {
			if startError != nil {
				cli.PrintErrln(startError)
			}
			if closeError != nil {
				cli.PrintErrln(closeError)
			}
			return 1
		} else {
			return 0
		}
	}

	// run a single command and exit
	if query, ok := apr.GetValue(queryFlag); ok {
		if newRoot, err := processQuery(query, dEnv, root); err != nil {
			return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		} else if newRoot != nil {
			return commands.HandleVErrAndExitCode(commands.UpdateWorkingWithVErr(dEnv, newRoot), usage)
		} else {
			return 0
		}
	}

	// start an interactive shell
	root = runShell(dEnv, root)

	// If the SQL session wrote a new root value, update the working set with it
	if root != nil {
		return commands.HandleVErrAndExitCode(commands.UpdateWorkingWithVErr(dEnv, root), usage)
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

	completionWords = append(completionWords, dsql.CommonKeywords...)

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
	db := dsqle.NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	ctx := sql.NewEmptyContext()

	var err error
	_, iter, err := engine.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var r sql.Row
	for r, err = iter.Next(); err == nil; r, err = iter.Next() {
		// TODO: make this print pretty tables like original sql commands
		cli.Println(r)
	}

	if err == io.EOF {
		return nil, nil
	}

	return nil, err
}