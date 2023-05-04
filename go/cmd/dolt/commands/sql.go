// Copyright 2019-2020 Dolthub, Inc.
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
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abiosoft/readline"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/ishell"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/dolthub/vitess/go/vt/vterrors"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
)

type batchMode int64

const (
	invalidBatchMode batchMode = iota
	insertBatchMode
	deleteBatchMode
)

var sqlDocs = cli.CommandDocumentationContent{
	ShortDesc: "Runs a SQL query",
	LongDesc: `Runs a SQL query you specify. With no arguments, begins an interactive shell to run queries and view the results. With the {{.EmphasisLeft}}-q{{.EmphasisRight}} option, runs the given query and prints any results, then exits.

Multiple SQL statements must be separated by semicolons. Use {{.EmphasisLeft}}-b{{.EmphasisRight}} to enable batch mode to speed up large batches of INSERT / UPDATE statements. Pipe SQL files to dolt sql (no {{.EmphasisLeft}}-q{{.EmphasisRight}}) to execute a SQL import or update script. 

Queries can be saved to the query catalog with {{.EmphasisLeft}}-s{{.EmphasisRight}}. Alternatively {{.EmphasisLeft}}-x{{.EmphasisRight}} can be used to execute a saved query by name.

By default this command uses the dolt database in the current working directory, as well as any dolt databases that are found in the current directory. Any databases created with CREATE DATABASE are placed in the current directory as well. Running with {{.EmphasisLeft}}--data-dir <directory>{{.EmphasisRight}} uses each of the subdirectories of the supplied directory (each subdirectory must be a valid dolt data repository) as databases. Subdirectories starting with '.' are ignored.`,

	Synopsis: []string{
		"",
		"< script.sql",
		"[--data-dir {{.LessThan}}directory{{.GreaterThan}}] [-r {{.LessThan}}result format{{.GreaterThan}}]",
		"-q {{.LessThan}}query{{.GreaterThan}} [-r {{.LessThan}}result format{{.GreaterThan}}] [-s {{.LessThan}}name{{.GreaterThan}} -m {{.LessThan}}message{{.GreaterThan}}] [-b]",
		"-q {{.LessThan}}query{{.GreaterThan}} --data-dir {{.LessThan}}directory{{.GreaterThan}} [-r {{.LessThan}}result format{{.GreaterThan}}] [-b]",
		"-x {{.LessThan}}name{{.GreaterThan}}",
		"--list-saved",
	},
}

var ErrMultipleDoltCfgDirs = errors.NewKind("multiple .doltcfg directories detected: '%s' and '%s'; pass one of the directories using option --doltcfg-dir")

const (
	QueryFlag             = "query"
	FormatFlag            = "result-format"
	saveFlag              = "save"
	executeFlag           = "execute"
	listSavedFlag         = "list-saved"
	messageFlag           = "message"
	BatchFlag             = "batch"
	DataDirFlag           = "data-dir"
	MultiDBDirFlag        = "multi-db-dir"
	CfgDirFlag            = "doltcfg-dir"
	DefaultCfgDirName     = ".doltcfg"
	PrivsFilePathFlag     = "privilege-file"
	BranchCtrlPathFlag    = "branch-control-file"
	DefaultPrivsName      = "privileges.db"
	DefaultBranchCtrlName = "branch_control.db"
	continueFlag          = "continue"
	fileInputFlag         = "file"
	UserFlag              = "user"
	DefaultUser           = "root"
	DefaultHost           = "localhost"

	welcomeMsg = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`
)

// TODO: get rid of me, use a real integration point to define system variables
func init() {
	dsqle.AddDoltSystemVariables()
}

type SqlCmd struct {
	VersionStr string
}

// The SQL shell installs its own signal handlers so that you can cancel a running query without ending the entire
// process
func (cmd SqlCmd) InstallsSignalHandlers() bool {
	return true
}

var _ cli.SignalCommand = SqlCmd{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SqlCmd) Name() string {
	return "sql"
}

// Description returns a description of the command
func (cmd SqlCmd) Description() string {
	return "Run a SQL query against tables in repository."
}

func (cmd SqlCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlDocs, ap)
}

func (cmd SqlCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits.")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json, vertical. Defaults to tabular.")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name.")
	ap.SupportsFlag(listSavedFlag, "l", "List all saved queries.")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also `--name`.")
	ap.SupportsFlag(BatchFlag, "b", "Use to enable more efficient batch processing for large SQL import scripts consisting of only INSERT statements. Other statements types are not guaranteed to work in this mode.")
	ap.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
	ap.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the DataDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd SqlCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
// Unlike other commands, sql doesn't set a new working root directly, as the SQL layer updates the working set as
// necessary when committing work.
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, help)
	err := validateSqlArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	globalArgs := cliCtx.GlobalArgs()
	err = validateSqlArgs(globalArgs)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// We need a username and password for many SQL commands, so set defaults if they don't exist
	dEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)

	format := engine.FormatTabular
	if formatSr, ok := apr.GetValue(FormatFlag); ok {
		var verr errhand.VerboseError
		format, verr = GetResultFormat(formatSr)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	if query, queryOK := apr.GetValue(QueryFlag); queryOK {
		if apr.Contains(saveFlag) {
			return execSaveQuery(sqlCtx, dEnv, queryist, apr, query, format, usage)
		}
		return queryMode(sqlCtx, queryist, apr, query, format, usage)
	} else if savedQueryName, exOk := apr.GetValue(executeFlag); exOk {
		return executeSavedQuery(sqlCtx, queryist, dEnv, savedQueryName, format, usage)
	} else if apr.Contains(listSavedFlag) {
		return listSavedQueries(sqlCtx, queryist, dEnv, format, usage)
	} else {
		// Run in either batch mode for piped input, or shell mode for interactive
		isTty := false
		runInBatchMode := apr.Contains(BatchFlag)

		fi, err := os.Stdin.Stat()
		if err != nil {
			if !osutil.IsWindows {
				return HandleVErrAndExitCode(errhand.BuildDError("Couldn't stat STDIN. This is a bug.").Build(), usage)
			}
		} else {
			isTty = fi.Mode()&os.ModeCharDevice != 0
		}

		_, continueOnError := apr.GetValue(continueFlag)

		input := os.Stdin
		if fileInput, ok := apr.GetValue(fileInputFlag); ok {
			isTty = false
			input, err = os.OpenFile(fileInput, os.O_RDONLY, os.ModePerm)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("couldn't open file %s", fileInput).Build(), usage)
			}
			info, err := os.Stat(fileInput)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("couldn't get file size %s", fileInput).Build(), usage)
			}

			// initialize fileReadProg global variable if there is a file to process queries from
			fileReadProg = &fileReadProgress{bytesRead: 0, totalBytes: info.Size(), printed: 0, displayStrLen: 0}
			defer fileReadProg.close()
		}

		if isTty {
			err := execShell(sqlCtx, queryist, format)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
		} else if runInBatchMode {
			se, ok := queryist.(*engine.SqlEngine)
			if !ok {
				misuse := fmt.Errorf("Using batch with non-local access pattern. Stop server if it is running")
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(misuse), usage)
			}

			verr := execBatch(sqlCtx, se, input, continueOnError, format)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		} else {
			err := execMultiStatements(sqlCtx, queryist, input, continueOnError, format)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
		}
	}

	return 0
}

func listSavedQueries(ctx *sql.Context, qryist cli.Queryist, dEnv *env.DoltEnv, format engine.PrintResultFormat, usage cli.UsagePrinter) int {
	if !dEnv.Valid() {
		return HandleVErrAndExitCode(errhand.BuildDError("error: --%s must be used in a dolt database directory.", listSavedFlag).Build(), usage)
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	hasQC, err := workingRoot.HasTable(ctx, doltdb.DoltQueryCatalogTableName)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to read from repository.").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	if !hasQC {
		return 0
	}

	query := "SELECT * FROM " + doltdb.DoltQueryCatalogTableName
	return HandleVErrAndExitCode(execQuery(ctx, qryist, query, format), usage)
}

func executeSavedQuery(ctx *sql.Context, qryist cli.Queryist, dEnv *env.DoltEnv, savedQueryName string, format engine.PrintResultFormat, usage cli.UsagePrinter) int {
	if !dEnv.Valid() {
		return HandleVErrAndExitCode(errhand.BuildDError("error: --%s must be used in a dolt database directory.", executeFlag).Build(), usage)
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	sq, err := dtables.RetrieveFromQueryCatalog(ctx, workingRoot, savedQueryName)

	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cli.PrintErrf("Executing saved query '%s':\n%s\n", savedQueryName, sq.Query)
	return HandleVErrAndExitCode(execQuery(ctx, qryist, sq.Query, format), usage)
}

func queryMode(
	ctx *sql.Context,
	qryist cli.Queryist,
	apr *argparser.ArgParseResults,
	query string,
	format engine.PrintResultFormat,
	usage cli.UsagePrinter,
) int {

	// query mode has 2 sub modes:
	// If --batch is provided, run with batch optimizations
	// Otherwise, attempt to parse and execute multiple queries separated by ';'
	batchMode := apr.Contains(BatchFlag)
	_, continueOnError := apr.GetValue(continueFlag)

	if batchMode {
		se, ok := qryist.(*engine.SqlEngine)
		if !ok {
			misuse := fmt.Errorf("Using batch with non-local access pattern. Stop server if it is running")
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(misuse), usage)
		}

		batchInput := strings.NewReader(query)
		verr := execBatch(ctx, se, batchInput, continueOnError, format)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	} else {
		input := strings.NewReader(query)
		err := execMultiStatements(ctx, qryist, input, continueOnError, format)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	}

	return 0
}

func execSaveQuery(ctx *sql.Context, dEnv *env.DoltEnv, qryist cli.Queryist, apr *argparser.ArgParseResults, query string, format engine.PrintResultFormat, usage cli.UsagePrinter) int {
	if !dEnv.Valid() {
		return HandleVErrAndExitCode(errhand.BuildDError("error: --%s must be used in a dolt database directory.", saveFlag).Build(), usage)
	}

	saveName := apr.GetValueOrDefault(saveFlag, "")

	verr := execQuery(ctx, qryist, query, format)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to get working root").AddCause(err).Build(), usage)
	}

	saveMessage := apr.GetValueOrDefault(messageFlag, "")
	newRoot, verr := saveQuery(ctx, workingRoot, query, saveName, saveMessage)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	err = dEnv.UpdateWorkingRoot(ctx, newRoot)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to update working root").AddCause(err).Build(), usage)
	}

	return 0
}

// getMultiRepoEnv returns an appropriate MultiRepoEnv for this invocation of the command
func getMultiRepoEnv(ctx context.Context, workingDir string, dEnv *env.DoltEnv) (mrEnv *env.MultiRepoEnv, resolvedDir string, verr errhand.VerboseError) {
	var err error
	fs := dEnv.FS
	if len(workingDir) > 0 {
		fs, err = fs.WithWorkingDir(workingDir)
	}
	if err != nil {
		return nil, "", errhand.VerboseErrorFromError(err)
	}
	resolvedDir, err = fs.Abs("")
	if err != nil {
		return nil, "", errhand.VerboseErrorFromError(err)
	}

	mrEnv, err = env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), fs, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	if err != nil {
		return nil, "", errhand.VerboseErrorFromError(err)
	}

	return mrEnv, resolvedDir, nil
}

func execBatch(
	sqlCtx *sql.Context,
	se *engine.SqlEngine,
	batchInput io.Reader,
	continueOnErr bool,
	format engine.PrintResultFormat,
) errhand.VerboseError {
	// In batch mode, we need to set a couple flags on the session to prevent constant flushes to disk
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()
	err := runBatchMode(sqlCtx, se, batchInput, continueOnErr, format)
	if err != nil {
		// If we encounter an error, attempt to flush what we have so far to disk before exiting
		flushErr := flushBatchedEdits(sqlCtx, se)
		if flushErr != nil {
			cli.PrintErrf("Could not flush batch: %s", err.Error())
		}

		return errhand.BuildDError("Error processing batch").AddCause(err).Build()
	}

	return nil
}

func execQuery(
	sqlCtx *sql.Context,
	qryist cli.Queryist,
	query string,
	format engine.PrintResultFormat,
) errhand.VerboseError {

	sqlSch, rowIter, err := processQuery(sqlCtx, query, qryist)
	if err != nil {
		return formatQueryError("", err)
	}

	if rowIter != nil {
		err = engine.PrettyPrintResults(sqlCtx, format, sqlSch, rowIter)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}

func formatQueryError(message string, err error) errhand.VerboseError {
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
		if len(message) > 0 {
			err = fmt.Errorf("%s: %v", message, err)
		}
		return errhand.VerboseErrorFromError(err)
	}
}

func GetResultFormat(format string) (engine.PrintResultFormat, errhand.VerboseError) {
	switch strings.ToLower(format) {
	case "tabular":
		return engine.FormatTabular, nil
	case "csv":
		return engine.FormatCsv, nil
	case "json":
		return engine.FormatJson, nil
	case "null":
		return engine.FormatNull, nil
	case "vertical":
		return engine.FormatVertical, nil
	default:
		return engine.FormatTabular, errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular, csv, json").Build()
	}
}

func validateSqlArgs(apr *argparser.ArgParseResults) error {
	_, query := apr.GetValue(QueryFlag)
	_, save := apr.GetValue(saveFlag)
	_, msg := apr.GetValue(messageFlag)
	_, batch := apr.GetValue(BatchFlag)
	_, list := apr.GetValue(listSavedFlag)
	_, execute := apr.GetValue(executeFlag)
	_, dataDir := apr.GetValue(DataDirFlag)
	_, multiDbDir := apr.GetValue(MultiDBDirFlag)

	if len(apr.Args) > 0 && !query {
		return errhand.BuildDError("Invalid Argument: use --query or -q to pass inline SQL queries").Build()
	}

	if dataDir && multiDbDir {
		return errhand.BuildDError("Invalid Argument: --data-dir is not compatible with --multi-db-dir").Build()
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
		} else if dataDir || multiDbDir {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --data-dir").Build()
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
		} else if dataDir || multiDbDir {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --data-dir").Build()
		}
	}

	if save && (dataDir || multiDbDir) {
		return errhand.BuildDError("Invalid Argument: --data-dir queries cannot be saved").Build()
	}

	if batch {
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

	if multiDbDir {
		cli.PrintErrln("WARNING: --multi-db-dir is deprecated, use --data-dir instead")
	}

	return nil
}

// Saves the query given to the catalog with the name and message given.
func saveQuery(ctx *sql.Context, root *doltdb.RootValue, query string, name string, message string) (*doltdb.RootValue, errhand.VerboseError) {
	_, newRoot, err := dtables.NewQueryCatalogEntryWithNameAsID(ctx, root, name, query, message)
	if err != nil {
		return nil, errhand.BuildDError("Couldn't save query").AddCause(err).Build()
	}

	return newRoot, nil
}

// execMultiStatements runs all the queries in the input reader without any batch optimizations
func execMultiStatements(ctx *sql.Context, qryist cli.Queryist, input io.Reader, continueOnErr bool, format engine.PrintResultFormat) error {
	scanner := NewSqlStatementScanner(input)
	var query string
	for scanner.Scan() {
		if fileReadProg != nil {
			updateFileReadProgressOutput()
			fileReadProg.setReadBytes(int64(len(scanner.Bytes())))
		}
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		sqlStatement, err := sqlparser.Parse(query)
		if err == sqlparser.ErrEmpty {
			continue
		} else if err != nil {
			handleError(scanner.statementStartLine, query, err)
			// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
			if !continueOnErr {
				return err
			}
		}

		// store start time for query
		ctx.SetQueryTime(time.Now())
		sqlSch, rowIter, err := processParsedQuery(ctx, query, qryist, sqlStatement)
		if err != nil {
			handleError(scanner.statementStartLine, query, err)
			// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
			if !continueOnErr {
				return err
			}
		}

		if rowIter != nil {
			switch sqlStatement.(type) {
			case *sqlparser.Select, *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
				*sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
				// For any statement that prints out result, print a newline to put the regular output on its own line
				if fileReadProg != nil {
					fileReadProg.printNewLineIfNeeded()
				}
			}
			err = engine.PrettyPrintResults(ctx, format, sqlSch, rowIter)
			if err != nil {
				handleError(scanner.statementStartLine, query, err)
				return err
			}
		}
		query = ""
	}

	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	return nil
}

func handleError(stmtStartLine int, query string, err error) {
	verr := formatQueryError(fmt.Sprintf("error on line %d for query %s", stmtStartLine, query), err)
	cli.PrintErrln(verr.Verbose())
}

// runBatchMode processes queries until EOF. The Root of the sqlEngine may be updated.
func runBatchMode(ctx *sql.Context, se *engine.SqlEngine, input io.Reader, continueOnErr bool, format engine.PrintResultFormat) error {
	scanner := NewSqlStatementScanner(input)

	var query string
	for scanner.Scan() {
		if fileReadProg != nil {
			updateFileReadProgressOutput()
			fileReadProg.setReadBytes(int64(len(scanner.Bytes())))
		}
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		if err := processBatchQuery(ctx, query, se, format); err != nil {
			// TODO: this line number will not be accurate for errors that occur when flushing a batch of inserts (as opposed
			//  to processing the query)
			verr := formatQueryError(fmt.Sprintf("error on line %d for query %s", scanner.statementStartLine, query), err)
			cli.PrintErrln(verr.Verbose())
			// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
			if !continueOnErr {
				return err
			}
		}
		query = ""
	}

	updateBatchEditOutput()
	cli.Println() // need a newline after all updates are executed

	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	return flushBatchedEdits(ctx, se)
}

// execShell starts a SQL shell. Returns when the user exits the shell. The Root of the sqlEngine may
// be updated by any queries which were processed.
func execShell(sqlCtx *sql.Context, qryist cli.Queryist, format engine.PrintResultFormat) error {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)

	historyFile := filepath.Join(".sqlhistory") // history file written to working dir
	initialPrompt := fmt.Sprintf("%s> ", sqlCtx.GetCurrentDatabase())
	initialMultilinePrompt := fmt.Sprintf(fmt.Sprintf("%%%ds", len(initialPrompt)), "-> ")

	rlConf := readline.Config{
		Prompt:                 initialPrompt,
		Stdout:                 cli.CliOut,
		Stderr:                 cli.CliOut,
		HistoryFile:            historyFile,
		HistoryLimit:           500,
		HistorySearchFold:      true,
		DisableAutoSaveHistory: true,
	}

	verticalOutputLineTerminators := []string{"\\g", "\\G"}

	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string{
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator: ";",
		MysqlShellCmds: verticalOutputLineTerminators,
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt(initialMultilinePrompt)
	// TODO: update completer on create / drop / alter statements
	completer, err := newCompleter(sqlCtx, qryist)
	if err != nil {
		return err
	}

	shell.CustomCompleter(completer)

	shell.EOF(func(c *ishell.Context) {
		c.Stop()
	})

	// The shell's interrupt handler handles an interrupt that occurs when it's accepting input. We also install our own
	// that handles interrupts during query execution or result printing, see below.
	shell.Interrupt(func(c *ishell.Context, count int, input string) {
		if count > 1 {
			c.Stop()
		} else {
			c.Println("Received SIGINT. Interrupt again to exit, or use ^D, quit, or exit")
		}
	})

	initialCtx := sqlCtx.Context

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		if len(strings.TrimSpace(query)) == 0 {
			return
		}

		closureFormat := format

		// TODO: there's a bug in the readline library when editing multi-line history entries.
		// Longer term we need to switch to a new readline library, like in this bug:
		// https://github.com/cockroachdb/cockroach/issues/15460
		// For now, we store all history entries as single-line strings to avoid the issue.
		singleLine := strings.ReplaceAll(query, "\n", " ")

		if err := shell.AddHistory(singleLine); err != nil {
			// TODO: handle better, like by turning off history writing for the rest of the session
			shell.Println(color.RedString(err.Error()))
		}

		query = strings.TrimSuffix(query, shell.LineTerminator())

		// TODO: it would be better to build this into the statement parser rather than special case it here
		for _, terminator := range verticalOutputLineTerminators {
			if strings.HasSuffix(query, terminator) {
				closureFormat = engine.FormatVertical
			}
			query = strings.TrimSuffix(query, terminator)
		}

		var nextPrompt string
		var sqlSch sql.Schema
		var rowIter sql.RowIter

		cont := func() bool {
			subCtx, stop := signal.NotifyContext(initialCtx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			sqlCtx := sql.NewContext(subCtx, sql.WithSession(sqlCtx.Session))

			if sqlSch, rowIter, err = processQuery(sqlCtx, query, qryist); err != nil {
				verr := formatQueryError("", err)
				shell.Println(verr.Verbose())
			} else if rowIter != nil {
				switch closureFormat {
				case engine.FormatTabular, engine.FormatVertical:
					err = engine.PrettyPrintResultsExtended(sqlCtx, closureFormat, sqlSch, rowIter)
				default:
					err = engine.PrettyPrintResults(sqlCtx, closureFormat, sqlSch, rowIter)
				}

				if err != nil {
					shell.Println(color.RedString(err.Error()))
				}
			}

			nextPrompt = fmt.Sprintf("%s> ", sqlCtx.GetCurrentDatabase())
			return true
		}()

		if !cont {
			return
		}

		shell.SetPrompt(nextPrompt)
		shell.SetMultiPrompt(fmt.Sprintf(fmt.Sprintf("%%%ds", len(nextPrompt)), "-> "))
	})

	shell.Run()
	_ = iohelp.WriteLine(cli.CliOut, "Bye")

	return nil
}

// Returns a new auto completer with table names, column names, and SQL keywords.
// TODO: update the completer on DDL, branch change, etc.
func newCompleter(
	ctx *sql.Context,
	qryist cli.Queryist,
) (completer *sqlCompleter, rerr error) {
	subCtx, stop := signal.NotifyContext(ctx.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()

	sqlCtx := sql.NewContext(subCtx, sql.WithSession(ctx.Session))

	_, iter, err := qryist.Query(sqlCtx, "select table_schema, table_name, column_name from information_schema.columns;")
	if err != nil {
		return nil, err
	}

	defer func(iter sql.RowIter, context *sql.Context) {
		err := iter.Close(context)
		if err != nil && rerr == nil {
			rerr = err
		}
	}(iter, sqlCtx)

	identifiers := make(map[string]struct{})
	var columnNames []string
	for {
		r, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		identifiers[r[0].(string)] = struct{}{}
		identifiers[r[1].(string)] = struct{}{}
		identifiers[r[2].(string)] = struct{}{}
		columnNames = append(columnNames, r[2].(string))
	}

	var completionWords []string
	for k := range identifiers {
		completionWords = append(completionWords, k)
	}

	completionWords = append(completionWords, dsqle.CommonKeywords...)

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

// processQuery processes a single query. The Root of the sqlEngine will be updated if necessary.
// Returns the schema and the row iterator for the results, which may be nil, and an error if one occurs.
func processQuery(ctx *sql.Context, query string, qryist cli.Queryist) (sql.Schema, sql.RowIter, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}
	return processParsedQuery(ctx, query, qryist, sqlStatement)
}

// processParsedQuery processes a single query with the parsed statement provided. The Root of the sqlEngine
// will be updated if necessary. Returns the schema and the row iterator for the results, which may be nil,
// and an error if one occurs.
func processParsedQuery(ctx *sql.Context, query string, qryist cli.Queryist, sqlStatement sqlparser.Statement) (sql.Schema, sql.RowIter, error) {
	switch s := sqlStatement.(type) {
	case *sqlparser.Use:
		sch, ri, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, nil, ri)
		if err != nil {
			return nil, nil, err
		}
		cli.Println("Database changed")
		return sch, nil, err
	case *sqlparser.MultiAlterDDL, *sqlparser.Set, *sqlparser.Commit:
		_, ri, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, nil, ri)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	case *sqlparser.DDL:
		_, ri, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, nil, ri)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	case *sqlparser.DBDDL:
		if se, ok := qryist.(*engine.SqlEngine); ok {
			// NM4 refactor this out
			return se.Dbddl(ctx, s, query)
		}
		return nil, nil, fmt.Errorf("unsupported statement: %s", query)
	case *sqlparser.Load:
		if s.Local {
			return nil, nil, fmt.Errorf("LOCAL supported only in sql-server mode")
		}
		return qryist.Query(ctx, query)
	default:
		return qryist.Query(ctx, query)
	}
}

type stats struct {
	rowsInserted   int
	rowsUpdated    int
	rowsDeleted    int
	unflushedEdits int
	unprintedEdits int
	displayStrLen  int
}

type fileReadProgress struct {
	bytesRead     int64
	totalBytes    int64
	printed       int64
	displayStrLen int
}

var batchEditStats = &stats{}
var fileReadProg *fileReadProgress

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

// printNewLineIfNeeded prints a new line when there are outputs printed other than its output line of batch read progress.
func (s *stats) printNewLineIfNeeded() {
	if s.displayStrLen > 0 {
		cli.Print("\n")
		s.displayStrLen = 0
	}
}

// close will print last updated line of processed 100.0% and a new line
func (f *fileReadProgress) close() {
	f.bytesRead = f.totalBytes
	updateFileReadProgressOutput()
	cli.Println() // need a newline after all updates are executed
}

// setReadBytes updates number of bytes that are read so far from the file
func (f *fileReadProgress) setReadBytes(b int64) {
	f.bytesRead = f.printed + b
}

// printNewLineIfNeeded prints a new line when there are outputs printed other than its output line of file read progress.
func (f *fileReadProgress) printNewLineIfNeeded() {
	if f.displayStrLen > 0 {
		cli.Print("\n")
		f.displayStrLen = 0
	}
}

func flushBatchedEdits(ctx *sql.Context, se *engine.SqlEngine) error {
	for i := range se.Databases(ctx) {
		if err := se.Databases(ctx)[i].Flush(ctx); err != nil {
			return err
		}
	}

	batchEditStats.unflushedEdits = 0
	return nil
}

// Processes a single query in batch mode. The Root of the sqlEngine may or may not be changed.
func processBatchQuery(ctx *sql.Context, query string, se *engine.SqlEngine, format engine.PrintResultFormat) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	currentBatchMode := invalidBatchMode
	if v, err := ctx.GetSessionVariable(ctx, dsess.CurrentBatchModeKey); err == nil {
		currentBatchMode = batchMode(v.(int64))
	} else {
		return err
	}

	newBatchMode, err := canProcessAsBatchEdit(ctx, sqlStatement, se, query)
	if err != nil {
		cli.PrintErrln(err)
		return err
	}

	if currentBatchMode != invalidBatchMode && currentBatchMode != newBatchMode {
		// We need to commit whatever batch edits we've accumulated so far before executing the query
		err := flushBatchedEdits(ctx, se)
		if err != nil {
			return err
		}
	}

	err = ctx.SetSessionVariable(ctx, dsess.CurrentBatchModeKey, int64(newBatchMode))
	if err != nil {
		return err
	}

	if newBatchMode != invalidBatchMode {
		err = processBatchableEditQuery(ctx, se, query, sqlStatement)
		if err != nil {
			return err
		}
	} else {
		err := processNonBatchableQuery(ctx, se, query, sqlStatement, format)
		if err != nil {
			return err
		}
	}

	if batchEditStats.shouldUpdateBatchModeOutput() {
		updateBatchEditOutput()
	}

	return nil
}

func processNonBatchableQuery(ctx *sql.Context, se *engine.SqlEngine, query string, sqlStatement sqlparser.Statement, format engine.PrintResultFormat) (returnErr error) {
	sqlSch, rowIter, err := processParsedQuery(ctx, query, se, sqlStatement)
	if err != nil {
		return err
	}

	if rowIter != nil {
		err = mergeResultIntoStats(ctx, sqlStatement, rowIter, batchEditStats)
		if err != nil {
			return err
		}

		// Some statement types should print results, even in batch mode.
		switch sqlStatement.(type) {
		case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
			// For any statement that prints out result, print a newline to put the regular output on its own line
			batchEditStats.printNewLineIfNeeded()
			if fileReadProg != nil {
				fileReadProg.printNewLineIfNeeded()
			}
			err = engine.PrettyPrintResults(ctx, format, sqlSch, rowIter)
			if err != nil {
				return err
			}
		default:
			err = rowIter.Close(ctx)
			if err != nil {
				return err
			}
		}
	}

	// And flush again afterwards, to make sure any following insert statements have the latest data
	return flushBatchedEdits(ctx, se)
}

func processBatchableEditQuery(ctx *sql.Context, se *engine.SqlEngine, query string, sqlStatement sqlparser.Statement) (returnErr error) {
	_, rowIter, err := se.Query(ctx, query)
	if err != nil {
		return err
	}

	if rowIter != nil {
		defer func() {
			err := rowIter.Close(ctx)
			if returnErr == nil {
				returnErr = err
			}
		}()
		err = mergeResultIntoStats(ctx, sqlStatement, rowIter, batchEditStats)
		if err != nil {
			return err
		}
	}

	if batchEditStats.shouldFlush() {
		return flushBatchedEdits(ctx, se)
	}

	return nil
}

// canProcessBatchEdit returns whether the given statement can be processed as a batch insert. Only simple inserts
// (inserting a list of values) and deletes can be processed in this way. Other kinds of insert (notably INSERT INTO
// SELECT AS and AUTO_INCREMENT) need a flushed root and can't benefit from batch optimizations.
func canProcessAsBatchEdit(ctx *sql.Context, sqlStatement sqlparser.Statement, se *engine.SqlEngine, query string) (batchMode, error) {
	switch s := sqlStatement.(type) {
	case *sqlparser.Delete:
		foundSubquery, err := checkForSubqueries(query)
		if err != nil {
			return invalidBatchMode, err
		}
		if foundSubquery {
			return invalidBatchMode, nil
		}

		return deleteBatchMode, nil

	case *sqlparser.Insert:
		if _, ok := s.Rows.(sqlparser.Values); !ok {
			return invalidBatchMode, nil
		}
		foundSubquery, err := checkForSubqueries(query)
		if err != nil {
			return invalidBatchMode, err
		}

		if foundSubquery {
			return invalidBatchMode, nil
		}

		// TODO: This check coming first seems to cause problems with ctx.Session. Perhaps in the analyzer.
		hasAutoInc, err := insertsIntoAutoIncrementCol(ctx, se, query)
		if err != nil {
			return invalidBatchMode, err
		}

		if hasAutoInc {
			return invalidBatchMode, nil
		}

		return insertBatchMode, nil

	case *sqlparser.Load:
		return insertBatchMode, nil

	default:
		return invalidBatchMode, nil
	}
}

// checkForSubqueries parses the insert query to check for a subquery.
func checkForSubqueries(query string) (bool, error) {
	p, err := sqlparser.Parse(query)

	if err != nil {
		return false, nil
	}

	return foundSubquery(p), nil
}

func foundSubquery(node sqlparser.SQLNode) bool {
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (keepGoing bool, err error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			has = true
			return false, nil
		}
		return true, nil
	}, node)
	return has
}

// parses the query to check if it inserts into a table with AUTO_INCREMENT
func insertsIntoAutoIncrementCol(ctx *sql.Context, se *engine.SqlEngine, query string) (bool, error) {
	p, err := parse.Parse(ctx, query)
	if err != nil {
		return false, err
	}

	if _, ok := p.(*plan.InsertInto); !ok {
		return false, nil
	}

	a, err := se.Analyze(ctx, p)
	if err != nil {
		return false, err
	}

	isAutoInc := false
	transform.Inspect(a, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.InsertInto:
			_, _, err = transform.NodeExprs(n.Source, func(exp sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if _, ok := exp.(*expression.AutoIncrement); ok {
					isAutoInc = true
				}
				return exp, transform.SameTree, nil
			})
			return false
		default:
			return true
		}
	})

	if err != nil {
		return false, err
	}
	return isAutoInc, nil
}

// updateBatchEditOutput will delete the line it printed before, and print the updated line.
// If there were other functions printed result, it will print update line on a new line.
// This function is used for only batch reads into dolt sql.
func updateBatchEditOutput() {
	if fileReadProg != nil {
		fileReadProg.printNewLineIfNeeded()
	}
	displayStr := fmt.Sprintf("Rows inserted: %d Rows updated: %d Rows deleted: %d",
		batchEditStats.rowsInserted, batchEditStats.rowsUpdated, batchEditStats.rowsDeleted)
	batchEditStats.displayStrLen = cli.DeleteAndPrint(batchEditStats.displayStrLen, displayStr)
	batchEditStats.unprintedEdits = 0
}

// updateFileReadProgressOutput will delete the line it printed before, and print the updated line.
// If there were other functions printed result, it will print update line on a new line.
// This function is used for only file reads for dolt sql when `--file` flag is used.
func updateFileReadProgressOutput() {
	if fileReadProg == nil {
		// this should not happen, but sanity check
		cli.Println("No file is being processed.")
	}
	// batch can be writing to the line, so print new line.
	batchEditStats.printNewLineIfNeeded()
	percent := float64(fileReadProg.bytesRead) / float64(fileReadProg.totalBytes) * 100
	fileReadProg.printed = fileReadProg.bytesRead
	displayStr := fmt.Sprintf("Processed %.1f%% of the file", percent)
	fileReadProg.displayStrLen = cli.DeleteAndPrint(fileReadProg.displayStrLen, displayStr)
}

// Updates the batch insert stats with the results of an INSERT, UPDATE, or DELETE statement.
func mergeResultIntoStats(ctx *sql.Context, statement sqlparser.Statement, rowIter sql.RowIter, s *stats) error {
	switch statement.(type) {
	case *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update, *sqlparser.Load:
		break
	default:
		return nil
	}

	for {
		row, err := rowIter.Next(ctx)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else {
			okResult := row[0].(types.OkResult)
			s.unflushedEdits += int(okResult.RowsAffected)
			s.unprintedEdits += int(okResult.RowsAffected)
			switch statement.(type) {
			case *sqlparser.Insert:
				s.rowsInserted += int(okResult.RowsAffected)
			case *sqlparser.Delete:
				s.rowsDeleted += int(okResult.RowsAffected)
			case *sqlparser.Update:
				s.rowsUpdated += int(okResult.RowsAffected)
			}
		}
	}
}
