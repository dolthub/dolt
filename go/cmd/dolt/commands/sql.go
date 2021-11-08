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
	"regexp"
	"runtime"
	"strings"
	"syscall"

	"github.com/abiosoft/readline"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/ishell"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/dolthub/vitess/go/vt/vterrors"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
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

By default, {{.EmphasisLeft}}-q{{.EmphasisRight}} executes a single statement. To execute multiple SQL statements separated by semicolons, use {{.EmphasisLeft}}-b{{.EmphasisRight}} to enable batch mode. Queries can be saved with {{.EmphasisLeft}}-s{{.EmphasisRight}}. Alternatively {{.EmphasisLeft}}-x{{.EmphasisRight}} can be used to execute a saved query by name. Pipe SQL statements to dolt sql (no {{.EmphasisLeft}}-q{{.EmphasisRight}}) to execute a SQL import or update script. 

By default this command uses the dolt data repository in the current working directory, as well as any dolt databases that are found in the current directory. Any databases created are placed in the current directory as well. Running with {{.EmphasisLeft}}--multi-db-dir <directory>{{.EmphasisRight}} uses each of the subdirectories of the supplied directory (each subdirectory must be a valid dolt data repository) as databases. Subdirectories starting with '.' are ignored.`,

	Synopsis: []string{
		"",
		"< script.sql",
		"[--multi-db-dir {{.LessThan}}directory{{.GreaterThan}}] [-r {{.LessThan}}result format{{.GreaterThan}}]",
		"-q {{.LessThan}}query{{.GreaterThan}} [-r {{.LessThan}}result format{{.GreaterThan}}] [-s {{.LessThan}}name{{.GreaterThan}} -m {{.LessThan}}message{{.GreaterThan}}] [-b]",
		"-q {{.LessThan}}query{{.GreaterThan}} --multi-db-dir {{.LessThan}}directory{{.GreaterThan}} [-r {{.LessThan}}result format{{.GreaterThan}}] [-b]",
		"-x {{.LessThan}}name{{.GreaterThan}}",
		"--list-saved",
	},
}

const (
	QueryFlag        = "query"
	FormatFlag       = "result-format"
	saveFlag         = "save"
	executeFlag      = "execute"
	listSavedFlag    = "list-saved"
	messageFlag      = "message"
	BatchFlag        = "batch"
	disableBatchFlag = "disable-batch"
	multiDBDirFlag   = "multi-db-dir"
	continueFlag     = "continue"
	welcomeMsg       = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`
)

var delimiterRegex = regexp.MustCompile(`(?i)^\s*DELIMITER\s+(\S+)\s*(\s+\S+\s*)?$`)

func init() {
	dsqle.AddDoltSystemVariables()
}

type SqlCmd struct {
	VersionStr string
}

// The SQL shell installs its own signal handlers so that you can cancel a running query without and still run a new one.
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd SqlCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, sqlDocs, ap))
}

func (cmd SqlCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json. Defaults to tabular. ")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name")
	ap.SupportsFlag(listSavedFlag, "l", "Lists all saved queries")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also --name")
	ap.SupportsFlag(BatchFlag, "b", "batch mode, to run more than one query with --query, separated by ';'. Piping input to sql with no arguments also uses batch mode")
	ap.SupportsFlag(disableBatchFlag, "", "When issuing multiple statements, used to override more efficient batch processing to give finer control over session")
	ap.SupportsString(multiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within ")
	ap.SupportsFlag(continueFlag, "c", "continue running queries on an error. Used for batch mode only.")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the multiDBDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd SqlCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
// Unlike other commands, sql doesn't set a new working root directly, as the SQL layer updates the working set as
// necessary when committing work.
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, help)
	err := validateSqlArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	mrEnv, verr := getMultiRepoEnv(ctx, apr, dEnv, cmd)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	initialRoots, err := mrEnv.GetWorkingRoots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var currentDb string
	mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		currentDb = name
		return true, nil
	})

	format := FormatTabular
	if formatSr, ok := apr.GetValue(FormatFlag); ok {
		var verr errhand.VerboseError
		format, verr = GetResultFormat(formatSr)
		if verr != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(verr), usage)
		}
	}

	_, continueOnError := apr.GetValue(continueFlag)

	if query, queryOK := apr.GetValue(QueryFlag); queryOK {
		return queryMode(ctx, mrEnv, initialRoots, apr, query, currentDb, format, usage)
	} else if savedQueryName, exOk := apr.GetValue(executeFlag); exOk {
		return savedQueryMode(ctx, mrEnv, initialRoots, savedQueryName, currentDb, format, usage)
	} else if apr.Contains(listSavedFlag) {
		return listSavedQueriesMode(ctx, mrEnv, initialRoots, currentDb, format, usage)
	} else {
		// Run in either batch mode for piped input, or shell mode for interactive
		runInBatchMode := true
		multiStatementMode := apr.Contains(disableBatchFlag)
		fi, err := os.Stdin.Stat()

		if err != nil {
			if !osutil.IsWindows {
				return HandleVErrAndExitCode(errhand.BuildDError("Couldn't stat STDIN. This is a bug.").Build(), usage)
			}
		} else {
			runInBatchMode = fi.Mode()&os.ModeCharDevice == 0
		}

		if multiStatementMode {
			verr := execMultiStatements(ctx, continueOnError, mrEnv, os.Stdin, format, currentDb)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		} else if runInBatchMode {
			verr := execBatch(ctx, continueOnError, mrEnv, os.Stdin, format, currentDb)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		} else {
			verr := execShell(ctx, mrEnv, format, currentDb)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		}
	}

	return 0
}

func listSavedQueriesMode(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	initialRoots map[string]*doltdb.RootValue,
	currentDb string,
	format resultFormat,
	usage cli.UsagePrinter,
) int {
	hasQC, err := initialRoots[currentDb].HasTable(ctx, doltdb.DoltQueryCatalogTableName)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to read from repository.").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	if !hasQC {
		return 0
	}

	query := "SELECT * FROM " + doltdb.DoltQueryCatalogTableName
	return HandleVErrAndExitCode(execQuery(ctx, mrEnv, query, format, currentDb), usage)
}

func savedQueryMode(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	initialRoots map[string]*doltdb.RootValue,
	savedQueryName string,
	currentDb string,
	format resultFormat,
	usage cli.UsagePrinter,
) int {
	sq, err := dtables.RetrieveFromQueryCatalog(ctx, initialRoots[currentDb], savedQueryName)

	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cli.PrintErrf("Executing saved query '%s':\n%s\n", savedQueryName, sq.Query)
	return HandleVErrAndExitCode(execQuery(ctx, mrEnv, sq.Query, format, currentDb), usage)
}

func queryMode(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	initialRoots map[string]*doltdb.RootValue,
	apr *argparser.ArgParseResults,
	query string,
	currentDb string,
	format resultFormat,
	usage cli.UsagePrinter,
) int {
	batchMode := apr.Contains(BatchFlag)
	multiStatementMode := apr.Contains(disableBatchFlag)
	_, continueOnError := apr.GetValue(continueFlag)

	if multiStatementMode {
		batchInput := strings.NewReader(query)
		verr := execMultiStatements(ctx, continueOnError, mrEnv, batchInput, format, currentDb)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	} else if batchMode {
		batchInput := strings.NewReader(query)
		verr := execBatch(ctx, continueOnError, mrEnv, batchInput, format, currentDb)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	} else {
		verr := execQuery(ctx, mrEnv, query, format, currentDb)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}

		saveName := apr.GetValueOrDefault(saveFlag, "")

		if saveName != "" {
			saveMessage := apr.GetValueOrDefault(messageFlag, "")
			newRoot, verr := saveQuery(ctx, initialRoots[currentDb], query, saveName, saveMessage)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}

			verr = UpdateWorkingWithVErr(mrEnv.GetEnv(currentDb), newRoot)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		}
	}

	return 0
}

// getMultiRepoEnv returns an appropriate MultiRepoEnv for this invocation of the command
func getMultiRepoEnv(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, cmd SqlCmd) (*env.MultiRepoEnv, errhand.VerboseError) {
	var mrEnv *env.MultiRepoEnv
	var err error
	multiDir, multiDbMode := apr.GetValue(multiDBDirFlag)
	if multiDbMode {
		mrEnv, err = env.LoadMultiEnvFromDir(ctx, env.GetCurrentUserHomeDir, dEnv.FS, multiDir, cmd.VersionStr)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	} else {
		if !cli.CheckEnvIsValid(dEnv) {
			return nil, errhand.BuildDError("Invalid working directory").Build()
		}

		mrEnv, err = env.DoltEnvAsMultiEnv(ctx, dEnv)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	return mrEnv, nil
}

func execShell(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	format resultFormat,
	initialDb string,
) errhand.VerboseError {
	dbs, err := CollectDBs(ctx, mrEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	se, err := newSqlEngine(ctx, mrEnv.Config(), mrEnv.FileSystem(), format, initialDb, dbs...)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = runShell(ctx, se, mrEnv)
	if err != nil {
		return errhand.BuildDError(err.Error()).Build()
	}
	return nil
}

func execBatch(
	ctx context.Context,
	continueOnErr bool,
	mrEnv *env.MultiRepoEnv,
	batchInput io.Reader,
	format resultFormat,
	initialDb string,
) errhand.VerboseError {
	dbs, err := CollectDBs(ctx, mrEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	se, err := newSqlEngine(ctx, mrEnv.Config(), mrEnv.FileSystem(), format, initialDb, dbs...)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sqlCtx, err := se.newContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// In batch mode, we need to set a couple flags on the session to prevent constant flushes to disk
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()
	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = runBatchMode(sqlCtx, se, batchInput, continueOnErr)
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

func execMultiStatements(
	ctx context.Context,
	continueOnErr bool,
	mrEnv *env.MultiRepoEnv,
	batchInput io.Reader,
	format resultFormat,
	initialDb string,
) errhand.VerboseError {
	dbs, err := CollectDBs(ctx, mrEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	se, err := newSqlEngine(ctx, mrEnv.Config(), mrEnv.FileSystem(), format, initialDb, dbs...)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sqlCtx, err := se.newContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = runMultiStatementMode(sqlCtx, se, batchInput, continueOnErr)
	if err != nil {
		// If we encounter an error, attempt to flush what we have so far to disk before exiting
		return errhand.BuildDError("Error processing batch").AddCause(err).Build()
	}

	return errhand.VerboseErrorFromError(err)
}

func newDatabase(name string, dEnv *env.DoltEnv) dsqle.Database {
	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}
	return dsqle.NewDatabase(name, dEnv.DbData(), opts)
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (dsqle.ReadReplicaDatabase, error) {
	var skipErrors bool
	if _, val, ok := sql.SystemVariables.GetGlobal(dsqle.SkipReplicationErrorsKey); !ok {
		return dsqle.ReadReplicaDatabase{}, sql.ErrUnknownSystemVariable.New(dsqle.SkipReplicationErrorsKey)
	} else if val == int8(1) {
		skipErrors = true
	}

	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}

	db := dsqle.NewDatabase(name, dEnv.DbData(), opts)

	rrd, err := dsqle.NewReadReplicaDatabase(ctx, db, remoteName, dEnv.RepoStateReader(), dEnv.TempTableFilesDir())
	if err != nil {
		err = fmt.Errorf("%w from remote '%s'; %s", dsqle.ErrFailedToLoadReplicaDB, remoteName, err.Error())
		if !skipErrors {
			return dsqle.ReadReplicaDatabase{}, err
		}
		cli.Println(err)
		return dsqle.ReadReplicaDatabase{Database: db}, nil
	}
	return rrd, nil
}

func execQuery(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	query string,
	format resultFormat,
	initialDb string,
) errhand.VerboseError {
	dbs, err := CollectDBs(ctx, mrEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	se, err := newSqlEngine(ctx, mrEnv.Config(), mrEnv.FileSystem(), format, initialDb, dbs...)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sqlCtx, err := se.newContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sqlSch, rowIter, err := processQuery(sqlCtx, query, se)
	if err != nil {
		return formatQueryError("", err)
	}

	if rowIter != nil {
		err = PrettyPrintResults(sqlCtx, se.resultFormat, sqlSch, rowIter, HasTopLevelOrderByClause(query))
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}

func getPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.PushOnWriteHook, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(dsqle.ReplicateToRemoteKey)
	if !ok {
		return nil, sql.ErrUnknownSystemVariable.New(dsqle.SkipReplicationErrorsKey)
	} else if val == "" {
		return nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes[remoteName]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		return nil, err
	}

	pushHook := doltdb.NewPushOnWriteHook(ddb, dEnv.TempTableFilesDir())
	return pushHook, nil
}

// GetCommitHooks creates a list of hooks to execute on database commit. If doltdb.SkipReplicationErrorsKey is set,
// replace misconfigured hooks with doltdb.LogHook instances that prints a warning when trying to execute.
func GetCommitHooks(ctx context.Context, dEnv *env.DoltEnv) ([]datas.CommitHook, error) {
	postCommitHooks := make([]datas.CommitHook, 0)
	var skipErrors bool
	if _, val, ok := sql.SystemVariables.GetGlobal(dsqle.SkipReplicationErrorsKey); !ok {
		return nil, sql.ErrUnknownSystemVariable.New(dsqle.SkipReplicationErrorsKey)
	} else if val == int8(1) {
		skipErrors = true
	}

	if hook, err := getPushOnWriteHook(ctx, dEnv); err != nil {
		err = fmt.Errorf("failure loading hook; %w", err)
		if skipErrors {
			postCommitHooks = append(postCommitHooks, doltdb.NewLogHook([]byte(err.Error()+"\n")))
		} else {
			return nil, err
		}
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	return postCommitHooks, nil
}

// CollectDBs takes a MultiRepoEnv and creates Database objects from each environment and returns a slice of these
// objects.
func CollectDBs(ctx context.Context, mrEnv *env.MultiRepoEnv) ([]dsqle.SqlDatabase, error) {
	var dbs []dsqle.SqlDatabase
	var db dsqle.SqlDatabase
	err := mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		postCommitHooks, err := GetCommitHooks(ctx, dEnv)
		if err != nil {
			return true, err
		}
		dEnv.DoltDB.SetCommitHooks(ctx, postCommitHooks)

		db = newDatabase(name, dEnv)

		if _, remote, ok := sql.SystemVariables.GetGlobal(dsqle.ReadReplicaRemoteKey); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return true, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			db, err = newReplicaDatabase(ctx, name, remoteName, dEnv)
			if err != nil {
				return true, err
			}
		}

		dbs = append(dbs, db)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return dbs, nil
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

func GetResultFormat(format string) (resultFormat, errhand.VerboseError) {
	switch strings.ToLower(format) {
	case "tabular":
		return FormatTabular, nil
	case "csv":
		return FormatCsv, nil
	case "json":
		return FormatJson, nil
	case "null":
		return FormatNull, nil
	default:
		return FormatTabular, errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular, csv, json").Build()
	}
}

func validateSqlArgs(apr *argparser.ArgParseResults) error {
	_, query := apr.GetValue(QueryFlag)
	_, save := apr.GetValue(saveFlag)
	_, msg := apr.GetValue(messageFlag)
	_, batch := apr.GetValue(BatchFlag)
	_, list := apr.GetValue(listSavedFlag)
	_, execute := apr.GetValue(executeFlag)
	_, multiDB := apr.GetValue(multiDBDirFlag)

	if len(apr.Args) > 0 && !query {
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
		} else if multiDB {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --multi-db-dir").Build()
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
		} else if multiDB {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --multi-db-dir").Build()
		}
	}

	if save && multiDB {
		return errhand.BuildDError("Invalid Argument: --multi-db-dir queries cannot be saved").Build()
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
func saveQuery(ctx context.Context, root *doltdb.RootValue, query string, name string, message string) (*doltdb.RootValue, errhand.VerboseError) {
	_, newRoot, err := dtables.NewQueryCatalogEntryWithNameAsID(ctx, root, name, query, message)
	if err != nil {
		return nil, errhand.BuildDError("Couldn't save query").AddCause(err).Build()
	}

	return newRoot, nil
}

// runMultiStatementMode alows for the execution of more than one query, but it doesn't attempt any batch optimizations
func runMultiStatementMode(ctx *sql.Context, se *sqlEngine, input io.Reader, continueOnErr bool) error {
	scanner := NewSqlStatementScanner(input)

	var query string
	for scanner.Scan() {
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		shouldProcessQuery := true
		if matches := delimiterRegex.FindStringSubmatch(query); len(matches) == 3 {
			// If we don't match from anything, then we just pass to the SQL engine and let it complain.
			scanner.Delimiter = matches[1]
			shouldProcessQuery = false
		}
		if shouldProcessQuery {
			sqlSch, rowIter, err := processQuery(ctx, query, se)
			if err != nil {
				verr := formatQueryError(fmt.Sprintf("error on line %d for query %s", scanner.statementStartLine, query), err)
				cli.PrintErrln(verr.Verbose())
				// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
				if !continueOnErr {
					return err
				}
			}

			if rowIter != nil {
				err = PrettyPrintResults(ctx, se.resultFormat, sqlSch, rowIter, HasTopLevelOrderByClause(query))
				if err != nil {
					return errhand.VerboseErrorFromError(err)
				}
				if err != nil {
					verr := formatQueryError(fmt.Sprintf("error on line %d for query %s", scanner.statementStartLine, query), err)
					cli.PrintErrln(verr.Verbose())
					// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
					if !continueOnErr {
						return err
					}
				}
			}
		}
		query = ""
	}

	cli.Println() // need a newline after all statements are executed
	if err := scanner.Err(); err != nil {
		cli.Println(err.Error())
	}

	return nil
}

// runBatchMode processes queries until EOF. The Root of the sqlEngine may be updated.
func runBatchMode(ctx *sql.Context, se *sqlEngine, input io.Reader, continueOnErr bool) error {
	scanner := NewSqlStatementScanner(input)

	var query string
	for scanner.Scan() {
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}
		shouldProcessQuery := true
		if matches := delimiterRegex.FindStringSubmatch(query); len(matches) == 3 {
			// If we don't match from anything, then we just pass to the SQL engine and let it complain.
			scanner.Delimiter = matches[1]
			shouldProcessQuery = false
		}
		if shouldProcessQuery {
			if err := processBatchQuery(ctx, query, se); err != nil {
				// TODO: this line number will not be accurate for errors that occur when flushing a batch of inserts (as opposed
				//  to processing the query)
				verr := formatQueryError(fmt.Sprintf("error on line %d for query %s", scanner.statementStartLine, query), err)
				cli.PrintErrln(verr.Verbose())
				// If continueOnErr is set keep executing the remaining queries but print the error out anyway.
				if !continueOnErr {
					return err
				}
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

// runShell starts a SQL shell. Returns when the user exits the shell. The Root of the sqlEngine may
// be updated by any queries which were processed.
func runShell(ctx context.Context, se *sqlEngine, mrEnv *env.MultiRepoEnv) error {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)

	sqlCtx, err := se.newContext(ctx)
	if err != nil {
		return err
	}

	currentDB := sqlCtx.Session.GetCurrentDatabase()
	currEnv := mrEnv.GetEnv(currentDB)

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
	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string{
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator: ";",
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt(initialMultilinePrompt)
	// TODO: update completer on create / drop / alter statements
	completer, err := newCompleter(ctx, currEnv)
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

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		if len(strings.TrimSpace(query)) == 0 {
			return
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

		//TODO: Handle comments and enforce the current line terminator
		if matches := delimiterRegex.FindStringSubmatch(query); len(matches) == 3 {
			// If we don't match from anything, then we just pass to the SQL engine and let it complain.
			shell.SetLineTerminator(matches[1])
			return
		}

		var nextPrompt string
		var sqlSch sql.Schema
		var rowIter sql.RowIter

		// The SQL parser does not understand any other terminator besides semicolon, so we remove it.
		if shell.LineTerminator() != ";" && strings.HasSuffix(query, shell.LineTerminator()) {
			query = query[:len(query)-len(shell.LineTerminator())]
		}

		cont := func() bool {
			subCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			sqlCtx, err = se.newContext(subCtx)
			if err != nil {
				shell.Println(color.RedString(err.Error()))
				return false
			}

			if sqlSch, rowIter, err = processQuery(sqlCtx, query, se); err != nil {
				verr := formatQueryError("", err)
				shell.Println(verr.Verbose())
			} else if rowIter != nil {
				err = PrettyPrintResults(sqlCtx, se.resultFormat, sqlSch, rowIter, HasTopLevelOrderByClause(query))
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
func newCompleter(ctx context.Context, dEnv *env.DoltEnv) (*sqlCompleter, error) {
	// TODO: change the sqlCompleter based on the current database and change it when the database changes.
	if dEnv == nil {
		return &sqlCompleter{}, nil
	}

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

// Processes a single query. The Root of the sqlEngine will be updated if necessary.
// Returns the schema and the row iterator for the results, which may be nil, and an error if one occurs.
func processQuery(ctx *sql.Context, query string, se *sqlEngine) (sql.Schema, sql.RowIter, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Use:
		sch, ri, err := se.query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, ri)
		if err != nil {
			return nil, nil, err
		}
		cli.Println("Database changed")
		return sch, nil, err
	case *sqlparser.MultiAlterDDL, *sqlparser.Set, *sqlparser.Commit:
		_, ri, err := se.query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, ri)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
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
	case *sqlparser.DBDDL:
		return se.dbddl(ctx, s, query)
	case *sqlparser.Load:
		if s.Local {
			return nil, nil, fmt.Errorf("LOCAL supported only in sql-server mode")
		}
		return se.query(ctx, query)
	default:
		return se.query(ctx, query)
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

func flushBatchedEdits(ctx *sql.Context, se *sqlEngine) error {
	err := se.iterDBs(func(_ string, db dsqle.SqlDatabase) (bool, error) {
		_, rowIter, err := se.engine.Query(ctx, "COMMIT;")
		if err != nil {
			return false, err
		}

		err = rowIter.Close(ctx)
		if err != nil {
			return false, err
		}

		err = db.Flush(ctx)
		if err != nil {
			return false, err
		}

		return false, nil
	})

	batchEditStats.unflushedEdits = 0

	return err
}

// Processes a single query in batch mode. The Root of the sqlEngine may or may not be changed.
func processBatchQuery(ctx *sql.Context, query string, se *sqlEngine) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	currentBatchMode := invalidBatchMode
	if v, err := ctx.GetSessionVariable(ctx, dsqle.CurrentBatchModeKey); err == nil {
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

	err = ctx.SetSessionVariable(ctx, dsqle.CurrentBatchModeKey, int64(newBatchMode))
	if err != nil {
		return err
	}

	if newBatchMode != invalidBatchMode {
		err = processBatchableEditQuery(ctx, se, query, sqlStatement)
		if err != nil {
			return err
		}
	} else {
		err := processNonBatchableQuery(ctx, se, query, sqlStatement)
		if err != nil {
			return err
		}
	}

	if batchEditStats.shouldUpdateBatchModeOutput() {
		updateBatchEditOutput()
	}

	return nil
}

func processNonBatchableQuery(ctx *sql.Context, se *sqlEngine, query string, sqlStatement sqlparser.Statement) (returnErr error) {
	sqlSch, rowIter, err := processQuery(ctx, query, se)
	if err != nil {
		return err
	}

	if rowIter != nil {
		err = mergeResultIntoStats(sqlStatement, rowIter, batchEditStats)
		if err != nil {
			return err
		}

		// Some statement types should print results, even in batch mode.
		switch sqlStatement.(type) {
		case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.Union:
			if displayStrLen > 0 {
				// If we've been printing in batch mode, print a newline to put the regular output on its own line
				cli.Print("\n")
				displayStrLen = 0
			}
			err = PrettyPrintResults(ctx, se.resultFormat, sqlSch, rowIter, HasTopLevelOrderByClause(query))
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

func processBatchableEditQuery(ctx *sql.Context, se *sqlEngine, query string, sqlStatement sqlparser.Statement) (returnErr error) {
	_, rowIter, err := se.query(ctx, query)
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
		err = mergeResultIntoStats(sqlStatement, rowIter, batchEditStats)
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
func canProcessAsBatchEdit(ctx *sql.Context, sqlStatement sqlparser.Statement, se *sqlEngine, query string) (batchMode, error) {
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

func HasTopLevelOrderByClause(query string) bool {
	st, _ := sqlparser.Parse(query)

	switch s := st.(type) {
	case *sqlparser.Select:
		return s.OrderBy != nil
	case *sqlparser.Union:
		return s.OrderBy != nil
	default:
		return false
	}
}

// parses the query to check if it inserts into a table with AUTO_INCREMENT
func insertsIntoAutoIncrementCol(ctx *sql.Context, se *sqlEngine, query string) (bool, error) {
	p, err := parse.Parse(ctx, query)
	if err != nil {
		return false, err
	}

	if _, ok := p.(*plan.InsertInto); !ok {
		return false, nil
	}

	a, err := se.engine.Analyzer.Analyze(ctx, p, nil)
	if err != nil {
		return false, err
	}

	isAutoInc := false
	plan.Inspect(a, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.InsertInto:
			_, err = plan.TransformExpressionsUp(n.Source, func(exp sql.Expression) (sql.Expression, error) {
				if _, ok := exp.(*expression.AutoIncrement); ok {
					isAutoInc = true
				}
				return exp, nil
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

func updateBatchEditOutput() {
	displayStr := fmt.Sprintf("Rows inserted: %d Rows updated: %d Rows deleted: %d",
		batchEditStats.rowsInserted, batchEditStats.rowsUpdated, batchEditStats.rowsDeleted)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
	batchEditStats.unprintedEdits = 0
}

// Updates the batch insert stats with the results of an INSERT, UPDATE, or DELETE statement.
func mergeResultIntoStats(statement sqlparser.Statement, rowIter sql.RowIter, s *stats) error {
	switch statement.(type) {
	case *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update, *sqlparser.Load:
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
			okResult := row[0].(sql.OkResult)
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

type sqlEngine struct {
	dbs            map[string]dsqle.SqlDatabase
	sess           *dsess.DoltSession
	contextFactory func(ctx context.Context) (*sql.Context, error)
	engine         *sqle.Engine
	resultFormat   resultFormat
}

var ErrDBNotFoundKind = errors.NewKind("database '%s' not found")

// sqlEngine packages up the context necessary to run sql queries against sqle.
func newSqlEngine(
	ctx context.Context,
	config config.ReadWriteConfig,
	fs filesys.Filesys,
	format resultFormat,
	initialDb string,
	dbs ...dsqle.SqlDatabase,
) (*sqlEngine, error) {
	au := new(auth.None)

	parallelism := runtime.GOMAXPROCS(0)

	infoDB := information_schema.NewInformationSchemaDatabase()
	all := append(dsqleDBsAsSqlDBs(dbs), infoDB)

	pro := dsqle.NewDoltDatabaseProvider(config, fs, all...)

	engine := sqle.New(analyzer.NewBuilder(pro).WithParallelism(parallelism).Build(), &sqle.Config{Auth: au})

	if dbg, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG"); ok && strings.ToLower(dbg) == "true" {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG_VERBOSE"); ok && strings.ToLower(verbose) == "true" {
			engine.Analyzer.Verbose = true
		}
	}

	nameToDB := make(map[string]dsqle.SqlDatabase)
	var dbStates []dsess.InitialDbState
	for _, db := range dbs {
		nameToDB[db.Name()] = db

		dbState, err := dsqle.GetInitialDBState(ctx, db)
		if err != nil {
			return nil, err
		}

		dbStates = append(dbStates, dbState)
	}

	// TODO: not having user and email for this command should probably be an error or warning, it disables certain functionality
	sess, err := dsess.NewDoltSession(sql.NewEmptyContext(), sql.NewBaseSession(), pro, config, dbStates...)
	if err != nil {
		return nil, err
	}

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	// TODO: this should just be the session default like it is with MySQL
	err = sess.SetSessionVariable(sql.NewContext(ctx), sql.AutoCommitSessionVar, true)
	if err != nil {
		return nil, err
	}

	return &sqlEngine{
		dbs:            nameToDB,
		sess:           sess,
		contextFactory: newSqlContext(sess, initialDb),
		engine:         engine,
		resultFormat:   format,
	}, nil
}

func newSqlContext(sess *dsess.DoltSession, initialDb string) func(ctx context.Context) (*sql.Context, error) {
	return func(ctx context.Context) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx,
			sql.WithSession(sess),
			sql.WithTracer(tracing.Tracer(ctx)))

		// If the session was already updated with a database then continue using it in the new session. Otherwise
		// use the initial one.
		if sessionDB := sess.GetCurrentDatabase(); sessionDB != "" {
			sqlCtx.SetCurrentDatabase(sessionDB)
		} else {
			sqlCtx.SetCurrentDatabase(initialDb)
		}

		return sqlCtx, nil
	}
}

func dbsAsDSQLDBs(dbs []sql.Database) []dsqle.Database {
	dsqlDBs := make([]dsqle.Database, 0, len(dbs))

	for _, db := range dbs {
		dsqlDB, ok := db.(dsqle.Database)

		if ok {
			dsqlDBs = append(dsqlDBs, dsqlDB)
		}
	}

	return dsqlDBs
}

func dsqleDBsAsSqlDBs(dbs []dsqle.SqlDatabase) []sql.Database {
	sqlDbs := make([]sql.Database, 0, len(dbs))
	for _, db := range dbs {
		sqlDbs = append(sqlDbs, db)
	}
	return sqlDbs
}

func getDbState(ctx context.Context, db dsqle.Database, mrEnv env.MultiRepoEnv) (dsess.InitialDbState, error) {
	var dEnv *env.DoltEnv
	mrEnv.Iter(func(name string, de *env.DoltEnv) (stop bool, err error) {
		if name == db.Name() {
			dEnv = de
			return true, nil
		}
		return false, nil
	})

	if dEnv == nil {
		return dsess.InitialDbState{}, fmt.Errorf("Couldn't find environment for database %s", db.Name())
	}

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}, nil
}

func (se *sqlEngine) iterDBs(cb func(name string, db dsqle.SqlDatabase) (stop bool, err error)) error {
	for name, db := range se.dbs {
		stop, err := cb(name, db)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

func (se *sqlEngine) getRoots(sqlCtx *sql.Context) (map[string]*doltdb.RootValue, error) {
	newRoots := make(map[string]*doltdb.RootValue)
	for name, db := range se.dbs {
		var err error
		newRoots[name], err = db.GetRoot(sqlCtx)

		if err != nil {
			return nil, err
		}
	}

	return newRoots, nil
}

func (se *sqlEngine) newContext(ctx context.Context) (*sql.Context, error) {
	return se.contextFactory(ctx)
}

// Execute a SQL statement and return values for printing.
func (se *sqlEngine) query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return se.engine.Query(ctx, query)
}

func (se *sqlEngine) dbddl(ctx *sql.Context, dbddl *sqlparser.DBDDL, query string) (sql.Schema, sql.RowIter, error) {
	action := strings.ToLower(dbddl.Action)
	var rowIter sql.RowIter = nil
	var err error = nil

	if action != sqlparser.CreateStr && action != sqlparser.DropStr {
		return nil, nil, fmt.Errorf("Unhandled DBDDL action %v in query %v", action, query)
	}

	if action == sqlparser.DropStr {
		// Should not be allowed to delete repo name and information schema
		if dbddl.DBName == information_schema.InformationSchemaDatabaseName {
			return nil, nil, fmt.Errorf("DROP DATABASE isn't supported for database %s", information_schema.InformationSchemaDatabaseName)
		}
	}

	sch, rowIter, err := se.query(ctx, query)

	if rowIter != nil {
		err = rowIter.Close(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}

	return sch, nil, nil
}

// Executes a SQL DDL statement (create, update, etc.). Updates the new root value in
// the sqlEngine if necessary.
func (se *sqlEngine) ddl(ctx *sql.Context, ddl *sqlparser.DDL, query string) (sql.Schema, sql.RowIter, error) {
	switch ddl.Action {
	case sqlparser.CreateStr, sqlparser.DropStr, sqlparser.AlterStr, sqlparser.RenameStr, sqlparser.TruncateStr:
		_, ri, err := se.query(ctx, query)
		if err == nil {
			for _, err = ri.Next(); err == nil; _, err = ri.Next() {
			}
			if err == io.EOF {
				err = ri.Close(ctx)
			} else {
				closeErr := ri.Close(ctx)
				if closeErr != nil {
					err = errhand.BuildDError("error while executing ddl").AddCause(err).AddCause(closeErr).Build()
				}
			}
		}
		return nil, nil, err
	default:
		return nil, nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}
