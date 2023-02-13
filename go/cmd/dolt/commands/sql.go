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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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
	ap := argparser.NewArgParser()
	ap.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits.")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json, vertical. Defaults to tabular.")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name.")
	ap.SupportsFlag(listSavedFlag, "l", "List all saved queries.")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also `--name`.")
	ap.SupportsFlag(BatchFlag, "b", "Use to enable more efficient batch processing for large SQL import scripts consisting of only INSERT statements. Other statements types are not guaranteed to work in this mode.")
	ap.SupportsString(DataDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory.")
	ap.SupportsString(MultiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory. This is deprecated, you should use `--data-dir` instead")
	ap.SupportsString(CfgDirFlag, "", "directory", "Defines a directory that contains configuration files for dolt. Defaults to `$data-dir/.doltcfg`. Will only be created if there is a change that affect configuration settings.")
	ap.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
	ap.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
	ap.SupportsString(PrivsFilePathFlag, "", "privilege file", "Path to a file to load and store users and grants. Defaults to `$doltcfg-dir/privileges.db`. Will only be created if there is a change to privileges.")
	ap.SupportsString(BranchCtrlPathFlag, "", "branch control file", "Path to a file to load and store branch control permissions. Defaults to `$doltcfg-dir/branch_control.db`. Will only be created if there is a change to branch control permissions.")
	ap.SupportsString(UserFlag, "u", "user", fmt.Sprintf("Defines the local superuser (defaults to `%v`). If the specified user exists, will take on permissions of that user.", DefaultUser))
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
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, help)
	err := validateSqlArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// We need a username and password for many SQL commands, so set defaults if they don't exist
	dEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)

	// Retrieve username and password from command line, if provided
	username := DefaultUser
	if user, ok := apr.GetValue(UserFlag); ok {
		username = user
	}

	mrEnv, verr := getMultiRepoEnv(ctx, apr, dEnv, cmd)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	// need to return cfgdirpath and error
	var cfgDirPath string
	var dataDir string
	if multiDbDir, ok := apr.GetValue(MultiDBDirFlag); ok {
		dataDir = multiDbDir
	} else if dataDirPath, ok := apr.GetValue(DataDirFlag); ok {
		dataDir = dataDirPath
	}

	cfgDir, cfgDirSpecified := apr.GetValue(CfgDirFlag)
	if cfgDirSpecified {
		cfgDirPath = cfgDir
	} else if len(dataDir) != 0 {
		cfgDirPath = filepath.Join(dataDir, DefaultCfgDirName)
	} else {
		// Look in parent directory for doltcfg
		parentDirCfg := filepath.Join("..", DefaultCfgDirName)
		parentExists, isDir := dEnv.FS.Exists(parentDirCfg)
		parentDirExists := parentExists && isDir

		// Look in data directory (which is necessarily current directory) for doltcfg
		currDirCfg := filepath.Join(dataDir, DefaultCfgDirName)
		currExists, isDir := dEnv.FS.Exists(currDirCfg)
		currDirExists := currExists && isDir

		// Error if both current and parent exist
		if currDirExists && parentDirExists {
			p1, err := dEnv.FS.Abs(cfgDirPath)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			p2, err := dEnv.FS.Abs(parentDirCfg)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(ErrMultipleDoltCfgDirs.New(p1, p2)), usage)
		}

		// Assign the one that exists, defaults to current if neither exist
		if parentDirExists {
			cfgDirPath = parentDirCfg
		} else {
			cfgDirPath = currDirCfg
		}
	}

	// If no privilege filepath specified, default to doltcfg directory
	privsFp, hasPrivsFp := apr.GetValue(PrivsFilePathFlag)
	if !hasPrivsFp {
		privsFp, err = dEnv.FS.Abs(filepath.Join(cfgDirPath, DefaultPrivsName))
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	}

	// If no branch control file path is specified, default to doltcfg directory
	branchControlFilePath, hasBCFilePath := apr.GetValue(BranchCtrlPathFlag)
	if !hasBCFilePath {
		branchControlFilePath, err = dEnv.FS.Abs(filepath.Join(cfgDirPath, DefaultBranchCtrlName))
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
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

	format := engine.FormatTabular
	if formatSr, ok := apr.GetValue(FormatFlag); ok {
		var verr errhand.VerboseError
		format, verr = GetResultFormat(formatSr)
		if verr != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(verr), usage)
		}
	}

	config := &engine.SqlEngineConfig{
		InitialDb:          currentDb,
		IsReadOnly:         false,
		DoltCfgDirPath:     cfgDirPath,
		PrivFilePath:       privsFp,
		BranchCtrlFilePath: branchControlFilePath,
		ServerUser:         username,
		ServerHost:         DefaultHost,
		Autocommit:         true,
	}

	if query, queryOK := apr.GetValue(QueryFlag); queryOK {
		return queryMode(ctx, mrEnv, initialRoots, apr, query, format, usage, config)
	} else if savedQueryName, exOk := apr.GetValue(executeFlag); exOk {
		return savedQueryMode(ctx, mrEnv, initialRoots, savedQueryName, format, usage, config)
	} else if apr.Contains(listSavedFlag) {
		return listSavedQueriesMode(ctx, mrEnv, initialRoots, format, usage, config)
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
			verr := execShell(ctx, mrEnv, format, config)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		} else if runInBatchMode {
			verr := execBatch(ctx, continueOnError, mrEnv, input, format, config)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		} else {
			verr := execMultiStatements(ctx, continueOnError, mrEnv, input, format, config)
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
	format engine.PrintResultFormat,
	usage cli.UsagePrinter,
	config *engine.SqlEngineConfig,
) int {
	hasQC, err := initialRoots[config.InitialDb].HasTable(ctx, doltdb.DoltQueryCatalogTableName)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to read from repository.").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	if !hasQC {
		return 0
	}

	query := "SELECT * FROM " + doltdb.DoltQueryCatalogTableName
	return HandleVErrAndExitCode(execQuery(ctx, mrEnv, query, format, config), usage)
}

func savedQueryMode(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	initialRoots map[string]*doltdb.RootValue,
	savedQueryName string,
	format engine.PrintResultFormat,
	usage cli.UsagePrinter,
	config *engine.SqlEngineConfig,
) int {
	sq, err := dtables.RetrieveFromQueryCatalog(ctx, initialRoots[config.InitialDb], savedQueryName)

	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cli.PrintErrf("Executing saved query '%s':\n%s\n", savedQueryName, sq.Query)
	return HandleVErrAndExitCode(execQuery(ctx, mrEnv, sq.Query, format, config), usage)
}

func queryMode(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	initialRoots map[string]*doltdb.RootValue,
	apr *argparser.ArgParseResults,
	query string,
	format engine.PrintResultFormat,
	usage cli.UsagePrinter,
	config *engine.SqlEngineConfig,
) int {

	// query mode has 3 sub modes:
	// If --batch is provided, run with batch optimizations
	// If --save is provided, run a single query and save it
	// Otherwise, attempt to parse and execute multiple queries separated by ';'
	batchMode := apr.Contains(BatchFlag)
	saveName := apr.GetValueOrDefault(saveFlag, "")

	_, continueOnError := apr.GetValue(continueFlag)

	if saveName != "" {
		verr := execQuery(ctx, mrEnv, query, format, config)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}

		if saveName != "" {
			saveMessage := apr.GetValueOrDefault(messageFlag, "")
			newRoot, verr := saveQuery(ctx, initialRoots[config.InitialDb], query, saveName, saveMessage)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}

			verr = UpdateWorkingWithVErr(mrEnv.GetEnv(config.InitialDb), newRoot)
			if verr != nil {
				return HandleVErrAndExitCode(verr, usage)
			}
		}
	} else if batchMode {
		batchInput := strings.NewReader(query)
		verr := execBatch(ctx, continueOnError, mrEnv, batchInput, format, config)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	} else {
		input := strings.NewReader(query)
		verr := execMultiStatements(ctx, continueOnError, mrEnv, input, format, config)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	return 0
}

// getMultiRepoEnv returns an appropriate MultiRepoEnv for this invocation of the command
func getMultiRepoEnv(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, cmd SqlCmd) (*env.MultiRepoEnv, errhand.VerboseError) {
	var err error
	fs := dEnv.FS

	if dataDir, ok := apr.GetValue(MultiDBDirFlag); ok {
		fs, err = fs.WithWorkingDir(dataDir)
	} else if dataDir, ok := apr.GetValue(DataDirFlag); ok {
		fs, err = fs.WithWorkingDir(dataDir)
	}

	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), fs, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return mrEnv, nil
}

func execShell(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	format engine.PrintResultFormat,
	config *engine.SqlEngineConfig,
) errhand.VerboseError {
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		format,
		config,
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	defer se.Close()

	err = runShell(ctx, se, mrEnv, config)
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
	format engine.PrintResultFormat,
	config *engine.SqlEngineConfig,
) errhand.VerboseError {
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		format,
		config,
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	defer se.Close()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// Add specified user as new superuser, if it doesn't already exist
	if user := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, config.ServerHost, false); user == nil {
		se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, config.ServerHost, config.ServerPass)
	}

	// Set client to specified user
	sqlCtx.Session.SetClient(sql.Client{User: config.ServerUser, Address: config.ServerHost, Capabilities: 0})

	// In batch mode, we need to set a couple flags on the session to prevent constant flushes to disk
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()
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
	format engine.PrintResultFormat,
	config *engine.SqlEngineConfig,
) errhand.VerboseError {
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		format,
		config,
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	defer se.Close()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// Add specified user as new superuser, if it doesn't already exist
	if user := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, config.ServerHost, false); user == nil {
		se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, config.ServerHost, config.ServerPass)
	}

	// Set client to specified user
	sqlCtx.Session.SetClient(sql.Client{User: config.ServerUser, Address: config.ServerHost, Capabilities: 0})

	err = runMultiStatementMode(sqlCtx, se, batchInput, continueOnErr)
	return errhand.VerboseErrorFromError(err)
}

func execQuery(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	query string,
	format engine.PrintResultFormat,
	config *engine.SqlEngineConfig,
) errhand.VerboseError {
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		format,
		config,
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	defer se.Close()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// Add specified user as new superuser, if it doesn't already exist
	if user := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, config.ServerHost, false); user == nil {
		se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, config.ServerHost, config.ServerPass)
	}

	// Set client to specified user
	sqlCtx.Session.SetClient(sql.Client{User: config.ServerUser, Address: config.ServerHost, Capabilities: 0})

	sqlSch, rowIter, err := processQuery(sqlCtx, query, se)
	if err != nil {
		return formatQueryError("", err)
	}

	if rowIter != nil {
		err = engine.PrettyPrintResults(sqlCtx, se.GetResultFormat(), sqlSch, rowIter)
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
func saveQuery(ctx context.Context, root *doltdb.RootValue, query string, name string, message string) (*doltdb.RootValue, errhand.VerboseError) {
	_, newRoot, err := dtables.NewQueryCatalogEntryWithNameAsID(ctx, root, name, query, message)
	if err != nil {
		return nil, errhand.BuildDError("Couldn't save query").AddCause(err).Build()
	}

	return newRoot, nil
}

// runMultiStatementMode allows for the execution of more than one query, but it doesn't attempt any batch optimizations
func runMultiStatementMode(ctx *sql.Context, se *engine.SqlEngine, input io.Reader, continueOnErr bool) error {
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
		sqlSch, rowIter, err := processParsedQuery(ctx, query, se, sqlStatement)
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
			err = engine.PrettyPrintResults(ctx, se.GetResultFormat(), sqlSch, rowIter)
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
func runBatchMode(ctx *sql.Context, se *engine.SqlEngine, input io.Reader, continueOnErr bool) error {
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
func runShell(ctx context.Context, se *engine.SqlEngine, mrEnv *env.MultiRepoEnv, config *engine.SqlEngineConfig) error {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return err
	}

	// Add specified user as new superuser, if it doesn't already exist
	if user := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, config.ServerHost, false); user == nil {
		se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, config.ServerHost, config.ServerPass)
	}

	// Add root client
	sqlCtx.Session.SetClient(sql.Client{User: config.ServerUser, Address: config.ServerHost, Capabilities: 0})

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

		query = strings.TrimSuffix(query, shell.LineTerminator())
		resultFormat := se.GetResultFormat()

		// TODO: it would be better to build this into the statement parser rather than special case it here
		for _, terminator := range verticalOutputLineTerminators {
			if strings.HasSuffix(query, terminator) {
				resultFormat = engine.FormatVertical
			}
			query = strings.TrimSuffix(query, terminator)
		}

		var nextPrompt string
		var sqlSch sql.Schema
		var rowIter sql.RowIter

		cont := func() bool {
			subCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			sqlCtx, err = se.NewContext(subCtx)
			if err != nil {
				shell.Println(color.RedString(err.Error()))
				return false
			}

			if sqlSch, rowIter, err = processQuery(sqlCtx, query, se); err != nil {
				verr := formatQueryError("", err)
				shell.Println(verr.Verbose())
			} else if rowIter != nil {
				switch resultFormat {
				case engine.FormatTabular, engine.FormatVertical:
					err = engine.PrettyPrintResultsExtended(sqlCtx, resultFormat, sqlSch, rowIter)
				default:
					err = engine.PrettyPrintResults(sqlCtx, resultFormat, sqlSch, rowIter)
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

// processQuery processes a single query. The Root of the sqlEngine will be updated if necessary.
// Returns the schema and the row iterator for the results, which may be nil, and an error if one occurs.
func processQuery(ctx *sql.Context, query string, se *engine.SqlEngine) (sql.Schema, sql.RowIter, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}
	return processParsedQuery(ctx, query, se, sqlStatement)
}

// processParsedQuery processes a single query with the parsed statement provided. The Root of the sqlEngine
// will be updated if necessary. Returns the schema and the row iterator for the results, which may be nil,
// and an error if one occurs.
func processParsedQuery(ctx *sql.Context, query string, se *engine.SqlEngine, sqlStatement sqlparser.Statement) (sql.Schema, sql.RowIter, error) {
	switch s := sqlStatement.(type) {
	case *sqlparser.Use:
		sch, ri, err := se.Query(ctx, query)
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
		_, ri, err := se.Query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, nil, ri)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	case *sqlparser.DDL:
		_, ri, err := se.Query(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, nil, ri)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	case *sqlparser.DBDDL:
		return se.Dbddl(ctx, s, query)
	case *sqlparser.Load:
		if s.Local {
			return nil, nil, fmt.Errorf("LOCAL supported only in sql-server mode")
		}
		return se.Query(ctx, query)
	default:
		return se.Query(ctx, query)
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
func processBatchQuery(ctx *sql.Context, query string, se *engine.SqlEngine) error {
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

func processNonBatchableQuery(ctx *sql.Context, se *engine.SqlEngine, query string, sqlStatement sqlparser.Statement) (returnErr error) {
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
			err = engine.PrettyPrintResults(ctx, se.GetResultFormat(), sqlSch, rowIter)
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
