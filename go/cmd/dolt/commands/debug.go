package commands

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/pkg/profile"
	textunicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"io"
	"os"
	"time"
)

type DebugCmd struct {
	VersionStr string
}

// The SQL shell installs its own signal handlers so that you can cancel a running query without ending the entire
// process
func (cmd DebugCmd) InstallsSignalHandlers() bool {
	return true
}

var _ cli.SignalCommand = DebugCmd{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DebugCmd) Name() string {
	return "debug"
}

// Description returns a description of the command
func (cmd DebugCmd) Description() string {
	return "Run a query in profile and trace mode"
}

func (cmd DebugCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlDocs, ap)
}

func (cmd DebugCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits.")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json, vertical, and parquet. Defaults to tabular.")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name.")
	ap.SupportsFlag(listSavedFlag, "l", "List all saved queries.")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also `--name`.")
	ap.SupportsFlag(BatchFlag, "b", "Use to enable more efficient batch processing for large SQL import scripts. This mode is no longer supported and this flag is a no-op. To speed up your SQL imports, use either LOAD DATA, or structure your SQL import script to insert many rows per statement.")
	ap.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
	ap.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
	return ap
}

// EventType returns the type of the event to log
func (cmd DebugCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the DataDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd DebugCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
// Unlike other commands, sql doesn't set a new working root directly, as the SQL layer updates the working set as
// necessary when committing work.
func (cmd DebugCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlDocs, ap))
	apr, err := ap.Parse(args)
	if err != nil {
		if errors.Is(err, argparser.ErrHelp) {
			help()
			return 0
		}
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

	// restrict LOAD FILE invocations to current directory
	wd, err := os.Getwd()
	if err != nil {
		wd = "/dev/null"
	}
	err = sql.SystemVariables.AssignValues(map[string]interface{}{
		"secure_file_priv": wd,
	})
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	sqlEng, ok := queryist.(*engine.SqlEngine)
	if !ok {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("cannot run debug mode on a pre-existing server").Build(), usage)
	}

	_, continueOnError := apr.GetValue(continueFlag)

	var input io.Reader = os.Stdin
	if query, queryOK := apr.GetValue(QueryFlag); queryOK {
		input = bytes.NewBuffer([]byte(query))
	} else if fileInput, ok := apr.GetValue(fileInputFlag); ok {
		input, err = os.OpenFile(fileInput, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't open file %s", fileInput).Build(), usage)
		}
		info, err := os.Stat(fileInput)
		if err != nil {
			return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't get file size %s", fileInput).Build(), usage)
		}

		input = transform.NewReader(input, textunicode.BOMOverride(transform.Nop))

		// initialize fileReadProg global variable if there is a file to process queries from
		fileReadProg = &fileReadProgress{bytesRead: 0, totalBytes: info.Size(), printed: 0, displayStrLen: 0}
		defer fileReadProg.close()
	}

	// create a temp directory
	tempDir, err := os.MkdirTemp("", "dolt-debug-*")
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't create tempdir %w", err).Build(), usage)
	}
	queryFile, err := os.CreateTemp(tempDir, "input.sql")
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't create tempfile %w", err).Build(), usage)
	}
	defer queryFile.Close()

	input = bufio.NewReader(transform.NewReader(input, textunicode.BOMOverride(transform.Nop)))
	_, err = io.Copy(queryFile, input)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't copy input sql %w", err).Build(), usage)
	}
	_, err = queryFile.Seek(0, 0)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("seek input sql %w", err).Build(), usage)
	}

	err = debugAnalyze(sqlCtx, tempDir, sqlEng, queryFile)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.VerboseErrorFromError(err), usage)
	}

	func() {
		defer profile.Start(profile.ProfilePath(tempDir), profile.CPUProfile).Stop()
		cli.Println("starting cpu profile...")
		for {
			select {
			case <-time.Tick(5 * time.Second):
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	func() {
		defer profile.Start(profile.ProfilePath(tempDir), profile.MemProfile).Stop()
		cli.Println("starting mem profile...")
		for {
			select {
			case <-time.Tick(5 * time.Second):
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	input = bufio.NewReader(transform.NewReader(queryFile, textunicode.BOMOverride(transform.Nop)))
	func() {
		defer profile.Start(profile.ProfilePath(tempDir), profile.TraceProfile).Stop()
		cli.Println("starting trace profile...")
		for {
			select {
			case <-time.Tick(5 * time.Second):
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	cli.Printf("debug results in: %s", tempDir)
	return 0
}

func debugAnalyze(ctx *sql.Context, tempDir string, sqlEng *engine.SqlEngine, sqlFile *os.File) error {
	_, err := sqlFile.Seek(0, 0)
	if err != nil {
		return err
	}
	defer func() {
		sqlFile.Seek(0, 0)
	}()

	eng := sqlEng.GetUnderlyingEngine()
	eng.Analyzer.Debug = true
	defer func() {
		eng.Analyzer.Debug = false
	}()
	analysisFile, err := os.CreateTemp(tempDir, "analysis.txt")
	if err != nil {
		return err
	}
	defer analysisFile.Close()

	planFile, err := os.CreateTemp(tempDir, "plan.txt")
	if err != nil {
		return err
	}
	defer planFile.Close()
	planBuf := bufio.NewWriter(planFile)

	origStdout := os.Stdout
	os.Stdout = analysisFile
	defer func() {
		os.Stdout = origStdout
	}()

	scanner := NewStreamScanner(sqlFile)
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

		cli.Println("analyzing: " + query[:min(len(query), 60)])

		planned, err := eng.AnalyzeQuery(ctx, query)
		if err != nil {
			return err
		}

		fmt.Fprintf(planBuf, "query: %s\n", query)
		fmt.Fprintf(planBuf, "plan: \n%s", planned.String())
		fmt.Fprintf(planBuf, "debug plan: \n%s", sql.DebugString(planned))
	}

	return planBuf.Flush()
}

func execDebugMode(ctx *sql.Context, qryist cli.Queryist, queryFile *os.File, continueOnErr bool, format engine.PrintResultFormat) error {
	_, err := queryFile.Seek(0, 0)
	if err != nil {
		return err
	}
	defer func() {
		queryFile.Seek(0, 0)
	}()
	input := bufio.NewReader(transform.NewReader(queryFile, textunicode.BOMOverride(transform.Nop)))

	return execBatchMode(ctx, qryist, input, continueOnErr, format)
}
