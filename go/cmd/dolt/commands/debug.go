// Copyright 2025 Dolthub, Inc.
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
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/pkg/profile"
	"github.com/sirupsen/logrus"
	textunicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

type DebugCmd struct {
	VersionStr string
}

const defaultDebugTime = 10

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
	ap.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
	ap.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
	ap.SupportsInt(timeFlag, "t", "benchmark time", "Execute for at least time seconds.")
	ap.SupportsString(outputFlag, "o", "output dir", "Result directory (Defaults to temporary director)")
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
	outDir, outputDirSpecified := apr.GetValue(outputFlag)
	if !outputDirSpecified {
		outDir, err = os.MkdirTemp("", "dolt-debug-*")
		if err != nil {
			return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't create tempdir %s", err.Error()).Build(), usage)
		}
	} else {
		err := os.Mkdir(outDir, os.ModePerm)
		if err != nil {
			return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("failed to create output directory %s", err.Error()).Build(), usage)
		}
	}

	if outputDirSpecified {
		defer func() {
			file, err := os.Create(outDir + ".tar.gz")
			if err != nil {
				cli.Println("failed to create output file " + err.Error())
				return
			}
			defer file.Close()

			gzipWriter := gzip.NewWriter(file)
			defer gzipWriter.Close()

			tarWriter := tar.NewWriter(gzipWriter)
			defer tarWriter.Close()

			if err := filepath.WalkDir(outDir, func(path string, d fs.DirEntry, err error) error {
				if d.IsDir() {
					return nil
				}
				return addFileToTar(tarWriter, path)
			}); err != nil {
				cli.Println("failed to create output tar " + err.Error())
			}

			cli.Println("zipped results in: " + outDir + ".tar.gz")
		}()
	}

	runTime := apr.GetIntOrDefault(timeFlag, defaultDebugTime)

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

	queryFile, err := os.Create(filepath.Join(outDir, "input.sql"))
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't create file %s", err.Error()).Build(), usage)
	}
	defer queryFile.Close()

	input = bufio.NewReader(transform.NewReader(input, textunicode.BOMOverride(transform.Nop)))
	_, err = io.Copy(queryFile, input)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't copy input sql %s", err.Error()).Build(), usage)
	}
	_, err = queryFile.Seek(0, 0)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("seek input sql %s", err.Error()).Build(), usage)
	}

	err = debugAnalyze(sqlCtx, outDir, sqlEng, queryFile)
	if err != nil {
		return sqlHandleVErrAndExitCode(queryist, errhand.VerboseErrorFromError(err), usage)
	}

	debugFile, err := os.Create(filepath.Join(outDir, "exec.txt"))
	if err != nil {
		if err != nil {
			return sqlHandleVErrAndExitCode(queryist, errhand.VerboseErrorFromError(err), usage)
		}
	}
	defer debugFile.Close()

	func() {
		defer profile.Start(profile.ProfilePath(outDir), profile.CPUProfile).Stop()
		cli.Println("starting cpu profile...")

		origStdout := cli.CliOut
		origStderr := cli.CliErr
		cli.CliOut = debugFile
		cli.CliErr = debugFile
		defer func() {
			cli.CliOut = origStdout
			cli.CliErr = origStderr
		}()

		var done bool
		wait := time.Tick(time.Duration(runTime) * time.Second)
		for !done {
			select {
			case <-wait:
				done = true
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	func() {
		defer profile.Start(profile.ProfilePath(outDir), profile.MemProfile).Stop()
		cli.Println("starting mem profile...")

		origStdout := cli.CliOut
		origStderr := cli.CliErr
		cli.CliOut = debugFile
		cli.CliErr = debugFile
		defer func() {
			cli.CliOut = origStdout
			cli.CliErr = origStderr
		}()

		var done bool
		wait := time.Tick(time.Duration(runTime) * time.Second)
		for !done {
			select {
			case <-wait:
				done = true
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	func() {
		defer profile.Start(profile.ProfilePath(outDir), profile.TraceProfile).Stop()
		cli.Println("starting trace profile...")

		origStdout := cli.CliOut
		origStderr := cli.CliErr
		cli.CliOut = debugFile
		cli.CliErr = debugFile
		defer func() {
			cli.CliOut = origStdout
			cli.CliErr = origStderr
		}()

		var done bool
		wait := time.Tick(time.Duration(runTime) * time.Second)
		for !done {
			select {
			case <-wait:
				done = true
			default:
				execDebugMode(sqlCtx, sqlEng, queryFile, continueOnError, format)
			}
		}
	}()

	defer cli.Printf("debug results in: %s\n", outDir)

	return 0
}

func addFileToTar(tarWriter *tar.Writer, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	header, err := tar.FileInfoHeader(stat, stat.Name())
	if err != nil {
		return err
	}

	header.Name = filePath
	err = tarWriter.WriteHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(tarWriter, file)
	return err
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
	eng.Analyzer.Verbose = true
	defer func() {
		eng.Analyzer.Debug = false
		eng.Analyzer.Verbose = false
	}()
	analysisFile, err := os.Create(filepath.Join(tempDir, "analysis.txt"))
	if err != nil {
		return err
	}
	defer analysisFile.Close()

	planFile, err := os.Create(filepath.Join(tempDir, "plan.txt"))
	if err != nil {
		return err
	}
	defer planFile.Close()
	planBuf := bufio.NewWriter(planFile)

	analyzer.SetOutput(analysisFile)
	logrus.SetOutput(analysisFile)
	log.SetOutput(analysisFile)
	origStdout := os.Stdout
	origStderr := os.Stderr
	origCliErr := cli.CliErr
	origCliOut := cli.CliOut
	os.Stdout = analysisFile
	os.Stderr = analysisFile
	cli.CliErr = analysisFile
	cli.CliOut = analysisFile
	defer func() {
		logrus.SetOutput(os.Stderr)
		analyzer.SetOutput(os.Stderr)
		log.SetOutput(os.Stderr)
		os.Stdout = origStdout
		os.Stderr = origStderr
		cli.CliOut = origCliOut
		cli.CliErr = origCliErr
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
