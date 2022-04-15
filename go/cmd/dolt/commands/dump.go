// Copyright 2021 Dolthub, Inc.
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
	"path/filepath"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const (
	forceParam    = "force"
	directoryFlag = "directory"
	filenameFlag  = "file-name"
	batchFlag     = "batch"

	sqlFileExt     = "sql"
	csvFileExt     = "csv"
	jsonFileExt    = "json"
	parquetFileExt = "parquet"
	emptyFileExt   = ""
	emptyStr       = ""
)

var dumpDocs = cli.CommandDocumentationContent{
	ShortDesc: `Export all tables.`,
	LongDesc: `{{.EmphasisLeft}}dolt dump{{.EmphasisRight}} dumps all tables in the working set. 
If a dump file already exists then the operation will fail, unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag 
is provided. The force flag forces the existing dump file to be overwritten.
`,

	Synopsis: []string{
		"[-f] [-r {{.LessThan}}result-format{{.GreaterThan}}] ",
	},
}

type DumpCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DumpCmd) Name() string {
	return "dump"
}

// Description returns a description of the command
func (cmd DumpCmd) Description() string {
	return "Export all tables in the working set into a file."
}

// CreateMarkdown creates a markdown file containing the help text for the command at the given path
func (cmd DumpCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, dumpDocs, ap))
}

func (cmd DumpCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(batchFlag, "", "Returns batch insert statements wherever possible.")
	ap.SupportsString(FormatFlag, "r", "result_file_type", "Define the type of the output file. Defaults to sql. Valid values are sql, csv, json and parquet.")
	ap.SupportsString(filenameFlag, "", "file_name", "Define file name for dump file. Defaults to `doltdump.sql`.")
	ap.SupportsString(directoryFlag, "", "directory_name", "Define directory name to dump the files in. Defaults to `doltdump/`.")

	return ap
}

// EventType returns the type of the event to log
func (cmd DumpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DUMP
}

// Exec executes the command
func (cmd DumpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, dumpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 0 {
		return HandleVErrAndExitCode(errhand.BuildDError("too many arguments").SetPrintUsage().Build(), usage)
	}

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	tblNames, err := doltdb.GetNonSystemTableNames(ctx, root)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to get tables").AddCause(err).Build(), usage)
	}
	if len(tblNames) == 0 {
		cli.Println("No tables to export.")
		return 0
	}

	force := apr.Contains(forceParam)
	resFormat, _ := apr.GetValue(FormatFlag)
	resFormat = strings.TrimPrefix(resFormat, ".")

	name, vErr := validateArgs(apr)
	if vErr != nil {
		return HandleVErrAndExitCode(vErr, usage)
	}

	// Look for schemas and procedures table, and add to tblNames only for sql dumps
	if resFormat == emptyFileExt || resFormat == sqlFileExt {
		sysTblNames, err := doltdb.GetSystemTableNames(ctx, root)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		for _, tblName := range sysTblNames {
			switch tblName {
			case doltdb.SchemasTableName:
				tblNames = append(tblNames, doltdb.SchemasTableName)
			case doltdb.ProceduresTableName:
				tblNames = append(tblNames, doltdb.ProceduresTableName)
			}
		}
	}

	switch resFormat {
	case emptyFileExt, sqlFileExt:
		if name == emptyStr {
			name = fmt.Sprintf("doltdump.sql")
		} else {
			if !strings.HasSuffix(name, ".sql") {
				name = fmt.Sprintf("%s.sql", name)
			}
		}

		dumpOpts := getDumpOptions(name, resFormat)
		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, name)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		for _, tbl := range tblNames {
			tblOpts := newTableArgs(tbl, dumpOpts.dest, apr.Contains(batchFlag))
			err = dumpTable(ctx, dEnv, tblOpts, fPath)
			if err != nil {
				return HandleVErrAndExitCode(err, usage)
			}
		}
	case csvFileExt:
		err = dumpTables(ctx, root, dEnv, force, tblNames, csvFileExt, name, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	case jsonFileExt:
		err = dumpTables(ctx, root, dEnv, force, tblNames, jsonFileExt, name, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	case parquetFileExt:
		err = dumpTables(ctx, root, dEnv, force, tblNames, parquetFileExt, name, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	default:
		return HandleVErrAndExitCode(errhand.BuildDError("invalid result format").SetPrintUsage().Build(), usage)
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))

	return 0
}

type dumpOptions struct {
	format string
	dest   mvdata.DataLocation
}

type tableOptions struct {
	tableName string
	dest      mvdata.DataLocation
	batched   bool
}

func (m tableOptions) IsBatched() bool {
	return m.batched
}

func (m tableOptions) WritesToTable() bool {
	return false
}

func (m tableOptions) SrcName() string {
	return m.tableName
}

func (m tableOptions) DestName() string {
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

func (m dumpOptions) DumpDestName() string {
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

// dumpTable dumps table in file given specific table and file location info
func dumpTable(ctx context.Context, dEnv *env.DoltEnv, tblOpts *tableOptions, filePath string) errhand.VerboseError {
	rd, err := mvdata.NewSqlEngineReader(ctx, dEnv, tblOpts.tableName)
	if err != nil {
		return errhand.BuildDError("Error creating reader for %s.", tblOpts.SrcName()).AddCause(err).Build()
	}

	wr, err := getTableWriter(ctx, dEnv, tblOpts, rd.GetSchema(), filePath)
	if err != nil {
		return errhand.BuildDError("Error creating writer for %s.", tblOpts.SrcName()).AddCause(err).Build()
	}

	pipeline := mvdata.NewDataMoverPipeline(ctx, rd, wr)
	err = pipeline.Execute()

	if err != nil {
		return errhand.BuildDError("Error with dumping %s.", tblOpts.SrcName()).AddCause(err).Build()
	}

	return nil
}

func getTableWriter(ctx context.Context, dEnv *env.DoltEnv, tblOpts *tableOptions, outSch schema.Schema, filePath string) (table.SqlTableWriter, errhand.VerboseError) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}

	writer, err := dEnv.FS.OpenForWriteAppend(filePath, os.ModePerm)
	if err != nil {
		return nil, errhand.BuildDError("Error opening writer for %s.", tblOpts.DestName()).AddCause(err).Build()
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", tblOpts.tableName).AddCause(err).Build()
	}

	wr, err := tblOpts.dest.NewCreatingWriter(ctx, tblOpts, root, outSch, opts, writer)
	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", tblOpts.tableName).AddCause(err).Build()
	}

	return wr, nil
}

// checkAndCreateOpenDestFile returns filePath to created dest file after checking for any existing file and handles it
func checkAndCreateOpenDestFile(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, force bool, dumpOpts *dumpOptions, fileName string) (string, errhand.VerboseError) {
	ow, err := checkOverwrite(ctx, root, dEnv.FS, force, dumpOpts.dest)
	if err != nil {
		return emptyStr, errhand.VerboseErrorFromError(err)
	}
	if ow {
		return emptyStr, errhand.BuildDError("%s already exists. Use -f to overwrite.", fileName).Build()
	}

	// create new file
	err = dEnv.FS.MkDirs(filepath.Dir(dumpOpts.DumpDestName()))
	if err != nil {
		return emptyStr, errhand.VerboseErrorFromError(err)
	}

	filePath, err := dEnv.FS.Abs(fileName)
	if err != nil {
		return emptyStr, errhand.VerboseErrorFromError(err)
	}

	os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	return filePath, nil
}

// checkOverwrite returns TRUE if the file exists and force flag not given and
// FALSE if the file is stream data / file does not exist / file exists and force flag is given
func checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, force bool, dest mvdata.DataLocation) (bool, error) {
	if _, isStream := dest.(mvdata.StreamDataLocation); isStream {
		return false, nil
	}
	if !force {
		return dest.Exists(ctx, root, fs)
	}
	return false, nil
}

// getDumpDestination returns a dump destination corresponding to the input parameters
func getDumpDestination(path string) mvdata.DataLocation {
	destLoc := mvdata.NewDataLocation(path, emptyStr)

	switch val := destLoc.(type) {
	case mvdata.FileDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			cli.PrintErrln(
				color.RedString("Could not infer type file '%s'\n", path),
				"File extensions should match supported file types, or should be explicitly defined via the result-format parameter")
			return nil
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			destLoc = val
		} else if val.Format != mvdata.CsvFile && val.Format != mvdata.PsvFile {
			cli.PrintErrln(color.RedString("Cannot export this format to stdout"))
			return nil
		}
	}

	return destLoc
}

// validateArgs returns either filename of directory name after checking each cases of user input arguments,
// handling errors for invalid arguments
func validateArgs(apr *argparser.ArgParseResults) (string, errhand.VerboseError) {
	rf, _ := apr.GetValue(FormatFlag)
	rf = strings.TrimPrefix(rf, ".")
	fn, fnOk := apr.GetValue(filenameFlag)
	dn, dnOk := apr.GetValue(directoryFlag)

	if fnOk && dnOk {
		return emptyStr, errhand.BuildDError("cannot pass both directory and file names").SetPrintUsage().Build()
	}
	switch rf {
	case emptyFileExt, sqlFileExt:
		if dnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", directoryFlag, sqlFileExt).SetPrintUsage().Build()
		}
		return fn, nil
	case csvFileExt:
		if fnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", filenameFlag, csvFileExt).SetPrintUsage().Build()
		}
		return dn, nil
	case jsonFileExt:
		if fnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", filenameFlag, jsonFileExt).SetPrintUsage().Build()
		}
		return dn, nil
	case parquetFileExt:
		if fnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", filenameFlag, parquetFileExt).SetPrintUsage().Build()
		}
		return dn, nil
	default:
		return emptyStr, errhand.BuildDError("invalid result format").SetPrintUsage().Build()
	}
}

// getDumpArgs returns dumpOptions of result format and dest file location corresponding to the input parameters
func getDumpOptions(fileName string, rf string) *dumpOptions {
	fileLoc := getDumpDestination(fileName)

	return &dumpOptions{
		format: rf,
		dest:   fileLoc,
	}
}

// newTableArgs returns tableOptions of table name and src table location and dest file location
// corresponding to the input parameters
func newTableArgs(tblName string, destination mvdata.DataLocation, batched bool) *tableOptions {
	return &tableOptions{
		tableName: tblName,
		dest:      destination,
		batched:   batched,
	}
}

// dumpTables returns nil if all tables is dumped successfully, and it returns err if there is one.
// It handles only csv and json file types(rf).
func dumpTables(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, force bool, tblNames []string, rf string, dirName string, batched bool) errhand.VerboseError {
	var fName string
	if dirName == emptyStr {
		dirName = fmt.Sprintf("doltdump/")
	} else {
		if !strings.HasSuffix(dirName, "/") {
			dirName = fmt.Sprintf("%s/", dirName)
		}
	}

	for _, tbl := range tblNames {
		fName = fmt.Sprintf("%s%s.%s", dirName, tbl, rf)
		dumpOpts := getDumpOptions(fName, rf)

		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, fName)
		if err != nil {
			return err
		}

		tblOpts := newTableArgs(tbl, dumpOpts.dest, batched)

		err = dumpTable(ctx, dEnv, tblOpts, fPath)
		if err != nil {
			return err
		}
	}
	return nil
}
