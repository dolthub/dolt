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
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const (
	forceParam       = "force"
	directoryFlag    = "directory"
	filenameFlag     = "file-name"
	batchFlag        = "batch"
	noBatchFlag      = "no-batch"
	noAutocommitFlag = "no-autocommit"
	schemaOnlyFlag   = "schema-only"

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
is provided. The force flag forces the existing dump file to be overwritten. The {{.EmphasisLeft}}-r{{.EmphasisRight}} flag 
is used to support different file formats of the dump. In the case of non .sql files each table is written to a separate
csv,json or parquet file. 
`,

	Synopsis: []string{
		"[-f] [-r {{.LessThan}}result-format{{.GreaterThan}}] [-fn {{.LessThan}}file_name{{.GreaterThan}}]  [-d {{.LessThan}}directory{{.GreaterThan}}] [--batch] [--no-batch] [--no-autocommit] ",
	},
}

type DumpCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DumpCmd) Name() string {
	return "dump"
}

// Description returns a description of the command
func (cmd DumpCmd) Description() string {
	return "Export all tables in the working set into a file."
}

// CreateMarkdown creates a markdown file containing the help text for the command at the given path
func (cmd DumpCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(dumpDocs, ap)
}

func (cmd DumpCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(FormatFlag, "r", "result_file_type", "Define the type of the output file. Defaults to sql. Valid values are sql, csv, json and parquet.")
	ap.SupportsString(filenameFlag, "fn", "file_name", "Define file name for dump file. Defaults to `doltdump.sql`.")
	ap.SupportsString(directoryFlag, "d", "directory_name", "Define directory name to dump the files in. Defaults to `doltdump/`.")
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(batchFlag, "", "Return batch insert statements wherever possible, enabled by default.")
	ap.SupportsFlag(noBatchFlag, "", "Emit one row per statement, instead of batching multiple rows into each statement.")
	ap.SupportsFlag(noAutocommitFlag, "na", "Turn off autocommit for each dumped table. Useful for speeding up loading of output SQL file.")
	ap.SupportsFlag(schemaOnlyFlag, "", "Dump a table's schema, without including any data, to the output SQL file.")
	return ap
}

// EventType returns the type of the event to log
func (cmd DumpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DUMP
}

// Exec executes the command
func (cmd DumpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, dumpDocs, ap))
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
	schemaOnly := apr.Contains(schemaOnlyFlag)
	resFormat, _ := apr.GetValue(FormatFlag)
	resFormat = strings.TrimPrefix(resFormat, ".")

	outputFileOrDirName, vErr := validateArgs(apr)
	if vErr != nil {
		return HandleVErrAndExitCode(vErr, usage)
	}

	// Look for schemas and procedures table, and add to tblNames only for sql dumps
	if !schemaOnly && (resFormat == emptyFileExt || resFormat == sqlFileExt) {
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
		var defaultName string
		if schemaOnly {
			defaultName = fmt.Sprintf("doltdump_schema_only.sql")
		} else {
			defaultName = fmt.Sprintf("doltdump.sql")
		}

		if outputFileOrDirName == emptyStr {
			outputFileOrDirName = defaultName
		} else {
			if !strings.HasSuffix(outputFileOrDirName, ".sql") {
				outputFileOrDirName = fmt.Sprintf("%s.sql", outputFileOrDirName)
			}
		}

		dumpOpts := getDumpOptions(outputFileOrDirName, resFormat, schemaOnly)
		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, outputFileOrDirName)
		if err != nil {
			return HandleVErrAndExitCode(err, usage)
		}

		err2 := addBulkLoadingParadigms(dEnv, fPath)
		if err2 != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err2), usage)
		}

		for _, tbl := range tblNames {
			tblOpts := newTableArgs(tbl, dumpOpts.dest, !apr.Contains(noBatchFlag), apr.Contains(noAutocommitFlag), schemaOnly)
			err = dumpTable(ctx, dEnv, tblOpts, fPath)
			if err != nil {
				return HandleVErrAndExitCode(err, usage)
			}
		}
	case csvFileExt, jsonFileExt, parquetFileExt:
		err = dumpNonSqlTables(ctx, root, dEnv, force, tblNames, resFormat, outputFileOrDirName, false)
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
	format     string
	schemaOnly bool
	dest       mvdata.DataLocation
}

type tableOptions struct {
	tableName     string
	schemaOnly    bool
	dest          mvdata.DataLocation
	batched       bool
	autocommitOff bool
}

func (m tableOptions) IsBatched() bool {
	return m.batched
}

func (m tableOptions) IsAutocommitOff() bool {
	return m.autocommitOff
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

	if tblOpts.schemaOnly {
		// table schema can be exported to only sql file.
		if sqlExpWr, ok := wr.(*sqlexport.SqlExportWriter); ok {
			err = sqlExpWr.WriteDropCreateOnly(ctx)
		} else {
			err = errhand.BuildDError("Cannot export table schemas to non-sql output file").Build()
		}
	} else {
		pipeline := mvdata.NewDataMoverPipeline(ctx, rd, wr)
		err = pipeline.Execute()
	}

	if err != nil {
		return errhand.BuildDError("Error with dumping %s.", tblOpts.SrcName()).AddCause(err).Build()
	}

	return nil
}

func getTableWriter(ctx context.Context, dEnv *env.DoltEnv, tblOpts *tableOptions, outSch schema.Schema, filePath string) (table.SqlRowWriter, errhand.VerboseError) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, errhand.BuildDError("error: ").AddCause(err).Build()
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}

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
	snOk := apr.Contains(schemaOnlyFlag)

	if fnOk && dnOk {
		return emptyStr, errhand.BuildDError("cannot pass both directory and file names").SetPrintUsage().Build()
	}
	switch rf {
	case emptyFileExt, sqlFileExt:
		if dnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", directoryFlag, sqlFileExt).SetPrintUsage().Build()
		}
		return fn, nil
	case csvFileExt, jsonFileExt, parquetFileExt:
		if fnOk {
			return emptyStr, errhand.BuildDError("%s is not supported for %s exports", filenameFlag, rf).SetPrintUsage().Build()
		}
		if snOk {
			return emptyStr, errhand.BuildDError("%s dump is not supported for %s exports", schemaOnlyFlag, rf).SetPrintUsage().Build()
		}
		return dn, nil
	default:
		return emptyStr, errhand.BuildDError("invalid result format").SetPrintUsage().Build()
	}
}

// getDumpArgs returns dumpOptions of result format and dest file location corresponding to the input parameters
func getDumpOptions(fileName string, rf string, schemaOnly bool) *dumpOptions {
	fileLoc := getDumpDestination(fileName)

	return &dumpOptions{
		format:     rf,
		schemaOnly: schemaOnly,
		dest:       fileLoc,
	}
}

// newTableArgs returns tableOptions of table name and src table location and dest file location
// corresponding to the input parameters
func newTableArgs(tblName string, destination mvdata.DataLocation, batched, autocommitOff, schemaOnly bool) *tableOptions {
	if schemaOnly {
		batched = false
	}
	return &tableOptions{
		tableName:     tblName,
		schemaOnly:    schemaOnly,
		dest:          destination,
		batched:       batched,
		autocommitOff: autocommitOff,
	}
}

// dumpNonSqlTables returns nil if all tables is dumped successfully, and it returns err if there is one.
// It handles only csv and json file types(rf).
func dumpNonSqlTables(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, force bool, tblNames []string, rf string, dirName string, batched bool) errhand.VerboseError {
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
		dumpOpts := getDumpOptions(fName, rf, false)

		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, fName)
		if err != nil {
			return err
		}

		tblOpts := newTableArgs(tbl, dumpOpts.dest, batched, false, false)

		err = dumpTable(ctx, dEnv, tblOpts, fPath)
		if err != nil {
			return err
		}
	}
	return nil
}

// addBulkLoadingParadigms adds statements that are used to expedite dump file ingestion.
// cc. https://dev.mysql.com/doc/refman/8.0/en/optimizing-innodb-bulk-data-loading.html
// This includes turning off FOREIGN_KEY_CHECKS and UNIQUE_CHECKS off at the beginning of the file.
// Note that the standard mysqldump program turns these variables off.
func addBulkLoadingParadigms(dEnv *env.DoltEnv, fPath string) error {
	writer, err := dEnv.FS.OpenForWriteAppend(fPath, os.ModePerm)
	if err != nil {
		return err
	}

	_, err = writer.Write([]byte("SET FOREIGN_KEY_CHECKS=0;\n"))
	if err != nil {
		return err
	}

	_, err = writer.Write([]byte("SET UNIQUE_CHECKS=0;\n"))
	if err != nil {
		return err
	}

	return writer.Close()
}
