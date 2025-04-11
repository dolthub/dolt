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

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
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
	noCreateDbFlag   = "no-create-db"

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
		"[-f] [-r {{.LessThan}}result-format{{.GreaterThan}}] [-fn {{.LessThan}}file_name{{.GreaterThan}}]  [-d {{.LessThan}}directory{{.GreaterThan}}] [--batch] [--no-batch] [--no-autocommit] [--no-create-db] ",
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

// Docs returns the documentation for this command, or nil if it's undocumented
func (cmd DumpCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(dumpDocs, ap)
}

func (cmd DumpCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(FormatFlag, "r", "result_file_type", "Define the type of the output file. Defaults to sql. Valid values are sql, csv, json and parquet.")
	ap.SupportsString(filenameFlag, "fn", "file_name", "Define file name for dump file. Defaults to `doltdump.sql`.")
	ap.SupportsString(directoryFlag, "d", "directory_name", "Define directory name to dump the files in. Defaults to `doltdump/`.")
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(batchFlag, "", "Return batch insert statements wherever possible, enabled by default.")
	ap.SupportsFlag(noBatchFlag, "", "Emit one row per statement, instead of batching multiple rows into each statement.")
	ap.SupportsFlag(noAutocommitFlag, "na", "Turn off autocommit for each dumped table. Useful for speeding up loading of output SQL file.")
	ap.SupportsFlag(schemaOnlyFlag, "", "Dump a table's schema, without including any data, to the output SQL file.")
	ap.SupportsFlag(noCreateDbFlag, "", "Do not write `CREATE DATABASE` statements in SQL files.")
	return ap
}

// EventType returns the type of the event to log
func (cmd DumpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DUMP
}

// Exec executes the command
func (cmd DumpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, dumpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

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

	outputFileOrDirName, vErr := validateDumpArgs(apr)
	if vErr != nil {
		return HandleVErrAndExitCode(vErr, usage)
	}

	engine, dbName, berr := engine.NewSqlEngineForEnv(ctx, dEnv)
	if berr != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(berr), usage)
	}
	defer engine.Close()
	sqlCtx, berr := engine.NewLocalContext(ctx)
	if berr != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(berr), usage)
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	sqlCtx.SetCurrentDatabase(dbName)

	switch resFormat {
	case emptyFileExt, sqlFileExt:
		var defaultName string
		if schemaOnly {
			defaultName = "doltdump_schema_only.sql"
		} else {
			defaultName = "doltdump.sql"
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

		if !apr.Contains(noCreateDbFlag) {
			err = addCreateDatabaseHeader(dEnv, fPath, dbName)
			if err != nil {
				return HandleVErrAndExitCode(err, usage)
			}
		}

		err = addBulkLoadingParadigms(dEnv, fPath)
		if err != nil {
			return HandleVErrAndExitCode(err, usage)
		}

		for _, tbl := range tblNames {
			tblOpts := newTableArgs(tbl, dumpOpts.dest, !apr.Contains(noBatchFlag), apr.Contains(noAutocommitFlag), schemaOnly)
			err = dumpTable(sqlCtx, dEnv, engine.GetUnderlyingEngine(), root, tblOpts, fPath)
			if err != nil {
				return HandleVErrAndExitCode(err, usage)
			}
		}

		err = dumpSchemaElements(sqlCtx, engine, root, dEnv.FS, fPath)
		if err != nil {
			return HandleVErrAndExitCode(err, usage)
		}
	case csvFileExt, jsonFileExt, parquetFileExt:
		err = dumpNonSqlTables(sqlCtx, engine.GetUnderlyingEngine(), root, dEnv, force, tblNames, resFormat, outputFileOrDirName, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	default:
		return HandleVErrAndExitCode(errhand.BuildDError("invalid result format").SetPrintUsage().Build(), usage)
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))

	return 0
}

// dumpSchemaElements writes the non-table schema elements (views, triggers, procedures) to the file path given
func dumpSchemaElements(ctx *sql.Context, eng *engine.SqlEngine, root doltdb.RootValue, fs filesys.Filesys, path string) errhand.VerboseError {
	writer, err := fs.OpenForWriteAppend(path, os.ModePerm)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = dumpViews(ctx, eng, root, writer)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = dumpTriggers(ctx, eng, root, writer)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = dumpProcedures(ctx, eng, root, writer)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = writer.Close()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func dumpProcedures(sqlCtx *sql.Context, engine *engine.SqlEngine, root doltdb.RootValue, writer io.WriteCloser) (rerr error) {
	_, _, ok, err := doltdb.GetTableInsensitive(sqlCtx, root, doltdb.TableName{Name: doltdb.ProceduresTableName})
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	sch, iter, _, err := engine.Query(sqlCtx, "select * from "+doltdb.ProceduresTableName)
	if err != nil {
		return err
	}

	stmtColIdx := sch.IndexOfColName(doltdb.ProceduresTableCreateStmtCol)
	// Note: 'sql_mode' column of `dolt_procedures` table is not present in databases that were created before this column got added
	sqlModeIdx := sch.IndexOfColName(doltdb.ProceduresTableSqlModeCol)

	defer func(iter sql.RowIter, context *sql.Context) {
		err := iter.Close(context)
		if rerr == nil && err != nil {
			rerr = err
		}
	}(iter, sqlCtx)

	for {
		row, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		sqlMode := ""
		if sqlModeIdx >= 0 {
			if s, ok := row[sqlModeIdx].(string); ok {
				sqlMode = s
			}
		}

		modeChanged, err := changeSqlMode(sqlCtx, writer, sqlMode)
		if err != nil {
			return err
		}

		err = iohelp.WriteLine(writer, "delimiter END_PROCEDURE")
		if err != nil {
			return err
		}

		err = iohelp.WriteLine(writer, fmt.Sprintf("%s;", row[stmtColIdx]))
		if err != nil {
			return err
		}

		err = iohelp.WriteLine(writer, "END_PROCEDURE\ndelimiter ;")
		if err != nil {
			return err
		}

		if modeChanged {
			if err := resetSqlMode(writer); err != nil {
				return err
			}
		}
	}

	return nil
}

func dumpTriggers(sqlCtx *sql.Context, engine *engine.SqlEngine, root doltdb.RootValue, writer io.WriteCloser) (rerr error) {
	_, _, ok, err := doltdb.GetTableInsensitive(sqlCtx, root, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	sch, iter, _, err := engine.Query(sqlCtx, "select * from "+doltdb.SchemasTableName)
	if err != nil {
		return err
	}

	typeColIdx := sch.IndexOfColName(doltdb.SchemasTablesTypeCol)
	fragColIdx := sch.IndexOfColName(doltdb.SchemasTablesFragmentCol)
	// Note: some columns of `dolt_schemas` table are not present in databases that were created before those columns got added
	sqlModeIdx := sch.IndexOfColName(doltdb.SchemasTablesSqlModeCol)

	defer func(iter sql.RowIter, context *sql.Context) {
		err := iter.Close(context)
		if rerr == nil && err != nil {
			rerr = err
		}
	}(iter, sqlCtx)

	for {
		row, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if row[typeColIdx] != "trigger" {
			continue
		}

		sqlMode := ""
		if sqlModeIdx >= 0 {
			if s, ok := row[sqlModeIdx].(string); ok {
				sqlMode = s
			}
		}

		modeChanged, err := changeSqlMode(sqlCtx, writer, sqlMode)
		if err != nil {
			return err
		}

		err = iohelp.WriteLine(writer, fmt.Sprintf("%s;", row[fragColIdx]))
		if err != nil {
			return err
		}

		if modeChanged {
			if err := resetSqlMode(writer); err != nil {
				return err
			}
		}
	}

	return nil
}

func dumpViews(ctx *sql.Context, engine *engine.SqlEngine, root doltdb.RootValue, writer io.WriteCloser) (rerr error) {
	_, _, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	sch, iter, _, err := engine.Query(ctx, "select * from "+doltdb.SchemasTableName)
	if err != nil {
		return err
	}

	typeColIdx := sch.IndexOfColName(doltdb.SchemasTablesTypeCol)
	fragColIdx := sch.IndexOfColName(doltdb.SchemasTablesFragmentCol)
	nameColIdx := sch.IndexOfColName(doltdb.SchemasTablesNameCol)
	// Note: some columns of `dolt_schemas` table are not present in databases that were created before those columns got added
	sqlModeIdx := sch.IndexOfColName(doltdb.SchemasTablesSqlModeCol)

	defer func(iter sql.RowIter, context *sql.Context) {
		err := iter.Close(context)
		if rerr == nil && err != nil {
			rerr = err
		}
	}(iter, ctx)

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if row[typeColIdx] != "view" {
			continue
		}

		sqlMode := ""
		if sqlModeIdx >= 0 {
			if s, ok := row[sqlModeIdx].(string); ok {
				sqlMode = s
			}
		}
		// We used to store just the SELECT part of a view, but now we store the entire CREATE VIEW statement
		sqlEngine := engine.GetUnderlyingEngine()
		binder := planbuilder.New(ctx, sqlEngine.Analyzer.Catalog, sqlEngine.EventScheduler, sqlEngine.Parser)
		binder.SetParserOptions(sql.NewSqlModeFromString(sqlMode).ParserOptions())
		cv, _, _, _, err := binder.Parse(row[fragColIdx].(string), nil, false)
		if err != nil {
			return err
		}
		modeChanged, err := changeSqlMode(ctx, writer, sqlMode)
		if err != nil {
			return err
		}

		_, ok := cv.(*plan.CreateView)
		if ok {
			err := iohelp.WriteLine(writer, fmt.Sprintf("%s;", row[fragColIdx]))
			if err != nil {
				return err
			}
		} else {
			err := iohelp.WriteLine(writer, fmt.Sprintf("CREATE VIEW %s AS %s;", row[nameColIdx], row[fragColIdx]))
			if err != nil {
				return err
			}
		}

		if modeChanged {
			if err := resetSqlMode(writer); err != nil {
				return err
			}
		}
	}

	return nil
}

// changeSqlMode checks if the current SQL session's @@SQL_MODE is different from the requested |newSqlMode| and if so,
// outputs a SQL statement to |writer| to save the current @@SQL_MODE to the @previousSqlMode variable and then outputs
// a SQL statement to set the @@SQL_MODE to |sqlMode|. If |newSqlMode| is the identical to the current session's
// SQL_MODE (the default, global @@SQL_MODE), then no statements are written to |writer|. The boolean return code
// indicates if any statements were written.
func changeSqlMode(ctx *sql.Context, writer io.WriteCloser, newSqlMode string) (bool, error) {
	if newSqlMode == "" {
		return false, nil
	}

	variable, err := ctx.Session.GetSessionVariable(ctx, "SQL_MODE")
	if err != nil {
		return false, err
	}
	currentSqlMode, ok := variable.(string)
	if !ok {
		return false, fmt.Errorf("unable to read @@SQL_MODE system variable from value '%v'", currentSqlMode)
	}

	if currentSqlMode == newSqlMode {
		return false, nil
	}

	err = iohelp.WriteLine(writer, "SET @previousSqlMode=@@SQL_MODE;")
	if err != nil {
		return false, err
	}

	err = iohelp.WriteLine(writer, fmt.Sprintf("SET @@SQL_MODE='%s';", newSqlMode))
	if err != nil {
		return false, err
	}

	return true, nil
}

// resetSqlMode outputs a SQL statement to |writer| to reset @@SQL_MODE back to the previous value stored
// by the last call to changeSqlMode. This function should only be called after changeSqlMode, otherwise the
// @previousSqlMode variable will not be set correctly.
func resetSqlMode(writer io.WriteCloser) error {
	return iohelp.WriteLine(writer, "SET @@SQL_MODE=@previousSqlMode;")
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
func dumpTable(ctx *sql.Context, dEnv *env.DoltEnv, engine *sqle.Engine, root doltdb.RootValue, tblOpts *tableOptions, filePath string) errhand.VerboseError {
	rd, err := mvdata.NewSqlEngineReader(ctx, engine, root, tblOpts.tableName)
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
	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}

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
func checkAndCreateOpenDestFile(ctx context.Context, root doltdb.RootValue, dEnv *env.DoltEnv, force bool, dumpOpts *dumpOptions, fileName string) (string, errhand.VerboseError) {
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
func checkOverwrite(ctx context.Context, root doltdb.RootValue, fs filesys.ReadableFS, force bool, dest mvdata.DataLocation) (bool, error) {
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

// validateDumpArgs returns either filename of directory name after checking each cases of user input arguments,
// handling errors for invalid arguments
func validateDumpArgs(apr *argparser.ArgParseResults) (string, errhand.VerboseError) {
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

// getDumpOptions returns dumpOptions of result format and dest file location corresponding to the input parameters
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
func dumpNonSqlTables(ctx *sql.Context, engine *sqle.Engine, root doltdb.RootValue, dEnv *env.DoltEnv, force bool, tblNames []string, rf string, dirName string, batched bool) errhand.VerboseError {
	var fName string
	if dirName == emptyStr {
		dirName = "doltdump/"
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

		err = dumpTable(ctx, dEnv, engine, root, tblOpts, fPath)
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
func addBulkLoadingParadigms(dEnv *env.DoltEnv, fPath string) errhand.VerboseError {
	writer, err := dEnv.FS.OpenForWriteAppend(fPath, os.ModePerm)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	_, err = writer.Write([]byte("SET FOREIGN_KEY_CHECKS=0;\n"))
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	_, err = writer.Write([]byte("SET UNIQUE_CHECKS=0;\n"))
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	_ = writer.Close()

	return nil
}

// addCreateDatabaseHeader adds a CREATE DATABASE header to prevent `no database selected` errors on dump file ingestion.
func addCreateDatabaseHeader(dEnv *env.DoltEnv, fPath, dbName string) errhand.VerboseError {
	writer, err := dEnv.FS.OpenForWriteAppend(fPath, os.ModePerm)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	str := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%[1]s`; USE `%[1]s`; \n", dbName)
	_, err = writer.Write([]byte(str))
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	_ = writer.Close()

	return nil
}
