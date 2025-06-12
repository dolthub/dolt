// Copyright 2019 Dolthub, Inc.
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

package tblcmds

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/message"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	createParam       = "create-table"
	updateParam       = "update-table"
	replaceParam      = "replace-table"
	appendParam       = "append-table"
	tableParam        = "table"
	fileParam         = "file"
	schemaParam       = "schema"
	mappingFileParam  = "map"
	forceParam        = "force"
	contOnErrParam    = "continue"
	primaryKeyParam   = "pk"
	fileTypeParam     = "file-type"
	delimParam        = "delim"
	quiet             = "quiet"
	ignoreSkippedRows = "ignore-skipped-rows" // alias for quiet
	disableFkChecks   = "disable-fk-checks"
	allTextParam      = "all-text"
	noHeaderParam     = "no-header" // for CSV files without header row
	columnsParam      = "columns"   // for specifying column names
)

var jsonInputFileHelp = "The expected JSON input file format is:" + `

	{ "rows":
		[
			{
				"column_name":"value"
				...
			}, ...
		]
	}

where column_name is the name of a column of the table being imported and value is the data for that column in the table.
`

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: `Imports data into a dolt table`,
	LongDesc: `If {{.EmphasisLeft}}--create-table | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} and import the contents of file into it.  If a table already exists at this location then the operation will fail, unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag is provided. The force flag forces the existing table to be overwritten.

The schema for the new table can be specified explicitly by providing a SQL schema definition file, or will be inferred from the imported file.  All schemas, inferred or explicitly defined must define a primary key.  If the file format being imported does not support defining a primary key, then the {{.EmphasisLeft}}--pk{{.EmphasisRight}} parameter must supply the name of the field that should be used as the primary key. If no primary key is explicitly defined, the first column in the import file will be used as the primary key.

If {{.EmphasisLeft}}--update-table | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} with the contents of file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If {{.EmphasisLeft}}--append-table | -a{{.EmphasisRight}} is given the operation will add the contents of the file to {{.LessThan}}table{{.GreaterThan}}, without modifying any of the rows of {{.LessThan}}table{{.GreaterThan}}. If the file contains a row that matches the primary key of a row already in the table, the import will be aborted unless the --continue flag is used (in which case that row will not be imported.) The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If {{.EmphasisLeft}}--replace-table | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with the contents of the file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If the schema for the existing table does not match the schema for the new file, the import will be aborted by default. To overwrite both the table and the schema, use {{.EmphasisLeft}}-c -f{{.EmphasisRight}}.

A mapping file can be used to map fields between the file being imported and the table being written to. This can be used when creating a new table, or updating or replacing an existing table.

During import, if there is an error importing any row, the import will be aborted by default. Use the {{.EmphasisLeft}}--continue{{.EmphasisRight}} flag to continue importing when an error is encountered. You can add the {{.EmphasisLeft}}--quiet{{.EmphasisRight}} flag to prevent the import utility from printing all the skipped rows. 

` + schcmds.MappingFileHelp +
		`
` + jsonInputFileHelp +
		`
In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (csv, psv, json, xlsx).  For files separated by a delimiter other than a ',' (type csv) or a '|' (type psv), the --delim parameter can be used to specify a delimiter`,

	Synopsis: []string{
		"-c [-f] [--pk {{.LessThan}}field{{.GreaterThan}}] [--all-text] [--schema {{.LessThan}}file{{.GreaterThan}}] [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--quiet] [--disable-fk-checks] [--file-type {{.LessThan}}type{{.GreaterThan}}] [--no-header] [--columns {{.LessThan}}col1,col2,...{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-u [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--quiet] [--file-type {{.LessThan}}type{{.GreaterThan}}] [--no-header] [--columns {{.LessThan}}col1,col2,...{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-a [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--quiet] [--file-type {{.LessThan}}type{{.GreaterThan}}] [--no-header] [--columns {{.LessThan}}col1,col2,...{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-r [--map {{.LessThan}}file{{.GreaterThan}}] [--file-type {{.LessThan}}type{{.GreaterThan}}] [--no-header] [--columns {{.LessThan}}col1,col2,...{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

var bitTypeRegex = regexp.MustCompile(`(?m)b\'(\d+)\'`)

type importOptions struct {
	operation       mvdata.TableImportOp
	destTableName   string
	contOnErr       bool
	force           bool
	schFile         string
	primaryKeys     []string
	nameMapper      rowconv.NameMapper
	src             mvdata.DataLocation
	srcOptions      interface{}
	quiet           bool
	disableFkChecks bool
	allText         bool
}

func (m importOptions) IsBatched() bool {
	return false
}

func (m importOptions) WritesToTable() bool {
	return true
}

func (m importOptions) SrcName() string {
	if f, fileSrc := m.src.(mvdata.FileDataLocation); fileSrc {
		return f.Path
	}
	return m.src.String()
}

func (m importOptions) DestName() string {
	return m.destTableName
}

func (m importOptions) ColNameMapper() rowconv.NameMapper {
	return m.nameMapper
}

func (m importOptions) FloatThreshold() float64 {
	return 0.0
}

func (m importOptions) checkOverwrite(ctx context.Context, root doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if !m.force && m.operation == mvdata.CreateOp {
		return root.HasTable(ctx, doltdb.TableName{Name: m.destTableName})
	}
	return false, nil
}

func (m importOptions) srcIsJson() bool {
	_, isJson := m.srcOptions.(mvdata.JSONOptions)
	return isJson
}

func (m importOptions) srcIsStream() bool {
	_, isStream := m.src.(mvdata.StreamDataLocation)
	return isStream
}

func getImportMoveOptions(ctx *sql.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, engine *sqle.Engine) (*importOptions, errhand.VerboseError) {
	tableName := apr.Arg(0)

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	fType, _ := apr.GetValue(fileTypeParam)
	srcLoc := mvdata.NewDataLocation(path, fType)
	delim, hasDelim := apr.GetValue(delimParam)

	schemaFile, _ := apr.GetValue(schemaParam)
	force := apr.Contains(forceParam)
	contOnErr := apr.Contains(contOnErrParam)
	quiet := apr.Contains(quiet)
	disableFks := apr.Contains(disableFkChecks)
	allText := apr.Contains(allTextParam)

	val, _ := apr.GetValue(primaryKeyParam)
	pks := funcitr.MapStrings(strings.Split(val, ","), strings.TrimSpace)
	pks = funcitr.FilterStrings(pks, func(s string) bool { return s != "" })

	mappingFile := apr.GetValueOrDefault(mappingFileParam, "")
	colMapper, err := rowconv.NameMapperFromFile(mappingFile, dEnv.FS)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	var srcOpts interface{}
	switch val := srcLoc.(type) {
	case mvdata.FileDataLocation:
		if val.Format == mvdata.CsvFile || val.Format == mvdata.PsvFile || (hasDelim && val.Format == mvdata.InvalidDataFormat) {
			if val.Format == mvdata.InvalidDataFormat {
				val = mvdata.FileDataLocation{Path: val.Path, Format: mvdata.CsvFile}
				srcLoc = val
			}

			srcOpts = extractCsvOptions(apr, hasDelim, delim)
		} else if val.Format == mvdata.XlsxFile {
			// table name must match sheet name currently
			srcOpts = mvdata.XlsxOptions{SheetName: tableName}
		} else if val.Format == mvdata.JsonFile {
			opts := mvdata.JSONOptions{TableName: tableName, SchFile: schemaFile}
			if schemaFile != "" {
				opts.SqlCtx = ctx
				opts.Engine = engine
			}
			srcOpts = opts
		} else if val.Format == mvdata.ParquetFile {
			opts := mvdata.ParquetOptions{TableName: tableName, SchFile: schemaFile}
			if schemaFile != "" {
				opts.SqlCtx = ctx
				opts.Engine = engine
			}
			srcOpts = opts
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			srcLoc = val
		}

		srcOpts = extractCsvOptions(apr, hasDelim, delim)
	}

	var moveOp mvdata.TableImportOp
	switch {
	case apr.Contains(createParam):
		moveOp = mvdata.CreateOp
	case apr.Contains(replaceParam):
		moveOp = mvdata.ReplaceOp
	case apr.Contains(appendParam):
		moveOp = mvdata.AppendOp
	default:
		moveOp = mvdata.UpdateOp
	}

	if moveOp != mvdata.CreateOp {
		root, err := dEnv.WorkingRoot(ctx)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		_, exists, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		if !exists {
			return nil, errhand.BuildDError("The following table could not be found: %s", tableName).Build()
		}
	}

	return &importOptions{
		operation:       moveOp,
		destTableName:   tableName,
		contOnErr:       contOnErr,
		force:           force,
		schFile:         schemaFile,
		nameMapper:      colMapper,
		primaryKeys:     pks,
		src:             srcLoc,
		srcOptions:      srcOpts,
		quiet:           quiet,
		disableFkChecks: disableFks,
		allText:         allText,
	}, nil

}

func validateImportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		return errhand.BuildDError("expected 1 or 2 arguments").SetPrintUsage().Build()
	}

	if apr.Contains(schemaParam) && apr.Contains(primaryKeyParam) {
		return errhand.BuildDError("parameters %s and %s are mutually exclusive", schemaParam, primaryKeyParam).Build()
	}

	if !apr.ContainsAny(createParam, updateParam, replaceParam, appendParam) {
		return errhand.BuildDError("Must specify exactly one of -c, -u, -a, or -r.").SetPrintUsage().Build()
	}

	if len(apr.ContainsMany(createParam, updateParam, replaceParam, appendParam)) > 1 {
		return errhand.BuildDError("Must specify exactly one of -c, -u, -a, or -r.").SetPrintUsage().Build()
	}

	if apr.Contains(schemaParam) && !apr.Contains(createParam) {
		return errhand.BuildDError("fatal: " + schemaParam + " is not supported for update or replace operations").Build()
	}

	if apr.Contains(createParam) && apr.NArg() <= 1 {
		if !apr.Contains(schemaParam) {
			return errhand.BuildDError("fatal: when importing from stdin with --create-table, you must provide a schema file with --schema").Build()
		}
	}

	if apr.Contains(allTextParam) && !apr.Contains(createParam) {
		return errhand.BuildDError("fatal: --%s is only supported for create operations", allTextParam).Build()
	}

	if apr.ContainsAll(allTextParam, schemaParam) {
		return errhand.BuildDError("parameters %s and %s are mutually exclusive", allTextParam, schemaParam).Build()
	}

	if apr.Contains(noHeaderParam) && !apr.Contains(columnsParam) {
		if apr.Contains(createParam) {
			return errhand.BuildDError("When using --%s with -c (create table), you must also specify --%s to define column names", noHeaderParam, columnsParam).Build()
		} else {
			return errhand.BuildDError("When using --%s with existing tables, you must also specify --%s to define the order of columns in your CSV file", noHeaderParam, columnsParam).Build()
		}
	}

	tableName := apr.Arg(0)
	if err := schcmds.ValidateTableNameForCreate(tableName); err != nil {
		return err
	}

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	fType, hasFileType := apr.GetValue(fileTypeParam)
	if hasFileType && mvdata.DFFromString(fType) == mvdata.InvalidDataFormat {
		return errhand.BuildDError("'%s' is not a valid file type.", fType).Build()
	}

	_, hasDelim := apr.GetValue(delimParam)
	srcLoc := mvdata.NewDataLocation(path, fType)

	switch val := srcLoc.(type) {
	case mvdata.FileDataLocation:
		if !hasDelim && val.Format == mvdata.InvalidDataFormat {
			return errhand.BuildDError("Could not infer type file '%s'\nFile extensions should match supported file types, or should be explicitly defined via the file-type parameter", path).Build()
		}
	}

	if srcFileLoc, isFileType := srcLoc.(mvdata.FileDataLocation); isFileType {
		if srcFileLoc.Format == mvdata.SqlFile {
			return errhand.BuildDError("For SQL import, please pipe SQL input files to `dolt sql`").Build()
		}

		_, hasSchema := apr.GetValue(schemaParam)
		if srcFileLoc.Format == mvdata.JsonFile && apr.Contains(createParam) && !hasSchema {
			return errhand.BuildDError("Please specify schema file for .json tables.").Build()
		} else if srcFileLoc.Format == mvdata.ParquetFile && apr.Contains(createParam) && !hasSchema {
			return errhand.BuildDError("Please specify schema file for .parquet tables.").Build()
		}
	}

	return nil
}

type ImportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ImportCmd) Name() string {
	return "import"
}

// Description returns a description of the command
func (cmd ImportCmd) Description() string {
	return "Creates, overwrites, replaces, or updates a table from the data in a file."
}

func (cmd ImportCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(importDocs, ap)
}

func (cmd ImportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{tableParam, "The new or existing table being imported to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{fileParam, "The file being imported. Supported file types are csv, psv, and nbf."})
	ap.SupportsFlag(createParam, "c", "Create a new table, or overwrite an existing table (with the -f flag) from the imported data.")
	ap.SupportsFlag(updateParam, "u", "Update an existing table with the imported data.")
	ap.SupportsFlag(appendParam, "a", "Require that the operation will not modify any rows in the table.")
	ap.SupportsFlag(replaceParam, "r", "Replace existing table with imported data while preserving the original schema.")
	ap.SupportsFlag(forceParam, "f", "If a create operation is being executed, data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(contOnErrParam, "", "Continue importing when row import errors are encountered.")
	ap.SupportsFlag(quiet, "", "Suppress any warning messages about invalid rows when using the --continue flag.")
	ap.SupportsAlias(ignoreSkippedRows, quiet)
	ap.SupportsFlag(disableFkChecks, "", "Disables foreign key checks.")
	ap.SupportsString(schemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimiter for a csv style file with a non-comma delimiter.")
	ap.SupportsFlag(allTextParam, "", "Treats all fields as text. Can only be used when creating a table.")
	ap.SupportsFlag(noHeaderParam, "", "Treats the first row of a CSV file as data instead of a header row with column names.")
	ap.SupportsString(columnsParam, "", "columns", "Comma-separated list of column names. If used with --no-header, defines column names for the file. If used without --no-header, overrides the column names in the file's header row.")
	return ap
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_IMPORT
}

// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, importDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	var verr errhand.VerboseError

	dEnv, err := commands.MaybeMigrateEnv(ctx, dEnv)
	if err != nil {
		verr = errhand.BuildDError("could not load manifest for gc").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = validateImportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		verr = errhand.BuildDError("Unable to get the working root value for this data repository.").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	eng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv, func(cfg *engine.SqlEngineConfig) {
		cfg.Autocommit = false
		cfg.Bulk = true
	})
	if err != nil {
		verr = errhand.BuildDError("could not build sql engine for import").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	sqlCtx, err := eng.NewLocalContext(ctx)
	if err != nil {
		verr = errhand.BuildDError("could not build sql context for import").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	sqlCtx.SetCurrentDatabase(dbName)

	mvOpts, verr := getImportMoveOptions(sqlCtx, apr, dEnv, eng.GetUnderlyingEngine())
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	rd, nDMErr := newImportDataReader(sqlCtx, root, dEnv, mvOpts)
	if nDMErr != nil {
		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	wr, nDMErr := newImportSqlEngineMover(sqlCtx, root, dEnv, rd.GetSchema(), eng.GetUnderlyingEngine(), mvOpts)
	if nDMErr != nil {
		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, err := move(sqlCtx, rd, wr, mvOpts)
	if err != nil {
		bdr := errhand.BuildDError("\nAn error occurred while moving data")
		bdr.AddCause(err)
		bdr.AddDetails("Errors during import can be ignored using '--continue'")
		return commands.HandleVErrAndExitCode(bdr.Build(), usage)
	}

	cli.PrintErrln()

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	cli.Println(color.CyanString("Import completed successfully."))

	return 0
}

var displayStrLen int

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	p := message.NewPrinter(message.MatchLanguage("en")) // adds commas
	displayStr := p.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

func newImportDataReader(ctx context.Context, root doltdb.RootValue, dEnv *env.DoltEnv, impOpts *importOptions) (table.SqlRowReader, *mvdata.DataMoverCreationError) {
	var err error

	// Checks whether import destination table already exists. This can probably be simplified to not need a root value...
	ow, err := impOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	if ow {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: fmt.Errorf("%s already exists. Use -f to overwrite.", impOpts.DestName())}
	}

	rd, _, err := impOpts.src.NewReader(ctx, dEnv, impOpts.srcOptions)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}

	return rd, nil
}

func newImportSqlEngineMover(ctx *sql.Context, root doltdb.RootValue, dEnv *env.DoltEnv, rdSchema schema.Schema, engine *sqle.Engine, imOpts *importOptions) (*mvdata.SqlEngineTableWriter, *mvdata.DataMoverCreationError) {
	moveOps := &mvdata.MoverOptions{Force: imOpts.force, TableToWriteTo: imOpts.destTableName, ContinueOnErr: imOpts.contOnErr, Operation: imOpts.operation, DisableFks: imOpts.disableFkChecks}

	// Returns the schema of the table to be created or the existing schema
	tableSchema, dmce := getImportSchema(ctx, root, dEnv, engine, imOpts)
	if dmce != nil {
		return nil, dmce
	}

	tableSchemaDiff := tableSchema.GetAllCols().NameToCol
	var rowOperationDiff []string
	// construct the schema of the set of column to be updated.
	rowOperationColColl := schema.NewColCollection()
	rdSchema.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		wrColName := imOpts.nameMapper.Map(col.Name)
		wrCol, ok := tableSchema.GetAllCols().GetByName(wrColName)
		if ok {
			rowOperationColColl = rowOperationColColl.Append(wrCol)
			delete(tableSchemaDiff, wrColName)
		} else {
			rowOperationDiff = append(rowOperationDiff, wrColName)
		}

		return false, nil
	})

	rowOperationSchema, err := schema.SchemaFromCols(rowOperationColColl)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
	}

	// Leave a warning if the import operation has a different schema than the relevant table's schema.
	// This can certainly be intentional, but it is often due to typos in the header of a csv file.
	if len(tableSchemaDiff) != 0 || len(rowOperationDiff) != 0 {
		cli.PrintErrln(color.YellowString("Warning: The import file's schema does not match the table's schema.\nIf unintentional, check for any typos in the import file's header."))
		if len(tableSchemaDiff) != 0 {
			cli.Printf("Missing columns in %s:\n", imOpts.destTableName)
			for _, col := range tableSchemaDiff {
				cli.Println("\t" + col.Name)
			}
		}
		if len(rowOperationDiff) != 0 {
			cli.Println("Extra columns in import file:")
			for _, col := range rowOperationDiff {
				cli.Println("\t" + col)
			}
		}
	}

	mv, err := mvdata.NewSqlEngineTableWriter(ctx, engine, tableSchema, rowOperationSchema, moveOps, importStatsCB)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateWriterErr, Cause: err}
	}

	return mv, nil
}

type badRowFn func(row sql.Row, rowSchema sql.PrimaryKeySchema, tableName string, lineNumber int, err error) (quit bool)

func move(ctx context.Context, rd table.SqlRowReader, wr *mvdata.SqlEngineTableWriter, options *importOptions) (int64, error) {
	g, ctx := errgroup.WithContext(ctx)

	// Set up the necessary data points for the import job
	parsedRowChan := make(chan sql.Row)
	var rowErr error
	var printBadRowsStarted bool
	var badCount int64

	badRowCB := func(row sql.Row, rowSchema sql.PrimaryKeySchema, tableName string, lineNumber int, err error) (quit bool) {
		// record the first error encountered unless asked to ignore it
		if row != nil && rowErr == nil && !options.contOnErr {
			var sqlRowWithColumns []string
			for i, val := range row {
				columnName := "<nil>"
				if len(rowSchema.Schema) > i {
					columnName = rowSchema.Schema[i].Name
				}
				sqlRowWithColumns = append(sqlRowWithColumns, fmt.Sprintf("\t%s: %v\n", columnName, val))
			}
			formattedSqlRow := strings.Join(sqlRowWithColumns, "")

			rowErr = fmt.Errorf("A bad row was encountered inserting into table %s (on line %d):\n%s", tableName, lineNumber, formattedSqlRow)
			if wie, ok := err.(sql.WrappedInsertError); ok {
				if e, ok := wie.Cause.(*errors.Error); ok {
					if ue, ok := e.Cause().(sql.UniqueKeyError); ok {
						rowErr = fmt.Errorf("row %s would be overwritten by %s: %w", sql.FormatRow(ue.Existing), sql.FormatRow(row), err)
					}
				}
			}
		}

		atomic.AddInt64(&badCount, 1)

		// only log info for the --continue option
		if !options.contOnErr {
			_ = wr.DropCreatedTable()
			return true
		}

		// Don't log the skipped rows when asked to suppress warning output
		if options.quiet {
			return false
		}

		if !printBadRowsStarted {
			cli.PrintErrln("The following rows were skipped:")
			printBadRowsStarted = true
		}

		cli.PrintErrln(sql.FormatRow(row))

		return false
	}

	// Start the group that reads rows from the reader
	g.Go(func() error {
		defer close(parsedRowChan)

		return moveRows(ctx, wr, rd, options, parsedRowChan, badRowCB)
	})

	// Start the group that writes rows
	g.Go(func() error {
		err := wr.WriteRows(ctx, parsedRowChan, badRowCB)
		if err != nil {
			return err
		}

		return nil
	})

	err := g.Wait()
	if err != nil && err != io.EOF {
		_ = wr.DropCreatedTable()
		// don't lose the rowErr if there is one
		if rowErr != nil {
			return badCount, fmt.Errorf("%w\n%s", err, rowErr.Error())
		}
		return badCount, err
	}

	if rowErr != nil {
		return badCount, rowErr
	}

	err = wr.Commit(ctx)
	if err != nil {
		return badCount, err
	}

	return badCount, nil
}

func moveRows(
	ctx context.Context,
	wr *mvdata.SqlEngineTableWriter,
	rd table.SqlRowReader,
	options *importOptions,
	parsedRowChan chan sql.Row,
	badRowCb badRowFn,
) error {
	rdSqlSch, err := sqlutil.FromDoltSchema("", options.destTableName, rd.GetSchema())
	if err != nil {
		return err
	}

	line := 1

	for {
		sqlRow, err := rd.ReadSqlRow(ctx)
		if err == io.EOF {
			return nil
		}
		line += 1

		if err != nil {
			if table.IsBadRow(err) {
				quit := badRowCb(sqlRow, rdSqlSch, options.destTableName, line, err)
				if quit {
					return err
				}
			} else {
				return err
			}
		} else {
			sqlRow, err = NameAndTypeTransform(sqlRow, wr.RowOperationSchema(), rdSqlSch, options.nameMapper)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case parsedRowChan <- sqlRow:
			}
		}
	}
}

func getImportSchema(ctx *sql.Context, root doltdb.RootValue, dEnv *env.DoltEnv, engine *sqle.Engine, impOpts *importOptions) (schema.Schema, *mvdata.DataMoverCreationError) {
	if impOpts.schFile != "" {
		tn, out, err := mvdata.SchAndTableNameFromFile(ctx, impOpts.schFile, dEnv.FS, root, engine)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
		}
		if err == nil && tn != impOpts.destTableName {
			err = fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, impOpts.schFile, impOpts.destTableName)
		}

		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
		}

		return out, nil
	}

	if impOpts.operation == mvdata.CreateOp {
		if impOpts.srcIsStream() {
			// todo: capture stream data to file so we can use schema inference
			return nil, nil
		}

		rd, _, err := impOpts.src.NewReader(ctx, dEnv, impOpts.srcOptions)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
		}
		defer rd.Close(ctx)

		if impOpts.allText {
			outSch, err := generateAllTextSchema(rd, impOpts)
			if err != nil {
				return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
			}
			return outSch, nil
		}

		if impOpts.srcIsJson() {
			return rd.GetSchema(), nil
		}

		outSch, err := mvdata.InferSchema(ctx, root, rd, impOpts.destTableName, impOpts.primaryKeys, impOpts)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
		}

		return outSch, nil
	}

	// UpdateOp || ReplaceOp
	tblRd, err := mvdata.NewSqlEngineReader(ctx, engine, root, impOpts.destTableName)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	defer tblRd.Close(ctx)

	return tblRd.GetSchema(), nil
}

// generateAllTextSchema returns a schema where each column has a text type. Primary key columns will have type
// varchar(16383) because text type is not supported for priamry keys.
func generateAllTextSchema(rd table.ReadCloser, impOpts *importOptions) (schema.Schema, error) {
	var cols []schema.Column
	err := rd.GetSchema().GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		var colType typeinfo.TypeInfo
		if slices.Contains(impOpts.primaryKeys, col.Name) || (len(impOpts.primaryKeys) == 0 && len(cols) == 0) {
			// text type is not supported for primary keys, pk is either explicitly set or is the first column
			colType = typeinfo.StringImportDefaultType
		} else {
			colType = typeinfo.TextType
		}

		col.Kind = colType.NomsKind()
		col.TypeInfo = colType
		col.Name = impOpts.ColNameMapper().Map(col.Name)
		col.Tag = schema.ReservedTagMin + tag
		// we don't check the file so can't safely add not null constraint
		col.Constraints = []schema.ColConstraint(nil)

		cols = append(cols, col)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return schema.SchemaFromCols(schema.NewColCollection(cols...))
}

func newDataMoverErrToVerr(mvOpts *importOptions, err *mvdata.DataMoverCreationError) errhand.VerboseError {
	switch err.ErrType {
	case mvdata.CreateReaderErr:
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.src.String())
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.src.String(), mvOpts.destTableName)
		return bdr.AddCause(err.Cause).Build()

	case mvdata.SchemaErr:
		bdr := errhand.BuildDError("Error determining the output schema.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not determine the output schema.", mvOpts.src.String(), mvOpts.destTableName)
		bdr.AddDetails(`Schema File: "%s"`, mvOpts.schFile)
		bdr.AddDetails(`explicit pks: "%s"`, strings.Join(mvOpts.primaryKeys, ","))
		return bdr.AddCause(err.Cause).Build()

	case mvdata.MappingErr:
		bdr := errhand.BuildDError("Error determining the mapping from input fields to output fields.")
		bdr.AddDetails("When attempting to move data from %s to %s, determine the mapping from input fields t, output fields.", mvOpts.src.String(), mvOpts.destTableName)
		bdr.AddDetails(`Mapping File: "%s"`, mvOpts.nameMapper)
		return bdr.AddCause(err.Cause).Build()

	case mvdata.ReplacingErr:
		bdr := errhand.BuildDError("Error replacing table")
		bdr.AddDetails("When attempting to replace data with %s, could not validate schema.", mvOpts.src.String())
		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateMapperErr:
		bdr := errhand.BuildDError("Error creating input to output mapper.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.src.String(), mvOpts.destTableName)
		bdr.AddCause(err.Cause)

		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateWriterErr:
		bdr := errhand.BuildDError("Error creating writer for %s.\n", mvOpts.destTableName)
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a writer.", mvOpts.src.String(), mvOpts.destTableName)
		return bdr.AddCause(err.Cause).Build()
	case mvdata.CreateSorterErr:
		bdr := errhand.BuildDError("Error creating sorting reader.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not open create sorting reader.", mvOpts.src.String(), mvOpts.destTableName)
		return bdr.AddCause(err.Cause).Build()
	}

	panic("Unhandled Error type")
}

// NameAndTypeTransform does 1) match the read and write schema with subsetting and name matching. 2) Address any
// type inconsistencies.
func NameAndTypeTransform(row sql.Row, rowOperationSchema sql.PrimaryKeySchema, rdSchema sql.PrimaryKeySchema, nameMapper rowconv.NameMapper) (sql.Row, error) {
	row = applyMapperToRow(row, rowOperationSchema, rdSchema, nameMapper)

	for i, col := range rowOperationSchema.Schema {
		// Check if this a string that can be converted to a boolean
		val, ok := detectAndConvertToBoolean(row[i], col.Type)
		if ok {
			row[i] = val
			continue
		}

		// Bit types need additional verification due to the differing values they can take on. "4", "0x04", b'100' should
		// be interpreted in the correct manner.
		if _, ok := col.Type.(gmstypes.BitType); ok {
			colAsString, ok := row[i].(string)
			if !ok {
				return nil, fmt.Errorf("error: column value should be of type string")
			}

			// Check if the column can be parsed an uint64
			val, err := strconv.ParseUint(colAsString, 10, 64)
			if err == nil {
				row[i] = val
				continue
			}

			// Check if the column is of type b'110'
			groups := bitTypeRegex.FindStringSubmatch(colAsString)
			// Note that we use the second element as the first value in `groups` is the entire string.
			if len(groups) > 1 {
				val, err = strconv.ParseUint(groups[1], 2, 64)
				if err == nil {
					row[i] = val
					continue
				}
			}

			// Check if the column can be parsed as a hex string
			numberStr := strings.Replace(colAsString, "0x", "", -1)
			val, err = strconv.ParseUint(numberStr, 16, 64)
			if err == nil {
				row[i] = val
			} else {
				return nil, fmt.Errorf("error: Unparsable bit value %s", colAsString)
			}
		}

		// For non string types we want empty strings to be converted to nils. String types should be allowed to take on
		// an empty string value
		switch col.Type.(type) {
		case sql.StringType, sql.EnumType, sql.SetType:
		default:
			row[i] = emptyStringToNil(row[i])
		}
	}

	return row, nil
}

// detectAndConvertToBoolean determines whether a column is potentially a boolean and converts it accordingly.
func detectAndConvertToBoolean(columnVal interface{}, columnType sql.Type) (bool, bool) {
	switch columnType.Type() {
	case sqltypes.Int8, sqltypes.Bit: // TODO: noms bool wraps MustCreateBitType
		switch columnVal.(type) {
		case int8:
			return stringToBoolean(strconv.Itoa(int(columnVal.(int8))))
		case string:
			return stringToBoolean(columnVal.(string))
		case bool:
			return columnVal.(bool), true
		}
	}

	return false, false
}

func stringToBoolean(s string) (result bool, canConvert bool) {
	lower := strings.ToLower(s)
	switch lower {
	case "true":
		return true, true
	case "false":
		return false, true
	case "0":
		return false, true
	case "1":
		return true, true
	default:
		return false, false
	}
}

func emptyStringToNil(val interface{}) interface{} {
	if val == nil {
		return val
	}

	if s, canConvert := val.(string); canConvert {
		if s == "" {
			return nil
		}
	}

	return val
}

func applyMapperToRow(row sql.Row, rowOperationSchema, rdSchema sql.PrimaryKeySchema, nameMapper rowconv.NameMapper) sql.Row {
	returnRow := make(sql.Row, len(rowOperationSchema.Schema))

	for i, col := range rowOperationSchema.Schema {
		rdIdx := rdSchema.IndexOf(nameMapper.PreImage(col.Name), col.Source)
		returnRow[i] = row[rdIdx]
	}

	return returnRow
}

// extractCsvOptions extracts the CSV options from argument parser results
func extractCsvOptions(apr *argparser.ArgParseResults, hasDelim bool, delim string) mvdata.CsvOptions {
	noHeaderFlag := apr.Contains(noHeaderParam)
	columnsVal, hasColumns := apr.GetValue(columnsParam)
	var columnsList []string
	if hasColumns {
		columnsList = strings.Split(columnsVal, ",")
	}

	csvOpts := mvdata.CsvOptions{NoHeader: noHeaderFlag}
	if hasDelim {
		csvOpts.Delim = delim
	}

	if len(columnsList) > 0 {
		csvOpts.Columns = columnsList
	}

	return csvOpts
}
