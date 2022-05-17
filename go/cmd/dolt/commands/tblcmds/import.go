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
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/message"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
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
	tableParam        = "table"
	fileParam         = "file"
	schemaParam       = "schema"
	mappingFileParam  = "map"
	forceParam        = "force"
	contOnErrParam    = "continue"
	primaryKeyParam   = "pk"
	fileTypeParam     = "file-type"
	delimParam        = "delim"
	ignoreSkippedRows = "ignore-skipped-rows"
	disableFkChecks   = "disable-fk-checks"
)

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: `Imports data into a dolt table`,
	LongDesc: `If {{.EmphasisLeft}}--create-table | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} and import the contents of file into it.  If a table already exists at this location then the operation will fail, unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag is provided. The force flag forces the existing table to be overwritten.

The schema for the new table can be specified explicitly by providing a SQL schema definition file, or will be inferred from the imported file.  All schemas, inferred or explicitly defined must define a primary key.  If the file format being imported does not support defining a primary key, then the {{.EmphasisLeft}}--pk{{.EmphasisRight}} parameter must supply the name of the field that should be used as the primary key.

If {{.EmphasisLeft}}--update-table | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} with the contents of file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

During import, if there is an error importing any row, the import will be aborted by default. Use the {{.EmphasisLeft}}--continue{{.EmphasisRight}} flag to continue importing when an error is encountered. You can add the {{.EmphasisLeft}}--ignore-skipped-rows{{.EmphasisRight}} flag to prevent the import utility from printing all the skipped rows. 

If {{.EmphasisLeft}}--replace-table | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with the contents of the file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If the schema for the existing table does not match the schema for the new file, the import will be aborted by default. To overwrite both the table and the schema, use {{.EmphasisLeft}}-c -f{{.EmphasisRight}}.

A mapping file can be used to map fields between the file being imported and the table being written to. This can be used when creating a new table, or updating or replacing an existing table.

` + schcmds.MappingFileHelp +
		`
In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (csv, psv, json, xlsx).  For files separated by a delimiter other than a ',' (type csv) or a '|' (type psv), the --delim parameter can be used to specify a delimiter`,

	Synopsis: []string{
		"-c [-f] [--pk {{.LessThan}}field{{.GreaterThan}}] [--schema {{.LessThan}}file{{.GreaterThan}}] [--map {{.LessThan}}file{{.GreaterThan}}] [--continue]  [--ignore-skipped-rows] [--disable-fk-checks] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-u [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--ignore-skipped-rows] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-r [--map {{.LessThan}}file{{.GreaterThan}}] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

type importOptions struct {
	operation         mvdata.TableImportOp
	destTableName     string
	contOnErr         bool
	force             bool
	schFile           string
	primaryKeys       []string
	nameMapper        rowconv.NameMapper
	src               mvdata.DataLocation
	srcOptions        interface{}
	ignoreSkippedRows bool
	disableFkChecks   bool
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

func (m importOptions) checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if !m.force && m.operation == mvdata.CreateOp {
		return root.HasTable(ctx, m.destTableName)
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

func getImportMoveOptions(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) (*importOptions, errhand.VerboseError) {
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
	ignore := apr.Contains(ignoreSkippedRows)
	disableFks := apr.Contains(disableFkChecks)

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
		if hasDelim {
			if val.Format == mvdata.InvalidDataFormat {
				val = mvdata.FileDataLocation{Path: val.Path, Format: mvdata.CsvFile}
				srcLoc = val
			}

			srcOpts = mvdata.CsvOptions{Delim: delim}
		}

		if val.Format == mvdata.XlsxFile {
			// table name must match sheet name currently
			srcOpts = mvdata.XlsxOptions{SheetName: tableName}
		} else if val.Format == mvdata.JsonFile {
			srcOpts = mvdata.JSONOptions{TableName: tableName, SchFile: schemaFile}
		} else if val.Format == mvdata.ParquetFile {
			srcOpts = mvdata.ParquetOptions{TableName: tableName, SchFile: schemaFile}
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			srcLoc = val
		}

		if hasDelim {
			srcOpts = mvdata.CsvOptions{Delim: delim}
		}
	}

	var moveOp mvdata.TableImportOp
	switch {
	case apr.Contains(createParam):
		moveOp = mvdata.CreateOp
	case apr.Contains(replaceParam):
		moveOp = mvdata.ReplaceOp
	default:
		moveOp = mvdata.UpdateOp
	}

	if moveOp != mvdata.CreateOp {
		root, err := dEnv.WorkingRoot(ctx)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		_, exists, err := root.GetTable(ctx, tableName)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		if !exists {
			return nil, errhand.BuildDError("The following table could not be found: %s", tableName).Build()
		}
	}

	return &importOptions{
		operation:         moveOp,
		destTableName:     tableName,
		contOnErr:         contOnErr,
		force:             force,
		schFile:           schemaFile,
		nameMapper:        colMapper,
		primaryKeys:       pks,
		src:               srcLoc,
		srcOptions:        srcOpts,
		ignoreSkippedRows: ignore,
		disableFkChecks:   disableFks,
	}, nil

}

func validateImportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		return errhand.BuildDError("expected 1 or 2 arguments").SetPrintUsage().Build()
	}

	if apr.Contains(schemaParam) && apr.Contains(primaryKeyParam) {
		return errhand.BuildDError("parameters %s and %s are mutually exclusive", schemaParam, primaryKeyParam).Build()
	}

	if !apr.Contains(createParam) && !apr.Contains(updateParam) && !apr.Contains(replaceParam) {
		return errhand.BuildDError("Must include '-c' for initial table import or -u to update existing table or -r to replace existing table.").Build()
	}

	if apr.Contains(schemaParam) && !apr.Contains(createParam) {
		return errhand.BuildDError("fatal: " + schemaParam + " is not supported for update or replace operations").Build()
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ImportCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, importDocs, ap))
}

func (cmd ImportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{tableParam, "The new or existing table being imported to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{fileParam, "The file being imported. Supported file types are csv, psv, and nbf."})
	ap.SupportsFlag(createParam, "c", "Create a new table, or overwrite an existing table (with the -f flag) from the imported data.")
	ap.SupportsFlag(updateParam, "u", "Update an existing table with the imported data.")
	ap.SupportsFlag(forceParam, "f", "If a create operation is being executed, data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(replaceParam, "r", "Replace existing table with imported data while preserving the original schema.")
	ap.SupportsFlag(contOnErrParam, "", "Continue importing when row import errors are encountered.")
	ap.SupportsFlag(ignoreSkippedRows, "", "Ignore the skipped rows printed by the --continue flag.")
	ap.SupportsFlag(disableFkChecks, "", "Disables foreign key checks.")
	ap.SupportsString(schemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimiter for a csv style file with a non-comma delimiter.")
	return ap
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_IMPORT
}

// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, importDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	dEnv, err := commands.MaybeMigrateEnv(ctx, dEnv)

	var verr errhand.VerboseError
	if err != nil {
		verr = errhand.BuildDError("could not load manifest for gc").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = validateImportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mvOpts, verr := getImportMoveOptions(ctx, apr, dEnv)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		verr = errhand.BuildDError("Unable to get the working root value for this data repository.").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	rd, nDMErr := newImportDataReader(ctx, root, dEnv, mvOpts)
	if nDMErr != nil {
		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	wr, nDMErr := newImportSqlEngineMover(ctx, dEnv, rd.GetSchema(), mvOpts)
	if nDMErr != nil {
		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, err := move(ctx, rd, wr, mvOpts)
	if err != nil {
		if pipeline.IsTransformFailure(err) {
			bdr := errhand.BuildDError("\nA bad row was encountered while moving data.")
			r := pipeline.GetTransFailureSqlRow(err)

			if r != nil {
				bdr.AddDetails("Bad Row: " + sql.FormatRow(r))
			}

			details := pipeline.GetTransFailureDetails(err)

			bdr.AddDetails(details)
			bdr.AddDetails("These can be ignored using '--continue'")

			return commands.HandleVErrAndExitCode(bdr.Build(), usage)
		}

		verr = errhand.BuildDError("An error occurred moving data:\n").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	cli.PrintErrln()

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	cli.PrintErrln(color.CyanString("Import completed successfully."))

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

func newImportDataReader(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, impOpts *importOptions) (table.SqlRowReader, *mvdata.DataMoverCreationError) {
	var err error

	// Checks whether import destination table already exists. This can probably be simplified to not need a root value...
	ow, err := impOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	if ow {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: fmt.Errorf("%s already exists. Use -f to overwrite.", impOpts.DestName())}
	}

	rd, _, err := impOpts.src.NewReader(ctx, root, dEnv.FS, impOpts.srcOptions)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}

	return rd, nil
}

func newImportSqlEngineMover(ctx context.Context, dEnv *env.DoltEnv, rdSchema schema.Schema, imOpts *importOptions) (*mvdata.SqlEngineTableWriter, *mvdata.DataMoverCreationError) {
	moveOps := &mvdata.MoverOptions{Force: imOpts.force, TableToWriteTo: imOpts.destTableName, ContinueOnErr: imOpts.contOnErr, Operation: imOpts.operation, DisableFks: imOpts.disableFkChecks}

	// Returns the schema of the table to be created or the existing schema
	tableSchema, dmce := getImportSchema(ctx, dEnv, imOpts)
	if dmce != nil {
		return nil, dmce
	}

	// Validate that the schema from files has primary keys.
	err := tableSchema.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		preImage := imOpts.nameMapper.PreImage(col.Name)
		_, found := rdSchema.GetAllCols().GetByName(preImage)
		if !found {
			err = fmt.Errorf("input primary keys do not match primary keys of existing table")
		}
		return err == nil, err
	})
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
	}

	// construct the schema of the set of column to be updated.
	rowOperationColColl := schema.NewColCollection()
	rdSchema.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		wrColName := imOpts.nameMapper.Map(col.Name)
		wrCol, ok := tableSchema.GetAllCols().GetByName(wrColName)
		if ok {
			rowOperationColColl = rowOperationColColl.Append(wrCol)
		}

		return false, nil
	})

	rowOperationSchema, err := schema.SchemaFromCols(rowOperationColColl)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
	}

	mv, err := mvdata.NewSqlEngineTableWriter(ctx, dEnv, tableSchema, rowOperationSchema, moveOps, importStatsCB)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateWriterErr, Cause: err}
	}

	return mv, nil
}

type badRowFn func(trf *pipeline.TransformRowFailure) (quit bool)

func move(ctx context.Context, rd table.SqlRowReader, wr *mvdata.SqlEngineTableWriter, options *importOptions) (int64, error) {
	g, ctx := errgroup.WithContext(ctx)

	// Setup the necessary data points for the import job
	parsedRowChan := make(chan sql.Row)
	var rowErr error
	var printStarted bool
	var badCount int64
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !options.contOnErr {
			rowErr = trf
			return true
		}

		atomic.AddInt64(&badCount, 1)

		// Don't log the skipped rows when the ignore-skipped-rows param is specified.
		if options.ignoreSkippedRows {
			return false
		}

		if !printStarted {
			cli.PrintErrln("The following rows were skipped:")
			printStarted = true
		}

		r := pipeline.GetTransFailureSqlRow(trf)

		if r != nil {
			cli.PrintErr(sql.FormatRow(r))
		}

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
	rdSqlSch, err := sqlutil.FromDoltSchema(options.destTableName, rd.GetSchema())
	if err != nil {
		return err
	}

	for {
		sqlRow, err := rd.ReadSqlRow(ctx)
		if err == io.EOF {
			return nil
		}

		if err != nil {
			if table.IsBadRow(err) {
				trf := &pipeline.TransformRowFailure{Row: nil, SqlRow: sqlRow, TransformName: "reader", Details: err.Error()}
				quit := badRowCb(trf)
				if quit {
					return trf
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

func getImportSchema(ctx context.Context, dEnv *env.DoltEnv, impOpts *importOptions) (schema.Schema, *mvdata.DataMoverCreationError) {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
	}

	if impOpts.schFile != "" {
		tn, out, err := mvdata.SchAndTableNameFromFile(ctx, impOpts.schFile, dEnv.FS, root)

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
			// todo: capture stream data to file so we can use schema inferrence
			return nil, nil
		}

		rd, _, err := impOpts.src.NewReader(ctx, root, dEnv.FS, impOpts.srcOptions)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
		}
		defer rd.Close(ctx)

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
	tblRd, err := mvdata.NewSqlEngineReader(ctx, dEnv, impOpts.destTableName)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	defer tblRd.Close(ctx)

	return tblRd.GetSchema(), nil
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
		details := fmt.Sprintf("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.src.String(), mvOpts.destTableName)
		bdr.AddDetails(details)
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
		switch col.Type {
		case sql.Boolean, sql.Int8, sql.MustCreateBitType(1): // TODO: noms bool wraps MustCreateBitType
			switch row[i].(type) {
			case int8:
				val, ok := stringToBoolean(strconv.Itoa(int(row[i].(int8))))
				if ok {
					row[i] = val
				}
			case string:
				val, ok := stringToBoolean(row[i].(string))
				if ok {
					row[i] = val
				}
			case bool:
				row[i] = row[i].(bool)
			}
		}

		switch col.Type.(type) {
		case sql.StringType:
		default:
			row[i] = emptyStringToNil(row[i])
		}
	}

	return row, nil
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
		return true, false
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
