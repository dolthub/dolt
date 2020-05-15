// Copyright 2019 Liquidata, Inc.
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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/mvdata"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	createParam      = "create-table"
	updateParam      = "update-table"
	replaceParam     = "replace-table"
	tableParam       = "table"
	fileParam        = "file"
	schemaParam      = "schema"
	mappingFileParam = "map"
	forceParam       = "force"
	contOnErrParam   = "continue"
	primaryKeyParam  = "pk"
	fileTypeParam    = "file-type"
	delimParam       = "delim"
)

var SchemaFileHelp = "Schema definition files are json files in the format:" + `

	{
		"fields": [
			{"name":"FIELD_NAME", "kind":"KIND", "Required":[true|false]},
			...
		],
		"constraints": [
			{"constraint_type":"primary_key", "field_indices":[INTEGER_FIELD_INDEX]}
		]
	}

where "fields" is the array of columns in each row of the table "constraints" is a list of table constraints. Only primary_key constraint types are supported currently. FIELD_NAME is the name of a column in a row and can be any valid string KIND must be a supported noms kind (bool, string, uuid, uint, int, float) INTEGER_FIELD_INDEX must be the 0 based index of the primary key in the "fields" array
`

var MappingFileHelp = "A mapping file is json in the format:" + `

	{
		"source_field_name":"dest_field_name"
		...
	}

where source_field_name is the name of a field in the file being imported and dest_field_name is the name of a field in the table being imported to.
`

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: `Imports data into a dolt table`,
	LongDesc: `If {{.EmphasisLeft}}--create-table | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} and import the contents of file into it.  If a table already exists at this location then the operation will fail, unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag is provided. The force flag forces the existing table to be overwritten.

The schema for the new table can be specified explicitly by providing a schema definition file, or will be inferred from the imported file.  All schemas, inferred or explicitly defined must define a primary key.  If the file format being imported does not support defining a primary key, then the {{.EmphasisLeft}}--pk{{.EmphasisRight}} parameter must supply the name of the field that should be used as the primary key.

` + SchemaFileHelp +
		`
If {{.EmphasisLeft}}--update-table | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} with the contents of file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

During import, if there is an error importing any row, the import will be aborted by default.  Use the {{.EmphasisLeft}}--continue{{.EmphasisRight}} flag to continue importing when an error is encountered.

If {{.EmphasisLeft}}--replace-table | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with the contents of the file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If the schema for the existing table does not match the schema for the new file, the import will be aborted by default. To overwrite both the table and the schema, use {{.EmphasisLeft}}-c -f{{.EmphasisRight}}.

A mapping file can be used to map fields between the file being imported and the table being written to. This can be used when creating a new table, or updating or replacing an existing table.

` + MappingFileHelp +

		`
In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (csv, psv, json, xlsx).  For files separated by a delimiter other than a ',' (type csv) or a '|' (type psv), the --delim parameter can be used to specify a delimeter`,

	Synopsis: []string{
		"-c [-f] [--pk {{.LessThan}}field{{.GreaterThan}}] [--schema {{.LessThan}}file{{.GreaterThan}}] [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-u [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-r [--map {{.LessThan}}file{{.GreaterThan}}] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

type tableImportOp string

const (
	CreateOp  tableImportOp = "overwrite" // todo: make CreateOp?
	ReplaceOp tableImportOp = "replace"
	UpdateOp  tableImportOp = "update"
	InvalidOp tableImportOp = "invalid"
)

type importOptions struct {
	Operation   tableImportOp
	TableName   string
	ContOnErr   bool
	Force       bool
	SchFile     string
	MappingFile string
	PrimaryKeys []string
	Src         mvdata.DataLocation
	Dest        mvdata.TableDataLocation
	SrcOptions  interface{}
}

func (m importOptions) checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if !m.Force && m.Operation == CreateOp {
		return m.Dest.Exists(ctx, root, fs)
	}
	return false, nil
}

func (m importOptions) WritesToTable() bool {
	return true
}

func (m importOptions) SrcName() string {
	if t, tblSrc := m.Src.(mvdata.TableDataLocation); tblSrc {
		return t.Name
	}
	if f, fileSrc := m.Src.(mvdata.FileDataLocation); fileSrc {
		return f.Path
	}
	return m.Src.String()
}

func (m importOptions) DestName() string {
	return m.Dest.Name
}

func getImportMoveOptions(apr *argparser.ArgParseResults) (*importOptions, errhand.VerboseError) {
	tableName := apr.Arg(0)

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	delim, hasDelim := apr.GetValue(delimParam)
	fType, _ := apr.GetValue(fileTypeParam)

	schemaFile, _ := apr.GetValue(schemaParam)
	mappingFile, _ := apr.GetValue(mappingFileParam)
	force := apr.Contains(forceParam)

	srcLoc := mvdata.NewDataLocation(path, fType)

	val, _ := apr.GetValue(primaryKeyParam)
	pks := funcitr.MapStrings(strings.Split(val, ","), strings.TrimSpace)
	pks = funcitr.FilterStrings(pks, func(s string) bool { return s != "" })

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

	var moveOp tableImportOp
	switch {
	case apr.Contains(createParam):
		moveOp = CreateOp
	case apr.Contains(replaceParam):
		moveOp = ReplaceOp
	default:
		moveOp = UpdateOp
	}

	tableLoc := mvdata.TableDataLocation{Name: tableName}

	return &importOptions{
		Operation:   moveOp,
		TableName:   tableName,
		ContOnErr:   apr.Contains(contOnErrParam),
		Force:       force,
		SchFile:     schemaFile,
		MappingFile: mappingFile,
		PrimaryKeys: pks,
		Src:         srcLoc,
		Dest:        tableLoc,
		SrcOptions:  srcOpts,
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
	if err := ValidateTableNameForCreate(tableName); err != nil {
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

	case mvdata.TableDataLocation:
		if hasDelim {
			return errhand.BuildDError("delim is not a valid parameter for this type of file").Build()
		}
	}

	if srcFileLoc, isFileType := srcLoc.(mvdata.FileDataLocation); isFileType {
		if srcFileLoc.Format == mvdata.SqlFile {
			return errhand.BuildDError("For SQL import, please pipe SQL input files to `dolt sql`").Build()
		}

		_, hasSchema := apr.GetValue(schemaParam)
		if srcFileLoc.Format == mvdata.JsonFile && apr.Contains(createParam) && !hasSchema {
			return errhand.BuildDError("Please specify schema file for .json tables.").Build()
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
func (cmd ImportCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, importDocs, ap))
}

func (cmd ImportCmd) createArgParser() *argparser.ArgParser {
	ap := createArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_IMPORT
}

// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, importDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	verr := validateImportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mvOpts, verr := getImportMoveOptions(apr)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		verr = errhand.BuildDError("Unable to get the working root value for this data repository.").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mover, nDMErr := newImportDataMover(ctx, root, dEnv.FS, mvOpts, importStatsCB)

	if nDMErr != nil {
		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, verr := mvdata.MoveData(ctx, dEnv, mover, mvOpts)
	//verr = executeImport(ctx, dEnv, mvOpts)

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = buildIndexes(ctx, dEnv, mvOpts.TableName)

	if verr == nil {
		cli.PrintErrln(color.CyanString("Import completed successfully."))
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{tableParam, "The new or existing table being imported to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{fileParam, "The file being imported. Supported file types are csv, psv, and nbf."})
	ap.SupportsFlag(createParam, "c", "Create a new table, or overwrite an existing table (with the -f flag) from the imported data.")
	ap.SupportsFlag(updateParam, "u", "Update an existing table with the imported data.")
	ap.SupportsFlag(forceParam, "f", "If a create operation is being executed, data already exists in the destination, the Force flag will allow the target to be overwritten.")
	ap.SupportsFlag(replaceParam, "r", "Replace existing table with imported data while preserving the original schema.")
	ap.SupportsFlag(contOnErrParam, "", "Continue importing when row import errors are encountered.")
	ap.SupportsString(schemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimeter for a csv style file with a non-comma delimiter.")
	return ap
}

var displayStrLen int

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	displayStr := fmt.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

func newImportDataMover(ctx context.Context, root *doltdb.RootValue, fs filesys.Filesys, impOpts *importOptions, statsCB noms.StatsCB) (*mvdata.DataMover, *mvdata.DataMoverCreationError) {
	var outSch schema.Schema
	var err error

	ow, err := impOpts.checkOverwrite(ctx, root, fs)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{mvdata.CreateReaderErr, err}
	}
	if ow {
		return nil, &mvdata.DataMoverCreationError{mvdata.CreateReaderErr, fmt.Errorf("%s already exists. Use -f to overwrite.", impOpts.DestName())}
	}

	if impOpts.SchFile != "" {
		var tn string
		tn, outSch, err = mvdata.SchAndTableNameFromFile(ctx, impOpts.SchFile, fs, root)
		if err == nil && tn != impOpts.TableName {
			err = fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, impOpts.SchFile, impOpts.TableName)
		}
	} else {
		var dmce *mvdata.DataMoverCreationError
		outSch, dmce = getImportSchema(ctx, root, fs, impOpts)
		if dmce != nil {
			return nil, dmce
		}
	}

	var rd table.TableReadCloser

	rd, srcIsSorted, err := impOpts.Src.NewReader(ctx, root, fs, impOpts.SrcOptions)

	if err != nil {
		return nil, &mvdata.DataMoverCreationError{mvdata.CreateReaderErr, err}
	}

	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	inSch := rd.GetSchema()

	if outSch == schema.EmptySchema {
		outSch = inSch
	}

	transforms, dmce := mvdata.MaybeMapFields(inSch, outSch, fs, impOpts.MappingFile)

	if dmce != nil {
		return nil, &mvdata.DataMoverCreationError{mvdata.CreateMapperErr, err}
	}

	var wr table.TableWriteCloser
	switch impOpts.Operation {
	case CreateOp:
		wr, err = impOpts.Dest.NewCreatingWriter(ctx, impOpts, root, fs, srcIsSorted, outSch, statsCB)
	case ReplaceOp:
		wr, err = impOpts.Dest.NewReplacingWriter(ctx, impOpts, root, fs, srcIsSorted, outSch, statsCB)
	case UpdateOp:
		wr, err = impOpts.Dest.NewUpdatingWriter(ctx, impOpts, root, fs, srcIsSorted, outSch, statsCB)
	default:
		err = errors.New("invalid move operation")
	}

	if err != nil {
		return nil, &mvdata.DataMoverCreationError{mvdata.CreateWriterErr, err}
	}

	imp := &mvdata.DataMover{rd, transforms, wr, impOpts.ContOnErr}
	rd = nil

	return imp, nil
}

func getImportSchema(ctx context.Context, root *doltdb.RootValue, fs filesys.Filesys, impOpts *importOptions) (schema.Schema, *mvdata.DataMoverCreationError) {
	var outSch schema.Schema
	if impOpts.Operation == CreateOp {
		_, isJson := impOpts.SrcOptions.(mvdata.JSONOptions)
		_, isStream := impOpts.Src.(mvdata.StreamDataLocation)

		if isJson || isStream {
			// used reader's schema
			return schema.EmptySchema, nil
		}

		rd, _, err := impOpts.Src.NewReader(ctx, root, fs, impOpts.SrcOptions)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{mvdata.SchemaErr, err}
		}
		defer rd.Close(ctx)

		outSch, err = inferSchema(ctx, root, fs, rd, impOpts)
	} else {
		tblRd, _, err := impOpts.Dest.NewReader(ctx, root, fs, nil)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{mvdata.SchemaErr, err}
		}
		defer tblRd.Close(ctx)
		outSch = tblRd.GetSchema()
	}

	return outSch, nil
}

func inferSchema(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, rd table.TableReadCloser, impOpts *importOptions) (schema.Schema, error) {
	var err error

	pks := impOpts.PrimaryKeys
	if len(pks) == 0 {
		pks = rd.GetSchema().GetPKCols().GetColumnNames()
	}

	cm, verr := actions.StrMapperFromFile(impOpts.SchFile, fs)

	if verr != nil {
		return nil, verr
	}

	infCols, err := actions.InferColumnTypesFromTableReader(ctx, root, rd, &actions.InferenceArgs{ColMapper: cm})

	if err != nil {
		return nil, err
	}

	pkSet := set.NewStrSet(pks)
	newCols, _ := schema.MapColCollection(infCols, func(col schema.Column) (schema.Column, error) {
		col.IsPartOfPK = pkSet.Contains(col.Name)
		return col, nil
	})

	newCols, err = root.GenerateTagsForNewColColl(ctx, impOpts.TableName, newCols)
	if err != nil {
		return nil, errhand.BuildDError("failed to generate new schema").AddCause(err).Build()
	}

	return schema.SchemaFromCols(newCols), nil
}

func buildIndexes(ctx context.Context, dEnv *env.DoltEnv, newTblName string) errhand.VerboseError {
	//TODO: change this to not use the executeImport function, and instead the SQL code path, so that we don't rebuild indexes on every import
	newWorking, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return errhand.BuildDError("Unable to load the working set to build the indexes.").AddCause(err).Build()
	}
	updatedTable, ok, err := newWorking.GetTable(ctx, newTblName)
	if err != nil {
		return errhand.BuildDError("Unable to load the table to build the indexes.").AddCause(err).Build()
	} else if !ok {
		return errhand.BuildDError("Unable to find the table to build the indexes.").Build()
	}
	updatedTable, err = updatedTable.RebuildIndexData(ctx)
	if err != nil {
		return errhand.BuildDError("Unable to build the indexes.").AddCause(err).Build()
	}
	newWorking, err = newWorking.PutTable(ctx, newTblName, updatedTable)
	if err != nil {
		return errhand.BuildDError("Unable to write the indexes to the working set.").AddCause(err).Build()
	}
	err = dEnv.UpdateWorkingRoot(ctx, newWorking)
	if err != nil {
		return errhand.BuildDError("Unable to update the working set containing the indexes.").AddCause(err).Build()
	}
	return nil
}

func newDataMoverErrToVerr(mvOpts *importOptions, err *mvdata.DataMoverCreationError) errhand.VerboseError {
	switch err.ErrType {
	case mvdata.CreateReaderErr:
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.Src.String())
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return bdr.AddCause(err.Cause).Build()

	case mvdata.SchemaErr:
		bdr := errhand.BuildDError("Error determining the output schema.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not determine the output schema.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(`Schema File: "%s"`, mvOpts.SchFile)
		bdr.AddDetails(`explicit pks: "%s"`, strings.Join(mvOpts.PrimaryKeys, ","))
		return bdr.AddCause(err.Cause).Build()

	case mvdata.MappingErr:
		bdr := errhand.BuildDError("Error determining the mapping from input fields to output fields.")
		bdr.AddDetails("When attempting to move data from %s to %s, determine the mapping from input fields t, output fields.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(`Mapping File: "%s"`, mvOpts.MappingFile)
		return bdr.AddCause(err.Cause).Build()

	case mvdata.ReplacingErr:
		bdr := errhand.BuildDError("Error replacing table")
		bdr.AddDetails("When attempting to replace data with %s, could not validate schema.", mvOpts.Src.String())
		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateMapperErr:
		bdr := errhand.BuildDError("Error creating input to output mapper.")
		details := fmt.Sprintf("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(details)
		bdr.AddCause(err.Cause)

		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateWriterErr:
		if err.Cause == mvdata.ErrNoPK {
			builder := errhand.BuildDError("Attempting to write to %s with a schema that does not contain a primary key.", mvOpts.Dest.String())
			builder.AddDetails("A primary key is required and can be specified by:\n" +
				"\tusing -pk option to designate a field as the primary key by name.\n" +
				"\tusing -schema to provide a schema descriptor file.")
			return builder.Build()
		} else {
			bdr := errhand.BuildDError("Error creating writer for %s.\n", mvOpts.Dest.String())
			bdr.AddDetails("When attempting to move data from %s to %s, could not open a writer.", mvOpts.Src.String(), mvOpts.Dest.String())
			return bdr.AddCause(err.Cause).Build()
		}

	case mvdata.CreateSorterErr:
		bdr := errhand.BuildDError("Error creating sorting reader.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not open create sorting reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return bdr.AddCause(err.Cause).Build()
	}

	panic("Unhandled Error type")
}
