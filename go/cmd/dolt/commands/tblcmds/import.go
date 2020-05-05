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
	"fmt"
	"os"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/mvdata"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
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

type importArgs map[string]string

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

func getImportMoveOptions(apr *argparser.ArgParseResults) (*mvdata.MoveOptions, errhand.VerboseError) {
	tableName := apr.Arg(0)

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	delim, hasDelim := apr.GetValue(delimParam)
	fType, _ := apr.GetValue(fileTypeParam)

	schemaFile, _ := apr.GetValue(schemaParam)
	mappingFile, _ := apr.GetValue(mappingFileParam)
	primaryKey, _ := apr.GetValue(primaryKeyParam)
	force := apr.Contains(forceParam)

	srcLoc := mvdata.NewDataLocation(path, fType)

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

	var moveOp mvdata.MoveOperation
	switch {
	case apr.Contains(createParam):
		moveOp = mvdata.OverwriteOp
	case apr.Contains(replaceParam):
		moveOp = mvdata.ReplaceOp
	default:
		moveOp = mvdata.UpdateOp
	}

	tableLoc := mvdata.TableDataLocation{Name: tableName}

	return &mvdata.MoveOptions{
		Operation:   moveOp,
		TableName:   tableName,
		ContOnErr:   apr.Contains(contOnErrParam),
		Force:       force,
		SchFile:     schemaFile,
		MappingFile: mappingFile,
		PrimaryKey:  primaryKey,
		Src:         srcLoc,
		Dest:        tableLoc,
		SrcOptions:  srcOpts,
	}, nil

}

func validateImportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		return errhand.BuildDError("expected 1 or 2 arguments").SetPrintUsage().Build()
	}

	if !apr.Contains(createParam) && !apr.Contains(updateParam) && !apr.Contains(replaceParam) {
		return errhand.BuildDError("Must include '-c' for initial table import or -u to update existing table or -r to replace existing table.").Build()
	} else if apr.Contains(createParam) {
		// nothing to validate
	} else {
		if apr.Contains(schemaParam) {
			// TODO: do we want to support schema changes via `table import --schema`?
			return errhand.BuildDError("fatal: " + schemaParam + " is not supported for update or replace operations").Build()
		}
	}

	if apr.Contains(schemaParam) && apr.Contains(primaryKeyParam) {
		return errhand.BuildDError("parameters %s and %s are mutually exclusive", schemaParam, primaryKeyParam).Build()
	}

	tableName := apr.Arg(0)
	if err := ValidateTableNameForCreate(tableName); err != nil {
		return err
	}

	// TODO: validate table existence?

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	_, hasDelim := apr.GetValue(delimParam)
	fType, hasFileType := apr.GetValue(fileTypeParam)

	if hasFileType {
		if mvdata.DFFromString(fType) == mvdata.InvalidDataFormat {
			return errhand.BuildDError("'%s' is not a valid file type.", fType).Build()
		}
	}

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

	verr = executeMove(ctx, dEnv, mvOpts)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	cli.PrintErrln(color.CyanString("Import completed successfully."))
	return 0
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

func executeMove(ctx context.Context, dEnv *env.DoltEnv, mvOpts *mvdata.MoveOptions) errhand.VerboseError {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return errhand.BuildDError("Unable to get the working root value for this data repository.").AddCause(err).Build()
	}


	if mvOpts.WillOverwrite() {
		destExists, err := mvOpts.Dest.Exists(ctx, root, dEnv.FS)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		} else if destExists {
			return errhand.BuildDError("%s already exists. Use -f to overwrite.", mvOpts.Dest.String()).Build()
		}
	}

	mover, nDMErr := mvdata.NewDataMover(ctx, root, dEnv.FS, mvOpts, importStatsCB)

	if nDMErr != nil {
		return newDataMoverErrToVerr(mvOpts, nDMErr)
	}

	var badCount int64
	badCount, err = mover.Move(ctx)

	if displayStrLen > 0 {
		displayStrLen = 0
		cli.PrintErrln("")
	}

	if err != nil {
		if pipeline.IsTransformFailure(err) {
			bdr := errhand.BuildDError("A bad row was encountered while moving data.")

			r := pipeline.GetTransFailureRow(err)
			if r != nil {
				bdr.AddDetails("Bad Row:" + row.Fmt(ctx, r, mover.Rd.GetSchema()))
			}

			details := pipeline.GetTransFailureDetails(err)

			bdr.AddDetails(details)
			bdr.AddDetails("These can be ignored using the '--continue'")

			return bdr.Build()
		}
		return errhand.BuildDError("An error occurred moving data:\n").AddCause(err).Build()
	}

	if nomsWr, ok := mover.Wr.(noms.NomsMapWriteCloser); ok {
		tableDest := mvOpts.Dest.(mvdata.TableDataLocation)
		err = dEnv.PutTableToWorking(ctx, *nomsWr.GetMap(), nomsWr.GetSchema(), tableDest.Name)

		if err != nil {
			return errhand.BuildDError("Failed to update the working value.").AddCause(err).Build()
		}
	}

	if badCount > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", badCount))
	}

	return nil
}

func newDataMoverErrToVerr(mvOpts *mvdata.MoveOptions, err *mvdata.DataMoverCreationError) errhand.VerboseError {
	switch err.ErrType {
	case mvdata.CreateReaderErr:
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.Src.String())
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return bdr.AddCause(err.Cause).Build()

	// todo: this error may be obselete[
	case mvdata.NomsKindSchemaErr:
		bdr := errhand.BuildDError("Error creating schema.")
		bdr.AddDetails("Column given invalid kind. Valid kinds include : string, int, bool, float, null.")
		return bdr.AddCause(err.Cause).Build()

	case mvdata.SchemaErr:
		bdr := errhand.BuildDError("Error determining the output schema.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not determine the output schema.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(`Schema File: "%s"`, mvOpts.SchFile)
		bdr.AddDetails(`explicit pk: "%s"`, mvOpts.PrimaryKey)
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
