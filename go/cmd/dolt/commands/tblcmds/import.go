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

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/mvdata"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	createParam      = "create-table"
	updateParam      = "update-table"
	tableParam       = "table"
	fileParam        = "file"
	outSchemaParam   = "schema"
	mappingFileParam = "map"
	forceParam       = "force"
	contOnErrParam   = "continue"
	primaryKeyParam  = "pk"
	fileTypeParam    = "file-type"
)

var schemaFileHelp = "Schema definition files are json files in the format:" + `
{
	"<b>fields</b>": [
		{"name":"<b>FIELD_NAME</b>", "kind":"<b>KIND</b>", "Required":[true|false]},
		...
	],
	"<b>constraints</b>": [
		{"constraint_type":"primary_key", "field_indices":[<b>INTEGER_FIELD_INDEX</b>]}
	]
}
	where "fields" is the array of columns in each row of the table
	"constraints" is a list of table constraints.  (Only primary_key constraint types are supported currently)
	FIELD_NAME is the name of a column in a row and can be any valid string
	KIND must be a supported noms kind (bool, string, uuid, uint, int, float)
	INTEGER_FIELD_INDEX must be the 0 based index of the primary key in the "fields" array
`

var mappingFileHelp = "A mapping file is json in the format:" + `
{
	"<b>source_field_name</b>":"<b>dest_field_name</b>"
	...
}

where source_field_name is the name of a field in the file being imported
and dest_field_name is the name of a field in the table being imported to.
`

var importShortDesc = `Imports data into a dolt table`
var importLongDesc = `If <b>--create-table | -c</b> is given the operation will create <table> and import the contents of file into it.  If a
table already exists at this location then the operation will fail, unless the <b>--force | -f</b> flag is provided. The
force flag forces the existing table to be overwritten.

The schema for the new table can be specified explicitly by providing a schema definition file, or will be inferred 
from the imported file.  All schemas, inferred or explicitly defined must define a primary key.  If the file format 
being imported does not support defining a primary key, then the <b>--pk</b> parameter must supply the name of the 
field that should be used as the primary key.

` + schemaFileHelp +
	`
If <b>--update-table | -u</b> is given the operation will update <table> with the contents of file. The table's existing 
schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

During import, if there is an error importing any row, the import will be aborted by default.  Use the <b>--continue</b>
flag to continue importing when an error is encountered.

A mapping file can be used to map fields between the file being imported and the table being written to.  This can 
be used when creating a new table, or updating an existing table.

` + mappingFileHelp +
	`
In both create and update scenarios the file's extension is used to infer the type of the file.  If a file does not 
have the expected extension then the <b>--file-type</b> parameter should be used to explicitly define the format of 
the file in one of the supported formats (csv, psv, nbf, json, xlsx)`

var importSynopsis = []string{
	"-c [-f] [--pk <field>] [--schema <file>] [--map <file>] [--continue] [--file-type <type>] <table> <file>",
	"-u [--schema <file>] [--map <file>] [--continue] [--file-type <type>] <table> <file>",
}

func validateImportArgs(apr *argparser.ArgParseResults, usage cli.UsagePrinter) (mvdata.MoveOperation, *mvdata.DataLocation, *mvdata.DataLocation) {
	if apr.NArg() != 2 {
		usage()
		return mvdata.InvalidOp, nil, nil
	}

	var mvOp mvdata.MoveOperation
	if !apr.Contains(createParam) && !apr.Contains(updateParam) {
		cli.PrintErrln("Must include '-c' for initial table import or -u to update existing table.")
		return mvdata.InvalidOp, nil, nil
	} else if apr.Contains(createParam) {
		mvOp = mvdata.OverwriteOp
	} else {
		mvOp = mvdata.UpdateOp
		if apr.Contains(outSchemaParam) {
			cli.PrintErrln("fatal:", outSchemaParam+"is not supported for update operations")
			usage()
			return mvdata.InvalidOp, nil, nil
		}
	}

	tableName := apr.Arg(0)
	if !doltdb.IsValidTableName(tableName) {
		cli.PrintErrln(
			color.RedString("'%s' is not a valid table name\n", tableName),
			"table names must match the regular expression:", doltdb.TableNameRegexStr)
		return mvdata.InvalidOp, nil, nil
	}

	path := apr.Arg(1)
	fType, _ := apr.GetValue(fileTypeParam)
	fileLoc := mvdata.NewDataLocation(path, fType)

	if fileLoc.Format == mvdata.InvalidDataFormat {
		cli.PrintErrln(
			color.RedString("Could not infer type file '%s'\n", path),
			"File extensions should match supported file types, or should be explicitly defined via the file-type parameter")
		return mvdata.InvalidOp, nil, nil
	}

	tableLoc := &mvdata.DataLocation{Path: tableName, Format: mvdata.DoltDB}

	return mvOp, tableLoc, fileLoc
}

func Import(commandStr string, args []string, dEnv *env.DoltEnv) int {
	force, mvOpts := parseCreateArgs(commandStr, args)

	if mvOpts == nil {
		return 1
	}

	res := executeMove(dEnv, force, mvOpts)

	if res == 0 {
		cli.Println(color.CyanString("\nImport completed successfully."))
	}

	return res
}

func parseCreateArgs(commandStr string, args []string) (bool, *mvdata.MoveOptions) {
	ap := createArgParser()

	help, usage := cli.HelpAndUsagePrinters(commandStr, importShortDesc, importLongDesc, importSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	moveOp, tableLoc, fileLoc := validateImportArgs(apr, usage)

	if fileLoc == nil || tableLoc == nil {
		return false, nil
	}

	schemaFile, _ := apr.GetValue(outSchemaParam)
	mappingFile, _ := apr.GetValue(mappingFileParam)
	primaryKey, _ := apr.GetValue(primaryKeyParam)

	return apr.Contains(forceParam), &mvdata.MoveOptions{
		Operation:   moveOp,
		ContOnErr:   apr.Contains(contOnErrParam),
		SchFile:     schemaFile,
		MappingFile: mappingFile,
		PrimaryKey:  primaryKey,
		Src:         fileLoc,
		Dest:        tableLoc,
	}
}

func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp[tableParam] = "The new or existing table being imported to."
	ap.ArgListHelp[fileParam] = "The file being imported. Supported file types are csv, psv, and nbf."
	ap.SupportsFlag(createParam, "c", "Create a new table, or overwrite an existing table (with the -f flag) from the imported data.")
	ap.SupportsFlag(updateParam, "u", "Update an existing table with the imported data.")
	ap.SupportsFlag(forceParam, "f", "If a create operation is being executed, data already exists in the destination, the Force flag will allow the target to be overwritten.")
	ap.SupportsFlag(contOnErrParam, "", "Continue importing when row import errors are encountered.")
	ap.SupportsString(outSchemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	return ap
}

var displayStrLen int

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	displayStr := fmt.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

func executeMove(dEnv *env.DoltEnv, force bool, mvOpts *mvdata.MoveOptions) int {
	root, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		cli.PrintErrln(color.RedString("Unable to get the working root value for this data repository."))
		return 1
	}

	if mvOpts.Operation == mvdata.OverwriteOp && !force {
		if exists, err := mvOpts.Dest.Exists(context.TODO(), root, dEnv.FS); err != nil {
			cli.Println(color.RedString(err.Error()))
			return 1
		} else if exists {
			cli.PrintErrln(color.RedString("Data already exists in %s.  Use -f to overwrite.", mvOpts.Dest.Path))
			return 1
		}
	}

	if mvOpts.Src.Format == mvdata.SqlFile {
		cli.Println(color.RedString("For SQL import, please pipe SQL input files to `dolt sql`"))
		return 1
	}

	if mvOpts.Src.Format == mvdata.JsonFile && mvOpts.SchFile == "" {
		cli.Println(color.RedString("Please specify schema file for .json tables."))
		return 1
	}

	mover, nDMErr := mvdata.NewDataMover(context.TODO(), root, dEnv.FS, mvOpts, importStatsCB)

	if nDMErr != nil {
		verr := newDataMoverErrToVerr(mvOpts, nDMErr)
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	err = mover.Move(context.TODO())

	if err != nil {
		cli.Println()

		if pipeline.IsTransformFailure(err) {
			bdr := errhand.BuildDError("A bad row was encountered while moving data.")

			r := pipeline.GetTransFailureRow(err)
			if r != nil {
				bdr.AddDetails("Bad Row:" + row.Fmt(context.TODO(), r, mover.Rd.GetSchema()))
			}

			details := pipeline.GetTransFailureDetails(err)

			bdr.AddDetails(details)
			bdr.AddDetails("These can be ignored using the '--continue'")

			cli.PrintErrln(bdr.Build().Verbose())
		} else {
			cli.PrintErrln("An error occurred moving data:\n", err.Error())
		}

		return 1
	}

	if nomsWr, ok := mover.Wr.(noms.NomsMapWriteCloser); ok {
		err = dEnv.PutTableToWorking(context.Background(), *nomsWr.GetMap(), nomsWr.GetSchema(), mvOpts.Dest.Path)

		if err != nil {
			cli.PrintErrln(color.RedString("Failed to update the working value."))
			return 1
		}
	}

	return 0
}

func newDataMoverErrToVerr(mvOpts *mvdata.MoveOptions, err *mvdata.DataMoverCreationError) errhand.VerboseError {
	switch err.ErrType {
	case mvdata.CreateReaderErr:
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.Src.Path)
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return bdr.AddCause(err.Cause).Build()

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

	case mvdata.CreateMapperErr:
		bdr := errhand.BuildDError("Error creating input to output mapper.")
		details := fmt.Sprintf("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(details)
		bdr.AddCause(err.Cause)

		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateWriterErr:
		if err.Cause == mvdata.ErrNoPK {
			builder := errhand.BuildDError("Attempting to write to a %s with a schema that does not contain a primary key.", mvOpts.Dest.Format.ReadableStr())
			builder.AddDetails("A primary key is required and can be specified by:\n" +
				"\tusing -pk option to designate a field as the primary key by name.\n" +
				"\tusing -schema to provide a schema descriptor file.")
			return builder.Build()
		} else {
			bdr := errhand.BuildDError("Error creating writer for %s.\n", mvOpts.Dest.Path)
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
