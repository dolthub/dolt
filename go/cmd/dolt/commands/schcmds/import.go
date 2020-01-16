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

package schcmds

import (
	"context"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	createFlag          = "create"
	updateFlag          = "update"
	replaceFlag         = "replace"
	dryRunFlag          = "dry-run"
	fileTypeParam       = "file-type"
	pksParam            = "pks"
	mappingParam        = "map"
	floatThresholdParam = "float-threshold"
	keepTypesParam      = "keep-types"
	delimParam          = "delim"
)

var schImportShortDesc = "Creates a new table with an inferred schema."
var schImportLongDesc = "If <b>--create | -c</b> is given the operation will create <table> with a schema that it infers" +
	"from the supplied file. One or more primary key columns must be specified using the <b>--pks</b> parameter.\n" +
	"\n" +
	//"If <b>--update | -u</b> is given the operation will update <table> any additional columns, or change the types of columns" +
	//"based on the file supplied.  If the <b>--keep-types</b> parameter is supplied then the types for existing columns will" +
	//"not be modified, even if they differ from what is in the supplied file." +
	//"\n" +
	"If <b>--replace | -r</b> is given the operation will replace <table> with a new, empty table which has a schema inferred from" +
	"the supplied file but columns tags will be maintained across schemas.  <b>--keep-types</b> can also be supplied here" +
	"to guarantee that types are the same in the file and in the pre-existing table.\n" +
	"\n" +
	"A mapping file can be used to map fields between the file being imported and the table's schema being inferred.  This can" +
	"be used when creating a new table, or updating or replacing an existing table.\n" +
	"\n" +
	tblcmds.MappingFileHelp +
	"\n" +
	"In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not" +
	"have the expected extension then the <b>--file-type</b> parameter should be used to explicitly define the format of" +
	"the file in one of the supported formats (Currently only csv is supported).  For files separated by a delimiter other than a" +
	"',', the --delim parameter can be used to specify a delimeter.\n" +
	"\n" +
	"If the parameter <b>--dry-run</b> is supplied a sql statement will be generated showing what would be executed if this" +
	"were run without the --dry-run flag\n" +
	"\n" +
	"<b>--float-threshold</b> is the threshold at which a string representing a floating point number should be interpreted as" +
	"a float versus an int.  If FloatThreshold is 0.0 then any number with a decimal point will be interpreted as a" +
	"float (such as 0.0, 1.0, etc).  If FloatThreshold is 1.0 then any number with a decimal point will be converted" +
	"to an int (0.5 will be the int 0, 1.99 will be the int 1, etc.  If the FloatThreshold is 0.001 then numbers with" +
	"a fractional component greater than or equal to 0.001 will be treated as a float (1.0 would be an int, 1.0009 would" +
	"be an int, 1.001 would be a float, 1.1 would be a float, etc)"

var schImportSynopsis = []string{
	"[--create|--replace] [--force] [--dry-run] [--lower|--upper] [--keep-types] [--file-type <type>] [--float-threshold] [--map <mapping-file>] [--delim <delimiter>]--pks <field>,... <table> <file>",
}

type importOp int

const (
	createOp importOp = iota
	updateOp
	replaceOp
)

type importArgs struct {
	op        importOp
	fileType  string
	fileName  string
	delim     string
	inferArgs *actions.InferenceArgs
}

type ImportCmd struct{}

func (cmd ImportCmd) Name() string {
	return "import"
}

func (cmd ImportCmd) Description() string {
	return "Creates a new table with an inferred schema."
}

func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

func (cmd ImportCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, schImportShortDesc, schImportLongDesc, schImportSynopsis, ap)
}

func (cmd ImportCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Name of the table to be created."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being used to infer the schema."})
	ap.SupportsFlag(createFlag, "c", "Create a table with the schema inferred from the <file> provided.")
	//ap.SupportsFlag(updateFlag, "u", "Update a table to match the inferred schema of the <file> provided")
	ap.SupportsFlag(replaceFlag, "r", "Replace a table with a new schema that has the inferred schema from the <file> provided. All previous data will be lost.")
	ap.SupportsFlag(dryRunFlag, "", "Print the sql statement that would be run if executed without the flag.")
	ap.SupportsFlag(keepTypesParam, "", "When a column already exists in the table, and it's also in the <file> provided, use the type from the table.")
	ap.SupportsString(fileTypeParam, "", "type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(pksParam, "", "comma-separated-col-names", "List of columns used as the primary key cols.  Order of the columns will determine sort order.")
	ap.SupportsString(mappingParam, "", "mapping-file", "A file that can map a column name in <file> to a new value.")
	ap.SupportsString(floatThresholdParam, "", "float", "Minimum value at which the fractional component of a value must exceed in order to be considered a float.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimiter for a csv style file with a non-comma delimiter.")
	return ap
}

// Exec implements the import schema command that will take a file and infer it's schema, and then create a table matching that schema.
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, schImportShortDesc, schImportLongDesc, schImportSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	verr := importSchema(ctx, dEnv, apr)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func importSchema(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr != nil {
		return verr
	}

	tblName := apr.Arg(0)
	fileName := apr.Arg(1)

	fileExists, _ := dEnv.FS.Exists(fileName)

	if !fileExists {
		return errhand.BuildDError("error: file '%s' not found.", fileName).Build()
	}

	op := createOp
	if !apr.ContainsAny(createFlag, updateFlag, replaceFlag) {
		return errhand.BuildDError("error: missing required parameter.").AddDetails("Must provide exactly one of the operation flags '--create', or '--replace'").SetPrintUsage().Build()
	} else if apr.Contains(updateFlag) {
		if apr.ContainsAny(createFlag, replaceFlag) {
			return errhand.BuildDError("error: multiple operations supplied").AddDetails("Only one of the flags '--create', '--update', or '--replace' may be provided").SetPrintUsage().Build()
		}
		op = updateOp
	} else if apr.Contains(replaceFlag) {
		if apr.Contains(createFlag) {
			return errhand.BuildDError("error: multiple operations supplied").AddDetails("Only one of the flags '--create', '--update', or '--replace' may be provided").SetPrintUsage().Build()
		}
		op = replaceOp
	} else {
		if apr.Contains(keepTypesParam) {
			return errhand.BuildDError("error: parameter keep-types not supported for create operations").AddDetails("keep-types parameter is used to keep the existing column types as is without modification.").Build()
		}
	}

	tbl, tblExists, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to read from database.").AddCause(err).Build()
	} else if tblExists && op == createOp {
		return errhand.BuildDError("error: failed to create table.").AddDetails("A table named '%s' already exists.", tblName).AddDetails("Use --replace or --update instead of --create.").Build()
	}

	var existingSch schema.Schema = schema.EmptySchema
	if tblExists {
		existingSch, err = tbl.GetSchema(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to read schema from '%s'", tblName).AddCause(err).Build()
		}
	}

	val, pksOK := apr.GetValue(pksParam)

	var pks []string
	if pksOK {
		pks = strings.Split(val, ",")
		goodPKS := make([]string, 0, len(pks))

		for _, pk := range pks {
			pk = strings.TrimSpace(pk)

			if pk != "" {
				goodPKS = append(goodPKS, pk)
			}
		}

		pks = goodPKS

		if len(pks) == 0 {
			return errhand.BuildDError("error: no valid columns provided in --pks argument").Build()
		}
	} else {
		return errhand.BuildDError("error: missing required parameter pks").SetPrintUsage().Build()
	}

	mappingFile := apr.GetValueOrDefault(mappingParam, "")

	var colMapper actions.StrMapper
	if mappingFile != "" {
		if mappingExists, _ := dEnv.FS.Exists(mappingFile); mappingExists {
			var m map[string]string
			err := filesys.UnmarshalJSONFile(dEnv.FS, mappingFile, &m)

			if err != nil {
				return errhand.BuildDError("error: invalid mapper file.").AddCause(err).Build()
			}

			colMapper = actions.MapMapper(m)
		} else {
			return errhand.BuildDError("error: '%s' does not exist.", mappingFile).Build()
		}
	} else {
		colMapper = actions.IdentityMapper{}
	}

	floatThresholdStr := apr.GetValueOrDefault(floatThresholdParam, "0.0")
	floatThreshold, err := strconv.ParseFloat(floatThresholdStr, 64)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not a valid float in the range 0.0 (all floats) to 1.0 (no floats)", floatThresholdStr).SetPrintUsage().Build()
	}

	delim := apr.GetValueOrDefault(delimParam, ",")

	impArgs := importArgs{
		op:       op,
		fileName: fileName,
		delim:    delim,
		fileType: apr.GetValueOrDefault(fileTypeParam, filepath.Ext(fileName)),
		inferArgs: &actions.InferenceArgs{
			ExistingSch:    existingSch,
			ColMapper:      colMapper,
			FloatThreshold: floatThreshold,
			KeepTypes:      apr.Contains(keepTypesParam),
		},
	}

	sch, verr := inferSchemaFromFile(ctx, dEnv.DoltDB.ValueReadWriter().Format(), pks, &impArgs)

	if verr != nil {
		return verr
	}

	cli.Println(sql.SchemaAsCreateStmt(tblName, sch))

	if !apr.Contains(dryRunFlag) {
		schVal, err := encoding.MarshalAsNomsValue(context.Background(), root.VRW(), sch)

		if err != nil {
			return errhand.BuildDError("error: failed to encode schema.").AddCause(err).Build()
		}

		m, err := types.NewMap(ctx, root.VRW())

		if err != nil {
			return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}

		tbl, err = doltdb.NewTable(ctx, root.VRW(), schVal, m)

		if err != nil {
			return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}

		root, err = root.PutTable(ctx, tblName, tbl)

		if err != nil {
			return errhand.BuildDError("error: failed to add table.").AddCause(err).Build()
		}

		err = dEnv.UpdateWorkingRoot(ctx, root)

		if err != nil {
			return errhand.BuildDError("error: failed to update the working set.").AddCause(err).Build()
		}

		cli.PrintErrln(color.CyanString("Created table successfully."))
	}

	return nil
}

func inferSchemaFromFile(ctx context.Context, nbf *types.NomsBinFormat, pkCols []string, args *importArgs) (schema.Schema, errhand.VerboseError) {
	if args.fileType[0] == '.' {
		args.fileType = args.fileType[1:]
	}

	var rd table.TableReadCloser
	switch args.fileType {
	case "csv":
		f, err := os.Open(args.fileName)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to open '%s'", args.fileName).Build()
		}

		defer f.Close()

		rd, err = csv.NewCSVReader(nbf, f, csv.NewCSVInfo().SetDelim(args.delim))

		if err != nil {
			return nil, errhand.BuildDError("error: failed to create a CSVReader.").AddCause(err).Build()
		}

		defer rd.Close(ctx)

	default:
		return nil, errhand.BuildDError("error: unsupported file type '%s'", args.fileType).Build()
	}

	sch, err := actions.InferSchemaFromTableReader(ctx, rd, pkCols, args.inferArgs)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to infer schema").AddCause(err).Build()
	}

	return sch, nil
}
