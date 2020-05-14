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
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
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

var schImportDocs = cli.CommandDocumentationContent{
	ShortDesc: "Creates a new table with an inferred schema.",
	LongDesc: `If {{.EmphasisLeft}}--create | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} with a schema that it infers from the supplied file. One or more primary key columns must be specified using the {{.EmphasisLeft}}--pks{{.EmphasisRight}} parameter.

If {{.EmphasisLeft}}--update | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} any additional columns, or change the types of columns based on the file supplied.  If the {{.EmphasisLeft}}--keep-types{{.EmphasisRight}} parameter is supplied then the types for existing columns will not be modified, even if they differ from what is in the supplied file.

If {{.EmphasisLeft}}--replace | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with a new, empty table which has a schema inferred from the supplied file but columns tags will be maintained across schemas.  {{.EmphasisLeft}}--keep-types{{.EmphasisRight}} can also be supplied here to guarantee that types are the same in the file and in the pre-existing table.

A mapping file can be used to map fields between the file being imported and the table's schema being inferred.  This can be used when creating a new table, or updating or replacing an existing table.

` + tblcmds.MappingFileHelp + `

In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (Currently only csv is supported).  For files separated by a delimiter other than a ',', the --delim parameter can be used to specify a delimeter.

If the parameter {{.EmphasisLeft}}--dry-run{{.EmphasisRight}} is supplied a sql statement will be generated showing what would be executed if this were run without the --dry-run flag

{{.EmphasisLeft}}--float-threshold{{.EmphasisRight}} is the threshold at which a string representing a floating point number should be interpreted as a float versus an int.  If FloatThreshold is 0.0 then any number with a decimal point will be interpreted as a float (such as 0.0, 1.0, etc).  If FloatThreshold is 1.0 then any number with a decimal point will be converted to an int (0.5 will be the int 0, 1.99 will be the int 1, etc.  If the FloatThreshold is 0.001 then numbers with a fractional component greater than or equal to 0.001 will be treated as a float (1.0 would be an int, 1.0009 would be an int, 1.001 would be a float, 1.1 would be a float, etc)
`,

	Synopsis: []string{
		`[--create|--replace] [--force] [--dry-run] [--lower|--upper] [--keep-types] [--file-type <type>] [--float-threshold] [--map {{.LessThan}}mapping-file{{.GreaterThan}}] [--delim {{.LessThan}}delimiter{{.GreaterThan}}]--pks {{.LessThan}}field{{.GreaterThan}},... {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}`,
	},
}

type importArgs struct {
	op          SchImportOp
	fileName    string
	fileType    string
	delim       string
	tableName   string
	existingSch schema.Schema
	pkCols      []string
	keepTypes   bool
	inferArgs   *actions.InferenceArgs
}

type SchImportOp int

const (
	CreateOp SchImportOp = iota
	UpdateOp
	ReplaceOp
)

type ImportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ImportCmd) Name() string {
	return "import"
}

// Description returns a description of the command
func (cmd ImportCmd) Description() string {
	return "Creates a new table with an inferred schema."
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ImportCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, schImportDocs, ap))
}

func (cmd ImportCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Name of the table to be created."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being used to infer the schema."})
	ap.SupportsFlag(createFlag, "c", "Create a table with the schema inferred from the {{.LessThan}}file{{.GreaterThan}} provided.")
	ap.SupportsFlag(updateFlag, "u", "Update a table to match the inferred schema of the {{.LessThan}}file{{.GreaterThan}} provided")
	ap.SupportsFlag(replaceFlag, "r", "Replace a table with a new schema that has the inferred schema from the {{.LessThan}}file{{.GreaterThan}} provided. All previous data will be lost.")
	ap.SupportsFlag(dryRunFlag, "", "Print the sql statement that would be run if executed without the flag.")
	ap.SupportsFlag(keepTypesParam, "", "When a column already exists in the table, and it's also in the {{.LessThan}}file{{.GreaterThan}} provided, use the type from the table.")
	ap.SupportsString(fileTypeParam, "", "type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(pksParam, "", "comma-separated-col-names", "List of columns used as the primary key cols.  Order of the columns will determine sort order.")
	ap.SupportsString(mappingParam, "", "mapping-file", "A file that can map a column name in {{.LessThan}}file{{.GreaterThan}} to a new value.")
	ap.SupportsString(floatThresholdParam, "", "float", "Minimum value at which the fractional component of a value must exceed in order to be considered a float.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimiter for a csv style file with a non-comma delimiter.")
	return ap
}

// Exec implements the import schema command that will take a file and infer it's schema, and then create a table matching that schema.
// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, schImportDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	return commands.HandleVErrAndExitCode(importSchema(ctx, dEnv, apr), usage)
}

func getSchemaImportArgs(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, root *doltdb.RootValue) (*importArgs, errhand.VerboseError) {
	tblName := apr.Arg(0)
	fileName := apr.Arg(1)

	fileExists, _ := dEnv.FS.Exists(fileName)

	if !fileExists {
		return nil, errhand.BuildDError("error: file '%s' not found.", fileName).Build()
	}

	if err := tblcmds.ValidateTableNameForCreate(tblName); err != nil {
		return nil, err
	}

	flags := apr.ContainsMany(createFlag, updateFlag, replaceFlag)
	if len(flags) == 0 {
		return nil, errhand.BuildDError("error: missing required parameter.").AddDetails("Must provide exactly one of the operation flags '--create', or '--replace'").SetPrintUsage().Build()
	} else if len(flags) > 1 {
		return nil, errhand.BuildDError("error: multiple operations supplied").AddDetails("Only one of the flags '--create', '--update', or '--replace' may be provided").SetPrintUsage().Build()
	}

	var op SchImportOp
	switch flags[0] {
	case createFlag:
		op = CreateOp
	case updateFlag:
		op = UpdateOp
	case replaceFlag:
		op = ReplaceOp
	}

	if apr.Contains(keepTypesParam) && op == CreateOp {
		return nil, errhand.BuildDError("error: parameter keep-types not supported for create operations").AddDetails("keep-types parameter is used to keep the existing column types as is without modification.").Build()
	}

	tbl, tblExists, err := root.GetTable(ctx, tblName)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to read from database.").AddCause(err).Build()
	} else if tblExists && op == CreateOp {
		return nil, errhand.BuildDError("error: failed to create table.").AddDetails("A table named '%s' already exists.", tblName).AddDetails("Use --replace or --update instead of --create.").Build()
	}

	var existingSch schema.Schema = schema.EmptySchema
	if tblExists {
		existingSch, err = tbl.GetSchema(ctx)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to read schema from '%s'", tblName).AddCause(err).Build()
		}
	}

	val, pksOK := apr.GetValue(pksParam)
	pks := funcitr.MapStrings(strings.Split(val, ","), strings.TrimSpace)
	pks = funcitr.FilterStrings(pks, func(s string) bool { return s != "" })

	if !pksOK {
		return nil, errhand.BuildDError("error: missing required parameter pks").SetPrintUsage().Build()
	}
	if len(pks) == 0 {
		return nil, errhand.BuildDError("error: no valid columns provided in --pks argument").Build()
	}

	mappingFile := apr.GetValueOrDefault(mappingParam, "")

	var colMapper actions.StrMapper
	if mappingFile != "" {
		if mappingExists, _ := dEnv.FS.Exists(mappingFile); mappingExists {
			var m map[string]string
			err := filesys.UnmarshalJSONFile(dEnv.FS, mappingFile, &m)

			if err != nil {
				return nil, errhand.BuildDError("error: invalid mapper file.").AddCause(err).Build()
			}

			colMapper = actions.MapMapper(m)
		} else {
			return nil, errhand.BuildDError("error: '%s' does not exist.", mappingFile).Build()
		}
	} else {
		colMapper = actions.IdentityMapper{}
	}

	floatThresholdStr := apr.GetValueOrDefault(floatThresholdParam, "0.0")
	floatThreshold, err := strconv.ParseFloat(floatThresholdStr, 64)

	if err != nil {
		return nil, errhand.BuildDError("error: '%s' is not a valid float in the range 0.0 (all floats) to 1.0 (no floats)", floatThresholdStr).SetPrintUsage().Build()
	}

	return &importArgs{
		op:          op,
		fileName:    fileName,
		fileType:    apr.GetValueOrDefault(fileTypeParam, filepath.Ext(fileName)),
		delim:       apr.GetValueOrDefault(delimParam, ","),
		tableName:   tblName,
		existingSch: existingSch,
		pkCols:      pks,
		keepTypes:   apr.Contains(keepTypesParam),
		inferArgs: &actions.InferenceArgs{
			ColMapper:      colMapper,
			FloatThreshold: floatThreshold,
		},
	}, nil
}

func importSchema(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr != nil {
		return verr
	}

	impArgs, verr := getSchemaImportArgs(ctx, apr, dEnv, root)

	if verr != nil {
		return verr
	}

	sch, verr := inferSchemaFromFile(ctx, dEnv.DoltDB.ValueReadWriter().Format(), impArgs, root)

	if verr != nil {
		return verr
	}

	tblName := impArgs.tableName
	cli.Println(sqlfmt.SchemaAsCreateStmt(tblName, sch))

	if !apr.Contains(dryRunFlag) {
		tbl, tblExists, err := root.GetTable(ctx, tblName)

		schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), root.VRW(), sch)

		if err != nil {
			return errhand.BuildDError("error: failed to encode schema.").AddCause(err).Build()
		}

		m, err := types.NewMap(ctx, root.VRW())

		if err != nil {
			return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}

		var indexData *types.Map
		if tblExists {
			existingIndexData, err := tbl.GetIndexData(ctx)
			if err != nil {
				return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
			}
			indexData = &existingIndexData
		}

		tbl, err = doltdb.NewTable(ctx, root.VRW(), schVal, m, indexData)

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

func inferSchemaFromFile(ctx context.Context, nbf *types.NomsBinFormat, args *importArgs, root *doltdb.RootValue) (schema.Schema, errhand.VerboseError) {
	if args.fileType[0] == '.' {
		args.fileType = args.fileType[1:]
	}

	var rd table.TableReadCloser
	csvInfo := csv.NewCSVInfo().SetDelim(",")

	switch args.fileType {
	case "csv":
		if args.delim != "" {
			csvInfo.SetDelim(args.delim)
		}
	case "psv":
		csvInfo.SetDelim("|")
	default:
		return nil, errhand.BuildDError("error: unsupported file type '%s'", args.fileType).Build()
	}

	f, err := os.Open(args.fileName)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to open '%s'", args.fileName).Build()
	}

	defer f.Close()

	rd, err = csv.NewCSVReader(nbf, f, csvInfo)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to create a CSVReader.").AddCause(err).Build()
	}

	defer rd.Close(ctx)

	infCols, err := actions.InferColumnTypesFromTableReader(ctx, rd, args.inferArgs, root)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to infer schema").AddCause(err).Build()
	}

	return combineColCollections(ctx, root, infCols, args)
}

func combineColCollections(ctx context.Context, root *doltdb.RootValue, inferrerCols *schema.ColCollection, args *importArgs) (schema.Schema, errhand.VerboseError) {
	existingCols := args.existingSch.GetAllCols()

	existingColNames := set.NewStrSet(existingCols.GetColumnNames())
	inferredColNames := set.NewStrSet(inferrerCols.GetColumnNames())

	// (L - R), (L ∩ R), (R - L)
	left, inter, right := existingColNames.LeftIntersectionRight(inferredColNames)

	// sameType is the (name) subset of the intersection
	// where the inferred type equals the existing type
	// we will use the existing columns for this subset
	sameType := set.NewStrSet(nil)
	inter.Iterate(func(colName string) (cont bool) {
		ec, _ := existingCols.GetByName(colName)
		ic, _ := inferrerCols.GetByName(colName)
		if ec.TypeInfo.Equals(ic.TypeInfo) {
			sameType.Add(colName)
		}
		return true
	})

	// oldCols is the subset of existingCols that will be kept in the new schema
	var oldCols *schema.ColCollection
	// newCols is the subset of inferrerCols that will be added to the new schema
	var newCols *schema.ColCollection

	switch {
	case args.op == CreateOp:
		oldCols = schema.EmptyColColl
		pks := set.NewStrSet(args.pkCols)
		newCols, _ = schema.MapColCollection(inferrerCols, func(col schema.Column) (schema.Column, error) {
			col.IsPartOfPK = pks.Contains(col.Name)
			return col, nil
		})

	case args.op == UpdateOp && args.keepTypes:
		oldCols = existingCols
		newCols, _ = schema.FilterColCollection(inferrerCols, func(col schema.Column) (bool, error) {
			return right.Contains(col.Name), nil
		})

	case args.op == UpdateOp:
		oldCols, _ = schema.FilterColCollection(existingCols, func(col schema.Column) (bool, error) {
			return left.Contains(col.Name) || sameType.Contains(col.Name), nil
		})
		newCols, _ = schema.FilterColCollection(inferrerCols, func(col schema.Column) (bool, error) {
			return !sameType.Contains(col.Name), nil
		})

	case args.op == ReplaceOp && args.keepTypes:
		oldCols, _ = schema.FilterColCollection(existingCols, func(col schema.Column) (bool, error) {
			return inter.Contains(col.Name), nil
		})
		newCols, _ = schema.FilterColCollection(inferrerCols, func(col schema.Column) (bool, error) {
			return right.Contains(col.Name), nil
		})

	case args.op == ReplaceOp:
		oldCols, _ = schema.FilterColCollection(existingCols, func(col schema.Column) (bool, error) {
			return sameType.Contains(col.Name), nil
		})
		newCols, _ = schema.FilterColCollection(inferrerCols, func(col schema.Column) (bool, error) {
			return !sameType.Contains(col.Name), nil
		})
	}

	newCols, err := root.GenerateTagsForNewColColl(ctx, args.tableName, newCols)
	if err != nil {
		return nil, errhand.BuildDError("failed to generate new schema").AddCause(err).Build()
	}

	combined, err := oldCols.AppendColl(newCols)
	if err != nil {
		return nil, errhand.BuildDError("failed to generate new schema").AddCause(err).Build()
	}

	if args.op != CreateOp {
		combinedPKs, _ := schema.FilterColCollection(combined, func(col schema.Column) (b bool, err error) {
			return col.IsPartOfPK, nil
		})

		if !schema.ColCollsAreEqual(args.existingSch.GetPKCols(), combinedPKs) {
			return nil, errhand.BuildDError("input primary keys do not match primary keys of existing table").Build()
		}
	}

	return schema.SchemaFromCols(combined), nil
}
