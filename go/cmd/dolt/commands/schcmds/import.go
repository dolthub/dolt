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

package schcmds

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
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

var MappingFileHelp = "A mapping file is json in the format:" + `

	{
		"source_field_name":"dest_field_name"
		...
	}

where source_field_name is the name of a field in the file being imported and dest_field_name is the name of a field in the table being imported to.
`

var schImportDocs = cli.CommandDocumentationContent{
	ShortDesc: "Creates or updates a table by inferring a schema from a file containing sample data.",
	LongDesc: `If {{.EmphasisLeft}}--create | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} with a schema that it infers from the supplied file. One or more primary key columns must be specified using the {{.EmphasisLeft}}--pks{{.EmphasisRight}} parameter.

If {{.EmphasisLeft}}--update | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} any additional columns, or change the types of columns based on the file supplied.  If the {{.EmphasisLeft}}--keep-types{{.EmphasisRight}} parameter is supplied then the types for existing columns will not be modified, even if they differ from what is in the supplied file.

If {{.EmphasisLeft}}--replace | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with a new, empty table which has a schema inferred from the supplied file but columns tags will be maintained across schemas.  {{.EmphasisLeft}}--keep-types{{.EmphasisRight}} can also be supplied here to guarantee that types are the same in the file and in the pre-existing table.

A mapping file can be used to map fields between the file being imported and the table's schema being inferred.  This can be used when creating a new table, or updating or replacing an existing table.

` + MappingFileHelp + `

In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (Currently only csv is supported).  For files separated by a delimiter other than a ',', the --delim parameter can be used to specify a delimiter.

If the parameter {{.EmphasisLeft}}--dry-run{{.EmphasisRight}} is supplied a sql statement will be generated showing what would be executed if this were run without the --dry-run flag

{{.EmphasisLeft}}--float-threshold{{.EmphasisRight}} is the threshold at which a string representing a floating point number should be interpreted as a float versus an int.  If FloatThreshold is 0.0 then any number with a decimal point will be interpreted as a float (such as 0.0, 1.0, etc).  If FloatThreshold is 1.0 then any number with a decimal point will be converted to an int (0.5 will be the int 0, 1.99 will be the int 1, etc.  If the FloatThreshold is 0.001 then numbers with a fractional component greater than or equal to 0.001 will be treated as a float (1.0 would be an int, 1.0009 would be an int, 1.001 would be a float, 1.1 would be a float, etc)
`,

	Synopsis: []string{
		`[--create|--replace] [--force] [--dry-run] [--lower|--upper] [--keep-types] [--file-type <type>] [--float-threshold] [--map {{.LessThan}}mapping-file{{.GreaterThan}}] [--delim {{.LessThan}}delimiter{{.GreaterThan}}]--pks {{.LessThan}}field{{.GreaterThan}},... {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}`,
	},
}

type SchImportOp int

const (
	CreateOp SchImportOp = iota
	UpdateOp
	ReplaceOp
)

type importOptions struct {
	op             SchImportOp
	fileName       string
	fileType       string
	delim          string
	tableName      string
	existingSch    schema.Schema
	PkCols         []string
	keepTypes      bool
	colMapper      rowconv.NameMapper
	floatThreshold float64
}

func (im *importOptions) ColNameMapper() rowconv.NameMapper {
	return im.colMapper
}
func (im *importOptions) FloatThreshold() float64 {
	return im.floatThreshold
}

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

func (cmd ImportCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(schImportDocs, ap)
}

func (cmd ImportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Name of the table to be created."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being used to infer the schema."})
	ap.SupportsFlag(createFlag, "c", "Create a table with the schema inferred from the {{.LessThan}}file{{.GreaterThan}} provided.")
	ap.SupportsFlag(updateFlag, "u", "Update a table to match the inferred schema of the {{.LessThan}}file{{.GreaterThan}} provided. All previous data will be lost.")
	ap.SupportsFlag(replaceFlag, "r", "Replace a table with a new schema that has the inferred schema from the {{.LessThan}}file{{.GreaterThan}} provided. All previous data will be lost.")
	ap.SupportsFlag(dryRunFlag, "", "Print the sql statement that would be run if executed without the flag.")
	ap.SupportsFlag(keepTypesParam, "", "When a column already exists in the table, and it's also in the {{.LessThan}}file{{.GreaterThan}} provided, use the type from the table.")
	ap.SupportsString(fileTypeParam, "", "type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(pksParam, "", "comma-separated-col-names", "List of columns used as the primary key cols.  Order of the columns will determine sort order.")
	ap.SupportsString(mappingParam, "m", "mapping-file", "A file that can map a column name in {{.LessThan}}file{{.GreaterThan}} to a new value.")
	ap.SupportsString(floatThresholdParam, "", "float", "Minimum value at which the fractional component of a value must exceed in order to be considered a float.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimiter for a csv style file with a non-comma delimiter.")
	return ap
}

// Exec implements the import schema command that will take a file and infer its schema, and then create a table matching that schema.
// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, schImportDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	return commands.HandleVErrAndExitCode(importSchema(ctx, dEnv, apr), usage)
}

func getSchemaImportArgs(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, root doltdb.RootValue) (*importOptions, errhand.VerboseError) {
	tblName := apr.Arg(0)
	fileName := apr.Arg(1)

	fileExists, _ := dEnv.FS.Exists(fileName)

	if !fileExists {
		return nil, errhand.BuildDError("error: file '%s' not found.", fileName).Build()
	}

	if err := ValidateTableNameForCreate(tblName); err != nil {
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

	tbl, tblExists, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})

	if err != nil {
		return nil, errhand.BuildDError("error: failed to read from database.").AddCause(err).Build()
	} else if tblExists && op == CreateOp {
		return nil, errhand.BuildDError("error: failed to create table.").AddDetails("A table named '%s' already exists.", tblName).AddDetails("Use --replace or --update instead of --create.").Build()
	}

	if op != CreateOp {
		rows, err := tbl.GetRowData(ctx)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}

		rowCnt, err := rows.Count()
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}

		if rowCnt > 0 {
			return nil, errhand.BuildDError("This operation will delete all row data. If this is your intent, "+
				"run dolt sql -q 'delete from %s' to delete all row data, then re-run this command.", tblName).Build()
		}
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
	colMapper, err := rowconv.NameMapperFromFile(mappingFile, dEnv.FS)

	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	floatThresholdStr := apr.GetValueOrDefault(floatThresholdParam, "0.0")
	floatThreshold, err := strconv.ParseFloat(floatThresholdStr, 64)

	if err != nil {
		return nil, errhand.BuildDError("error: '%s' is not a valid float in the range 0.0 (all floats) to 1.0 (no floats)", floatThresholdStr).SetPrintUsage().Build()
	}

	return &importOptions{
		op:             op,
		fileName:       fileName,
		fileType:       apr.GetValueOrDefault(fileTypeParam, filepath.Ext(fileName)),
		delim:          apr.GetValueOrDefault(delimParam, ","),
		tableName:      tblName,
		existingSch:    existingSch,
		PkCols:         pks,
		keepTypes:      apr.Contains(keepTypesParam),
		colMapper:      colMapper,
		floatThreshold: floatThreshold,
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

	sch, verr := inferSchemaFromFile(ctx, dEnv.DoltDB(ctx).ValueReadWriter().Format(), impArgs, root)
	if verr != nil {
		return verr
	}

	tblName := impArgs.tableName
	root, verr = putEmptyTableWithSchema(ctx, tblName, root, sch)
	if verr != nil {
		return verr
	}

	sqlDb := sqle.NewUserSpaceDatabase(root, editor.Options{})
	sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)

	stmt, err := sqle.GetCreateTableStmt(sqlCtx, engine, tblName)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	cli.Println(stmt)

	if !apr.Contains(dryRunFlag) {
		err = dEnv.UpdateWorkingRoot(ctx, root)
		if err != nil {
			return errhand.BuildDError("error: failed to update the working set.").AddCause(err).Build()
		}

		cli.PrintErrln(color.CyanString("Created table successfully."))
	}

	return nil
}

func putEmptyTableWithSchema(ctx context.Context, tblName string, root doltdb.RootValue, sch schema.Schema) (doltdb.RootValue, errhand.VerboseError) {
	tbl, tblExists, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	empty, err := durable.NewEmptyPrimaryIndex(ctx, root.VRW(), root.NodeStore(), sch)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	var indexSet durable.IndexSet
	if tblExists {
		indexSet, err = tbl.GetIndexSet(ctx)
		if err != nil {
			return nil, errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}
	} else {
		indexSet, err = durable.NewIndexSetWithEmptyIndexes(ctx, root.VRW(), root.NodeStore(), sch)
		if err != nil {
			return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
		}
	}

	tbl, err = doltdb.NewTable(ctx, root.VRW(), root.NodeStore(), sch, empty, indexSet, nil)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	root, err = root.PutTable(ctx, doltdb.TableName{Name: tblName}, tbl)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to add table.").AddCause(err).Build()
	}

	return root, nil
}

func inferSchemaFromFile(ctx context.Context, nbf *types.NomsBinFormat, impOpts *importOptions, root doltdb.RootValue) (schema.Schema, errhand.VerboseError) {
	if impOpts.fileType[0] == '.' {
		impOpts.fileType = impOpts.fileType[1:]
	}

	var rd table.ReadCloser
	csvInfo := csv.NewCSVInfo().SetDelim(",")

	switch impOpts.fileType {
	case "csv":
		if impOpts.delim != "" {
			csvInfo.SetDelim(impOpts.delim)
		}
	case "psv":
		csvInfo.SetDelim("|")
	default:
		return nil, errhand.BuildDError("error: unsupported file type '%s'", impOpts.fileType).Build()
	}

	f, err := os.Open(impOpts.fileName)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to open '%s'", impOpts.fileName).Build()
	}

	defer f.Close()

	rd, err = csv.NewCSVReader(nbf, f, csvInfo)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to create a CSVReader.").AddCause(err).Build()
	}

	defer rd.Close(ctx)

	infCols, err := actions.InferColumnTypesFromTableReader(ctx, rd, impOpts)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to infer schema").AddCause(err).Build()
	}

	return CombineColCollections(ctx, root, infCols, impOpts)
}

func CombineColCollections(ctx context.Context, root doltdb.RootValue, inferredCols *schema.ColCollection, impOpts *importOptions) (schema.Schema, errhand.VerboseError) {
	existingCols := impOpts.existingSch.GetAllCols()

	// oldCols is the subset of existingCols that will be kept in the new schema
	var oldCols *schema.ColCollection
	// newCols is the subset of inferredCols that will be added to the new schema
	var newCols *schema.ColCollection

	var verr errhand.VerboseError
	switch impOpts.op {
	case CreateOp:
		oldCols = schema.EmptyColColl
		newCols = columnsForSchemaCreate(inferredCols, impOpts.PkCols)
	case UpdateOp:
		oldCols, newCols, verr = columnsForSchemaUpdate(existingCols, inferredCols, impOpts.keepTypes)
	case ReplaceOp:
		oldCols, newCols, verr = columnsForSchemaReplace(existingCols, inferredCols, impOpts.keepTypes)
	}

	if verr != nil {
		return nil, verr
	}

	// NOTE: This code is only used in the import codepath for Dolt, so we don't use a schema to qualify the table name
	newCols, err := doltdb.GenerateTagsForNewColColl(ctx, root, impOpts.tableName, newCols)
	if err != nil {
		return nil, errhand.BuildDError("failed to generate new schema").AddCause(err).Build()
	}

	combined := oldCols.AppendColl(newCols)

	err = schema.ValidateForInsert(combined)
	if err != nil {
		return nil, errhand.BuildDError("invalid schema").AddCause(err).Build()
	}

	sch, err := schema.SchemaFromCols(combined)
	if err != nil {
		return nil, errhand.BuildDError("failed to get schema from cols").AddCause(err).Build()
	}
	return sch, nil
}

func columnsForSchemaCreate(inferredCols *schema.ColCollection, pkNames []string) (newCols *schema.ColCollection) {
	pks := set.NewStrSet(pkNames)
	newCols = schema.MapColCollection(inferredCols, func(col schema.Column) schema.Column {
		col.IsPartOfPK = pks.Contains(col.Name)
		return col
	})
	return newCols
}

func columnsForSchemaUpdate(existingCols, inferredCols *schema.ColCollection, keepTypes bool) (oldCols, newCols *schema.ColCollection, verr errhand.VerboseError) {
	ecn := set.NewStrSet(existingCols.GetColumnNames())
	icn := set.NewStrSet(inferredCols.GetColumnNames())

	// (L - R), (L ∩ R), (R - L)
	left, inter, right := ecn.LeftIntersectionRight(icn)

	// intersection columns with the same types are added to oldCols
	sameType := set.NewStrSet(nil)
	inter.Iterate(func(colName string) (cont bool) {
		ec, _ := existingCols.GetByName(colName)
		ic, _ := inferredCols.GetByName(colName)
		if ec.TypeInfo.Equals(ic.TypeInfo) {
			sameType.Add(colName)
		}
		return true
	})

	if keepTypes {
		oldCols = existingCols
		newCols = schema.FilterColCollection(inferredCols, func(col schema.Column) bool {
			return right.Contains(col.Name)
		})
	} else {
		oldCols = schema.FilterColCollection(existingCols, func(col schema.Column) bool {
			return left.Contains(col.Name) || sameType.Contains(col.Name)
		})
		newCols = schema.FilterColCollection(inferredCols, func(col schema.Column) bool {
			return !sameType.Contains(col.Name)
		})
	}

	verr = verifyPKsUnchanged(existingCols, oldCols, newCols)
	if verr != nil {
		return nil, nil, verr
	}

	return oldCols, newCols, nil
}

func columnsForSchemaReplace(existingCols, inferredCols *schema.ColCollection, keepTypes bool) (oldCols, newCols *schema.ColCollection, verr errhand.VerboseError) {
	ecn := set.NewStrSet(existingCols.GetColumnNames())
	icn := set.NewStrSet(inferredCols.GetColumnNames())

	// (L - R), (L ∩ R), (R - L)
	_, inter, right := ecn.LeftIntersectionRight(icn)

	// intersection columns with the same types are added to oldCols
	sameType := set.NewStrSet(nil)
	inter.Iterate(func(colName string) (cont bool) {
		ec, _ := existingCols.GetByName(colName)
		ic, _ := inferredCols.GetByName(colName)
		if ec.TypeInfo.Equals(ic.TypeInfo) {
			sameType.Add(colName)
		}
		return true
	})

	if keepTypes {
		oldCols = schema.FilterColCollection(existingCols, func(col schema.Column) bool {
			return inter.Contains(col.Name)
		})
		newCols = schema.FilterColCollection(inferredCols, func(col schema.Column) bool {
			return right.Contains(col.Name)
		})
	} else {
		oldCols = schema.FilterColCollection(existingCols, func(col schema.Column) bool {
			return sameType.Contains(col.Name)
		})
		newCols = schema.FilterColCollection(inferredCols, func(col schema.Column) bool {
			return !sameType.Contains(col.Name)
		})
	}

	verr = verifyPKsUnchanged(existingCols, oldCols, newCols)
	if verr != nil {
		return nil, nil, verr
	}

	return oldCols, newCols, nil
}

func verifyPKsUnchanged(existingCols, oldCols, newCols *schema.ColCollection) errhand.VerboseError {
	err := newCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.IsPartOfPK {
			return true, fmt.Errorf("Cannot add primary keys using schema import")
		}
		return false, nil
	})
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	existingPKs := schema.FilterColCollection(existingCols, func(col schema.Column) bool {
		return col.IsPartOfPK
	})
	newPKs := schema.FilterColCollection(oldCols, func(col schema.Column) bool {
		return col.IsPartOfPK
	})
	if !schema.ColCollsAreEqual(existingPKs, newPKs) {
		return errhand.BuildDError("input primary keys do not match primary keys of existing table").Build()
	}

	return nil
}
