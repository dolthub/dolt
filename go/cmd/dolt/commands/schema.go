package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"strings"

	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

const (
	exportFlag      = "export"
	defaultParam    = "default"
	tagParam        = "tag"
	notNullFlag     = "not-null"
	addFieldFlag    = "add-column"
	renameFieldFlag = "rename-column"
	dropFieldFlag   = "drop-column"
)

var tblSchemaShortDesc = "Displays and modifies table schemas"
var tblSchemaLongDesc = "dolt table schema displays the schema of tables at a given commit.  If no commit is provided the " +
	"working set will be used.\n" +
	"\n" +
	"A list of tables can optionally be provided.  If it is omitted all table schemas will be shown." + "\n" +
	"\n" +
	"dolt table schema --export exports a table's schema into a specified file. Both table and file must be specified." + "\n" +
	"\n" +
	"dolt table schema --add-Column adds a column to specified table's schema. If no default value is provided" +
	"the column will be empty.\n" +
	"\n" +
	"dolt table schema --rename-column renames a column of the specified table.\n" +
	"\n" +
	"dolt table schema --drop-column removes a column of the specified table."

var tblSchemaSynopsis = []string{
	"[<commit>] [<table>...]",
	"--export <table> <file>",
	"--add-column [--default <default_value>] [--not-null] [--tag <tag-number>] <table> <name> <type>",
	"--rename-column <table> <old> <new>]",
	"--drop-column <table> <column>",
}

var bold = color.New(color.Bold)

func Schema(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table(s) whose schema is being displayed."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."
	ap.SupportsFlag(exportFlag, "", "exports schema into file.")
	ap.SupportsString(defaultParam, "", "default-value", "If provided all existing rows will be given this value as their default.")
	ap.SupportsUint(tagParam, "", "tag-number", "The numeric tag for the new column.")
	ap.SupportsFlag(notNullFlag, "", "If provided rows without a value in this column will be considered invalid.  If rows already exist and not-null is specified then a default value must be provided.")
	ap.SupportsFlag(addFieldFlag, "", "add columm to table schema.")
	ap.SupportsFlag(renameFieldFlag, "", "rename column for specified table.")
	ap.SupportsFlag(dropFieldFlag, "", "removes column from specified table.")

	help, usage := cli.HelpAndUsagePrinters(commandStr, tblSchemaShortDesc, tblSchemaLongDesc, tblSchemaSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	var root *doltdb.RootValue
	root, _ = GetWorkingWithVErr(dEnv)

	var verr errhand.VerboseError
	if apr.Contains(renameFieldFlag) {
		verr = renameColumn(apr, root, dEnv)
	} else if apr.Contains(addFieldFlag) {
		verr = addField(apr, root, dEnv)
	} else if apr.Contains(exportFlag) {
		verr = exportSchemas(apr, root, dEnv)
	} else if apr.Contains(dropFieldFlag) {
		verr = removeColumn(apr, root, dEnv)
	} else {
		verr = printSchemas(apr, dEnv)
	}

	return HandleVErrAndExitCode(verr, usage)
}

func badRowCB(_ *pipeline.TransformRowFailure) (quit bool) {
	panic("Should only get here is there is a bug.")
}

const fwtChName = "fwt"

func printSchemas(apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	cmStr := "working"
	args := apr.Args()

	var root *doltdb.RootValue
	var verr errhand.VerboseError
	var cm *doltdb.Commit

	if apr.NArg() == 0 {
		cm, verr = nil, nil
	} else {
		cm, verr = MaybeGetCommitWithVErr(dEnv, args[0])
	}

	if verr == nil {
		if cm != nil {
			cmStr = args[0]
			args = args[1:]
			root = cm.GetRootValue()
		} else {
			root, verr = GetWorkingWithVErr(dEnv)
		}
	}

	if verr == nil {
		tables := args

		// If the user hasn't specified table names, try to grab them all;
		// show usage and error out if there aren't any
		if len(tables) == 0 {
			tables = root.GetTableNames()

			if len(tables) == 0 {
				return errhand.BuildDError("").SetPrintUsage().Build()
			}
		}

		var notFound []string
		for _, tblName := range tables {
			tbl, ok := root.GetTable(tblName)

			if !ok {
				notFound = append(notFound, tblName)
			} else {
				verr = printTblSchema(cmStr, tblName, tbl, root)
				cli.Println()
			}
		}

		for _, tblName := range notFound {
			cli.PrintErrln(color.YellowString("%s not found", tblName))
		}
	}

	return verr
}

func printTblSchema(cmStr string, tblName string, tbl *doltdb.Table, root *doltdb.RootValue) errhand.VerboseError {
	cli.Println(bold.Sprint(tblName), "@", cmStr)
	sch := tbl.GetSchema()
	//schStr, err := encoding.MarshalAsJson(sch)
	schStr, err := sql.SchemaAsCreateStmt(tblName, sch)
	if err != nil {
		return errhand.BuildDError("Failed to encode as json").AddCause(err).Build()
	}

	cli.Println(schStr)
	return nil
}

func exportSchemas(apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Must specify table and file to which table will be exported.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	fileName := apr.Arg(1)
	root, _ = GetWorkingWithVErr(dEnv)
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	err := exportTblSchema(tblName, tbl, fileName, dEnv)
	if err != nil {
		return errhand.BuildDError("file path not valid.").Build()
	}

	return nil
}

func exportTblSchema(tblName string, tbl *doltdb.Table, filename string, dEnv *env.DoltEnv) errhand.VerboseError {
	sch := tbl.GetSchema()
	jsonSchStr, err := encoding.MarshalAsJson(sch)
	if err != nil {
		return errhand.BuildDError("Failed to encode as json").AddCause(err).Build()
	}

	err = dEnv.FS.WriteFile(filename, []byte(jsonSchStr))
	return errhand.BuildIf(err, "Unable to write "+filename).AddCause(err).Build()
}

func addField(apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Must specify table name, column name, column type, and if column required.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	tblSch := tbl.GetSchema()
	newFieldName := apr.Arg(1)

	var defaultVal *string
	if val, ok := apr.GetValue(defaultParam); ok {
		defaultVal = &val
	}

	var tag uint64
	if val, ok := apr.GetUint(tagParam); ok {
		tag = val
	} else {
		tag = schema.AutoGenerateTag(tblSch)
	}

	var verr errhand.VerboseError

	cols := tblSch.GetAllCols()
	cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool) {
		if currColTag == tag {
			verr = errhand.BuildDError("A column with the tag %d already exists.", tag).Build()
			return true
		} else if currCol.Name == newFieldName {
			verr = errhand.BuildDError("A column with the name %s already exists.", newFieldName).Build()
			return true
		}

		return false
	})

	if verr != nil {
		return verr
	}

	newFieldType := strings.ToLower(apr.Arg(2))

	newFieldKind, ok := schema.LwrStrToKind[newFieldType]
	if !ok {
		return errhand.BuildDError(newFieldType + " is not a valid type for this new column.").SetPrintUsage().Build()
	}

	notNull := apr.Contains(notNullFlag)

	if notNull && defaultVal == nil && tbl.GetRowData().Len() > 0 {
		return errhand.BuildDError("When adding a column that may not be null to a table with existing rows, a default value must be provided.").Build()
	}

	newTable, err := addFieldToSchema(tbl, dEnv, newFieldName, newFieldKind, tag, notNull, defaultVal)
	if err != nil {
		return errhand.BuildDError("failed to add column").AddCause(err).Build()
	}

	root = root.PutTable(context.Background(), dEnv.DoltDB, tblName, newTable)
	return UpdateWorkingWithVErr(dEnv, root)
}

// need to refactor this so it can be moved into the libraries
func addFieldToSchema(tbl *doltdb.Table, dEnv *env.DoltEnv, name string, kind types.NomsKind, tag uint64, notNull bool, defaultVal *string) (*doltdb.Table, error) {
	var col schema.Column
	if notNull {
		col = schema.NewColumn(name, tag, kind, false, schema.NotNullConstraint{})
	} else {
		col = schema.NewColumn(name, tag, kind, false)
	}

	sch := tbl.GetSchema()
	updatedCols, err := sch.GetAllCols().Append(col)

	if err != nil {
		return nil, err
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	newSchema := schema.SchemaFromCols(updatedCols)
	newSchemaVal, err := encoding.MarshalAsNomsValue(context.TODO(), vrw, newSchema)

	if err != nil {
		return nil, err
	}

	if defaultVal == nil {
		newTable := doltdb.NewTable(context.TODO(), vrw, newSchemaVal, tbl.GetRowData())
		return newTable, nil
	}

	rowData := tbl.GetRowData()
	me := rowData.Edit()
	defVal, _ := doltcore.StringToValue(*defaultVal, kind)

	rowData.Iter(context.TODO(), func(k, v types.Value) (stop bool) {
		oldRow, _ := tbl.GetRow(k.(types.Tuple), newSchema)
		newRow, err := oldRow.SetColVal(tag, defVal, newSchema)

		if err != nil {
			return true
		}

		me.Set(newRow.NomsMapKey(newSchema), newRow.NomsMapValue(newSchema))

		return false
	})

	updatedTbl := doltdb.NewTable(context.TODO(), vrw, newSchemaVal, me.Map(context.TODO()))
	return updatedTbl, nil
}

func renameColumn(apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Table name, current column name, and new column name are needed to rename column.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	oldColName := apr.Arg(1)
	newColName := apr.Arg(2)

	newTbl, err := actions.RenameColumnOfSchema(context.Background(), oldColName, newColName, tbl, dEnv.DoltDB)
	if err != nil {
		return errToVerboseErr(oldColName, newColName, err)
	}

	root = root.PutTable(context.Background(), dEnv.DoltDB, tblName, newTbl)

	return UpdateWorkingWithVErr(dEnv, root)
}

func errToVerboseErr(oldName, newName string, err error) errhand.VerboseError {
	switch err {
	case schema.ErrColNameCollision:
		return errhand.BuildDError("error: A column already exists with the name %s", newName).Build()

	case schema.ErrColNotFound:
		return errhand.BuildDError("error: Column %s unknown", oldName).Build()

	default:
		return errhand.BuildDError("error: Failed to alter schema").AddCause(err).Build()
	}
}

func removeColumn(apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Table name and column to be removed must be specified.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	colName := apr.Arg(1)

	newTbl, err := actions.RemoveColumnFromTable(context.Background(), colName, tbl, dEnv.DoltDB)

	if err != nil {
		return errToVerboseErr(colName, "", err)
	}

	root = root.PutTable(context.Background(), dEnv.DoltDB, tblName, newTbl)
	return UpdateWorkingWithVErr(dEnv, root)
}
