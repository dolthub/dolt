package tblcmds

import (
	"strings"

	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
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
	addFieldFlag    = "add-field"
	renameFieldFlag = "rename-field"
	dropFieldFlag   = "drop-field"
)

var tblSchemaShortDesc = "Displays table schemas"
var tblSchemaLongDesc = "dolt table schema displays the schema of tables at a given commit.  If no commit is provided the " +
	"working set will be used.\n" +
	"\n" +
	"A list of tables can optionally be provided.  If it is omitted all table schemas will be shown." + "\n" +
	"\n" +
	"dolt table schema --export exports a table's schema into a specified file. Both table and file must be specified." + "\n" +
	"\n" +
	"dolt table schema --add-field adds a column to specified table's schema. If no default value is provided" +
	"the column will be empty.\n" +
	"\n" +
	"dolt table schema --rename-field renames a column of the specified table.\n" +
	"\n" +
	"dolt table schema --drop-field removes a column of the specified table."

var tblSchemaSynopsis = []string{
	"[<commit>] [<table>...]",
	"--export <table> <file>",
	"--add-field [--default <default_value>] [--not-null] [--tag <tag-number>] <table> <name> <type>",
	//"--rename-field <table> <old> <new>]",
	//"--drop-field <table> <field>",
}

var bold = color.New(color.Bold)

func Schema(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table(s) whose schema is being displayed."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."
	ap.SupportsFlag(exportFlag, "", "exports schema into file.")
	ap.SupportsString(defaultParam, "", "default-value", "If provided all existing rows will be given this value as their default.")
	ap.SupportsUint(tagParam, "", "tag-number", "The numeric tag for the new field.")
	ap.SupportsFlag(notNullFlag, "", "If provided rows without a value in this field will be considered invalid.  If rows already exist and not-null is specified then a default value must be provided.")
	ap.SupportsFlag(addFieldFlag, "", "add columm to table schema.")
	//ap.SupportsFlag(renameFieldFlag, "", "rename column for specified table.")
	//ap.SupportsFlag(dlopFieldFlag, "", "removes column from specified table.")

	help, usage := cli.HelpAndUsagePrinters(commandStr, tblSchemaShortDesc, tblSchemaLongDesc, tblSchemaSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	var root *doltdb.RootValue
	root, _ = commands.GetWorkingWithVErr(dEnv)

	var verr errhand.VerboseError
	/*if apr.Contains("rename-field") {
		verr = renameColumn(args, root, dEnv)
	} else if apr.Contains("drop-field") {
		verr = removeColumns(args, root, dEnv)
	} else*/
	if apr.Contains(addFieldFlag) {
		verr = addField(apr, root, dEnv)
	} else if apr.Contains(exportFlag) {
		verr = exportSchemas(apr.Args(), root, dEnv)
	} else {
		verr = printSchemas(apr, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func badRowCB(_ *pipeline.TransformRowFailure) (quit bool) {
	panic("Should only get here is there is a bug.")
}

const fwtChName = "fwt"

func printSchemas(apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	cmStr := "working"
	args := apr.Args()
	tables := args

	var root *doltdb.RootValue
	var verr errhand.VerboseError
	var cm *doltdb.Commit

	if apr.NArg() == 0 {
		cm, verr = nil, nil
	} else {
		cm, verr = commands.MaybeGetCommitWithVErr(dEnv, cmStr)
	}

	if verr == nil {
		if cm != nil {
			cmStr = args[0]
			args = args[1:]
			root = cm.GetRootValue()
		} else {
			root, verr = commands.GetWorkingWithVErr(dEnv)
		}
	}

	if verr == nil {
		if len(tables) == 0 {
			tables = root.GetTableNames()
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
	jsonSchStr, err := encoding.MarshalAsJson(sch)
	if err != nil {
		return errhand.BuildDError("Failed to encode as json").AddCause(err).Build()
	}

	cli.Println(jsonSchStr)
	return nil
}

func exportSchemas(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(args) < 2 {
		return errhand.BuildDError("Must specify table and file to which table will be exported.").SetPrintUsage().Build()
	}

	tblName := args[0]
	fileName := args[1]
	root, _ = commands.GetWorkingWithVErr(dEnv)
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
		return errhand.BuildDError("Must specify table name, field name, field type, and if field required.").SetPrintUsage().Build()
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
			verr = errhand.BuildDError("A field with the tag %d already exists.", tag).Build()
			return true
		} else if currCol.Name == newFieldName {
			verr = errhand.BuildDError("A field with the name %s already exists.", newFieldName).Build()
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
		return errhand.BuildDError(newFieldType + " is not a valid type for this new field.").SetPrintUsage().Build()
	}

	notNull := apr.Contains(notNullFlag)

	if notNull && defaultVal == nil && tbl.GetRowData().Len() > 0 {
		return errhand.BuildDError("When adding a field that may not be null to a table with existing rows, a default value must be provided.").Build()
	}

	newTable, err := addFieldToSchema(tbl, dEnv, newFieldName, newFieldKind, tag, notNull, defaultVal)
	if err != nil {
		return errhand.BuildDError("failed to add field").AddCause(err).Build()
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTable)
	commands.UpdateWorkingWithVErr(dEnv, root)

	return nil
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
	newSchemaVal, err := encoding.MarshalAsNomsValue(vrw, newSchema)

	if err != nil {
		return nil, err
	}

	if defaultVal == nil {
		newTable := doltdb.NewTable(vrw, newSchemaVal, tbl.GetRowData())
		return newTable, nil
	}

	rowData := tbl.GetRowData()
	me := rowData.Edit()
	defVal, _ := doltcore.StringToValue(*defaultVal, kind)

	rowData.Iter(func(k, v types.Value) (stop bool) {
		oldRow, _ := tbl.GetRow(k.(types.Tuple), newSchema)
		newRow, err := oldRow.SetColVal(tag, defVal, newSchema)

		if err != nil {
			return true
		}

		me.Set(newRow.NomsMapKey(newSchema), newRow.NomsMapValue(newSchema))

		return false
	})

	updatedTbl := doltdb.NewTable(vrw, newSchemaVal, me.Map())
	return updatedTbl, nil
}

/*func renameColumn(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(args) < 3 {
		return errhand.BuildDError("Table name, current column name, and new column name are needed to rename column.").SetPrintUsage().Build()
	}

	tblName := args[0]
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	oldColName := args[1]
	newColName := args[2]

	newTbl, err := renameColumnOfSchema(oldColName, newColName, tbl, dEnv)
	if err != nil {
		return errhand.BuildDError("failed to rename column").AddCause(err).Build()
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTbl)
	commands.UpdateWorkingWithVErr(dEnv, root)

	return nil
}

func renameColumnOfSchema(oldName string, newName string, tbl *doltdb.Table, dEnv *env.DoltEnv) (*doltdb.Table, error) {
	var newFields []*schema.Field
	tblSch := tbl.GetSchema()

	for i := 0; i < len(tblSch.GetFieldNames()); i++ {
		origFieldName := tblSch.GetField(i).NameStr()
		origFieldKind := tblSch.GetField(i).NomsKind()
		origFieldIsRequired := tblSch.GetField(i).IsRequired()
		if origFieldName == oldName {
			newFields = append(newFields, schema.NewField(newName, origFieldKind, origFieldIsRequired))
		} else {
			newFields = append(newFields, tblSch.GetField(i))
		}
	}

	newSchema := schema.NewSchema(newFields)

	origConstraints := make([]*schema.Constraint, 0, tblSch.TotalNumConstraints())
	for i := 0; i < tblSch.TotalNumConstraints(); i++ {
		origConstraints = append(origConstraints, tblSch.GetConstraint(i))
	}

	for _, c := range origConstraints {
		newSchema.AddConstraint(c)
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schemaVal, err := noms.MarshalAsNomsValue(vrw, newSchema)

	if err != nil {
		return nil, err
	}
	newTable := doltdb.NewTable(vrw, schemaVal, tbl.GetRowData())

	return newTable, nil

}

func removeColumnFromTable(tbl *doltdb.Table, fieldName string, dEnv *env.DoltEnv) (*doltdb.Table, error) {
	sch := tbl.GetSchema()
	fieldIndToDelete := sch.GetFieldIndex(fieldName)
	pkInd := sch.GetPKIndex()
	var fieldsForNewSchema []*schema.Field

	if fieldIndToDelete == -1 {
		return nil, errors.New("field does not exist")
	}

	if fieldIndToDelete == pkInd || sch.GetField(fieldIndToDelete).IsRequired() {
		return nil, errors.New("can't remove primary key or required field")
	}

	for i := 0; i < sch.NumFields(); i++ {
		if i != fieldIndToDelete {
			fieldsForNewSchema = append(fieldsForNewSchema, sch.GetField(i))
		}

	}

	newSchema := schema.NewSchema(fieldsForNewSchema)
	vrw := dEnv.DoltDB.ValueReadWriter()

	for i := 0; i < sch.TotalNumConstraints(); i++ {
		newSchema.AddConstraint(sch.GetConstraint(i))
	}

	rowData := tbl.GetRowData()
	me := rowData.Edit()
	rowData.Iter(func(k, v types.Value) (stop bool) {
		oldRowData := table.RowDataFromPKAndValueList(sch, k, v.(types.Tuple))
		fieldVals := make([]types.Value, newSchema.NumFields())
		oldRowData.CopyValues(fieldVals[0:fieldIndToDelete], 0, fieldIndToDelete)
		oldRowData.CopyValues(fieldVals[fieldIndToDelete:], fieldIndToDelete+1, newSchema.NumFields()-fieldIndToDelete)

		newRowData := table.RowDataFromValues(newSchema, fieldVals)
		newRow := table.NewRow(newRowData)

		me.Set(table.GetPKFromRow(newRow), table.GetNonPKFieldListFromRow(newRow, vrw))

		return false
	})

	updatedTbl := tbl.UpdateRows(me.Map())

	schemaVal, err := noms.MarshalAsNomsValue(vrw, newSchema)

	if err != nil {
		return nil, err
	}
	newTable := doltdb.NewTable(vrw, schemaVal, updatedTbl.GetRowData())
	return newTable, nil
}

func removeColumns(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(args) < 2 {
		return errhand.BuildDError("Table name and column to be removed must be specified.").SetPrintUsage().Build()
	}

	tblName := args[0]
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	colName := args[1]

	newTbl, err := removeColumnFromTable(tbl, colName, dEnv)
	if err != nil {
		return errhand.BuildDError("failed to remove column").AddCause(err).Build()
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTbl)
	commands.UpdateWorkingWithVErr(dEnv, root)

	return nil
}
*/
