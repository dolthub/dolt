package tblcmds

import (
	"errors"
	"strconv"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/jsonenc"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"

	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
)

var tblSchemaShortDesc = "Displays table schemas"
var tblSchemaLongDesc = "dolt table schema displays the schema of tables at a given commit.  If no commit is provided the " +
	"working set will be used.\n" +
	"\n" +
	"A list of tables can optionally be provided.  If it is omitted all table schemas will be shown." + "\n" +
	"\n" +
	"dolt table schema --export exports a table's schema into a specified file. Both table and file must be specified." + "\n" +
	"\n" +
	"dolt table schema --add-field adds a column to specified table's schema." + "\n" +
	"\n" +
	"dolt table schema --rename-field renames a column of the specified table."

var tblSchemaSynopsis = []string{
	"[<commit>] [<table>...]",
	"--export <table> <file>",
	"--add-field <table> <name> <type> <is_required>",
	"--rename-field <table> <old> <new>]",
}

var schColumns = []string{"idx", "name", "type", "nullable", "primary key"}
var schOutSchema = untyped.NewUntypedSchema(schColumns)
var headerRow = untyped.NewRowFromStrings(schOutSchema, schColumns)
var bold = color.New(color.Bold)

func Schema(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table(s) whose schema is being displayed."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."
	ap.SupportsFlag("export", "", "exports schema into file.")
	ap.SupportsFlag("add-field", "", "add columm to table schema.")
	ap.SupportsFlag("rename-field", "", "rename column for specified table.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblSchemaShortDesc, tblSchemaLongDesc, tblSchemaSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()
	var root *doltdb.RootValue
	root, _ = commands.GetWorkingWithVErr(dEnv)

	var verr errhand.VerboseError
	if apr.Contains("rename-field") {
		verr = renameColumn(args, root, dEnv)
	} else if apr.Contains("add-field") {
		verr = addField(args, root, dEnv)
	} else if apr.Contains("export") {
		verr = exportSchemas(args, root, dEnv)
	} else {
		cmStr := "working"

		var cm *doltdb.Commit
		if apr.NArg() == 0 {
			cm, verr = nil, nil
		} else {
			cm, verr = commands.MaybeGetCommitWithVErr(dEnv, cmStr)
		}

		if verr == nil {
			var root *doltdb.RootValue
			if cm != nil {
				cmStr = args[0]
				args = args[1:]
				root = cm.GetRootValue()
			} else {
				root, verr = commands.GetWorkingWithVErr(dEnv)
			}

			if verr == nil {
				printSchemas(cmStr, root, args)
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func badRowCB(_ *pipeline.TransformRowFailure) (quit bool) {
	panic("Should only get here is there is a bug.")
}

const fwtChName = "fwt"

func printSchemas(cmStr string, root *doltdb.RootValue, tables []string) {
	if len(tables) == 0 {
		tables = root.GetTableNames()
	}

	var notFound []string
	for _, tblName := range tables {
		tbl, ok := root.GetTable(tblName)

		if !ok {
			notFound = append(notFound, tblName)
		} else {
			printTblSchema(cmStr, tblName, tbl, root)
			cli.Println()
		}
	}

	for _, tblName := range notFound {
		cli.PrintErrln(color.YellowString("%s not found", tblName))
	}
}

func printTblSchema(cmStr string, tblName string, tbl *doltdb.Table, root *doltdb.RootValue) {
	cli.Println(bold.Sprint(tblName), "@", cmStr)

	imt := schemaAsInMemTable(tbl, root)
	rd := table.NewInMemTableReader(imt)
	defer rd.Close()

	wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), schOutSchema, &csv.CSVFileInfo{Delim: '|'})
	defer wr.Close()

	autoSize := fwt.NewAutoSizingFWTTransformer(schOutSchema, fwt.HashFillWhenTooLong, -1)
	transforms := pipeline.NewTransformCollection(
		pipeline.NamedTransform{fwtChName, autoSize.TransformToFWT})
	p, start := pipeline.NewAsyncPipeline(rd, transforms, wr, badRowCB)

	p.InsertRow(fwtChName, headerRow)
	start()
	_ = p.Wait()
}

func schemaAsInMemTable(tbl *doltdb.Table, root *doltdb.RootValue) *table.InMemTable {
	sch := tbl.GetSchema()
	imt := table.NewInMemTable(schOutSchema)
	for i := 0; i < sch.NumFields(); i++ {
		fld := sch.GetField(i)
		idxStr := strconv.FormatInt(int64(i), 10)
		nullableStr := strconv.FormatBool(!fld.IsRequired())
		isPKStr := strconv.FormatBool(sch.GetPKIndex() == i)
		strs := []string{idxStr, fld.NameStr(), fld.KindString(), nullableStr, isPKStr}
		row := untyped.NewRowFromStrings(schOutSchema, strs)
		_ = imt.AppendRow(row)
	}

	return imt
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
	jsonSch, err := jsonenc.SchemaToJSON(sch)
	if err != nil {
		return errhand.BuildDError("Failed to encode as json").AddCause(err).Build()
	}

	err = dEnv.FS.WriteFile(filename, jsonSch)
	return errhand.BuildIf(err, "Unable to write "+filename).AddCause(err).Build()
}

func addField(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(args) < 4 {
		return errhand.BuildDError("Must specify table name, field name, field type, and if field required.").SetPrintUsage().Build()
	}

	tblName := args[0]
	if !root.HasTable(tblName) {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _ := root.GetTable(tblName)
	origFieldNames := tbl.GetSchema().GetFieldNames()
	newFieldName := args[1]
	newFieldType := args[2]
	isFieldRequired := args[3]

	for _, name := range origFieldNames {
		if newFieldName == name {
			return errhand.BuildDError("this field already exists.").Build()
		}
	}

	newTable, err := addFieldToSchema(tblName, tbl, dEnv, newFieldName, newFieldType, isFieldRequired)
	if err != nil {
		return errhand.BuildDError("failed to add field").AddCause(err).Build()
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTable)
	commands.UpdateWorkingWithVErr(dEnv, root)
	return nil
}

func addFieldToSchema(tblName string, tbl *doltdb.Table, dEnv *env.DoltEnv, newColName string, colType string, required string) (*doltdb.Table, error) {
	tblSch := tbl.GetSchema()

	origTblFields := make([]*schema.Field, 0, tblSch.NumFields())
	for i := 0; i < tblSch.NumFields(); i++ {
		origTblFields = append(origTblFields, tblSch.GetField(i))
	}

	origConstraints := make([]*schema.Constraint, 0, tblSch.TotalNumConstraints())
	for i := 0; i < tblSch.TotalNumConstraints(); i++ {
		origConstraints = append(origConstraints, tblSch.GetConstraint(i))
	}

	if newColType, ok := schema.LwrStrToKind[colType]; ok {
		isRequired, err := strconv.ParseBool(required)
		if err != nil {
			return nil, err
		}
		newField := schema.NewField(newColName, newColType, isRequired)
		fieldsForNewSchema := append(origTblFields, newField)

		vrw := dEnv.DoltDB.ValueReadWriter()
		newSchema := schema.NewSchema(fieldsForNewSchema)

		for _, c := range origConstraints {
			newSchema.AddConstraint(c)
		}

		schemaVal, err := noms.MarshalAsNomsValue(vrw, newSchema)

		if err != nil {
			return nil, err
		}
		newTable := doltdb.NewTable(vrw, schemaVal, tbl.GetRowData())

		return newTable, nil

	}
	return nil, errors.New("invalid noms kind")
}

func renameColumn(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
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
