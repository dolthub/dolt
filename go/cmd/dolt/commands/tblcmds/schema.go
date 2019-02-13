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
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
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
	"[<commit>] [<table>...] [--export <table> <file>] [--add-field <table> <name> <type> <is_required>] [--rename-field <table> <old> <new>]",
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

	if apr.Contains("rename-field") {
		return renameColumn(args, root, dEnv)
	}

	if apr.Contains("add-field") {
		tbl, _ := root.GetTable(args[0])
		return addField(args, tbl, root, dEnv)
	}

	if apr.Contains("export") {
		return exportSchemas(args, root, dEnv)

	}

	cmStr := "working"

	var cm *doltdb.Commit
	var verr errhand.VerboseError
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
	wr := fwt.NewTextWriter(cli.CliOut, schOutSchema, " | ")
	defer wr.Close()
	autoSize := fwt.NewAutoSizingFWTTransformer(schOutSchema, fwt.HashFillWhenTooLong, -1)
	transforms := pipeline.NewTransformCollection(
		pipeline.NamedTransform{fwtChName, autoSize.TransformToFWT})
	p, start := pipeline.NewAsyncPipeline(rd, transforms, wr, badRowCB)

	in, _ := p.GetInChForTransf(fwtChName)
	in <- headerRow
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

func exportSchemas(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) int {
	if len(args) < 2 {
		cli.Println("Must specify table and file to which table will be exported.")
		return 1
	}

	tblName := args[0]
	fileName := args[1]
	root, _ = commands.GetWorkingWithVErr(dEnv)
	if !root.HasTable(tblName) {
		cli.Println(tblName + " not found")
		return 1
	}

	tbl, _ := root.GetTable(tblName)
	err := exportTblSchema(tblName, tbl, fileName, dEnv)
	if err != nil {
		cli.Println("file path not valid.")
		return 1
	}
	return 0

}

func exportTblSchema(tblName string, tbl *doltdb.Table, filename string, dEnv *env.DoltEnv) error {
	sch := tbl.GetSchema()
	jsonSch, err := jsonenc.SchemaToJSON(sch)
	if err != nil {
		return err
	}
	return dEnv.FS.WriteFile(filename, jsonSch)
}

func addField(args []string, tbl *doltdb.Table, root *doltdb.RootValue, dEnv *env.DoltEnv) int {
	if len(args) < 4 {
		cli.Println("Must specify table name, field name, field type, and if field required.")
		return 1
	}

	tblName := args[0]
	if !root.HasTable(tblName) {
		cli.Println(tblName + " not found")
		return 1
	}

	origFieldNames := tbl.GetSchema().GetFieldNames()
	newFieldName := args[1]
	newFieldType := args[2]
	isFieldRequired := args[3]

	for _, name := range origFieldNames {
		if newFieldName == name {
			cli.Println("this field already exists.")
			return 1
		}
	}

	newTable, err := addFieldToSchema(tblName, tbl, dEnv, newFieldName, newFieldType, isFieldRequired)
	if err != nil {
		cli.Println(err)
		return 1
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTable)
	commands.UpdateWorkingWithVErr(dEnv, root)
	return 0
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

func renameColumn(args []string, root *doltdb.RootValue, dEnv *env.DoltEnv) int {
	if len(args) < 3 {
		cli.Println("Table name, current column name, and new column name are needed to rename column.")
		return 1
	}

	tblName := args[0]
	if !root.HasTable(tblName) {
		cli.Println(tblName + " not found")
		return 1
	}

	tbl, _ := root.GetTable(tblName)
	oldColName := args[1]
	newColName := args[2]

	newTbl, err := renameColumnOfSchema(oldColName, newColName, tbl, dEnv)
	if err != nil {
		cli.Println(err)
		return 1
	}

	root = root.PutTable(dEnv.DoltDB, tblName, newTbl)
	commands.UpdateWorkingWithVErr(dEnv, root)

	return 0
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
