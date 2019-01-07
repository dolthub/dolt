package tblcmds

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/fwt"
	"os"
	"strconv"
)

var tblSchemaShortDesc = "Displays table schemas"
var tblSchemaLongDesc = "dolt table schema displays the schema of tables at a given commit.  If no commit is provided the " +
	"working set will be used.\n" +
	"\n" +
	"A list of tables can optionally be provided.  If it is omitted all table schemas will be shown."
var tblSchemaSynopsis = []string{
	"[<commit>] [<table>...]",
}

var schColumns = []string{"idx", "name", "type", "nullable", "primary key"}
var schOutSchema = untyped.NewUntypedSchema(schColumns)
var headerRow = untyped.NewRowFromStrings(schOutSchema, schColumns)
var bold = color.New(color.Bold)

func Schema(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table(s) whose schema is being displayed."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblSchemaShortDesc, tblSchemaLongDesc, tblSchemaSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

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

func badRowCB(transfName string, row *table.Row, errDetails string) (quit bool) {
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
			fmt.Println()
		}
	}

	for _, tblName := range notFound {
		fmt.Fprintln(os.Stderr, color.YellowString("%s not found", tblName))
	}
}

func printTblSchema(cmStr string, tblName string, tbl *doltdb.Table, root *doltdb.RootValue) {
	fmt.Println(bold.Sprint(tblName), "@", cmStr)

	imt := schemaAsInMemTable(tbl, root)
	rd := table.NewInMemTableReader(imt)
	defer rd.Close()
	wr := fwt.NewTextWriter(os.Stdout, schOutSchema, " | ")
	defer wr.Close()
	autoSize := fwt.NewAutoSizingFWTTransformer(schOutSchema, fwt.HashFillWhenTooLong, -1)
	transforms := table.NewTransformCollection(
		table.NamedTransform{fwtChName, autoSize.TransformToFWT})
	p, start := table.NewAsyncPipeline(rd, transforms, wr, badRowCB)

	in, _ := p.GetInChForTransf(fwtChName)
	in <- headerRow
	start()
	_ = p.Wait()
}

func schemaAsInMemTable(tbl *doltdb.Table, root *doltdb.RootValue) *table.InMemTable {
	sch := tbl.GetSchema(root.VRW())
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
