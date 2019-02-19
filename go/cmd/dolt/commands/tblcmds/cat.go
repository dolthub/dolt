package tblcmds

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
)

var catShortDesc = "print tables"
var catLongDesc = `The dolt table cat command reads tables and writes them to the standard output.`
var catSynopsis = []string{
	"[<commit>] <table>...",
}

func Cat(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "List of tables to be printed."
	help, usage := cli.HelpAndUsagePrinters(commandStr, catShortDesc, catLongDesc, catSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	if len(args) == 0 {
		usage()
		return 1
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		var cm *doltdb.Commit
		cm, verr = commands.MaybeGetCommitWithVErr(dEnv, args[0])

		if verr == nil {
			if cm != nil {
				args = args[1:]
				root = cm.GetRootValue()
			}

			if len(args) == 0 {
				cli.Println("No tables specified")
				usage()
				return 1
			}

			verr = printTable(root, args)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func printTable(working *doltdb.RootValue, tblNames []string) errhand.VerboseError {
	var verr errhand.VerboseError
	for _, tblName := range tblNames {
		func() {
			if !working.HasTable(tblName) {
				verr = errhand.BuildDError("error: unknown table '%s'", tblName).Build()
				return
			}

			tbl, _ := working.GetTable(tblName)
			tblSch := tbl.GetSchema()

			transforms := pipeline.NewTransformCollection()
			sch := maybeAddCnfColTransform(transforms, tbl, tblSch)
			outSch := addMapTransform(sch, transforms)
			addSizingTransform(outSch, transforms)

			rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
			defer rd.Close()

			wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), outSch, &csv.CSVFileInfo{Delim: '|'})
			defer wr.Close()

			badRowCB := func(tff *pipeline.TransformRowFailure) (quit bool) {
				cli.PrintErrln(color.RedString("Failed to transform row %s.", table.RowFmt(tff.Row)))
				return true
			}
			p, start := pipeline.NewAsyncPipeline(rd, transforms, wr, badRowCB)

			p.InsertRow("fwt", untyped.NewRowFromStrings(outSch, outSch.GetFieldNames()))

			start()
			p.Wait()
		}()
	}

	return verr
}

func addSizingTransform(outSch *schema.Schema, transforms *pipeline.TransformCollection) {
	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.PrintAllWhenTooLong, 10000)
	transforms.AppendTransforms(pipeline.NamedTransform{"fwt", autoSizeTransform.TransformToFWT})
}

func addMapTransform(sch *schema.Schema, transforms *pipeline.TransformCollection) *schema.Schema {
	mapping := untyped.TypedToUntypedMapping(sch)
	rConv, _ := table.NewRowConverter(mapping)
	transform := pipeline.NewRowTransformer("schema mapping transform", pipeline.GetRowConvTransformFunc(rConv))
	transforms.AppendTransforms(pipeline.NamedTransform{"map", transform})

	return mapping.DestSch
}

func maybeAddCnfColTransform(transColl *pipeline.TransformCollection, tbl *doltdb.Table, tblSch *schema.Schema) *schema.Schema {
	if tbl.HasConflicts() {
		// this is so much code to add a column
		const transCnfSetName = "set cnf col"

		confSchema := untyped.NewUntypedSchema([]string{"Cnf"})
		schWithConf := typed.TypedSchemaUnion(confSchema, tblSch)
		schWithConf.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{tblSch.GetPKIndex() + 1}))

		_, confData, _ := tbl.GetConflicts()

		cnfTransform := pipeline.NewRowTransformer(transCnfSetName, CnfTransformer(tblSch, schWithConf, confData))
		transColl.AppendTransforms(pipeline.NamedTransform{transCnfSetName, cnfTransform})

		return schWithConf
	}

	return tblSch
}

var confLabel = types.String(" ! ")
var noConfLabel = types.String("   ")

func CnfTransformer(inSch, outSch *schema.Schema, conflicts types.Map) func(inRow *table.Row) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	numCols := inSch.NumFields()
	pkIndex := inSch.GetPKIndex()
	return func(inRow *table.Row) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
		inData := inRow.CurrData()
		fieldVals := make([]types.Value, numCols+1)

		pk, _ := inData.GetField(pkIndex)
		if conflicts.Has(pk) {
			fieldVals[0] = confLabel
		} else {
			fieldVals[0] = noConfLabel
		}

		inData.CopyValues(fieldVals[1:], 0, numCols)
		return []*pipeline.TransformedRowResult{{RowData: table.RowDataFromValues(outSch, fieldVals)}}, ""
	}
}
