package tblcmds

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var catShortDesc = "print tables"
var catLongDesc = `The dolt cat command reads tables and writes them to the standard output.`
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
			tblSch := tbl.GetSchema(working.VRW())
			rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
			defer rd.Close()

			mapping := untyped.TypedToUntypedMapping(tblSch)
			outSch := mapping.DestSch
			wr := fwt.NewTextWriter(cli.CliOut, outSch, " | ")
			defer wr.Close()

			rConv, _ := table.NewRowConverter(mapping)
			transform := table.NewRowTransformer("schema mapping transform", rConv.TransformRow)
			autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.HashFillWhenTooLong, 0)
			badRowCB := func(tff *table.TransformRowFailure) (quit bool) {
				cli.PrintErrln(color.RedString("Failed to transform row %s.", table.RowFmt(tff.Row)))
				return true
			}

			transforms := table.NewTransformCollection(
				table.NamedTransform{"map", transform},
				table.NamedTransform{"fwt", autoSizeTransform.TransformToFWT})
			pipeline, start := table.NewAsyncPipeline(rd, transforms, wr, badRowCB)

			ch, _ := pipeline.GetInChForTransf("fwt")
			ch <- untyped.NewRowFromStrings(outSch, outSch.GetFieldNames())

			start()
			pipeline.Wait()
		}()
	}

	return verr
}
