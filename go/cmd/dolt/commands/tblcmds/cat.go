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
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/fwt"
	"os"
)

var catShortDesc = "print tables"
var catLongDesc = `The dolt cat command reads tables and writes them to the standard output.`
var catSynopsis = []string{
	"<table>...",
}

func Cat(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "List of tables to be printed."
	help, usage := cli.HelpAndUsagePrinters(commandStr, catShortDesc, catLongDesc, catSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	working, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = printTable(working, apr.Args())
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
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
			wr := fwt.NewTextWriter(os.Stdout, outSch, " | ")
			defer wr.Close()

			rConv, _ := table.NewRowConverter(mapping)
			transform := table.NewRowTransformer("schema mapping transform", rConv.TransformRow)
			autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.HashFillWhenTooLong, 0)
			badRowCB := func(transfName string, row *table.Row, errDetails string) (quit bool) {
				fmt.Fprintln(os.Stderr, color.RedString("Failed to transform row %s.", table.RowFmt(row)))
				return true
			}
			pipeline := table.StartAsyncPipeline(rd, []table.TransformFunc{transform, autoSizeTransform.TransformToFWT}, wr, badRowCB)
			pipeline.Wait()
		}()
	}

	return verr
}
