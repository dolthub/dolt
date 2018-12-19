package commands

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/fwt"
	"os"
)

func showUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Show(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = showUsage(fs)

	tblName := fs.String("table", "", "A table to show")

	fs.Parse(args)

	if *tblName == "" {
		fmt.Fprintln(os.Stderr, "Missing required parameter \"-table\"")
		return 1
	}

	working, verr := getWorking(cliEnv)

	if verr == nil {
		verr = printTable(working, *tblName)
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	return 0
}

func printTable(working *doltdb.RootValue, tblName string) errhand.VerboseError {
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

	return nil
}
