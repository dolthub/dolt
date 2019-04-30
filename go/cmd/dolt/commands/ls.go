package commands

import (
	"context"
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var lsShortDesc = "List tables"
var lsLongDesc = "Lists the tables within a commit.  By default will list the tables in the current working set" +
	"but if a commit is specified it will list the tables in that commit."
var lsSynopsis = []string{
	"[<commit>]",
}

func Ls(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "show the hash of the table")
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() > 1 {
		usage()
		return 1
	}

	var root *doltdb.RootValue
	var verr errhand.VerboseError
	var label string
	if apr.NArg() == 0 {
		label = "working set"
		root, verr = GetWorkingWithVErr(dEnv)
	} else {
		label, root, verr = getRootForCommitSpecStr(apr.Arg(0), dEnv)
	}

	if verr == nil {
		verr = printTables(root, label, apr.Contains(verboseFlag))
		return 0
	}

	cli.PrintErrln(verr.Verbose())
	return 1
}

func printTables(root *doltdb.RootValue, label string, verbose bool) errhand.VerboseError {
	tblNames := root.GetTableNames(context.TODO())
	sort.Strings(tblNames)

	if len(tblNames) == 0 {
		cli.Println("No tables in", label)
		return nil
	}

	cli.Printf("Tables in %s:\n", label)
	for _, tbl := range tblNames {
		if verbose {
			h, _ := root.GetTableHash(context.TODO(), tbl)
			cli.Printf("\t%-32s %s\n", tbl, h.String())
		} else {
			cli.Println("\t", tbl)
		}
	}

	return nil
}
