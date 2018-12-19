package commands

import (
	"flag"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"os"
	"sort"
)

func lsUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Ls(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = lsUsage(fs)
	fs.Parse(args)

	working, verr := getWorking(cliEnv)

	if verr == nil {
		verr = printTables(working)
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	return 0
}

func printTables(root *doltdb.RootValue) errhand.VerboseError {
	tblNames := root.GetTableNames()
	sort.Strings(tblNames)

	fmt.Println("Tables in the working set:")
	for _, tbl := range tblNames {
		fmt.Println("\t", tbl)
	}

	return nil
}
