package commands

import (
	"flag"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"os"
)

func rmUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Rm(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = rmUsage(fs)
	fs.Parse(args)

	if fs.NArg() == 0 {
		fs.Usage()
		return 1
	}

	working, verr := getWorking(cliEnv)

	if verr == nil {
		verr = validateTables(fs.Args(), working)

		if verr == nil {
			verr = removeTables(cliEnv, fs.Args(), working)
		}
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	return 0
}

func getWorking(cliEnv *env.DoltCLIEnv) (*doltdb.RootValue, errhand.VerboseError) {
	working, err := cliEnv.WorkingRoot()

	if err != nil {
		return nil, errhand.BuildDError("Unable to get working.").AddCause(err).Build()
	}

	return working, nil
}

func removeTables(cliEnv *env.DoltCLIEnv, tables []string, working *doltdb.RootValue) errhand.VerboseError {
	working, err := working.RemoveTabels(tables)

	if err != nil {
		return errhand.BuildDError("Unable to remove table(s)").AddCause(err).Build()
	}

	return cliEnv.UpdateWorkingRoot(working)
}
