package commands

import (
	"flag"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/set"
	"os"
	"strings"
)

func addUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Add(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = addUsage(fs)
	fs.Parse(args)

	stagedRoot, workingRoot, verr := getStagedAndWorking(cliEnv)

	if verr == nil {
		tbls := fs.Args()
		if fs.NArg() == 0 {
			fmt.Println("Nothing specified, nothing added.\n Maybe you wanted to say 'dolt add .'?")
		} else if fs.NArg() == 1 && fs.Arg(0) == "." {
			tbls = allTables(stagedRoot, workingRoot)
		}

		verr = validateTables(tbls, stagedRoot, workingRoot)

		if verr == nil {
			verr = updateStaged(cliEnv, tbls, stagedRoot, workingRoot)

			if verr == nil {
				return 0
			}
		}
	}

	fmt.Fprintln(os.Stderr, verr.Verbose())
	return 1
}

func updateStaged(cliEnv *env.DoltCLIEnv, tbls []string, staged, working *doltdb.RootValue) errhand.VerboseError {
	updatedRoot := staged.UpdateTablesFromOther(tbls, working)

	return cliEnv.UpdateStagedRoot(updatedRoot)
}

func getStagedAndWorking(cliEnv *env.DoltCLIEnv) (*doltdb.RootValue, *doltdb.RootValue, errhand.VerboseError) {
	stagedRoot, err := cliEnv.StagedRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	workingRoot, err := cliEnv.WorkingRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get working.").AddCause(err).Build()
	}

	return stagedRoot, workingRoot, nil
}

func validateTables(tbls []string, roots ...*doltdb.RootValue) errhand.VerboseError {
	var missing []string
	for _, tbl := range tbls {
		found := false
		for _, root := range roots {
			if root.HasTable(tbl) {
				found = true
				break
			}
		}

		if !found {
			missing = append(missing, tbl)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return errhand.BuildDError("Unknown table(s): %s", strings.Join(missing, " ")).Build()
}

func allTables(stagedRoot, workingRoot *doltdb.RootValue) []string {
	allTblNames := make([]string, 0, 16)
	allTblNames = append(allTblNames, stagedRoot.GetTableNames()...)
	allTblNames = append(allTblNames, workingRoot.GetTableNames()...)

	return set.Unique(allTblNames)
}
