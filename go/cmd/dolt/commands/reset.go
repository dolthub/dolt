package commands

import (
	"flag"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"os"
	"strings"
)

func resetUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Reset(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = resetUsage(fs)
	fs.Parse(args)

	stagedRoot, headRoot, verr := getStagedAndHead(cliEnv)

	if verr == nil {
		tbls := fs.Args()

		if len(tbls) == 0 {
			tbls = allTables(stagedRoot, headRoot)
		}

		verr = validateTables(tbls, stagedRoot, headRoot)

		if verr == nil {
			stagedRoot, verr = resetStaged(cliEnv, tbls, stagedRoot, headRoot)

			if verr == nil {
				printNotStaged(cliEnv, stagedRoot)
				return 0
			}
		}
	}

	fmt.Fprintln(os.Stderr, verr.Verbose())
	return 1
}

func printNotStaged(cliEnv *env.DoltCLIEnv, staged *doltdb.RootValue) {
	// Printing here is best effort.  Fail silently
	working, err := cliEnv.WorkingRoot()

	if err != nil {
		return
	}

	notStaged := NewTableDiffs(working, staged)

	if notStaged.numRemoved+notStaged.numModified > 0 {
		fmt.Println("Unstaged changes after reset:")

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.sortedTables {
			tdt := notStaged.tableToType[tblName]

			if tdt != addedTable {
				lines = append(lines, fmt.Sprintf("%s\t%s", tdt.ShortLabel(), tblName))
			}
		}

		fmt.Println(strings.Join(lines, "\n"))
	}
}

func resetStaged(cliEnv *env.DoltCLIEnv, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	updatedRoot := staged.UpdateTablesFromOther(tbls, head)

	return updatedRoot, cliEnv.UpdateStagedRoot(updatedRoot)
}

func getStagedAndHead(cliEnv *env.DoltCLIEnv) (*doltdb.RootValue, *doltdb.RootValue, errhand.VerboseError) {
	stagedRoot, err := cliEnv.StagedRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	headRoot, err := cliEnv.HeadRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get at HEAD.").AddCause(err).Build()
	}

	return stagedRoot, headRoot, nil
}
