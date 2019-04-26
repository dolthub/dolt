package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var resetShortDesc = "Resets staged tables to their HEAD state"
var resetLongDesc = `Sets the state of a table in the staging area to be that tables value from HEAD

dolt reset <tables>...
	This form resets the values for all staged <tables> to their values at HEAD. (It does not affect the working tree or the current branch.)

	This means that dolt <b>reset <tables></b> is the opposite of <b>dolt add <tables></b>.

	After running <b>dolt reset <tables></b> to update the staged tables, you can use <b>dolt checkout</b> to check the contents out of the staged tables to the working tables.

dolt reset .
	This form resets <b>all</b> staged tables to their values at HEAD. It is the opposite of <b>dolt add .</b>`

var resetSynopsis = []string{
	"<tables>...",
}

func Reset(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, _ := cli.HelpAndUsagePrinters(commandStr, resetShortDesc, resetLongDesc, resetSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	stagedRoot, headRoot, verr := getStagedAndHead(dEnv)

	if verr == nil {
		tbls := apr.Args()

		if len(tbls) == 0 || (len(tbls) == 1 && tbls[0] == ".") {
			tbls = actions.AllTables(context.TODO(), stagedRoot, headRoot)
		}

		verr = ValidateTablesWithVErr(tbls, stagedRoot, headRoot)

		if verr == nil {
			stagedRoot, verr = resetStaged(dEnv, tbls, stagedRoot, headRoot)

			if verr == nil {
				printNotStaged(dEnv, stagedRoot)
				return 0
			}
		}
	}

	cli.PrintErrln(verr.Verbose())
	return 1
}

func printNotStaged(dEnv *env.DoltEnv, staged *doltdb.RootValue) {
	// Printing here is best effort.  Fail silently
	working, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		return
	}

	notStaged := actions.NewTableDiffs(context.TODO(), working, staged)

	if notStaged.NumRemoved+notStaged.NumModified > 0 {
		cli.Println("Unstaged changes after reset:")

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.Tables {
			tdt := notStaged.TableToType[tblName]

			if tdt != actions.AddedTable {
				lines = append(lines, fmt.Sprintf("%s\t%s", tblDiffTypeToShortLabel[tdt], tblName))
			}
		}

		cli.Println(strings.Join(lines, "\n"))
	}
}

func resetStaged(dEnv *env.DoltEnv, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	updatedRoot := staged.UpdateTablesFromOther(context.TODO(), tbls, head)

	return updatedRoot, UpdateStagedWithVErr(dEnv, updatedRoot)
}

func getStagedAndHead(dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, errhand.VerboseError) {
	stagedRoot, err := dEnv.StagedRoot(context.Background())

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	headRoot, err := dEnv.HeadRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get at HEAD.").AddCause(err).Build()
	}

	return stagedRoot, headRoot, nil
}
