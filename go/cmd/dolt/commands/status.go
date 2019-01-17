package commands

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
	"strings"
)

var statusShortDesc = "Show the working status"
var statusLongDesc = "Displays working tables that differ from the current HEAD commit, tables that have differ from the " +
	"staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would " +
	"commit by running <b>dolt commit</b>; the second and third are what you could commit by running <b>dolt add</b> " +
	"before running <b>dolt commit</b>."
var statusSynopsis = []string{""}

func Status(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, _ := cli.HelpAndUsagePrinters(commandStr, statusShortDesc, statusLongDesc, statusSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	stagedDiffs, notStagedDiffs, err := actions.GetTableDiffs(dEnv)

	if err != nil {
		panic(err) // fix
		return 1
	}

	workingInConflict, _, _, err := actions.GetTablesInConflict(dEnv)

	if err != nil {
		panic(err) // fix
		return 1
	}

	printStatus(dEnv, stagedDiffs, notStagedDiffs, workingInConflict)
	return 0
}

var tblDiffTypeToLabel = map[actions.TableDiffType]string{
	actions.ModifiedTable: "modified:",
	actions.RemovedTable:  "deleted:",
	actions.AddedTable:    "new table:",
}

var tblDiffTypeToShortLabel = map[actions.TableDiffType]string{
	actions.ModifiedTable: "M",
	actions.RemovedTable:  "D",
	actions.AddedTable:    "N",
}

const (
	branchHeader     = "On branch %s\n"
	stagedHeader     = `Changes to be committed:`
	stagedHeaderHelp = `  (use "dolt reset <table>..." to unstage)`

	workingHeader     = `Changes not staged for commit:`
	workingHeaderHelp = `  (use "dolt add <table>" to update what will be committed)
  (use "dolt checkout <table>" to discard changes in working directory)`

	untrackedHeader     = `Untracked files:`
	untrackedHeaderHelp = `  (use "dolt add <table>" to include in what will be committed)`

	statusFmt         = "\t%-16s%s"
	bothModifiedLabel = "both modified:"
)

func printStagedDiffs(staged *actions.TableDiffs, printHelp bool) int {
	if staged.Len() > 0 {
		cli.Println(stagedHeader)

		if printHelp {
			cli.Println(stagedHeaderHelp)
		}

		lines := make([]string, 0, staged.Len())
		for _, tblName := range staged.Tables {
			tdt := staged.TableToType[tblName]
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
		}

		cli.Println(color.GreenString(strings.Join(lines, "\n")))
		return len(lines)
	}

	return 0
}

func printDiffsNotStaged(notStaged *actions.TableDiffs, printHelp bool, linesPrinted int, workingNotStaged []string) int {
	if notStaged.NumRemoved+notStaged.NumModified > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		cli.Println(workingHeader)

		if printHelp {
			cli.Println(workingHeaderHelp)
		}

		inCnfSet := set.NewStrSet(workingNotStaged)

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.Tables {
			tdt := notStaged.TableToType[tblName]

			if tdt != actions.AddedTable {
				if tdt == actions.ModifiedTable && inCnfSet.Contains(tblName) {
					lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
				} else {
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
				}
			}
		}

		cli.Println(color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStaged.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		cli.Println(untrackedHeader)

		if printHelp {
			cli.Println(untrackedHeaderHelp)
		}

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.Tables {
			tdt := notStaged.TableToType[tblName]

			if tdt == actions.AddedTable {
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		cli.Println(color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	return linesPrinted
}

func printStatus(dEnv *env.DoltEnv, staged, notStaged *actions.TableDiffs, workingInConflict []string) {
	cli.Printf(branchHeader, dEnv.RepoState.Branch)

	n := printStagedDiffs(staged, true)
	n = printDiffsNotStaged(notStaged, true, n, workingInConflict)

	if n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}
}
