// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
)

var statusShortDesc = "Show the working status"
var statusLongDesc = `Displays working tables that differ from the current HEAD commit, tables that differ from the 
staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would 
commit by running <b>dolt commit</b>; the second and third are what you could commit by running <b>dolt add .</b> 
before running <b>dolt commit</b>.`

var statusSynopsis = []string{""}

func Status(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, _ := cli.HelpAndUsagePrinters(commandStr, statusShortDesc, statusLongDesc, statusSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	stagedDiffs, notStagedDiffs, err := actions.GetTableDiffs(ctx, dEnv)

	if err != nil {
		panic(err) // fix
	}

	workingInConflict, _, _, err := actions.GetTablesInConflict(ctx, dEnv)

	if err != nil {
		panic(err) // fix
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

	unmergedTablesHeader = `You have unmerged tables.
  (fix conflicts and run "dolt commit")
  (use "dolt merge --abort" to abort the merge)
`

	allMergedHeader = `All conflicts fixed but you are still merging.
  (use "dolt commit" to conclude merge)
`

	mergedTableHeader = `Unmerged paths:`
	mergedTableHelp   = `  (use "dolt add <file>..." to mark resolution)`

	workingHeader     = `Changes not staged for commit:`
	workingHeaderHelp = `  (use "dolt add <table>" to update what will be committed)
  (use "dolt checkout <table>" to discard changes in working directory)`

	untrackedHeader     = `Untracked files:`
	untrackedHeaderHelp = `  (use "dolt add <table>" to include in what will be committed)`

	statusFmt         = "\t%-16s%s"
	bothModifiedLabel = "both modified:"
)

func printStagedDiffs(wr io.Writer, staged *actions.TableDiffs, printHelp bool) int {
	if staged.Len() > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, staged.Len())
		for _, tblName := range staged.Tables {
			tdt := staged.TableToType[tblName]
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
		}

		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
		return len(lines)
	}

	return 0
}

func printDiffsNotStaged(wr io.Writer, notStaged *actions.TableDiffs, printHelp bool, linesPrinted int, workingInConflict []string) int {
	inCnfSet := set.NewStrSet(workingInConflict)

	if len(workingInConflict) > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, mergedTableHeader)

		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range workingInConflict {
			lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStaged.NumRemoved+notStaged.NumModified-inCnfSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, workingHeader)

		if printHelp {
			iohelp.WriteLine(wr, workingHeaderHelp)
		}

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.Tables {
			tdt := notStaged.TableToType[tblName]

			if tdt != actions.AddedTable && !inCnfSet.Contains(tblName) {
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStaged.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, untrackedHeader)

		if printHelp {
			iohelp.WriteLine(wr, untrackedHeaderHelp)
		}

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.Tables {
			tdt := notStaged.TableToType[tblName]

			if tdt == actions.AddedTable {
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	return linesPrinted
}

func printStatus(dEnv *env.DoltEnv, staged, notStaged *actions.TableDiffs, workingInConflict []string) {
	cli.Printf(branchHeader, dEnv.RepoState.Head.Ref.GetPath())

	if dEnv.RepoState.Merge != nil {
		if len(workingInConflict) > 0 {
			cli.Println(unmergedTablesHeader)
		} else {
			cli.Println(allMergedHeader)
		}
	}

	n := printStagedDiffs(cli.CliOut, staged, true)
	n = printDiffsNotStaged(cli.CliOut, notStaged, true, n, workingInConflict)

	if dEnv.RepoState.Merge == nil && n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}
}
