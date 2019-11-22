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

	stagedTblDiffs, notStagedTblDiffs, err := actions.GetTableDiffs(ctx, dEnv)

	if err != nil {
		panic(err) // fix
	}
	workingTblsInConflict, _, _, err := actions.GetTablesInConflict(ctx, dEnv)

	if err != nil {
		panic(err) // fix
	}

	_, notStagedDcDiffs, err := actions.GetDocDiffs(ctx, dEnv)

	if err != nil {
		panic(err)
	}

	workingDcsInConflicts, _, _, err := actions.GetDocsInConflict(ctx, dEnv)

	if err != nil {
		panic(err)
	}

	printStatus(dEnv, stagedTblDiffs, notStagedTblDiffs, workingTblsInConflict, notStagedDcDiffs, workingDcsInConflicts)
	return 0
}

var tblDiffTypeToLabel = map[actions.TableDiffType]string{
	actions.ModifiedTable: "modified:",
	actions.RemovedTable:  "deleted:",
	actions.AddedTable:    "new table:",
}

var dcDiffTypeToLabel = map[actions.DocDiffType]string{
	actions.ModifiedDoc: "modified:",
	actions.RemovedDoc:  "deleted:",
	actions.AddedDoc:    "new doc:",
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
	untrackedHeaderHelp = `  (use "dolt add <table|doc>" to include in what will be committed)`

	statusFmt         = "\t%-16s%s"
	bothModifiedLabel = "both modified:"
)

func printStagedDiffs(wr io.Writer, stagedTbls *actions.TableDiffs, printHelp bool) int {
	if stagedTbls.Len() > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, stagedTbls.Len())
		for _, tblName := range stagedTbls.Tables {
			tdt := stagedTbls.TableToType[tblName]
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
		}

		// This code is irrelevant until docs can be staged via `dolt add`
		// if stagedDcs.Len() > 0 {
		// 	for _, dcName := range stagedDcs.Docs {
		// 		ndt := stagedDcs.DocToType[dcName]
		// 		lines = append(lines, fmt.Sprintf(statusFmt, dcDiffTypeToLabel[ddt], dcName))
		// 	}
		// }

		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
	}

	return 0
}

func printDiffsNotStaged(wr io.Writer, notStagedTbls *actions.TableDiffs, notStagedDcs *actions.DocDiffs, printHelp bool, linesPrinted int, workingTblsInConflict []string, workingDcsInConflict []string) int {
	inCnfTblSet := set.NewStrSet(workingTblsInConflict)
	inCnfDcSet := set.NewStrSet(workingDcsInConflict)

	if len(workingTblsInConflict) > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, mergedTableHeader)

		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		lines := make([]string, 0, notStagedTbls.Len())
		for _, tblName := range workingTblsInConflict {
			lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if len(workingDcsInConflict) > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, mergedTableHeader)

		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		lines := make([]string, 0, notStagedDcs.Len())
		for _, dcName := range workingDcsInConflict {
			lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, dcName))
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStagedTbls.NumRemoved+notStagedTbls.NumModified-inCnfTblSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, workingHeader)

		if printHelp {
			iohelp.WriteLine(wr, workingHeaderHelp)
		}

		lines := make([]string, 0, notStagedTbls.Len())
		for _, tblName := range notStagedTbls.Tables {
			tdt := notStagedTbls.TableToType[tblName]

			if tdt != actions.AddedTable && !inCnfTblSet.Contains(tblName) {
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStagedDcs.NumRemoved+notStagedDcs.NumModified-inCnfDcSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, workingHeader)

		if printHelp {
			iohelp.WriteLine(wr, workingHeaderHelp)
		}

		lines := make([]string, 0, notStagedDcs.Len())
		for _, dcName := range notStagedDcs.Docs {
			ddt := notStagedDcs.DocToType[dcName]

			if ddt != actions.AddedDoc && !inCnfTblSet.Contains(dcName) {
				lines = append(lines, fmt.Sprintf(statusFmt, dcDiffTypeToLabel[ddt], dcName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStagedTbls.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, untrackedHeader)

		if printHelp {
			iohelp.WriteLine(wr, untrackedHeaderHelp)
		}

		lines := make([]string, 0, notStagedTbls.Len())
		for _, tblName := range notStagedTbls.Tables {
			tdt := notStagedTbls.TableToType[tblName]

			if tdt == actions.AddedTable {
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if notStagedDcs.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, untrackedHeader)

		if printHelp {
			iohelp.WriteLine(wr, untrackedHeaderHelp)
		}

		lines := make([]string, 0, notStagedDcs.Len())
		for _, dcName := range notStagedDcs.Docs {
			dct := notStagedDcs.DocToType[dcName]

			if dct == actions.AddedDoc {
				lines = append(lines, fmt.Sprintf(statusFmt, dcDiffTypeToLabel[dct], dcName))
			}
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	return linesPrinted
}

func printStatus(dEnv *env.DoltEnv, stagedTbls, notStagedTbls *actions.TableDiffs, workingTblsInConflict []string, notStagedDcs *actions.DocDiffs, workingDcsInConflict []string) {
	cli.Printf(branchHeader, dEnv.RepoState.Head.Ref.GetPath())

	if dEnv.RepoState.Merge != nil {
		if len(workingTblsInConflict) > 0 || len(workingDcsInConflict) > 0 {
			cli.Println(unmergedTablesHeader)
		} else {
			cli.Println(allMergedHeader)
		}
	}

	n := printStagedDiffs(cli.CliOut, stagedTbls, true)
	n = printDiffsNotStaged(cli.CliOut, notStagedTbls, notStagedDcs, true, n, workingTblsInConflict, workingDcsInConflict)

	if dEnv.RepoState.Merge == nil && n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}
}
