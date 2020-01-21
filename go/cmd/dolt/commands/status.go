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
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
)

var statusShortDesc = "Show the working status"
var statusLongDesc = `Displays working tables that differ from the current HEAD commit, tables that differ from the 
staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would 
commit by running <b>dolt commit</b>; the second and third are what you could commit by running <b>dolt add .</b> 
before running <b>dolt commit</b>.`

var statusSynopsis = []string{""}

type StatusCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StatusCmd) Name() string {
	return "status"
}

// Description returns a description of the command
func (cmd StatusCmd) Description() string {
	return "Show the working tree status."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd StatusCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, statusShortDesc, statusLongDesc, statusSynopsis, ap)
}

func (cmd StatusCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, _ := cli.HelpAndUsagePrinters(commandStr, statusShortDesc, statusLongDesc, statusSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	stagedTblDiffs, notStagedTblDiffs, err := actions.GetTableDiffs(ctx, dEnv)

	if err != nil {
		cli.PrintErrln(toStatusVErr((err)))
		return 1
	}
	workingTblsInConflict, _, _, err := actions.GetTablesInConflict(ctx, dEnv)

	if err != nil {
		cli.PrintErrln(toStatusVErr((err)))
		return 1
	}

	stagedDocDiffs, notStagedDocDiffs, err := actions.GetDocDiffs(ctx, dEnv)

	if err != nil {
		cli.PrintErrln(toStatusVErr((err)))
		return 1
	}

	workingDocsInConflict, err := actions.GetDocsInConflict(ctx, dEnv)

	if err != nil {
		cli.PrintErrln(toStatusVErr((err)))
		return 1
	}

	printStatus(ctx, dEnv, stagedTblDiffs, notStagedTblDiffs, workingTblsInConflict, workingDocsInConflict, stagedDocDiffs, notStagedDocDiffs)
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

var docDiffTypeToLabel = map[actions.DocDiffType]string{
	actions.ModifiedDoc: "modified:",
	actions.RemovedDoc:  "deleted:",
	actions.AddedDoc:    "new doc:",
}

var docDiffTypeToShortLabel = map[actions.DocDiffType]string{
	actions.ModifiedDoc: "M",
	actions.RemovedDoc:  "D",
	actions.AddedDoc:    "N",
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

func printStagedDiffs(wr io.Writer, stagedTbls *actions.TableDiffs, stagedDocs *actions.DocDiffs, printHelp bool) int {
	if stagedTbls.Len()+stagedDocs.Len() > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, stagedTbls.Len()+stagedDocs.Len())
		for _, tblName := range stagedTbls.Tables {
			if !sqle.HasDoltPrefix(tblName) {
				tdt := stagedTbls.TableToType[tblName]
				lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
			}
		}

		for _, docName := range stagedDocs.Docs {
			dtt := stagedDocs.DocToType[docName]
			lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[dtt], docName))
		}

		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
	}

	return 0
}

func printDiffsNotStaged(ctx context.Context, dEnv *env.DoltEnv, wr io.Writer, notStagedTbls *actions.TableDiffs, notStagedDocs *actions.DocDiffs, printHelp bool, linesPrinted int, workingTblsInConflict []string) int {
	inCnfSet := set.NewStrSet(workingTblsInConflict)

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

	numRemovedOrModified := notStagedTbls.NumRemoved + notStagedTbls.NumModified + notStagedDocs.NumRemoved + notStagedDocs.NumModified
	docsInCnf, _ := docCnfsOnWorkingRoot(ctx, dEnv)

	if numRemovedOrModified-inCnfSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		printChanges := !(notStagedTbls.NumRemoved+notStagedTbls.NumModified == 1 && docsInCnf)

		if printChanges {
			iohelp.WriteLine(wr, workingHeader)

			if printHelp {
				iohelp.WriteLine(wr, workingHeaderHelp)
			}

			lines := getModifiedAndRemovedNotStaged(notStagedTbls, notStagedDocs, inCnfSet)

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

	}

	if notStagedTbls.NumAdded > 0 || notStagedDocs.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		printChanges := !(notStagedTbls.NumAdded == 1 && docsInCnf)

		if printChanges {
			iohelp.WriteLine(wr, untrackedHeader)

			if printHelp {
				iohelp.WriteLine(wr, untrackedHeaderHelp)
			}

			lines := getAddedNotStaged(notStagedTbls, notStagedDocs)

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)

		}

	}

	return linesPrinted
}

func getModifiedAndRemovedNotStaged(notStagedTbls *actions.TableDiffs, notStagedDocs *actions.DocDiffs, inCnfSet *set.StrSet) (lines []string) {
	lines = make([]string, 0, notStagedTbls.Len()+notStagedDocs.Len())
	for _, tblName := range notStagedTbls.Tables {
		tdt := notStagedTbls.TableToType[tblName]

		if tdt != actions.AddedTable && !inCnfSet.Contains(tblName) && tblName != doltdb.DocTableName {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
		}
	}

	if notStagedDocs.NumRemoved+notStagedDocs.NumModified > 0 {
		for _, docName := range notStagedDocs.Docs {
			dtt := notStagedDocs.DocToType[docName]

			if dtt != actions.AddedDoc {
				lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[dtt], docName))
			}
		}
	}
	return lines
}

func getAddedNotStaged(notStagedTbls *actions.TableDiffs, notStagedDocs *actions.DocDiffs) (lines []string) {
	lines = make([]string, 0, notStagedTbls.Len()+notStagedDocs.Len())
	for _, tblName := range notStagedTbls.Tables {
		tdt := notStagedTbls.TableToType[tblName]

		if tdt == actions.AddedTable {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[tdt], tblName))
		}
	}

	for _, docName := range notStagedDocs.Docs {
		doct := notStagedDocs.DocToType[docName]

		if doct == actions.AddedDoc {
			lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[doct], docName))
		}
	}

	return lines
}

func printStatus(ctx context.Context, dEnv *env.DoltEnv, stagedTbls, notStagedTbls *actions.TableDiffs, workingTblsInConflict []string, workingDocsInConflict *actions.DocDiffs, stagedDocs, notStagedDocs *actions.DocDiffs) {
	cli.Printf(branchHeader, dEnv.RepoState.Head.Ref.GetPath())

	if dEnv.RepoState.Merge != nil {
		if len(workingTblsInConflict) > 0 {
			cli.Println(unmergedTablesHeader)
		} else {
			cli.Println(allMergedHeader)
		}
	}

	n := printStagedDiffs(cli.CliOut, stagedTbls, stagedDocs, true)
	n = printDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, notStagedDocs, true, n, workingTblsInConflict)

	if dEnv.RepoState.Merge == nil && n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}
}

func toStatusVErr(err error) errhand.VerboseError {
	switch {
	case actions.IsRootValUnreachable(err):
		rt := actions.GetUnreachableRootType(err)
		bdr := errhand.BuildDError("Unable to read %s.", rt.String())
		bdr.AddCause(actions.GetUnreachableRootCause(err))
		return bdr.Build()

	default:
		return errhand.BuildDError("Unknown error").AddCause(err).Build()
	}
}

func docCnfsOnWorkingRoot(ctx context.Context, dEnv *env.DoltEnv) (bool, error) {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return false, err
	}

	docTbl, found, err := workingRoot.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	return docTbl.HasConflicts()
}
