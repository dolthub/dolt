package actions

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

func DocCnfsOnWorkingRoot(ctx context.Context, dEnv *env.DoltEnv) (bool, error) {
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

func PrintDiffsNotStaged(
	ctx context.Context,
	dEnv *env.DoltEnv,
	wr io.Writer,
	notStagedTbls []diff.TableDelta,
	notStagedDocs *diff.DocDiffs,
	printHelp bool,
	linesPrinted int,
	workingTblsInConflict, workingTblsWithViolations []string,
) int {
	inCnfSet := set.NewStrSet(workingTblsInConflict)
	violationSet := set.NewStrSet(workingTblsWithViolations)

	if len(workingTblsInConflict) > 0 || len(workingTblsWithViolations) > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}
		iohelp.WriteLine(wr, mergedTableHeader)
		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		if len(workingTblsInConflict) > 0 {
			lines := make([]string, 0, len(notStagedTbls))
			for _, tblName := range workingTblsInConflict {
				lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
			}
			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

		if len(workingTblsWithViolations) > 0 {
			violationOnly, _, _ := violationSet.LeftIntersectionRight(inCnfSet)
			lines := make([]string, 0, len(notStagedTbls))
			for _, tblName := range violationOnly.AsSortedSlice() {
				lines = append(lines, fmt.Sprintf(statusFmt, "modified", tblName))
			}
			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}
	}

	added := 0
	removeModified := 0
	for _, td := range notStagedTbls {
		if td.IsAdd() {
			added++
		} else if td.IsRename() {
			added++
			removeModified++
		} else {
			removeModified++
		}
	}

	numRemovedOrModified := removeModified + notStagedDocs.NumRemoved + notStagedDocs.NumModified
	docsInCnf, _ := DocCnfsOnWorkingRoot(ctx, dEnv)

	if numRemovedOrModified-inCnfSet.Size()-violationSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		printChanges := !(removeModified == 1 && docsInCnf)

		if printChanges {
			iohelp.WriteLine(wr, workingHeader)

			if printHelp {
				iohelp.WriteLine(wr, workingHeaderHelp)
			}

			lines := getModifiedAndRemovedNotStaged(notStagedTbls, notStagedDocs, inCnfSet, violationSet)

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

	}

	if added > 0 || notStagedDocs.NumAdded > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		printChanges := !(added == 1 && docsInCnf)

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

func getModifiedAndRemovedNotStaged(notStagedTbls []diff.TableDelta, notStagedDocs *diff.DocDiffs, inCnfSet, violationSet *set.StrSet) (lines []string) {
	lines = make([]string, 0, len(notStagedTbls)+notStagedDocs.Len())
	for _, td := range notStagedTbls {
		if td.IsAdd() || inCnfSet.Contains(td.CurName()) || violationSet.Contains(td.CurName()) || td.CurName() == doltdb.DocTableName {
			continue
		}
		if td.IsDrop() {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], td.CurName()))
		} else if td.IsRename() {
			// per Git, unstaged renames are shown as drop + add
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], td.FromName))
		} else {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.ModifiedTable], td.CurName()))
		}
	}

	if notStagedDocs.NumRemoved+notStagedDocs.NumModified > 0 {
		for _, docName := range notStagedDocs.Docs {
			dtt := notStagedDocs.DocToType[docName]

			if dtt != diff.AddedDoc {
				lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[dtt], docName))
			}
		}
	}
	return lines
}

func getAddedNotStaged(notStagedTbls []diff.TableDelta, notStagedDocs *diff.DocDiffs) (lines []string) {
	lines = make([]string, 0, len(notStagedTbls)+notStagedDocs.Len())
	for _, td := range notStagedTbls {
		if td.IsAdd() || td.IsRename() {
			// per Git, unstaged renames are shown as drop + add
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], td.CurName()))
		}
	}

	for _, docName := range notStagedDocs.Docs {
		doct := notStagedDocs.DocToType[docName]

		if doct == diff.AddedDoc {
			lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[doct], docName))
		}
	}

	return lines
}

// TODO: working docs in conflict param not used here
func PrintStatus(ctx context.Context, dEnv *env.DoltEnv, stagedTbls, notStagedTbls []diff.TableDelta, workingTblsInConflict, workingTblsWithViolations []string, stagedDocs, notStagedDocs *diff.DocDiffs) error {
	cli.Printf(branchHeader, dEnv.RepoStateReader().CWBHeadRef().GetPath())

	mergeActive, err := dEnv.IsMergeActive(ctx)
	if err != nil {
		return err
	}

	if mergeActive {
		if len(workingTblsInConflict) > 0 && len(workingTblsWithViolations) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts and constraint violations"))
		} else if len(workingTblsInConflict) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts"))
		} else if len(workingTblsWithViolations) > 0 {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "constraint violations"))
		} else {
			cli.Println(allMergedHeader)
		}
	}

	n := printStagedDiffs(cli.CliOut, stagedTbls, stagedDocs, true)
	n = PrintDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, notStagedDocs, true, n, workingTblsInConflict, workingTblsWithViolations)

	if !mergeActive && n == 0 {
		cli.Println("nothing to commit, working tree clean")
	}

	return nil
}

const (
	branchHeader     = "On branch %s\n"
	stagedHeader     = `Changes to be committed:`
	stagedHeaderHelp = `  (use "dolt reset <table>..." to unstage)`

	unmergedTablesHeader = `You have unmerged tables.
  (fix %s and run "dolt commit")
  (use "dolt merge --abort" to abort the merge)
`

	allMergedHeader = `All conflicts and constraint violations fixed but you are still merging.
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
	statusRenameFmt   = "\t%-16s%s -> %s"
	bothModifiedLabel = "both modified:"
)

var tblDiffTypeToLabel = map[diff.TableDiffType]string{
	diff.ModifiedTable: "modified:",
	diff.RenamedTable:  "renamed:",
	diff.RemovedTable:  "deleted:",
	diff.AddedTable:    "new table:",
}

var docDiffTypeToLabel = map[diff.DocDiffType]string{
	diff.ModifiedDoc: "modified:",
	diff.RemovedDoc:  "deleted:",
	diff.AddedDoc:    "new doc:",
}

func printStagedDiffs(wr io.Writer, stagedTbls []diff.TableDelta, stagedDocs *diff.DocDiffs, printHelp bool) int {
	if len(stagedTbls)+stagedDocs.Len() > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, len(stagedTbls)+stagedDocs.Len())
		for _, td := range stagedTbls {
			if !doltdb.IsReadOnlySystemTable(td.CurName()) {
				if td.IsAdd() {
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], td.CurName()))
				} else if td.IsDrop() {
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], td.CurName()))
				} else if td.IsRename() {
					lines = append(lines, fmt.Sprintf(statusRenameFmt, tblDiffTypeToLabel[diff.RenamedTable], td.FromName, td.ToName))
				} else {
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.ModifiedTable], td.CurName()))
				}

			}
		}

		for _, docName := range stagedDocs.Docs {
			dtt := stagedDocs.DocToType[docName]
			lines = append(lines, fmt.Sprintf(statusFmt, docDiffTypeToLabel[dtt], docName))
		}

		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
		return len(stagedTbls) + stagedDocs.Len()
	}

	return 0
}
