// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/editor"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var commitDocs = cli.CommandDocumentationContent{
	ShortDesc: "Record changes to the repository",
	LongDesc: `
	Stores the current contents of the staged tables in a new commit along with a log message from the user describing the changes.
	
	The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables before using the commit command (Note: even modified files must be \"added\").
	
	The log message can be added with the parameter {{.EmphasisLeft}}-m <msg>{{.EmphasisRight}}.  If the {{.LessThan}}-m{{.GreaterThan}} parameter is not provided an editor will be opened where you can review the commit and provide a log message.
	
	The commit timestamp can be modified using the --date parameter.  Dates can be specified in the formats {{.LessThan}}YYYY-MM-DD{{.GreaterThan}}, {{.LessThan}}YYYY-MM-DDTHH:MM:SS{{.GreaterThan}}, or {{.LessThan}}YYYY-MM-DDTHH:MM:SSZ07:00{{.GreaterThan}} (where {{.LessThan}}07:00{{.GreaterThan}} is the time zone offset)."
	`,
	Synopsis: []string{
		"[options]",
	},
}

type CommitCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CommitCmd) Name() string {
	return "commit"
}

// Description returns a description of the command
func (cmd CommitCmd) Description() string {
	return "Record changes to the repository."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CommitCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cli.CreateCommitArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, commitDocs, ap))
}

func (cmd CommitCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCommitArgParser()
}

// Exec executes the command
func (cmd CommitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateCommitArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, commitDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	allFlag := apr.Contains(cli.AllFlag)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working root").AddCause(err).Build(), usage)
	}

	if allFlag {
		roots, err = actions.StageAllTables(ctx, roots, dEnv.Docs)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, help)
		}
	}

	var name, email string
	// Check if the author flag is provided otherwise get the name and email stored in configs
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
	} else {
		// This command creates a commit, so we need user identity
		if !cli.CheckUserNameAndEmail(dEnv) {
			return 1
		}
		name, email, err = env.GetNameAndEmail(dEnv.Config)
	}

	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage)
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		msg, err = getCommitMessageFromEditor(ctx, dEnv)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, usage)
		}
	}

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: invalid date").AddCause(err).Build(), usage)
		}
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working set").AddCause(err).Build(), usage)
	}

	prevHash, err := ws.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working set").AddCause(err).Build(), usage)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(cli.ForceFlag),
		Name:       name,
		Email:      email,
	})
	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage)
	}

	_, err = dEnv.DoltDB.CommitWithWorkingSet(
		ctx,
		dEnv.RepoStateReader().CWBHeadRef(),
		ws.Ref(),
		pendingCommit,
		ws.WithStagedRoot(pendingCommit.Roots.Staged).WithWorkingRoot(pendingCommit.Roots.Working).ClearMerge(),
		prevHash,
		dEnv.NewWorkingSetMeta(fmt.Sprintf("Updated by %s %s", commandStr, strings.Join(args, " "))),
	)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't commit").AddCause(err).Build(), usage)
	}

	// if the commit was successful, print it out using the log command
	return LogCmd{}.Exec(ctx, "log", []string{"-n=1"}, dEnv)
}

func handleCommitErr(ctx context.Context, dEnv *env.DoltEnv, err error, usage cli.UsagePrinter) int {
	if err == nil {
		return 0
	}

	if err == doltdb.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", env.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == doltdb.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == doltdb.ErrEmptyCommitMessage {
		bdr := errhand.BuildDError("Aborting commit due to empty commit message.")
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if actions.IsNothingStaged(err) {
		notStagedTbls := actions.NothingStagedTblDiffs(err)
		notStagedDocs := actions.NothingStagedDocsDiffs(err)
		n := PrintDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, notStagedDocs, false, 0, nil, nil)

		if n == 0 {
			bdr := errhand.BuildDError(`no changes added to commit (use "dolt add")`)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}
	}

	if actions.IsTblInConflict(err) {
		inConflict := actions.GetTablesForError(err)
		bdr := errhand.BuildDError(`tables %v have unresolved conflicts from the merge. resolve the conflicts before commiting`, inConflict)
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
	return HandleVErrAndExitCode(verr, usage)
}

func getCommitMessageFromEditor(ctx context.Context, dEnv *env.DoltEnv) (string, error) {
	var finalMsg string
	initialMsg, err := buildInitalCommitMsg(ctx, dEnv)
	if err != nil {
		return "", err
	}
	backupEd := "vim"
	if ed, edSet := os.LookupEnv("EDITOR"); edSet {
		backupEd = ed
	}
	editorStr := dEnv.Config.GetStringOrDefault(env.DoltEditor, backupEd)

	cli.ExecuteWithStdioRestored(func() {
		commitMsg, _ := editor.OpenCommitEditor(editorStr, initialMsg)
		finalMsg = parseCommitMessage(commitMsg)
	})
	return finalMsg, nil
}

func buildInitalCommitMsg(ctx context.Context, dEnv *env.DoltEnv) (string, error) {
	initialNoColor := color.NoColor
	color.NoColor = true

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		panic(err)
	}

	stagedTblDiffs, notStagedTblDiffs, _ := diff.GetStagedUnstagedTableDeltas(ctx, roots)

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, roots)
	if err != nil {
		workingTblsInConflict = []string{}
	}
	workingTblsWithViolations, _, _, err := merge.GetTablesWithConstraintViolations(ctx, roots)
	if err != nil {
		workingTblsWithViolations = []string{}
	}

	docsOnDisk, err := dEnv.DocsReadWriter().GetDocsOnDisk()
	if err != nil {
		return "", err
	}
	stagedDocDiffs, notStagedDocDiffs, err := diff.GetDocDiffs(ctx, roots, docsOnDisk)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer([]byte{})
	n := printStagedDiffs(buf, stagedTblDiffs, stagedDocDiffs, true)
	n = PrintDiffsNotStaged(ctx, dEnv, buf, notStagedTblDiffs, notStagedDocDiffs, true, n, workingTblsInConflict, workingTblsWithViolations)

	currBranch := dEnv.RepoStateReader().CWBHeadRef()
	initialCommitMessage := "\n" + "# Please enter the commit message for your changes. Lines starting" + "\n" +
		"# with '#' will be ignored, and an empty message aborts the commit." + "\n# On branch " + currBranch.GetPath() + "\n#" + "\n"

	msgLines := strings.Split(buf.String(), "\n")
	for i, msg := range msgLines {
		msgLines[i] = "# " + msg
	}
	statusMsg := strings.Join(msgLines, "\n")

	color.NoColor = initialNoColor
	return initialCommitMessage + statusMsg, nil
}

func parseCommitMessage(cm string) string {
	lines := strings.Split(cm, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if len(line) >= 1 && line[0] == '#' {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
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

	return docTbl.HasConflicts(ctx)
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
	docsInCnf, _ := docCnfsOnWorkingRoot(ctx, dEnv)

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
