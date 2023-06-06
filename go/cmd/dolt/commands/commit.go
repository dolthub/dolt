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
	goisatty "github.com/mattn/go-isatty"

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
	"github.com/dolthub/dolt/go/store/datas"
)

var commitDocs = cli.CommandDocumentationContent{
	ShortDesc: "Record changes to the database",
	LongDesc: `
Stores the current contents of the staged tables in a new commit along with a log message from the user describing the changes.

The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables before using the commit command (Note: even modified tables must be \"added\").

The log message can be added with the parameter {{.EmphasisLeft}}-m <msg>{{.EmphasisRight}}.  If the {{.LessThan}}-m{{.GreaterThan}} parameter is not provided an editor will be opened where you can review the commit and provide a log message.

The commit timestamp can be modified using the --date parameter.  Dates can be specified in the formats {{.LessThan}}YYYY-MM-DD{{.GreaterThan}}, {{.LessThan}}YYYY-MM-DDTHH:MM:SS{{.GreaterThan}}, or {{.LessThan}}YYYY-MM-DDTHH:MM:SSZ07:00{{.GreaterThan}} (where {{.LessThan}}07:00{{.GreaterThan}} is the time zone offset)."`,
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

func (cmd CommitCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCommitArgParser()
	return cli.NewCommandDocumentation(commitDocs, ap)
}

func (cmd CommitCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCommitArgParser()
}

// Exec executes the command
func (cmd CommitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	res, skipped := performCommit(ctx, commandStr, args, dEnv)
	if res == 1 {
		return res
	}

	if skipped {
		iohelp.WriteLine(cli.CliOut, "Skipping empty commit")
		return res
	}

	// if the commit was successful, print it out using the log command
	return LogCmd{}.Exec(ctx, "log", []string{"-n=1"}, dEnv, nil)
}

// performCommit creates a new Dolt commit using the specified |commandStr| and |args| for the specified Dolt environment
// |dEnv|. The response is an integer status code indicating success or failure, as well as a boolean that indicates
// if the commit was skipped (e.g. because --skip-empty was specified as an argument).
func performCommit(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) (int, bool) {
	ap := cli.CreateCommitArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, commitDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if err := cli.VerifyCommitArgs(apr); err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), help), false
	}

	allFlag := apr.Contains(cli.AllFlag)
	upperCaseAllFlag := apr.Contains(cli.UpperCaseAllFlag)

	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help), false
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working root").AddCause(err).Build(), usage), false
	}

	if upperCaseAllFlag {
		roots, err = actions.StageAllTables(ctx, roots, true)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, help), false
		}
	} else if allFlag {
		roots, err = actions.StageModifiedAndDeletedTables(ctx, roots)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, help), false
		}
	}

	headCommit, _ := dEnv.HeadCommit(ctx)
	headHash, _ := headCommit.HashOf()

	var name, email string
	// Check if the author flag is provided otherwise get the name and email stored in configs
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
	} else {
		// This command creates a commit, so we need user identity
		if !cli.CheckUserNameAndEmail(dEnv) {
			return 1, false
		}
		name, email, err = env.GetNameAndEmail(dEnv.Config)
	}

	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage), false
	}

	msg, msgOk := apr.GetValue(cli.MessageArg)
	if !msgOk {
		amendStr := ""
		if apr.Contains(cli.AmendFlag) {
			commitMeta, cmErr := headCommit.GetCommitMeta(ctx)
			if cmErr != nil {
				return handleCommitErr(ctx, dEnv, cmErr, usage), false
			}
			amendStr = commitMeta.Description
		}
		msg, err = getCommitMessageFromEditor(ctx, dEnv, "", amendStr, false)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, usage), false
		}
	}

	t := datas.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: invalid date").AddCause(err).Build(), usage), false
		}
	}

	var parentsHeadForAmend []*doltdb.Commit
	if apr.Contains(cli.AmendFlag) {
		numParentsHeadForAmend := headCommit.NumParents()
		for i := 0; i < numParentsHeadForAmend; i++ {
			parentCommit, err := headCommit.GetParent(ctx, i)
			if err == nil {
				parentsHeadForAmend = append(parentsHeadForAmend, parentCommit)
			}
		}

		newRoots, err := actions.ResetSoftToRef(ctx, dEnv.DbData(), "HEAD~1")
		if err != nil {
			return handleResetError(err, usage), false
		}

		err = dEnv.UpdateStagedRoot(ctx, newRoots.Staged)
		if err != nil {
			return handleResetError(err, usage), false
		}
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working set").AddCause(err).Build(), usage), false
	}

	prevHash, err := ws.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working set").AddCause(err).Build(), usage), false
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	} else if apr.Contains(cli.AmendFlag) && len(parentsHeadForAmend) > 1 {
		mergeParentCommits = parentsHeadForAmend
	}

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, ws, mergeParentCommits, dEnv.DbData().Ddb, actions.CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag) || apr.Contains(cli.AmendFlag),
		SkipEmpty:  apr.Contains(cli.SkipEmptyFlag),
		Force:      apr.Contains(cli.ForceFlag),
		Name:       name,
		Email:      email,
	})
	if err != nil {
		if apr.Contains(cli.AmendFlag) {
			newRoots, errRes := actions.ResetSoftToRef(ctx, dEnv.DbData(), headHash.String())
			if errRes != nil {
				return handleResetError(errRes, usage), false
			}

			err = dEnv.UpdateStagedRoot(ctx, newRoots.Staged)
			if err != nil {
				return handleResetError(err, usage), false
			}
		}
		return handleCommitErr(ctx, dEnv, err, usage), false
	}

	// If no error was reported and there is no pending commit, then no commit was created, likely
	// because of --skip-empty, so return a success code, but indicate that no commit was created.
	if pendingCommit == nil {
		return 0, true
	}

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage), false
	}
	_, err = dEnv.DoltDB.CommitWithWorkingSet(
		ctx,
		headRef,
		ws.Ref(),
		pendingCommit,
		ws.WithStagedRoot(pendingCommit.Roots.Staged).WithWorkingRoot(pendingCommit.Roots.Working).ClearMerge(),
		prevHash,
		dEnv.NewWorkingSetMeta(fmt.Sprintf("Updated by %s %s", commandStr, strings.Join(args, " "))),
		nil,
	)
	if err != nil {
		if apr.Contains(cli.AmendFlag) {
			newRoots, errRes := actions.ResetSoftToRef(ctx, dEnv.DbData(), headHash.String())
			if errRes != nil {
				return handleResetError(errRes, usage), false
			}

			err = dEnv.UpdateStagedRoot(ctx, newRoots.Staged)
			if err != nil {
				return handleResetError(err, usage), false
			}
		}
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't commit").AddCause(err).Build(), usage), false
	}

	return 0, false
}

func handleCommitErr(ctx context.Context, dEnv *env.DoltEnv, err error, usage cli.UsagePrinter) int {
	if err == nil {
		return 0
	}

	if err == datas.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", env.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == datas.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == datas.ErrEmptyCommitMessage {
		bdr := errhand.BuildDError("Aborting commit due to empty commit message.")
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if actions.IsNothingStaged(err) {
		notStagedTbls := actions.NothingStagedTblDiffs(err)
		n, newErr := PrintDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, false, false, 0, merge.ArtifactStatus{})
		if newErr != nil {
			bdr := errhand.BuildDError(`No changes added to commit (use "dolt add")\nCould not print diff because of additional error`)
			bdr.AddCause(newErr)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}

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

// getCommitMessageFromEditor opens editor to ask user for commit message if none defined from command line.
// suggestedMsg will be returned if no-edit flag is defined or if this function was called from sql dolt_merge command.
func getCommitMessageFromEditor(ctx context.Context, dEnv *env.DoltEnv, suggestedMsg, amendString string, noEdit bool) (string, error) {
	if cli.ExecuteWithStdioRestored == nil || noEdit {
		return suggestedMsg, nil
	}

	if !checkIsTerminal() {
		return suggestedMsg, nil
	}

	var finalMsg string
	initialMsg, err := buildInitalCommitMsg(ctx, dEnv, suggestedMsg)
	if err != nil {
		return "", err
	}
	if amendString != "" {
		initialMsg = fmt.Sprintf("%s\n%s", amendString, initialMsg)
	}

	backupEd := "vim"
	// try getting default editor on the user system
	if ed, edSet := os.LookupEnv("EDITOR"); edSet {
		backupEd = ed
	}
	// try getting Dolt config core.editor
	editorStr := dEnv.Config.GetStringOrDefault(env.DoltEditor, backupEd)

	cli.ExecuteWithStdioRestored(func() {
		commitMsg, cErr := editor.OpenCommitEditor(editorStr, initialMsg)
		if cErr != nil {
			err = cErr
		}
		finalMsg = parseCommitMessage(commitMsg)
	})

	if err != nil {
		return "", fmt.Errorf("Failed to open commit editor: %v \n Check your `EDITOR` environment variable with `echo $EDITOR` or your dolt config with `dolt config --list` to ensure that your editor is valid", err)
	}

	return finalMsg, nil
}

func checkIsTerminal() bool {
	isTerminal := false
	cli.ExecuteWithStdioRestored(func() {
		if goisatty.IsTerminal(os.Stdout.Fd()) || os.Getenv("DOLT_TEST_FORCE_OPEN_EDITOR") == "1" {
			isTerminal = true
		}
	})
	return isTerminal
}

func buildInitalCommitMsg(ctx context.Context, dEnv *env.DoltEnv, suggestedMsg string) (string, error) {
	initialNoColor := color.NoColor
	color.NoColor = true

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		panic(err)
	}

	stagedTblDiffs, notStagedTblDiffs, _ := diff.GetStagedUnstagedTableDeltas(ctx, roots)

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return "", err
	}

	as, err := merge.GetMergeArtifactStatus(ctx, ws)
	if err != nil {
		return "", nil
	}

	buf := bytes.NewBuffer([]byte{})
	n := printStagedDiffs(buf, stagedTblDiffs, true)
	n, err = PrintDiffsNotStaged(ctx, dEnv, buf, notStagedTblDiffs, true, false, n, as)
	if err != nil {
		return "", err
	}

	currBranch, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return "", err
	}
	initialCommitMessage := fmt.Sprintf("%s\n# Please enter the commit message for your changes. Lines starting"+
		"\n# with '#' will be ignored, and an empty message aborts the commit."+
		"\n# On branch %s\n#\n", suggestedMsg, currBranch)

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

func PrintDiffsNotStaged(
	ctx context.Context,
	dEnv *env.DoltEnv,
	wr io.Writer,
	notStagedTbls []diff.TableDelta,
	printHelp bool,
	printIgnored bool,
	linesPrinted int,
	as merge.ArtifactStatus,
) (int, error) {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return 0, err
	}

	inCnfSet := set.NewStrSet(as.DataConflictTables)
	inCnfSet.Add(as.SchemaConflictsTables...)
	violationSet := set.NewStrSet(as.ConstraintViolationsTables)

	if as.HasConflicts() || as.HasConstraintViolations() {
		if linesPrinted > 0 {
			cli.Println()
		}
		iohelp.WriteLine(wr, unmergedPathsHeader)
		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		if as.HasConflicts() {
			lines := make([]string, 0, len(notStagedTbls))
			for _, tblName := range as.SchemaConflictsTables {
				lines = append(lines, fmt.Sprintf(statusFmt, schemaConflictLabel, tblName))
			}
			for _, tblName := range as.DataConflictTables {
				lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
			}
			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

		if as.HasConstraintViolations() {
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

	numRemovedOrModified := removeModified

	if numRemovedOrModified-inCnfSet.Size()-violationSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, workingHeader)

		if printHelp {
			iohelp.WriteLine(wr, workingHeaderHelp)
		}

		lines := getModifiedAndRemovedNotStaged(notStagedTbls, inCnfSet, violationSet)

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if added > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, untrackedHeader)

		if printHelp {
			iohelp.WriteLine(wr, untrackedHeaderHelp)
		}

		addedNotStagedTables := getAddedNotStagedTables(notStagedTbls)
		filteredTables, err := doltdb.FilterIgnoredTables(ctx, addedNotStagedTables, roots)
		if err != nil && doltdb.AsDoltIgnoreInConflict(err) == nil {
			return 0, err
		}

		lines := make([]string, len(filteredTables.DontIgnore))
		for i, tableName := range filteredTables.DontIgnore {
			lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], tableName)
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)

		if printIgnored && len(filteredTables.Ignore) > 0 {
			if linesPrinted > 0 {
				cli.Println()
			}

			iohelp.WriteLine(wr, ignoredHeader)

			if printHelp {
				iohelp.WriteLine(wr, ignoredHeaderHelp)
			}

			lines := make([]string, len(filteredTables.Ignore))
			for i, tableName := range filteredTables.Ignore {
				lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], tableName)
			}

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

		if len(filteredTables.Conflicts) > 0 {
			if linesPrinted > 0 {
				cli.Println()
			}

			iohelp.WriteLine(wr, conflictedIgnoredHeader)

			if printHelp {
				iohelp.WriteLine(wr, conflictedIgnoredHeaderHelp)
			}

			lines := make([]string, len(filteredTables.Conflicts))
			for i, conflict := range filteredTables.Conflicts {
				lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], conflict.Table)
			}

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}
	}

	return linesPrinted, nil
}

func getModifiedAndRemovedNotStaged(notStagedTbls []diff.TableDelta, inCnfSet, violationSet *set.StrSet) (lines []string) {
	lines = make([]string, 0, len(notStagedTbls))
	for _, td := range notStagedTbls {
		if td.IsAdd() || inCnfSet.Contains(td.CurName()) || violationSet.Contains(td.CurName()) {
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
	return lines
}

func getAddedNotStagedTables(notStagedTbls []diff.TableDelta) (tables []string) {
	tables = make([]string, 0, len(notStagedTbls))
	for _, td := range notStagedTbls {
		if td.IsAdd() || td.IsRename() {
			tables = append(tables, td.CurName())
		}
	}
	return tables
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
  (use "dolt commit" to conclude merge)`

	unmergedPathsHeader = `Unmerged paths:`
	mergedTableHelp     = `  (use "dolt add <table>..." to mark resolution)`

	workingHeader     = `Changes not staged for commit:`
	workingHeaderHelp = `  (use "dolt add <table>" to update what will be committed)
  (use "dolt checkout <table>" to discard changes in working directory)`

	untrackedHeader     = `Untracked tables:`
	untrackedHeaderHelp = `  (use "dolt add <table>" to include in what will be committed)`

	ignoredHeader     = `Ignored tables:`
	ignoredHeaderHelp = `  (use "dolt add -f <table>" to include in what will be committed)`

	conflictedIgnoredHeader     = `Tables with conflicting dolt_ignore patterns:`
	conflictedIgnoredHeaderHelp = `  (use "dolt add -f <table>" to include in what will be committed)`

	statusFmt           = "\t%-18s%s"
	statusRenameFmt     = "\t%-18s%s -> %s"
	schemaConflictLabel = "schema conflict:"
	bothModifiedLabel   = "both modified:"
)

var tblDiffTypeToLabel = map[diff.TableDiffType]string{
	diff.ModifiedTable: "modified:",
	diff.RenamedTable:  "renamed:",
	diff.RemovedTable:  "deleted:",
	diff.AddedTable:    "new table:",
}

func printStagedDiffs(wr io.Writer, stagedTbls []diff.TableDelta, printHelp bool) int {
	if len(stagedTbls) > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, len(stagedTbls))
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
		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
		return len(stagedTbls)
	}

	return 0
}
