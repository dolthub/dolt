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
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

var mergeDocs = cli.CommandDocumentationContent{
	ShortDesc: "Join two or more development histories together",
	LongDesc: `Incorporates changes from the named commits (since the time their histories diverged from the current branch) into the current branch.

The second syntax ({{.LessThan}}dolt merge --abort{{.GreaterThan}}) can only be run after the merge has resulted in conflicts. dolt merge {{.EmphasisLeft}}--abort{{.EmphasisRight}} will abort the merge process and try to reconstruct the pre-merge state. However, if there were uncommitted changes when the merge started (and especially if those changes were further modified after the merge was started), dolt merge {{.EmphasisLeft}}--abort{{.EmphasisRight}} will in some cases be unable to reconstruct the original (pre-merge) changes. Therefore: 

{{.LessThan}}Warning{{.GreaterThan}}: Running dolt merge with non-trivial uncommitted changes is discouraged: while possible, it may leave you in a state that is hard to back out of in the case of a conflict.
`,

	Synopsis: []string{
		"[--squash] {{.LessThan}}branch{{.GreaterThan}}",
		"--no-ff [-m message] {{.LessThan}}branch{{.GreaterThan}}",
		"--abort",
	},
}

type MergeCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MergeCmd) Name() string {
	return "merge"
}

// Description returns a description of the command
func (cmd MergeCmd) Description() string {
	return "Merge a branch."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd MergeCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cli.CreateMergeArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, mergeDocs, ap))
}

// EventType returns the type of the event to log
func (cmd MergeCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_MERGE
}

// Exec executes the command
func (cmd MergeCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateMergeArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, mergeDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		cli.PrintErrf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
		return 1
	}

	var verr errhand.VerboseError
	if apr.Contains(cli.AbortParam) {
		if !dEnv.IsMergeActive() {
			cli.PrintErrln("fatal: There is no merge to abort")
			return 1
		}

		verr = abortMerge(ctx, dEnv)
	} else {
		if apr.NArg() != 1 {
			usage()
			return 1
		}

		commitSpecStr := apr.Arg(0)

		var root *doltdb.RootValue
		root, verr = GetWorkingWithVErr(dEnv)

		if verr == nil {
			if has, err := root.HasConflicts(ctx); err != nil {
				verr = errhand.BuildDError("error: failed to get conflicts").AddCause(err).Build()
			} else if has {
				cli.Println("error: Merging is not possible because you have unmerged files.")
				cli.Println("hint: Fix them up in the work tree, and then use 'dolt add <table>'")
				cli.Println("hint: as appropriate to mark resolution and make a commit.")
				cli.Println("fatal: Exiting because of an unresolved conflict.")
				return 1
			} else if dEnv.IsMergeActive() {
				cli.Println("error: Merging is not possible because you have not committed an active merge.")
				cli.Println("hint: add affected tables using 'dolt add <table>' and commit using {{.EmphasisLeft}}dolt commit -m <msg>{{.EmphasisRight}}")
				cli.Println("fatal: Exiting because of active merge")
				return 1
			}

			if verr == nil {
				verr = mergeCommitSpec(ctx, apr, dEnv, commitSpecStr)
			}
		}
	}

	return handleCommitErr(ctx, dEnv, verr, usage)
}

func abortMerge(ctx context.Context, doltEnv *env.DoltEnv) errhand.VerboseError {
	err := actions.CheckoutAllTables(ctx, doltEnv.DbData())

	if err == nil {
		err = doltEnv.RepoState.ClearMerge(doltEnv.FS)

		if err == nil {
			return nil
		}
	}

	return errhand.BuildDError("fatal: failed to revert changes").AddCause(err).Build()
}

func mergeCommitSpec(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, commitSpecStr string) errhand.VerboseError {
	cm1, verr := ResolveCommitWithVErr(dEnv, "HEAD")

	if verr != nil {
		return verr
	}

	cm2, verr := ResolveCommitWithVErr(dEnv, commitSpecStr)

	if verr != nil {
		return verr
	}

	h1, err := cm1.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
	}

	h2, err := cm2.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
	}

	if h1 == h2 {
		cli.Println("Everything up-to-date")
		return nil
	}

	cli.Println("Updating", h1.String()+".."+h2.String())

	squash := apr.Contains(cli.SquashParam)
	if squash {
		cli.Println("Squash commit -- not updating HEAD")
	}

	tblNames, workingDiffs, err := env.MergeWouldStompChanges(ctx, cm2, dEnv.DbData())

	if err != nil {
		return errhand.BuildDError("error: failed to determine mergability.").AddCause(err).Build()
	}

	if len(tblNames) != 0 {
		bldr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by merge:")
		for _, tName := range tblNames {
			bldr.AddDetails(tName)
		}
		bldr.AddDetails("Please commit your changes before you merge.")
		return bldr.Build()
	}

	if ok, err := cm1.CanFastForwardTo(ctx, cm2); ok {
		if apr.Contains(cli.NoFFParam) {
			return execNoFFMerge(ctx, apr, dEnv, cm2, verr, workingDiffs)
		} else {
			return executeFFMerge(ctx, squash, dEnv, cm2, workingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
		return nil
	} else {
		return executeMerge(ctx, squash, dEnv, cm1, cm2, workingDiffs)
	}
}

func execNoFFMerge(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, cm2 *doltdb.Commit, verr errhand.VerboseError, workingDiffs map[string]hash.Hash) errhand.VerboseError {
	mergedRoot, err := cm2.GetRootValue()

	if err != nil {
		return errhand.BuildDError("error: reading from database").AddCause(err).Build()
	}

	verr = mergedRootToWorking(ctx, false, dEnv, mergedRoot, workingDiffs, cm2, map[string]*merge.MergeStats{})

	if verr != nil {
		return verr
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		msg = getCommitMessageFromEditor(ctx, dEnv)
	}

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return errhand.BuildDError("error: invalid date").AddCause(err).Build()
		}
	}

	name, email, err := actions.GetNameAndEmail(dEnv.Config)

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	dbData := dEnv.DbData()

	_, err = actions.CommitStaged(ctx, dbData, actions.CommitStagedProps{
		Message:          msg,
		Date:             t,
		AllowEmpty:       apr.Contains(cli.AllowEmptyFlag),
		CheckForeignKeys: !apr.Contains(forceFlag),
		Name:             name,
		Email:            email,
	})

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	return nil
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, errhand.VerboseError) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
		}
	}

	return root, nil
}

func executeFFMerge(ctx context.Context, squash bool, dEnv *env.DoltEnv, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) errhand.VerboseError {
	cli.Println("Fast-forward")

	rv, err := cm2.GetRootValue()

	if err != nil {
		return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
	}

	stagedHash, err := dEnv.DoltDB.WriteRootValue(ctx, rv)
	if err != nil {
		return errhand.BuildDError("Failed to write database").AddCause(err).Build()
	}

	workingHash := stagedHash
	if len(workingDiffs) > 0 {
		rv, err = applyChanges(ctx, rv, workingDiffs)

		if err != nil {
			return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
		}

		workingHash, err = dEnv.DoltDB.WriteRootValue(ctx, rv)

		if err != nil {
			return errhand.BuildDError("Failed to write database").AddCause(err).Build()
		}
	}

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv.DbData())
	if err != nil {
		return errhand.BuildDError("error: unable to determine unstaged docs").AddCause(err).Build()
	}

	if !squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoState.CWBHeadRef(), cm2)

		if err != nil {
			return errhand.BuildDError("Failed to write database").AddCause(err).Build()
		}
	}

	dEnv.RepoState.Working = workingHash.String()
	dEnv.RepoState.Staged = stagedHash.String()

	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return errhand.BuildDError("unable to execute repo state update.").
			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
At the moment the best way to fix this is to run:

    dolt branch -v

and take the hash for your current branch and use it for the value for "staged" and "working"`).
			AddCause(err).Build()
	}

	err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
	}

	return nil
}

func executeMerge(ctx context.Context, squash bool, dEnv *env.DoltEnv, cm1, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) errhand.VerboseError {
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, cm1, cm2)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return errhand.BuildDError("Already up to date.").AddCause(err).Build()
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return errhand.BuildDError("Bad merge").AddCause(err).Build()
		}
	}

	return mergedRootToWorking(ctx, squash, dEnv, mergedRoot, workingDiffs, cm2, tblToStats)
}

func mergedRootToWorking(ctx context.Context, squash bool, dEnv *env.DoltEnv, mergedRoot *doltdb.RootValue, workingDiffs map[string]hash.Hash, cm2 *doltdb.Commit, tblToStats map[string]*merge.MergeStats) errhand.VerboseError {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}
	}

	h2, err := cm2.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to hash commit").AddCause(err).Build()
	}

	if !squash {
		err = dEnv.RepoState.StartMerge(h2.String(), dEnv.FS)

		if err != nil {
			return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
		}
	}

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv.DbData())
	if err != nil {
		return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
	}

	verr := UpdateWorkingWithVErr(dEnv, workingRoot)

	if verr == nil {
		hasConflicts := printSuccessStats(tblToStats)

		if hasConflicts {
			cli.Println("Automatic merge failed; fix conflicts and then commit the result.")
		} else {
			err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
			if err != nil {
				return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
			}
			verr = UpdateStagedWithVErr(dEnv.DoltDB, dEnv.RepoStateWriter(), mergedRoot)
			if verr != nil {
				// Log a new message here to indicate that merge was successful, only staging failed.
				cli.Println("Unable to stage changes: add and commit to finish merge")
			}
		}
	}

	return verr
}

func printSuccessStats(tblToStats map[string]*merge.MergeStats) bool {
	printModifications(tblToStats)
	printAdditions(tblToStats)
	printDeletions(tblToStats)
	return printConflicts(tblToStats)
}

func printAdditions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableRemoved {
			cli.Println(tblName, "added")
		}
	}
}

func printDeletions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableRemoved {
			cli.Println(tblName, "deleted")
		}
	}
}

func printConflicts(tblToStats map[string]*merge.MergeStats) bool {
	hasConflicts := false
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts > 0 {
			cli.Println("Auto-merging", tblName)
			cli.Println("CONFLICT (content): Merge conflict in", tblName)

			hasConflicts = true
		}
	}

	return hasConflicts
}

func printModifications(tblToStats map[string]*merge.MergeStats) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts == 0 {
			tbls = append(tbls, tblName)
			nameLen := len(tblName)
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.Conflicts

			if nameLen > maxNameLen {
				maxNameLen = nameLen
			}

			if modCount > maxModCount {
				maxModCount = modCount
			}

			rowsAdded += stats.Adds
			rowsChanged += stats.Modifications + stats.Conflicts
			rowsDeleted += stats.Deletes
		}
	}

	if len(tbls) == 0 {
		return
	}

	sort.Strings(tbls)
	modCountStrLen := len(strconv.FormatInt(int64(maxModCount), 10))
	format := fmt.Sprintf("%%-%ds | %%-%ds %%s", maxNameLen, modCountStrLen)

	for _, tbl := range tbls {
		stats := tblToStats[tbl]
		if stats.Operation == merge.TableModified {
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.Conflicts
			modCountStr := strconv.FormatInt(int64(modCount), 10)
			visualizedChanges := visualizeChangeTypes(stats, maxModCount)

			cli.Println(fmt.Sprintf(format, tbl, modCountStr, visualizedChanges))
		}
	}

	details := fmt.Sprintf("%d tables changed, %d rows added(+), %d rows modified(*), %d rows deleted(-)", len(tbls), rowsAdded, rowsChanged, rowsDeleted)
	cli.Println(details)
}

func visualizeChangeTypes(stats *merge.MergeStats, maxMods int) string {
	const maxVisLen = 30 //can be a bit longer due to min len and rounding

	resultStr := ""
	if stats.Adds > 0 {
		addLen := int(maxVisLen * (float64(stats.Adds) / float64(maxMods)))
		if addLen > stats.Adds {
			addLen = stats.Adds
		}
		addStr := fillStringWithChar('+', addLen)
		resultStr += color.GreenString(addStr)
	}

	if stats.Modifications > 0 {
		modLen := int(maxVisLen * (float64(stats.Modifications) / float64(maxMods)))
		if modLen > stats.Modifications {
			modLen = stats.Modifications
		}
		modStr := fillStringWithChar('*', modLen)
		resultStr += color.YellowString(modStr)
	}

	if stats.Deletes > 0 {
		delLen := int(maxVisLen * (float64(stats.Deletes) / float64(maxMods)))
		if delLen > stats.Deletes {
			delLen = stats.Deletes
		}
		delStr := fillStringWithChar('-', delLen)
		resultStr += color.GreenString(delStr)
	}

	return resultStr
}

func fillStringWithChar(ch rune, strLen int) string {
	if strLen == 0 {
		strLen = 1
	}

	runes := make([]rune, strLen)
	for i := 0; i < strLen; i++ {
		runes[i] = ch
	}

	return string(runes)
}
