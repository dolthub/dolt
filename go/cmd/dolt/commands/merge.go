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
	"sort"
	"strconv"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/merge"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

const (
	abortParam = "abort"
)

var mergeShortDest = "Join two or more development histories together"
var mergeLongDesc = "Incorporates changes from the named commits (since the time their histories diverged from the " +
	"current branch) into the current branch.\n" +
	"\n" +
	"The second syntax (\"<b>dolt merge --abort</b>\") can only be run after the merge has resulted in conflicts. " +
	"git merge --abort will abort the merge process and try to reconstruct the pre-merge state. However, if there were " +
	"uncommitted changes when the merge started (and especially if those changes were further modified after the merge " +
	"was started), dolt merge --abort will in some cases be unable to reconstruct the original (pre-merge) changes. " +
	"Therefore: \n" +
	"\n" +
	"<b>Warning</b>: Running dolt merge with non-trivial uncommitted changes is discouraged: while possible, it may " +
	"leave you in a state that is hard to back out of in the case of a conflict."
var mergeSynopsis = []string{
	"<branch>",
	"--abort",
}

var abortDetails = "Abort the current conflict resolution process, and try to reconstruct the pre-merge state.\n" +
	"\n" +
	"If there were uncommitted working set changes present when the merge started, dolt merge --abort will be " +
	"unable to reconstruct these changes. It is therefore recommended to always commit or stash your changes before " +
	"running git merge."

func Merge(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(abortParam, "", abortDetails)
	help, usage := cli.HelpAndUsagePrinters(commandStr, mergeShortDest, mergeLongDesc, mergeSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.Contains(abortParam) {
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

		branchName := apr.Arg(0)
		dref, err := dEnv.FindRef(ctx, branchName)

		if err != nil {
			cli.PrintErrln(color.RedString("unknown branch: %s", branchName))
			usage()
		}

		isUnchanged, _ := dEnv.IsUnchangedFromHead(ctx)

		if !isUnchanged {
			cli.Println("error: Your local changes would be overwritten.")
			cli.Println("Please commit your changes before you merge.")
			cli.PrintErrln("Aborting")
			return 1
		}

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
				cli.Println("hint: add affected tables using 'dolt add <table>' and commit using 'dolt commit -m <msg>'")
				cli.Println("fatal: Exiting because of active merge")
				return 1
			}

			if verr == nil {
				verr = mergeBranch(ctx, dEnv, dref)
			}
		}
	}

	return handleCommitErr(verr, usage)
}

func abortMerge(ctx context.Context, doltEnv *env.DoltEnv) errhand.VerboseError {
	err := actions.CheckoutAllTables(ctx, doltEnv)

	if err == nil {
		err = doltEnv.RepoState.ClearMerge()

		if err == nil {
			return nil
		}
	}

	return errhand.BuildDError("fatal: failed to revert changes").AddCause(err).Build()
}

func mergeBranch(ctx context.Context, dEnv *env.DoltEnv, dref ref.DoltRef) errhand.VerboseError {
	cm1, verr := ResolveCommitWithVErr(dEnv, "HEAD", dEnv.RepoState.Head.Ref.String())

	if verr != nil {
		return verr
	}

	cm2, verr := ResolveCommitWithVErr(dEnv, dref.String(), dEnv.RepoState.Head.Ref.String())

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

	if ok, err := cm1.CanFastForwardTo(ctx, cm2); ok {
		return executeFFMerge(ctx, dEnv, cm2)
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
		return nil
	} else {
		return executeMerge(ctx, dEnv, cm1, cm2, dref)
	}
}

func executeFFMerge(ctx context.Context, dEnv *env.DoltEnv, cm2 *doltdb.Commit) errhand.VerboseError {
	cli.Println("Fast-forward")

	rv, err := cm2.GetRootValue()

	if err != nil {
		return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
	}

	h, err := dEnv.DoltDB.WriteRootValue(ctx, rv)

	if err != nil {
		return errhand.BuildDError("Failed to write database").AddCause(err).Build()
	}

	err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoState.Head.Ref, cm2)

	if err != nil {
		return errhand.BuildDError("Failed to write database").AddCause(err).Build()
	}

	dEnv.RepoState.Working = h.String()
	dEnv.RepoState.Staged = h.String()
	err = dEnv.RepoState.Save()

	if err != nil {
		return errhand.BuildDError("unable to execute repo state update.").
			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
At the moment the best way to fix this is to run:

    dolt branch -v

and take the hash for your current branch and use it for the value for "staged" and "working"`).
			AddCause(err).Build()
	}

	return nil
}

func executeMerge(ctx context.Context, dEnv *env.DoltEnv, cm1, cm2 *doltdb.Commit, dref ref.DoltRef) errhand.VerboseError {
	mergedRoot, tblToStats, err := actions.MergeCommits(ctx, dEnv.DoltDB, cm1, cm2)

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

	h2, err := cm2.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to hash commit").AddCause(err).Build()
	}

	err = dEnv.RepoState.StartMerge(dref, h2.String())

	if err != nil {
		return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
	}

	verr := UpdateWorkingWithVErr(dEnv, mergedRoot)

	if verr == nil {
		hasConflicts := printSuccessStats(tblToStats)

		if hasConflicts {
			cli.Println("Automatic merge failed; fix conflicts and then commit the result.")
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
