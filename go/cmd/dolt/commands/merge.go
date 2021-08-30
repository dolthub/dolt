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
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
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
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		cli.PrintErrf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
		return 1
	}

	var verr errhand.VerboseError
	if apr.Contains(cli.AbortParam) {
		mergeActive, err := dEnv.IsMergeActive(ctx)
		if err != nil {
			cli.PrintErrln("fatal:", err.Error())
			return 1
		}

		if !mergeActive {
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

		t := doltdb.CommitNowFunc()
		if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
			var err error
			t, err = cli.ParseDate(commitTimeStr)

			if err != nil {
				verr = errhand.BuildDError("error: invalid date").AddCause(err).Build()
				return handleCommitErr(ctx, dEnv, verr, usage)
				//return nil ,nil, err
			}
		}

		var root *doltdb.RootValue
		root, verr = GetWorkingWithVErr(dEnv)

		if verr == nil {
			mergeActive, err := dEnv.IsMergeActive(ctx)
			if err != nil {
				cli.PrintErrln(err.Error())
				return 1
			}

			// If there are any conflicts or constraint violations then we disallow the merge
			hasCnf, err := root.HasConflicts(ctx)
			if err != nil {
				verr = errhand.BuildDError("error: failed to get conflicts").AddCause(err).Build()
			}
			hasCV, err := root.HasConstraintViolations(ctx)
			if err != nil {
				verr = errhand.BuildDError("error: failed to get constraint violations").AddCause(err).Build()
			}
			if hasCnf || hasCV {
				cli.Println("error: Merging is not possible because you have unmerged tables.")
				cli.Println("hint: Fix them up in the working tree, and then use 'dolt add <table>'")
				cli.Println("hint: as appropriate to mark resolution and make a commit.")
				if hasCnf && hasCV {
					cli.Println("fatal: Exiting because of an unresolved conflict and constraint violation.")
				} else if hasCnf {
					cli.Println("fatal: Exiting because of an unresolved conflict.")
				} else {
					cli.Println("fatal: Exiting because of an unresolved constraint violation.")
				}
				return 1
			} else if mergeActive {
				cli.Println("error: Merging is not possible because you have not committed an active merge.")
				cli.Println("hint: add affected tables using 'dolt add <table>' and commit using 'dolt commit -m <msg>'")
				cli.Println("fatal: Exiting because of active merge")
				return 1
			}

			msg := ""
			if m, ok := apr.GetValue(cli.CommitMessageArg); ok {
				msg = m
			}

			//mergeSpec, ok, err := actions.PrepareMergeSpec(ctx, squash, dEnv, commitSpecStr, msg, apr.Contains(cli.NoFFParam))
			mergeSpec, ok, err := env.ParseMergeSpec(ctx, dEnv, msg, commitSpecStr, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag), t)
			if err != nil {
				// TODO: the build error handling
				// invalid commit
				// failed to get hash of commit
				return handleCommitErr(ctx, dEnv, errhand.VerboseErrorFromError(err), usage)
			}
			if !ok {
				// TODO: everything up to date
				cli.Println("Everything up-to-date")
				return handleCommitErr(ctx, dEnv, nil, usage)
			}

			err = mergePrinting(ctx, dEnv, mergeSpec)
			if err != nil {
				return handleCommitErr(ctx, dEnv, err, usage)
			}
			//if !msgOk {
			//	msg, err = getCommitMessageFromEditor(ctx, dEnv)
			//	if err != nil {
			//		return handleCommitErr(ctx, dEnv, errhand.VerboseErrorFromError(err), usage)
			//	}
			//}

			tblToStats, err := actions.MergeCommitSpec(ctx, dEnv, mergeSpec)
			printSuccessStats(tblToStats)
			if err != nil {
				//TODO handle error building
				//return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
				cli.Println("Unable to stage changes: add and commit to finish merge")
				return handleCommitErr(ctx, dEnv, errhand.VerboseErrorFromError(err), usage)
			}

			//if len(conflicts) > 0 && len(constraintViolations) > 0 {
			//	cli.Println("Automatic merge failed; fix conflicts and constraint violations and then commit the result.")
			//} else if len(conflicts) > 0 {
			//	cli.Println("Automatic merge failed; fix conflicts and then commit the result.")
			//} else if len(constraintViolations) > 0 {
			//	cli.Println("Automatic merge failed; fix constraint violations and then commit the result.\n" +
			//		"Constraint violations for the working set may be viewed using the 'dolt_constraint_violations' system table.\n" +
			//		"They may be queried and removed per-table using the 'dolt_constraint_violations_TABLENAME' system table.")
			//} else {
			//	cli.Println("Already up to date.")
			//}
		}
	}

	return handleCommitErr(ctx, dEnv, verr, usage)
}

func mergePrinting(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *env.MergeSpec) errhand.VerboseError {
	cli.Println("Updating", mergeSpec.H1.String()+".."+mergeSpec.H2.String())

	if mergeSpec.Squash {
		cli.Println("Squash commit -- not updating HEAD")
	}
	fmt.Println(mergeSpec.TblNames, mergeSpec.WorkingDiffs)
	if len(mergeSpec.TblNames) != 0 {
		bldr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by merge:")
		for _, tName := range mergeSpec.TblNames {
			bldr.AddDetails(tName)
		}
		bldr.AddDetails("Please commit your changes before you merge.")
		return bldr.Build()
	}

	if ok, err := mergeSpec.Cm1.CanFastForwardTo(ctx, mergeSpec.Cm2); ok {
		ancRoot, err := mergeSpec.Cm1.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		mergedRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		if _, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		if mergeSpec.Noff {
			if mergeSpec.Msg == "" {
				msg , err := getCommitMessageFromEditor(ctx, dEnv)
				if err != nil {
					return errhand.VerboseErrorFromError(err)
				}
				mergeSpec.Msg = msg
			}
			//return execNoFFMerge(ctx, apr, dEnv, roots, cm2, verr, workingDiffs)
		} else {
			cli.Println("Fast-forward")
			//return executeFFMerge(ctx, squash, dEnv, cm2, workingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
	}
	return nil
}
func abortMerge(ctx context.Context, doltEnv *env.DoltEnv) errhand.VerboseError {
	roots, err := doltEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = actions.CheckoutAllTables(ctx, roots, doltEnv.DbData())
	if err == nil {
		err = doltEnv.AbortMerge(ctx)

		if err == nil {
			return nil
		}
	}

	return errhand.BuildDError("fatal: failed to revert changes").AddCause(err).Build()
}

// printSuccessStats returns whether there are conflicts or constraint violations.
func printSuccessStats(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	printModifications(tblToStats)
	printAdditions(tblToStats)
	printDeletions(tblToStats)
	return printConflictsAndViolations(tblToStats)
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

func printConflictsAndViolations(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	hasConflicts := false
	hasConstraintViolations := false
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && (stats.Conflicts > 0 || stats.ConstraintViolations > 0) {
			cli.Println("Auto-merging", tblName)
			if stats.Conflicts > 0 {
				cli.Println("CONFLICT (content): Merge conflict in", tblName)
				hasConflicts = true
			}
			if stats.ConstraintViolations > 0 {
				cli.Println("CONSTRAINT VIOLATION (content): Merge created constraint violation in", tblName)
				hasConstraintViolations = true
			}
		}
	}

	return hasConflicts, hasConstraintViolations
}

func printModifications(tblToStats map[string]*merge.MergeStats) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts == 0 && stats.ConstraintViolations == 0 {
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
