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

package actions

import (
	"context"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

const forceFlag = "force"

func ResolveCommitWithVErr(dEnv *env.DoltEnv, cSpecStr string) (*doltdb.Commit, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)

	if err != nil {
		return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs, dEnv.RepoStateReader().CWBHeadRef())

	if err != nil {
		if err == doltdb.ErrInvalidAncestorSpec {
			return nil, errhand.BuildDError("'%s' could not resolve ancestor spec", cSpecStr).Build()
		} else if err == doltdb.ErrBranchNotFound {
			return nil, errhand.BuildDError("unknown ref in commit spec: '%s'", cSpecStr).Build()
		} else if doltdb.IsNotFoundErr(err) {
			return nil, errhand.BuildDError("'%s' not found", cSpecStr).Build()
		} else if err == doltdb.ErrFoundHashNotACommit {
			return nil, errhand.BuildDError("'%s' is not a commit", cSpecStr).Build()
		} else {
			return nil, errhand.BuildDError("Unexpected error resolving '%s'", cSpecStr).AddCause(err).Build()
		}
	}

	return cm, nil
}

func MergeCommitSpec(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, commitSpecStr string, msg string) (error, []string, []string) {
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

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	tblNames, workingDiffs, err := merge.MergeWouldStompChanges(ctx, roots, cm2)

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
		ancRoot, err := cm1.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		mergedRoot, err := cm2.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return errhand.VerboseErrorFromError(err)
		} else if cvPossible {
			return ExecuteMerge(ctx, squash, dEnv, cm1, cm2, workingDiffs)
		}
		if apr.Contains(cli.NoFFParam) {
			return ExecNoFFMerge(ctx, apr, dEnv, roots, cm2, verr, workingDiffs, msg)
		} else {
			return ExecuteFFMerge(ctx, squash, dEnv, cm2, workingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
		return nil
	} else {
		return ExecuteMerge(ctx, squash, dEnv, cm1, cm2, workingDiffs)
	}
}

func ExecNoFFMerge(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, roots doltdb.Roots, cm2 *doltdb.Commit, verr errhand.VerboseError, workingDiffs map[string]hash.Hash, msg string) errhand.VerboseError {
	mergedRoot, err := cm2.GetRootValue()

	if err != nil {
		return errhand.BuildDError("error: reading from database").AddCause(err).Build()
	}

	verr = mergedRootToWorking(ctx, false, dEnv, mergedRoot, workingDiffs, cm2, map[string]*merge.MergeStats{})

	if verr != nil {
		return verr
	}

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return errhand.BuildDError("error: invalid date").AddCause(err).Build()
		}
	}

	name, email, err := GetNameAndEmail(dEnv.Config)

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	// Reload roots since the above method writes new values to the working set
	roots, err = dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(forceFlag),
		Name:       name,
		Email:      email,
	})

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
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

func ExecuteFFMerge(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergeCommit *doltdb.Commit,
	workingDiffs map[string]hash.Hash,
) errhand.VerboseError {
	cli.Println("Fast-forward")

	stagedRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
	}

	workingRoot := stagedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, workingDiffs)

		if err != nil {
			return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: unable to determine unstaged docs").AddCause(err).Build()
	}

	if !squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), mergeCommit)

		if err != nil {
			return errhand.BuildDError("Failed to write database").AddCause(err).Build()
		}
	}

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
	if err != nil {
		return errhand.BuildDError("unable to execute repo state update.").
			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
At the moment the best way to fix this is to run:

    dolt branch -v

and take the hash for your current branch and use it for the value for "staged" and "working"`).
			AddCause(err).Build()
	}

	err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
	}

	return nil
}

func ExecuteMerge(ctx context.Context, squash bool, dEnv *env.DoltEnv, cm1, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) (error, []string, []string){
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, cm1, cm2)
	if err != nil {
		return err,[]string{}, []string{}
	}
	//if err != nil {
	//	switch err {
	//	case doltdb.ErrUpToDate:
	//		return errhand.BuildDError("Already up to date.").AddCause(err).Build()
	//	case merge.ErrFastForward:
	//		panic("fast forward merge")
	//	default:
	//		return errhand.BuildDError("Bad merge").AddCause(err).Build()
	//	}
	//}

	return mergedRootToWorking(ctx, squash, dEnv, mergedRoot, workingDiffs, cm2, tblToStats)
}

// TODO: change this to be functional and not write to repo state
func mergedRootToWorking(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergedRoot *doltdb.RootValue,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	tblToStats map[string]*merge.MergeStats,
) (error, []string, []string) {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			//return errhand.BuildDError("").AddCause(err).Build()
			return err, []string{}, []string{}

		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2)

		if err != nil {
			//return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
			return err, []string{}, []string{}
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
		return err, []string{}, []string{}
	}

	//verr := UpdateWorkingWithVErr(dEnv, workingRoot)
	err := dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		panic("max fix")
	}

	conflicts, constraintViolations := conflictsAndViolations(tblToStats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return nil, conflicts, constraintViolations
	}

	if err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs); err != nil {
		return err, conflicts, constraintViolations
	}

	if err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot); err != nil {
		return err, conflicts, constraintViolations
	}

	return nil, conflicts, constraintViolations

	//
	//if verr == nil {
	//	hasConflicts, hasConstraintViolations := printSuccessStats(tblToStats)
	//
	//	if hasConflicts && hasConstraintViolations {
	//		cli.Println("Automatic merge failed; fix conflicts and constraint violations and then commit the result.")
	//	} else if hasConflicts {
	//		cli.Println("Automatic merge failed; fix conflicts and then commit the result.")
	//	} else if hasConstraintViolations {
	//		cli.Println("Automatic merge failed; fix constraint violations and then commit the result.\n" +
	//			"Constraint violations for the working set may be viewed using the 'dolt_constraint_violations' system table.\n" +
	//			"They may be queried and removed per-table using the 'dolt_constraint_violations_TABLENAME' system table.")
	//	} else {
	//		err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	//		if err != nil {
	//			return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
	//		}
	//		verr = UpdateStagedWithVErr(dEnv, mergedRoot)
	//		if verr != nil {
	//			// Log a new message here to indicate that merge was successful, only staging failed.
	//			cli.Println("Unable to stage changes: add and commit to finish merge")
	//		}
	//	}
	//}

}

func conflictsAndViolations(tblToStats map[string]*merge.MergeStats) (conflicts []string, constraintViolations []string) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && (stats.Conflicts > 0 || stats.ConstraintViolations > 0) {
			if stats.Conflicts > 0 {
				conflicts = append(conflicts, tblName)
			}
			if stats.ConstraintViolations > 0 {
				constraintViolations = append(constraintViolations, tblName)
			}
		}
	}
}

