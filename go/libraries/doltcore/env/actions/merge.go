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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrFailedToDetermineUnstagedDocs = errors.New("failed to determine unstaged docs")
var ErrFailedToReadDatabase = errors.New("failed to read database")
var ErrMergeFailedToUpdateDocs = errors.New("failed to update docs to the new working root")
var ErrMergeFailedToUpdateRepoState = errors.New("unable to execute repo state update")

func MergeCommitSpec(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *env.MergeSpec) (map[string]*merge.MergeStats, error) {

	if ok, err := mergeSpec.Cm1.CanFastForwardTo(ctx, mergeSpec.Cm2); ok {
		ancRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return nil, err
		}
		mergedRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return nil, err
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return nil, err
		} else if cvPossible {
			return ExecuteMerge(ctx, dEnv, mergeSpec)
		}
		if mergeSpec.Noff {
			return ExecNoFFMerge(ctx, dEnv, mergeSpec)
		} else {
			return nil, ExecuteFFMerge(ctx, dEnv, mergeSpec)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		return nil, err
	} else {
		return ExecuteMerge(ctx, dEnv, mergeSpec)
	}
}

func ExecNoFFMerge(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *env.MergeSpec) (map[string]*merge.MergeStats, error) {
	mergedRoot, err := mergeSpec.Cm2.GetRootValue()

	if err != nil {
		//return errhand.BuildDError("error: reading from database").AddCause(err).Build()
		return nil, ErrFailedToReadDatabase
	}

	tblToStats := make(map[string]*merge.MergeStats)
	err = mergedRootToWorking(ctx, false, dEnv, mergedRoot, mergeSpec.WorkingDiffs, mergeSpec.Cm2, tblToStats)

	if err != nil {
		return tblToStats, err
	}

	// Reload roots since the above method writes new values to the working set
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return tblToStats, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return tblToStats, err
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), CommitStagedProps{
		Message:    mergeSpec.Msg,
		Date:       mergeSpec.Date,
		AllowEmpty: mergeSpec.AllowEmpty,
		Force:      mergeSpec.Force,
		Name:       mergeSpec.Name,
		Email:      mergeSpec.Email,
	})

	if err != nil {
		//return errhand.BuildDError("error: committing").AddCause(err).Build()
		return tblToStats, fmt.Errorf("%w; failed to commit", err)
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return tblToStats, err
	}

	return tblToStats, err
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, error) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			//return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
			return nil, fmt.Errorf("failed to update table; %w", err)
		}
	}

	return root, nil
}

func ExecuteFFMerge(
	ctx context.Context,
	dEnv *env.DoltEnv,
	mergeSpec *env.MergeSpec,
) error {
	stagedRoot, err := mergeSpec.Cm2.GetRootValue()
	if err != nil {
		//return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
		return err
	}

	workingRoot := stagedRoot
	if len(mergeSpec.WorkingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, mergeSpec.WorkingDiffs)

		if err != nil {
			//return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
			return err
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: unable to determine unstaged docs").AddCause(err).Build()
		return err
	}

	if !mergeSpec.Squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), mergeSpec.Cm2)

		if err != nil {
			//return errhand.BuildDError("Failed to write database").AddCause(err).Build()
			return err
		}
	}

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
	if err != nil {
		//		return errhand.BuildDError("unable to execute repo state update.").
		//			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
		//At the moment the best way to fix this is to run:
		//
		//    dolt branch -v
		//
		//and take the hash for your current branch and use it for the value for "staged" and "working"`).
		//			AddCause(err).Build()
		return ErrMergeFailedToUpdateRepoState
	}

	err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		//return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
		return ErrMergeFailedToUpdateDocs
	}

	return nil
}

func ExecuteMerge(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *env.MergeSpec) (map[string]*merge.MergeStats, error) {
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, mergeSpec.Cm1, mergeSpec.Cm2)
	if err != nil {
		return tblToStats, err
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

	return tblToStats, mergedRootToWorking(ctx, mergeSpec.Squash, dEnv, mergedRoot, mergeSpec.WorkingDiffs, mergeSpec.Cm2, tblToStats)
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
) error {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			return err
		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2)

		if err != nil {
			//return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
			return ErrFailedToSaveRepoState
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
		return ErrFailedToDetermineUnstagedDocs
	}

	//verr := UpdateWorkingWithVErr(dEnv, workingRoot)
	err = dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		return err
	}

	conflicts, constraintViolations := conflictsAndViolations(tblToStats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return err
	}

	if err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs); err != nil {
		return err
	}

	if err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot); err != nil {
		return err
	}

	return nil
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
	return
}
