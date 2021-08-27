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
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

const forceFlag = "force"

type MergeSpec struct {
	H1           hash.Hash
	H2           hash.Hash
	Cm1          *doltdb.Commit
	Cm2          *doltdb.Commit
	TblNames     []string
	WorkingDiffs map[string]hash.Hash
	squash       bool
	msg          string
	noff         bool
}

func PrepareMergeSpec(ctx context.Context, squash bool, dEnv *env.DoltEnv, commitSpecStr string, msg string, noff bool) (*MergeSpec, bool, error) {
	cs1, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm1, err := dEnv.DoltDB.Resolve(context.TODO(), cs1, dEnv.RepoStateReader().CWBHeadRef())

	cs2, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm2, err := dEnv.DoltDB.Resolve(context.TODO(), cs2, dEnv.RepoStateReader().CWBHeadRef())

	h1, err := cm1.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err
	}

	h2, err := cm2.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err

	}

	if h1 == h2 {
		return nil, false, err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return nil, false, err
	}

	tblNames, workingDiffs, err := merge.MergeWouldStompChanges(ctx, roots, cm2)
	if err != nil {
		return nil, false, err
		//return errhand.BuildDError("error: failed to determine mergability.").AddCause(err).Build()
	}

	return &MergeSpec{
		H1:           h1,
		H2:           h2,
		Cm1:          cm1,
		Cm2:          cm2,
		TblNames:     tblNames,
		WorkingDiffs: workingDiffs,
		squash:       squash,
		msg:          msg,
		noff:         noff,
	}, true, nil
}
func MergeCommitSpec(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, mergeSpec *MergeSpec) ([]string, []string, error) {

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			//verr = errhand.BuildDError("error: invalid date").AddCause(err).Build()
			//return handleCommitErr(ctx, dEnv, verr, usage)
			return []string{}, []string{}, err
		}
	}

	name, email, err := GetNameAndEmail(dEnv.Config)
	if err != nil {
		//verr = errhand.BuildDError("error: committing").AddCause(err).Build()
		//return handleCommitErr(ctx, dEnv, verr, usage)
		return []string{}, []string{}, err
	}

	if ok, err := mergeSpec.Cm1.CanFastForwardTo(ctx, mergeSpec.Cm2); ok {
		ancRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return []string{}, []string{}, err
		}
		mergedRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return []string{}, []string{}, err
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return []string{}, []string{}, err
		} else if cvPossible {
			return ExecuteMerge(ctx, mergeSpec.squash, dEnv, mergeSpec.Cm1, mergeSpec.Cm2, mergeSpec.WorkingDiffs)
		}
		if mergeSpec.noff {
			return ExecNoFFMerge(ctx, apr, dEnv, mergeSpec.Cm2, mergeSpec.WorkingDiffs, mergeSpec.msg, name, email, t)
		} else {
			return []string{}, []string{}, ExecuteFFMerge(ctx, mergeSpec.squash, dEnv, mergeSpec.Cm2, mergeSpec.WorkingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		return []string{}, []string{}, nil
	} else {
		return ExecuteMerge(ctx, mergeSpec.squash, dEnv, mergeSpec.Cm1, mergeSpec.Cm2, mergeSpec.WorkingDiffs)
	}
}

func ExecNoFFMerge(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash, msg string, name string, email string, t time.Time) ([]string, []string, error) {
	mergedRoot, err := cm2.GetRootValue()

	if err != nil {
		//return errhand.BuildDError("error: reading from database").AddCause(err).Build()
		return []string{}, []string{}, err
	}

	conflicts, constraintViolations, err := mergedRootToWorking(ctx, false, dEnv, mergedRoot, workingDiffs, cm2, map[string]*merge.MergeStats{})

	if err != nil {
		return conflicts, constraintViolations, err
	}

	// Reload roots since the above method writes new values to the working set
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return conflicts, constraintViolations, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return conflicts, constraintViolations, err
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
		//return errhand.BuildDError("error: committing").AddCause(err).Build()
		return conflicts, constraintViolations, err
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return conflicts, constraintViolations, err
	}

	return conflicts, constraintViolations, nil
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, error) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			//return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
			return nil, err
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
) error {
	stagedRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		//return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
		return err
	}

	workingRoot := stagedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, workingDiffs)

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

	if !squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), mergeCommit)

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
		return err
	}

	err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		//return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
		return err
	}

	return nil
}

func ExecuteMerge(ctx context.Context, squash bool, dEnv *env.DoltEnv, cm1, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) ([]string, []string, error) {
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, cm1, cm2)
	if err != nil {
		return []string{}, []string{}, err
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
) ([]string, []string, error) {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			//return errhand.BuildDError("").AddCause(err).Build()
			return []string{}, []string{}, err

		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2)

		if err != nil {
			//return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
			return []string{}, []string{}, err
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
		return []string{}, []string{}, err
	}

	//verr := UpdateWorkingWithVErr(dEnv, workingRoot)
	err = dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		panic("max fix")
	}

	conflicts, constraintViolations := conflictsAndViolations(tblToStats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return conflicts, constraintViolations, nil
	}

	if err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs); err != nil {
		return conflicts, constraintViolations, err
	}

	if err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot); err != nil {
		return conflicts, constraintViolations, err
	}

	return conflicts, constraintViolations, nil
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
