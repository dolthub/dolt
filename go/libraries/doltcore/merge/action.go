// Copyright 2021 Dolthub, Inc.
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

package merge

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrFailedToDetermineUnstagedDocs = errors.New("failed to determine unstaged docs")
var ErrFailedToReadDatabase = errors.New("failed to read database")
var ErrMergeFailedToUpdateDocs = errors.New("failed to update docs to the new working root")
var ErrMergeFailedToUpdateRepoState = errors.New("unable to execute repo state update")
var ErrFailedToDetermineMergeability = errors.New("failed to determine mergeability")

type MergeSpec struct {
	HeadH           hash.Hash
	MergeH          hash.Hash
	HeadC           *doltdb.Commit
	MergeC          *doltdb.Commit
	MergeCSpecStr   string
	StompedTblNames []string
	WorkingDiffs    map[string]hash.Hash
	Squash          bool
	Msg             string
	Noff            bool
	NoCommit        bool
	NoEdit          bool
	Force           bool
	AllowEmpty      bool
	Email           string
	Name            string
	Date            time.Time
}

// NewMergeSpec returns MergeSpec object using arguments passed into this function, which are doltdb.Roots, username,
// user email, commit msg, commitSpecStr, to squash, to noff, to force, noCommit, noEdit and date. This function
// resolves head and merge commit, and it gets current diffs between current head and working set if it exists.
func NewMergeSpec(ctx context.Context, rsr env.RepoStateReader, ddb *doltdb.DoltDB, roots doltdb.Roots, name, email, msg, commitSpecStr string, squash, noff, force, noCommit, noEdit bool, date time.Time) (*MergeSpec, error) {
	headCS, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return nil, err
	}

	headCM, err := ddb.Resolve(context.TODO(), headCS, rsr.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	mergeCS, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		return nil, err
	}

	mergeCM, err := ddb.Resolve(context.TODO(), mergeCS, rsr.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	headH, err := headCM.HashOf()
	if err != nil {
		return nil, err
	}

	mergeH, err := mergeCM.HashOf()
	if err != nil {
		return nil, err

	}

	stompedTblNames, workingDiffs, err := MergeWouldStompChanges(ctx, roots, mergeCM)
	if err != nil {
		return nil, fmt.Errorf("%w; %s", ErrFailedToDetermineMergeability, err.Error())
	}

	return &MergeSpec{
		HeadH:           headH,
		MergeH:          mergeH,
		HeadC:           headCM,
		MergeCSpecStr:   commitSpecStr,
		MergeC:          mergeCM,
		StompedTblNames: stompedTblNames,
		WorkingDiffs:    workingDiffs,
		Squash:          squash,
		Msg:             msg,
		Noff:            noff,
		NoCommit:        noCommit,
		NoEdit:          noEdit,
		Force:           force,
		Email:           email,
		Name:            name,
		Date:            date,
	}, nil
}

func ExecNoFFMerge(ctx context.Context, dEnv *env.DoltEnv, spec *MergeSpec) (map[string]*MergeStats, error) {
	mergedRoot, err := spec.MergeC.GetRootValue(ctx)
	if err != nil {
		return nil, ErrFailedToReadDatabase
	}
	result := &Result{Root: mergedRoot, Stats: make(map[string]*MergeStats)}

	err = mergedRootToWorking(ctx, false, dEnv, result, spec.WorkingDiffs, spec.MergeC, spec.MergeCSpecStr)

	return result.Stats, err
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, error) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			return nil, fmt.Errorf("failed to update table; %w", err)
		}
	}

	return root, nil
}

func ExecuteFFMerge(
	ctx context.Context,
	dEnv *env.DoltEnv,
	spec *MergeSpec,
) error {
	stagedRoot, err := spec.MergeC.GetRootValue(ctx)
	if err != nil {
		return err
	}

	workingRoot := stagedRoot
	if len(spec.WorkingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, spec.WorkingDiffs)

		if err != nil {
			//return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
			return err
		}
	}

	if !spec.Squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), spec.MergeC)

		if err != nil {
			return err
		}
	}

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
}

func ExecuteMerge(ctx context.Context, dEnv *env.DoltEnv, spec *MergeSpec) (map[string]*MergeStats, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: tmpDir}
	result, err := MergeCommits(ctx, spec.HeadC, spec.MergeC, opts)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return result.Stats, fmt.Errorf("already up to date; %w", err)
		case ErrFastForward:
			panic("fast forward merge")
		}
		return result.Stats, err
	}

	err = mergedRootToWorking(ctx, spec.Squash, dEnv, result, spec.WorkingDiffs, spec.MergeC, spec.MergeCSpecStr)
	if err != nil {
		return nil, err
	}
	return result.Stats, nil
}

// TODO: change this to be functional and not write to repo state
func mergedRootToWorking(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	result *Result,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	cm2SpecStr string,
) error {
	var err error

	workingRoot := result.Root
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, result.Root, workingDiffs)

		if err != nil {
			return err
		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2, cm2SpecStr)
		if err != nil {
			return actions.ErrFailedToSaveRepoState
		}
		// todo: update merge state with schema conflicts
	}

	err = dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		return err
	}

	conflicts, constraintViolations := conflictsAndViolations(result.Stats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return err
	}

	return dEnv.UpdateStagedRoot(context.Background(), result.Root)
}

// conflictsAndViolations returns array of conflicts and constraintViolations
func conflictsAndViolations(tblToStats map[string]*MergeStats) (conflicts []string, constraintViolations []string) {
	for tblName, stats := range tblToStats {
		if stats.Operation == TableModified && (stats.Conflicts > 0 || stats.ConstraintViolations > 0) {
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
