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
	HeadH        hash.Hash
	MergeH       hash.Hash
	HeadC        *doltdb.Commit
	MergeC       *doltdb.Commit
	TblNames     []string
	WorkingDiffs map[string]hash.Hash
	Squash       bool
	Msg          string
	Noff         bool
	Force        bool
	AllowEmpty   bool
	Email        string
	Name         string
	Date         time.Time
}

func NewMergeSpec(ctx context.Context, rsr env.RepoStateReader, ddb *doltdb.DoltDB, roots doltdb.Roots, name, email, msg string, commitSpecStr string, squash bool, noff bool, force bool, date time.Time) (*MergeSpec, bool, error) {
	cs1, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return nil, false, err
	}

	cm1, err := ddb.Resolve(context.TODO(), cs1, rsr.CWBHeadRef())
	if err != nil {
		return nil, false, err
	}

	cs2, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		return nil, false, err
	}

	cm2, err := ddb.Resolve(context.TODO(), cs2, rsr.CWBHeadRef())
	if err != nil {
		return nil, false, err
	}

	h1, err := cm1.HashOf()
	if err != nil {
		return nil, false, err
	}

	h2, err := cm2.HashOf()
	if err != nil {
		return nil, false, err

	}

	tblNames, workingDiffs, err := MergeWouldStompChanges(ctx, roots, cm2)
	if err != nil {
		return nil, false, fmt.Errorf("%w; %s", ErrFailedToDetermineMergeability, err.Error())
	}

	return &MergeSpec{
		HeadH:        h1,
		MergeH:       h2,
		HeadC:        cm1,
		MergeC:       cm2,
		TblNames:     tblNames,
		WorkingDiffs: workingDiffs,
		Squash:       squash,
		Msg:          msg,
		Noff:         noff,
		Force:        force,
		Email:        email,
		Name:         name,
		Date:         date,
	}, true, nil
}

// TODO forcing a commit with a constrain violation should warn users that subsequest
//      FF merges will not surface constraint violations on their own; constraint verify --all
//      is required to reify violations.
func MergeCommitSpec(ctx context.Context, dEnv *env.DoltEnv, spec *MergeSpec) (map[string]*MergeStats, error) {
	if ok, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC); err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
		return nil, err
	} else if ok {
		if spec.Noff {
			return ExecNoFFMerge(ctx, dEnv, spec)
		}
		return nil, ExecuteFFMerge(ctx, dEnv, spec)
	}
	return ExecuteMerge(ctx, dEnv, spec)
}

func ExecNoFFMerge(ctx context.Context, dEnv *env.DoltEnv, spec *MergeSpec) (map[string]*MergeStats, error) {
	mergedRoot, err := spec.MergeC.GetRootValue(ctx)

	if err != nil {
		return nil, ErrFailedToReadDatabase
	}

	tblToStats := make(map[string]*MergeStats)
	err = mergedRootToWorking(ctx, false, dEnv, mergedRoot, spec.WorkingDiffs, spec.MergeC, tblToStats)

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

	_, err = actions.CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    spec.Msg,
		Date:       spec.Date,
		AllowEmpty: spec.AllowEmpty,
		Force:      spec.Force,
		Name:       spec.Name,
		Email:      spec.Email,
	})

	if err != nil {
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

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		return err
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

	err = dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
	if err != nil {
		return ErrMergeFailedToUpdateRepoState
	}

	err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		return ErrMergeFailedToUpdateDocs
	}

	return nil
}

func ExecuteMerge(ctx context.Context, dEnv *env.DoltEnv, spec *MergeSpec) (map[string]*MergeStats, error) {
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
	mergedRoot, tblToStats, err := MergeCommits(ctx, spec.HeadC, spec.MergeC, opts)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return tblToStats, fmt.Errorf("already up to date; %w", err)
		case ErrFastForward:
			panic("fast forward merge")
		}
		return tblToStats, err
	}

	return tblToStats, mergedRootToWorking(ctx, spec.Squash, dEnv, mergedRoot, spec.WorkingDiffs, spec.MergeC, tblToStats)
}

// TODO: change this to be functional and not write to repo state
func mergedRootToWorking(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergedRoot *doltdb.RootValue,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	tblToStats map[string]*MergeStats,
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
			return actions.ErrFailedToSaveRepoState
		}
	}

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		return ErrFailedToDetermineUnstagedDocs
	}

	err = dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		return err
	}

	conflicts, constraintViolations := conflictsAndViolations(tblToStats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return err
	}

	if err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs); err != nil {
		return err
	}

	if err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot); err != nil {
		return err
	}

	return nil
}

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
