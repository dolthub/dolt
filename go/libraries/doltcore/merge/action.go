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
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrFailedToDetermineMergeability = errors.New("failed to determine mergeability")

type MergeSpec struct {
	HeadH           hash.Hash
	MergeH          hash.Hash
	HeadC           *doltdb.Commit
	MergeC          *doltdb.Commit
	MergeCSpecStr   string
	StompedTblNames []doltdb.TableName
	WorkingDiffs    map[doltdb.TableName]hash.Hash
	Squash          bool
	NoFF            bool
	NoCommit        bool
	NoEdit          bool
	Force           bool
	Email           string
	Name            string
	Date            time.Time
}

type MergeSpecOpt func(*MergeSpec)

func WithNoFF(noFF bool) MergeSpecOpt {
	return func(ms *MergeSpec) {
		ms.NoFF = noFF
	}
}

func WithNoCommit(noCommit bool) MergeSpecOpt {
	return func(ms *MergeSpec) {
		ms.NoCommit = noCommit
	}
}

func WithNoEdit(noEdit bool) MergeSpecOpt {
	return func(ms *MergeSpec) {
		ms.NoEdit = noEdit
	}
}

func WithForce(force bool) MergeSpecOpt {
	return func(ms *MergeSpec) {
		ms.Force = force
	}
}

func WithSquash(squash bool) MergeSpecOpt {
	return func(ms *MergeSpec) {
		ms.Squash = squash
	}
}

// NewMergeSpec returns a MergeSpec with the arguments provided.
func NewMergeSpec[C doltdb.Context](
	ctx C,
	rsr env.RepoStateReader[C],
	ddb *doltdb.DoltDB,
	roots doltdb.Roots,
	name, email, commitSpecStr string,
	date time.Time,
	opts ...MergeSpecOpt,
) (*MergeSpec, error) {
	headCS, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return nil, err
	}

	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return nil, err
	}

	optCmt, err := ddb.Resolve(ctx, headCS, headRef)
	if err != nil {
		return nil, err
	}
	headCM, ok := optCmt.ToCommit()
	if !ok {
		// HEAD should always resolve to a commit, so this should never happen.
		return nil, doltdb.ErrGhostCommitRuntimeFailure
	}

	mergeCS, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		return nil, err
	}

	optCmt, err = ddb.Resolve(ctx, mergeCS, headRef)
	if err != nil {
		return nil, err
	}
	mergeCM, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
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

	spec := &MergeSpec{
		HeadH:           headH,
		MergeH:          mergeH,
		HeadC:           headCM,
		MergeCSpecStr:   commitSpecStr,
		MergeC:          mergeCM,
		StompedTblNames: stompedTblNames,
		WorkingDiffs:    workingDiffs,
		Email:           email,
		Name:            name,
		Date:            date,
	}

	for _, opt := range opts {
		opt(spec)
	}

	return spec, nil
}

// AbortMerge returns a new WorkingSet instance, with the active merge aborted, by clearing and
// resetting the merge state in |workingSet| and using |roots| to identify the existing tables
// and reset them, excluding any ignored tables. The caller must then set the new WorkingSet in
// the session before the aborted merge is finalized. If no merge is in progress, this function
// returns an error.
func AbortMerge(ctx *sql.Context, workingSet *doltdb.WorkingSet, roots doltdb.Roots) (*doltdb.WorkingSet, error) {
	if !workingSet.MergeActive() {
		return nil, fmt.Errorf("there is no merge to abort")
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots.Working, roots.Staged, roots.Head)
	if err != nil {
		return nil, err
	}
	tbls, err = doltdb.ExcludeIgnoredTables(ctx, roots, tbls)
	if err != nil {
		return nil, err
	}

	roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, tbls)
	if err != nil {
		return nil, err
	}

	preMergeWorkingRoot := workingSet.MergeState().PreMergeWorkingRoot()
	preMergeWorkingTables, err := preMergeWorkingRoot.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	nonIgnoredTables, err := doltdb.ExcludeIgnoredTables(ctx, roots, doltdb.ToTableNames(preMergeWorkingTables, doltdb.DefaultSchemaName))
	if err != nil {
		return nil, err
	}
	someTablesAreIgnored := len(nonIgnoredTables) != len(preMergeWorkingTables)

	if someTablesAreIgnored {
		newWorking, err := actions.MoveTablesBetweenRoots(ctx, nonIgnoredTables, preMergeWorkingRoot, roots.Working)
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithWorkingRoot(newWorking)
	} else {
		workingSet = workingSet.WithWorkingRoot(preMergeWorkingRoot)
	}
	// Unstage everything by making Staged match Head
	workingSet = workingSet.WithStagedRoot(roots.Head)
	workingSet = workingSet.ClearMerge()

	return workingSet, nil
}
