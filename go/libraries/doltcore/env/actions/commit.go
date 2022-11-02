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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/datas"
)

type CommitStagedProps struct {
	Message    string
	Date       time.Time
	AllowEmpty bool
	Amend      bool
	Force      bool
	Name       string
	Email      string
}

// CommitStaged adds a new commit to HEAD with the given props. Returns the new commit's hash as a string and an error.
func CommitStaged(ctx context.Context, roots doltdb.Roots, mergeActive bool, mergeParents []*doltdb.Commit, dbData env.DbData, props CommitStagedProps) (*doltdb.Commit, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw

	if props.Message == "" {
		return nil, datas.ErrEmptyCommitMessage
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var stagedTblNames []string
	for _, td := range staged {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	if len(staged) == 0 && !mergeActive && !props.AllowEmpty {
		return nil, NothingStaged{notStaged}
	}

	if !props.Force {
		inConflict, err := roots.Working.TablesInConflict(ctx)
		if err != nil {
			return nil, err
		}
		if len(inConflict) > 0 {
			return nil, NewTblInConflictError(inConflict)
		}
		violatesConstraints, err := roots.Working.TablesWithConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		if len(violatesConstraints) > 0 {
			return nil, NewTblHasConstraintViolations(violatesConstraints)
		}
	}

	stagedRoot := roots.Staged
	if err != nil {
		return nil, err
	}

	if !props.Force {
		stagedRoot, err = stagedRoot.ValidateForeignKeysOnSchemas(ctx)
		if err != nil {
			return nil, err
		}
	}

	// TODO: combine into a single update
	err = rsw.UpdateStagedRoot(ctx, stagedRoot)
	if err != nil {
		return nil, err
	}

	workingRoot := roots.Working
	if err != nil {
		return nil, err
	}

	err = rsw.UpdateWorkingRoot(ctx, workingRoot)
	if err != nil {
		return nil, err
	}

	meta, err := datas.NewCommitMetaWithUserTS(props.Name, props.Email, props.Message, props.Date)
	if err != nil {
		return nil, err
	}

	// TODO: this is only necessary in some contexts (SQL). Come up with a more coherent set of interfaces to
	//  rationalize where the root value writes happen before a commit is created.
	r, h, err := ddb.WriteRootValue(ctx, stagedRoot)
	if err != nil {
		return nil, err
	}
	stagedRoot = r

	// logrus.Errorf("staged root is %s", stagedRoot.DebugString(ctx, true))

	// DoltDB resolves the current working branch head ref to provide a parent commit.
	c, err := ddb.CommitWithParentCommits(ctx, h, rsr.CWBHeadRef(), mergeParents, meta)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// GetCommitStaged returns a new pending commit with the roots and commit properties given.
func GetCommitStaged(
	ctx context.Context,
	roots doltdb.Roots,
	mergeActive bool,
	mergeParents []*doltdb.Commit,
	db *doltdb.DoltDB,
	props CommitStagedProps,
) (*doltdb.PendingCommit, error) {
	if props.Message == "" {
		return nil, datas.ErrEmptyCommitMessage
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var stagedTblNames []string
	for _, td := range staged {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	isEmpty := len(staged) == 0
	allowEmpty := mergeActive || props.AllowEmpty || props.Amend
	if isEmpty && !allowEmpty {
		return nil, NothingStaged{notStaged}
	}

	if !props.Force {
		inConflict, err := roots.Working.TablesInConflict(ctx)
		if err != nil {
			return nil, err
		}
		if len(inConflict) > 0 {
			return nil, NewTblInConflictError(inConflict)
		}
		violatesConstraints, err := roots.Working.TablesWithConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		if len(violatesConstraints) > 0 {
			return nil, NewTblHasConstraintViolations(violatesConstraints)
		}
	}

	if !props.Force {
		roots.Staged, err = roots.Staged.ValidateForeignKeysOnSchemas(ctx)
		if err != nil {
			return nil, err
		}
	}

	meta, err := datas.NewCommitMetaWithUserTS(props.Name, props.Email, props.Message, props.Date)
	if err != nil {
		return nil, err
	}

	return db.NewPendingCommit(ctx, roots, mergeParents, meta)
}
