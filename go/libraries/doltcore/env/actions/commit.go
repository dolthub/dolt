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
	"github.com/dolthub/dolt/go/store/datas"
)

type CommitStagedProps struct {
	Message    string
	Date       time.Time
	AllowEmpty bool
	SkipEmpty  bool
	Amend      bool
	Force      bool
	Name       string
	Email      string
}

// GetCommitStaged returns a new pending commit with the roots and commit properties given.
func GetCommitStaged(
	ctx context.Context,
	roots doltdb.Roots,
	ws *doltdb.WorkingSet,
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
	for _, delta := range staged {
		td := delta.GetBaseInfo()
		n := td.ToName
		if delta.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	isEmpty := len(staged) == 0
	allowEmpty := ws.MergeActive() || props.AllowEmpty || props.Amend

	if isEmpty && props.SkipEmpty {
		return nil, nil
	}
	if isEmpty && !allowEmpty {
		notStagedTDs := make([]diff.TableDeltaEngine, len(notStaged))
		for i, td := range notStaged {
			notStagedTDs[i] = td
		}
		return nil, NothingStaged{notStagedTDs}
	}

	if !props.Force {
		inConflict, err := roots.Working.TablesWithDataConflicts(ctx)
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

		if ws.MergeActive() {
			schConflicts := ws.MergeState().TablesWithSchemaConflicts()
			if len(schConflicts) > 0 {
				return nil, NewTblSchemaConflictError(schConflicts)
			}
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
