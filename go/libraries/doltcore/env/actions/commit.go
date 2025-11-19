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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

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

	CommitterDate  *time.Time
	CommitterName  string
	CommitterEmail string
}

// NewCommitStagedProps creates a new CommitStagedProps with the given author information. Committer fields are
// automatically populated from environment variables (DOLT_COMMITTER_NAME, DOLT_COMMITTER_EMAIL, DOLT_COMMITTER_DATE)
// if set, otherwise they default to the author values.
func NewCommitStagedProps(name, email string, date time.Time, message string) CommitStagedProps {
	committerName := datas.CommitterName
	if committerName == "" {
		committerName = name
	}
	committerEmail := datas.CommitterEmail
	if committerEmail == "" {
		committerEmail = email
	}

	return CommitStagedProps{
		Message:        message,
		Date:           date,
		Name:           name,
		Email:          email,
		CommitterName:  committerName,
		CommitterEmail: committerEmail,
		// CommitterDate if defined overrides the env var set for CommitterDate() later when the commit is written.
	}
}

// GetCommitStaged returns a new pending commit with the roots and commit properties given.
func GetCommitStaged(
	ctx *sql.Context,
	tableResolver doltdb.TableResolver,
	roots doltdb.Roots,
	ws *doltdb.WorkingSet,
	mergeParents []*doltdb.Commit,
	db *doltdb.DoltDB,
	props CommitStagedProps,
) (*doltdb.PendingCommit, error) {
	if props.Message == "" {
		return nil, datas.ErrEmptyCommitMessage
	}

	stagedTables, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var stagedTblNames []doltdb.TableName
	for _, td := range stagedTables {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	stagedSchemas, _, err := diff.GetStagedUnstagedDatabaseSchemaDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	isEmpty := len(stagedTables) == 0 && len(stagedSchemas) == 0
	allowEmpty := ws.MergeActive() || props.AllowEmpty || props.Amend

	if isEmpty && props.SkipEmpty {
		return nil, nil
	}
	if isEmpty && !allowEmpty {
		return nil, NothingStaged{notStaged}
	}

	if !props.Force {
		inConflict, err := doltdb.TablesWithDataConflicts(ctx, roots.Working)
		if err != nil {
			return nil, err
		}
		if len(inConflict) > 0 {
			return nil, NewTblInConflictError(inConflict)
		}
		violatesConstraints, err := doltdb.TablesWithConstraintViolations(ctx, roots.Working)
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

		roots.Staged, err = doltdb.ValidateForeignKeysOnSchemas(ctx, tableResolver, roots.Staged)
		if err != nil {
			return nil, err
		}
	}

	meta, err := datas.NewCommitMetaWithAuthorCommitter(props.Name, props.Email, props.Message, props.Date, props.CommitterName, props.CommitterEmail, props.CommitterDate)
	if err != nil {
		return nil, err
	}

	return db.NewPendingCommit(ctx, roots, mergeParents, props.Amend, meta)
}
