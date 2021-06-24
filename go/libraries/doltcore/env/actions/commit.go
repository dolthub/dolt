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
	"fmt"
	"sort"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/fkconstrain"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/hash"
)

type CommitStagedProps struct {
	Message          string
	Date             time.Time
	AllowEmpty       bool
	CheckForeignKeys bool
	Name             string
	Email            string
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
	name, err := cfg.GetString(env.UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(env.UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}

// CommitStaged adds a new commit to HEAD with the given props. Returns the new commit's hash as a string and an error.
func CommitStaged(ctx context.Context, roots doltdb.Roots, dbData env.DbData, props CommitStagedProps) (string, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw
	drw := dbData.Drw

	if props.Message == "" {
		return "", doltdb.ErrEmptyCommitMessage
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return "", err
	}

	var stagedTblNames []string
	for _, td := range staged {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	mergeActive, err := rsr.IsMergeActive(ctx)
	if err != nil {
		return "", err
	}

	if len(staged) == 0 && !mergeActive && !props.AllowEmpty {
		_, notStagedDocs, err := diff.GetDocDiffs(ctx, roots, drw)
		if err != nil {
			return "", err
		}
		return "", NothingStaged{notStaged, notStagedDocs}
	}

	var mergeParentCommits []*doltdb.Commit
	if mergeActive {
		inConflict, err := roots.Working.TablesInConflict(ctx)
		if err != nil {
			return "", err
		}
		if len(inConflict) > 0 {
			return "", NewTblInConflictError(inConflict)
		}

		commit, err := rsr.GetMergeCommit(ctx)
		if err != nil {
			return "", err
		}

		mergeParentCommits = []*doltdb.Commit{commit}
	}

	stagedRoot, err := roots.Staged.UpdateSuperSchemasFromOther(ctx, stagedTblNames, roots.Staged)
	if err != nil {
		return "", err
	}

	if props.CheckForeignKeys {
		stagedRoot, err = stagedRoot.ValidateForeignKeysOnSchemas(ctx)

		if err != nil {
			return "", err
		}

		err = fkconstrain.Validate(ctx, roots.Head, stagedRoot)
		if err != nil {
			return "", err
		}
	}

	// TODO: combine into a single update
	err = env.UpdateStagedRoot(ctx, rsw, stagedRoot)
	if err != nil {
		return "", err
	}

	workingRoot, err := roots.Working.UpdateSuperSchemasFromOther(ctx, stagedTblNames, stagedRoot)
	if err != nil {
		return "", err
	}

	err = env.UpdateWorkingRoot(ctx, rsw, workingRoot)
	if err != nil {
		return "", err
	}

	meta, err := doltdb.NewCommitMetaWithUserTS(props.Name, props.Email, props.Message, props.Date)
	if err != nil {
		return "", err
	}

	h, err := stagedRoot.HashOf()
	if err != nil {
		return "", err
	}

	// DoltDB resolves the current working branch head ref to provide a parent commit.
	c, err := ddb.CommitWithParentCommits(ctx, h, rsr.CWBHeadRef(), mergeParentCommits, meta)

	if err != nil {
		return "", err
	}

	err = rsw.ClearMerge(ctx)

	if err != nil {
		return "", err
	}

	h, err = c.HashOf()

	if err != nil {
		return "", err
	}

	return h.String(), nil
}

func ValidateForeignKeysOnCommit(ctx context.Context, srt *doltdb.RootValue, stagedTblNames []string) (*doltdb.RootValue, error) {
	// Validate schemas
	srt, err := srt.ValidateForeignKeysOnSchemas(ctx)
	if err != nil {
		return nil, err
	}
	// Validate data
	//TODO: make this more efficient, perhaps by leveraging diffs?
	fkColl, err := srt.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	fksToCheck := make(map[string]doltdb.ForeignKey)
	for _, tblName := range stagedTblNames {
		declaredFk, referencedByFk := fkColl.KeysForTable(tblName)
		for _, fk := range declaredFk {
			fksToCheck[fk.Name] = fk
		}
		for _, fk := range referencedByFk {
			fksToCheck[fk.Name] = fk
		}
	}

	for _, fk := range fksToCheck {
		childTbl, _, ok, err := srt.GetTableInsensitive(ctx, fk.TableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("foreign key '%s' references missing table '%s'", fk.Name, fk.TableName)
		}
		childSch, err := childTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		childIdx := childSch.Indexes().GetByName(fk.TableIndex)
		childIdxRowData, err := childTbl.GetIndexRowData(ctx, fk.TableIndex)
		if err != nil {
			return nil, err
		}
		parentTbl, _, ok, err := srt.GetTableInsensitive(ctx, fk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("foreign key '%s' references missing table '%s'", fk.Name, fk.ReferencedTableName)
		}
		parentTblSch, err := parentTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		parentIdx := parentTblSch.Indexes().GetByName(fk.ReferencedTableIndex)
		parentIdxRowData, err := parentTbl.GetIndexRowData(ctx, fk.ReferencedTableIndex)
		if err != nil {
			return nil, err
		}
		err = fk.ValidateData(ctx, childIdxRowData, parentIdxRowData, childIdx, parentIdx)
		if err != nil {
			return nil, err
		}
	}
	return srt, nil
}

// TimeSortedCommits returns a reverse-chronological (latest-first) list of the most recent `n` ancestors of `commit`.
// Passing a negative value for `n` will result in all ancestors being returned.
func TimeSortedCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, n int) ([]*doltdb.Commit, error) {
	hashToCommit := make(map[hash.Hash]*doltdb.Commit)
	err := AddCommits(ctx, ddb, commit, hashToCommit, n)

	if err != nil {
		return nil, err
	}

	idx := 0
	uniqueCommits := make([]*doltdb.Commit, len(hashToCommit))
	for _, v := range hashToCommit {
		uniqueCommits[idx] = v
		idx++
	}

	var sortErr error
	var metaI, metaJ *doltdb.CommitMeta
	sort.Slice(uniqueCommits, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		metaI, sortErr = uniqueCommits[i].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		metaJ, sortErr = uniqueCommits[j].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		return metaI.UserTimestamp > metaJ.UserTimestamp
	})

	if sortErr != nil {
		return nil, sortErr
	}

	return uniqueCommits, nil
}

func AddCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, hashToCommit map[hash.Hash]*doltdb.Commit, n int) error {
	hash, err := commit.HashOf()

	if err != nil {
		return err
	}

	if _, ok := hashToCommit[hash]; ok {
		return nil
	}

	hashToCommit[hash] = commit

	numParents, err := commit.NumParents()

	if err != nil {
		return err
	}

	for i := 0; i < numParents && len(hashToCommit) != n; i++ {
		parentCommit, err := ddb.ResolveParent(ctx, commit, i)

		if err != nil {
			return err
		}

		err = AddCommits(ctx, ddb, parentCommit, hashToCommit, n)

		if err != nil {
			return err
		}
	}

	return nil
}
