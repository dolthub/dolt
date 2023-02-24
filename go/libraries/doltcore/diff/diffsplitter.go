// Copyright 2022 Dolthub, Inc.
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

package diff

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

type DiffSplitter struct {
	diffQuerySch  sql.Schema
	targetSch     sql.Schema
	queryToTarget map[int]int
	fromTo        map[int]int
	toFrom        map[int]int
	fromLen       int
}

type RowDiff struct {
	Row      sql.Row
	RowDiff  ChangeType
	ColDiffs []ChangeType
}

// NewDiffSplitter returns a splitter that knows how to split unified diff query rows with the schema given into
// |old| and |new| rows in the union schema given. In the diff query schema, all |from| columns are expected to precede
// all |to| columns
func NewDiffSplitter(diffQuerySch sql.Schema, targetSch sql.Schema) (*DiffSplitter, error) {
	resultToTarget := make(map[int]int)
	fromTo := make(map[int]int)
	toFrom := make(map[int]int)
	fromLen := -1

	for i := 0; i < len(diffQuerySch)-1; i++ {
		var baseColName string
		if strings.HasPrefix(diffQuerySch[i].Name, "from_") {
			baseColName = diffQuerySch[i].Name[5:]
			if to := diffQuerySch.IndexOfColName("to_" + baseColName); to >= 0 {
				fromTo[i] = to
			}
		} else if strings.HasPrefix(diffQuerySch[i].Name, "to_") {
			// we order the columns so that all from_ come first
			if fromLen == -1 {
				fromLen = i
			}
			baseColName = diffQuerySch[i].Name[3:]
			if from := diffQuerySch.IndexOfColName("from_" + baseColName); from >= 0 {
				toFrom[i] = from
			}
		}

		targetIdx := targetSch.IndexOfColName(baseColName)
		if targetIdx < 0 {
			return nil, fmt.Errorf("couldn't find a column named %s", baseColName)
		}

		resultToTarget[i] = targetIdx
	}

	if fromLen == -1 {
		fromLen = len(diffQuerySch) - 1
	}

	return &DiffSplitter{
		diffQuerySch:  diffQuerySch,
		targetSch:     targetSch,
		fromLen:       fromLen,
		queryToTarget: resultToTarget,
		fromTo:        fromTo,
		toFrom:        toFrom,
	}, nil
}

func newRowDiff(size int) RowDiff {
	return RowDiff{
		ColDiffs: make([]ChangeType, size),
	}
}

func (ds DiffSplitter) SplitDiffResultRow(row sql.Row) (RowDiff, RowDiff, error) {
	// split rows in the result set into old, new
	diffTypeColIdx := ds.diffQuerySch.IndexOfColName("diff_type")
	if diffTypeColIdx < 0 {
		return RowDiff{}, RowDiff{}, fmt.Errorf("expected a diff_type column")
	}

	diffType := row[diffTypeColIdx]

	oldRow, newRow := newRowDiff(len(ds.targetSch)), newRowDiff(len(ds.targetSch))

	diffTypeStr := diffType.(string)
	if diffTypeStr == "removed" || diffTypeStr == "modified" {
		oldRow.Row = make(sql.Row, len(ds.targetSch))
		if diffTypeStr == "modified" {
			oldRow.RowDiff = ModifiedOld
		} else {
			oldRow.RowDiff = Removed
		}

		for i := 0; i < ds.fromLen; i++ {
			cmp := ds.diffQuerySch[i].Type.Compare
			oldRow.Row[ds.queryToTarget[i]] = row[i]

			if diffTypeStr == "modified" {
				fromToIndex, ok := ds.fromTo[i]
				if ok {
					if n, err := cmp(row[i], row[fromToIndex]); err != nil {
						return RowDiff{}, RowDiff{}, err
					} else if n != 0 {
						oldRow.ColDiffs[ds.queryToTarget[i]] = ModifiedOld
					}
				} else {
					oldRow.ColDiffs[ds.queryToTarget[i]] = ModifiedOld
				}
			} else {
				oldRow.ColDiffs[ds.queryToTarget[i]] = Removed
			}
		}
	}

	if diffTypeStr == "added" || diffTypeStr == "modified" {
		newRow.Row = make(sql.Row, len(ds.targetSch))
		if diffTypeStr == "modified" {
			newRow.RowDiff = ModifiedNew
		} else {
			newRow.RowDiff = Added
		}

		for i := ds.fromLen; i < len(ds.diffQuerySch)-1; i++ {
			cmp := ds.diffQuerySch[i].Type.Compare
			newRow.Row[ds.queryToTarget[i]] = row[i]

			if diffTypeStr == "modified" {
				// need this to compare map[string]interface{} and other incomparable result types
				if n, err := cmp(row[i], row[ds.toFrom[i]]); err != nil {
					return RowDiff{}, RowDiff{}, err
				} else if n != 0 {
					newRow.ColDiffs[ds.queryToTarget[i]] = ModifiedNew
				}
			} else {
				newRow.ColDiffs[ds.queryToTarget[i]] = Added
			}
		}
	}

	return oldRow, newRow, nil
}

// MaybeResolveRoot returns a root value and true if the a commit exists for given spec string; nil and false if it does not exist.
// todo: distinguish between non-existent CommitSpec and other errors, don't assume non-existent
func MaybeResolveRoot(ctx context.Context, rsr env.RepoStateReader, doltDB *doltdb.DoltDB, spec string) (*doltdb.RootValue, bool) {
	cs, err := doltdb.NewCommitSpec(spec)
	if err != nil {
		// it's non-existent CommitSpec
		return nil, false
	}

	cm, err := doltDB.Resolve(ctx, cs, rsr.CWBHeadRef())
	if err != nil {
		return nil, false
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, false
	}

	return root, true
}
