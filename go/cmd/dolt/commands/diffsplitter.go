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

package commands

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
)

type diffSplitter struct {
	diffQuerySch  sql.Schema
	targetSch     sql.Schema
	queryToTarget map[int]int
	fromTo        map[int]int
	toFrom        map[int]int
	fromLen       int
}

type rowDiff struct {
	row      sql.Row
	rowDiff  diff.ChangeType
	colDiffs []diff.ChangeType
}

// newDiffSplitter returns a splitter that knows how to split unified diff query rows with the schema given into
// |old| and |new| rows in the union schema given. In the diff query schema, all |from| columns are expected to precede
// all |to| columns
func newDiffSplitter(diffQuerySch sql.Schema, targetSch sql.Schema) (*diffSplitter, error) {
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

	return &diffSplitter{
		diffQuerySch:  diffQuerySch,
		targetSch:     targetSch,
		fromLen:       fromLen,
		queryToTarget: resultToTarget,
		fromTo:        fromTo,
		toFrom:        toFrom,
	}, nil
}

func newRowDiff(size int) rowDiff {
	return rowDiff{
		colDiffs: make([]diff.ChangeType, size),
	}
}

func (ds diffSplitter) splitDiffResultRow(row sql.Row) (rowDiff, rowDiff, error) {
	// split rows in the result set into old, new
	diffTypeColIdx := ds.diffQuerySch.IndexOfColName("diff_type")
	if diffTypeColIdx < 0 {
		return rowDiff{}, rowDiff{}, fmt.Errorf("expected a diff_type column")
	}

	diffType := row[diffTypeColIdx]

	oldRow, newRow := newRowDiff(len(ds.targetSch)), newRowDiff(len(ds.targetSch))

	diffTypeStr := diffType.(string)
	if diffTypeStr == "removed" || diffTypeStr == "modified" {
		oldRow.row = make(sql.Row, len(ds.targetSch))
		if diffTypeStr == "modified" {
			oldRow.rowDiff = diff.ModifiedOld
		} else {
			oldRow.rowDiff = diff.Deleted
		}

		for i := 0; i < ds.fromLen; i++ {
			oldRow.row[ds.queryToTarget[i]] = row[i]

			if diffTypeStr == "modified" {
				if row[i] != row[ds.fromTo[i]] {
					oldRow.colDiffs[ds.queryToTarget[i]] = diff.ModifiedOld
				}
			} else {
				oldRow.colDiffs[ds.queryToTarget[i]] = diff.Deleted
			}
		}
	}

	if diffTypeStr == "added" || diffTypeStr == "modified" {
		newRow.row = make(sql.Row, len(ds.targetSch))
		if diffTypeStr == "modified" {
			newRow.rowDiff = diff.ModifiedNew
		} else {
			newRow.rowDiff = diff.Inserted
		}

		for i := ds.fromLen; i < len(ds.diffQuerySch)-1; i++ {
			newRow.row[ds.queryToTarget[i]] = row[i]

			if diffTypeStr == "modified" {
				if row[i] != row[ds.toFrom[i]] {
					newRow.colDiffs[ds.queryToTarget[i]] = diff.ModifiedNew
				}
			} else {
				newRow.colDiffs[ds.queryToTarget[i]] = diff.Inserted
			}
		}
	}

	return oldRow, newRow, nil
}
