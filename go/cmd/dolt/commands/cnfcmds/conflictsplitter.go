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

package cnfcmds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
)

const (
	basePrefix  = "base_"
	theirPrefix = "their_"
	ourPrefix   = "our_"
)

type conflictSplitter struct {
	conflictQuerySch                         sql.Schema
	targetSch                                sql.Schema
	baseToTarget, ourToTarget, theirToTarget map[int]int
	ourToBase, theirToBase                   map[int]int
	ourDiffTypeIdx, theirDiffTypeIdx         int
}

func newConflictSplitter(conflictQuerySch sql.Schema, targetSch sql.Schema) (*conflictSplitter, error) {
	baseToTarget, ourToTarget, theirToTarget := make(map[int]int), make(map[int]int), make(map[int]int)
	ourToBase, theirToBase := make(map[int]int), make(map[int]int)
	ourDiffTypeIdx, theirDiffTypeIdx := -1, -1

	for i := 0; i < len(conflictQuerySch); i++ {
		if conflictQuerySch[i].Name == "our_diff_type" {
			ourDiffTypeIdx = i
			continue
		}

		if conflictQuerySch[i].Name == "their_diff_type" {
			theirDiffTypeIdx = i
			continue
		}

		var colName string
		var mapper map[int]int
		if strings.HasPrefix(conflictQuerySch[i].Name, basePrefix) {
			colName = conflictQuerySch[i].Name[5:]
			mapper = baseToTarget
		} else if strings.HasPrefix(conflictQuerySch[i].Name, ourPrefix) {
			colName = conflictQuerySch[i].Name[4:]
			mapper = ourToTarget
			if base := conflictQuerySch.IndexOfColName(basePrefix + colName); base >= 0 {
				ourToBase[i] = base
			}
		} else if strings.HasPrefix(conflictQuerySch[i].Name, theirPrefix) {
			colName = conflictQuerySch[i].Name[6:]
			mapper = theirToTarget
			if base := conflictQuerySch.IndexOfColName(basePrefix + colName); base >= 0 {
				theirToBase[i] = base
			}
		}

		if colName == "" {
			// not a column we care about
			continue
		}

		targetIdx := targetSch.IndexOfColName(colName)
		if targetIdx < 0 {
			return nil, fmt.Errorf("couldn't find a column named %s", colName)
		}

		mapper[i] = targetIdx
	}

	if ourDiffTypeIdx == -1 || theirDiffTypeIdx == -1 {
		return nil, errors.New("our_diff_type or their_diff_type missing from conflict sql results")
	}

	return &conflictSplitter{
		conflictQuerySch: conflictQuerySch,
		targetSch:        targetSch,
		baseToTarget:     baseToTarget,
		ourToTarget:      ourToTarget,
		theirToTarget:    theirToTarget,
		ourDiffTypeIdx:   ourDiffTypeIdx,
		theirDiffTypeIdx: theirDiffTypeIdx,
	}, nil
}

type conflictRow struct {
	version  string
	row      sql.Row
	diffType diff.ChangeType
}

func (cs conflictSplitter) splitConflictRow(row sql.Row) ([]conflictRow, error) {
	baseRow, ourRow, theirRow := make(sql.UntypedSqlRow, len(cs.targetSch)), make(sql.UntypedSqlRow, len(cs.targetSch)), make(sql.UntypedSqlRow, len(cs.targetSch))

	ourDiffType := changeTypeFromString(row.GetValue(cs.ourDiffTypeIdx).(string))
	theirDiffType := changeTypeFromString(row.GetValue(cs.theirDiffTypeIdx).(string))

	for from, to := range cs.baseToTarget {
		baseRow.SetValue(to, row.GetValue(from))
	}

	if ourDiffType == diff.Removed {
		ourRow = baseRow
	} else {
		for from, to := range cs.ourToTarget {
			ourRow.SetValue(to, row.GetValue(from))
		}
	}

	if theirDiffType == diff.Removed {
		theirRow = baseRow
	} else {
		for from, to := range cs.theirToTarget {
			theirRow.SetValue(to, row.GetValue(from))
		}
	}

	if ourDiffType == diff.Added || theirDiffType == diff.Added {
		return []conflictRow{
			{version: "ours", row: ourRow, diffType: ourDiffType},
			{version: "theirs", row: theirRow, diffType: theirDiffType},
		}, nil
	}

	return []conflictRow{
		{version: "base", row: baseRow, diffType: diff.None},
		{version: "ours", row: ourRow, diffType: ourDiffType},
		{version: "theirs", row: theirRow, diffType: theirDiffType},
	}, nil
}

func changeTypeFromString(str string) diff.ChangeType {
	switch str {
	case merge.ConflictDiffTypeAdded:
		return diff.Added
	case merge.ConflictDiffTypeRemoved:
		return diff.Removed
	case merge.ConflictDiffTypeModified:
		return diff.ModifiedNew
	default:
		panic(fmt.Sprintf("unhandled diff type string %s", str))
	}
}
