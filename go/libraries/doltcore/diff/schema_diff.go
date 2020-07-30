// Copyright 2019 Liquidata, Inc.
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
	"reflect"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

type SchemaChangeType int

const (
	// SchDiffNone is the SchemaChangeType for two columns with the same tag that are identical
	SchDiffNone SchemaChangeType = iota
	// SchDiffAdded is the SchemaChangeType when a column is in the new schema but not the old
	SchDiffAdded
	// SchDiffRemoved is the SchemaChangeType when a column is in the old schema but not the new
	SchDiffRemoved
	// SchDiffModified is the SchemaChangeType for two columns with the same tag that are different
	SchDiffModified
)

// ColumnDifference is the result of comparing two columns from two schemas.
type ColumnDifference struct {
	DiffType SchemaChangeType
	Tag      uint64
	Old      *schema.Column
	New      *schema.Column
}

type columnPair [2]*schema.Column

// DiffSchColumns compares two schemas by looking at columns with the same tag.
func DiffSchColumns(fromSch, toSch schema.Schema) (map[uint64]ColumnDifference, []uint64) {
	colPairMap, unionTags := pairColumns(fromSch, toSch)

	diffs := make(map[uint64]ColumnDifference)
	for _, tag := range unionTags {
		colPair := colPairMap[tag]
		if colPair[0] == nil {
			diffs[tag] = ColumnDifference{SchDiffAdded, tag, nil, colPair[1]}
		} else if colPair[1] == nil {
			diffs[tag] = ColumnDifference{SchDiffRemoved, tag, colPair[0], nil}
		} else if !colPair[0].Equals(*colPair[1]) {
			diffs[tag] = ColumnDifference{SchDiffModified, tag, colPair[0], colPair[1]}
		} else {
			diffs[tag] = ColumnDifference{SchDiffNone, tag, colPair[0], colPair[1]}
		}
	}

	return diffs, unionTags
}

// pairColumns loops over both sets of columns pairing columns with the same tag.
func pairColumns(fromSch, toSch schema.Schema) (map[uint64]columnPair, []uint64) {
	// collect the tag union of the two schemas, ordering fromSch before toSch
	var unionTags []uint64
	colPairMap := make(map[uint64]columnPair)

	_ = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colPairMap[tag] = columnPair{&col, nil}
		unionTags = append(unionTags, tag)

		return false, nil
	})

	_ = toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if pair, ok := colPairMap[tag]; ok {
			pair[1] = &col
			colPairMap[tag] = pair
		} else {
			colPairMap[tag] = columnPair{nil, &col}
			unionTags = append(unionTags, tag)
		}

		return false, nil
	})

	return colPairMap, unionTags
}

type IndexDifference struct {
	DiffType SchemaChangeType
	From     schema.Index
	To       schema.Index
}

// DiffSchIndexes
func DiffSchIndexes(fromSch, toSch schema.Schema) (diffs []IndexDifference) {
	_ = fromSch.Indexes().Iter(func(fromIdx schema.Index) (stop bool, err error) {
		toIdx, ok := toSch.Indexes().GetIndexByTags(fromIdx.IndexedColumnTags()...)

		if !ok {
			diffs = append(diffs, IndexDifference{
				DiffType: SchDiffRemoved,
				From:     fromIdx,
			})
		}

		d := IndexDifference{
			DiffType: SchDiffModified,
			From:     fromIdx,
			To:       toIdx,
		}

		if fromIdx.Equals(toIdx) {
			d.DiffType = SchDiffNone
		}
		diffs = append(diffs, d)

		return false, nil
	})

	_ = toSch.Indexes().Iter(func(toIdx schema.Index) (stop bool, err error) {
		// if we've seen this index, skip
		for _, d := range diffs {
			if d.To.Equals(toIdx) {
				return false, nil
			}
		}

		diffs = append(diffs, IndexDifference{
			DiffType: SchDiffAdded,
			To:       toIdx,
		})

		return false, nil
	})

	return diffs
}

type ForeignKeyDifference struct {
	DiffType SchemaChangeType
	From     doltdb.ForeignKey
	To       doltdb.ForeignKey
}

func DiffSchForeignKeys(fromFks, toFKs []doltdb.ForeignKey) (diffs []ForeignKeyDifference) {
	for _, from := range fromFks {
		matched := false
		for _, to := range toFKs {
			if reflect.DeepEqual(from.ReferencedTableColumns, to.ReferencedTableColumns) &&
				reflect.DeepEqual(from.TableColumns, to.TableColumns) {

				matched = true
				d := ForeignKeyDifference{
					DiffType: SchDiffModified,
					From:     from,
					To:       to,
				}

				if from.Equals(to) {
					d.DiffType = SchDiffNone
				}
				diffs = append(diffs, d)

				break
			}
		}

		if !matched {
			diffs = append(diffs, ForeignKeyDifference{
				DiffType: SchDiffRemoved,
				From:     from,
			})
		}
	}

	for _, to := range toFKs {
		seen := false
		for _, d := range diffs {
			if d.To.Equals(to) {
				seen = true
				break
			}
		}
		if seen {
			continue
		}

		diffs = append(diffs, ForeignKeyDifference{
			DiffType: SchDiffAdded,
			To:       to,
		})
	}
	return diffs
}
