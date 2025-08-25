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

package diff

import (
	"reflect"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typecompatibility"
	storetypes "github.com/dolthub/dolt/go/store/types"
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
// For unmatched columns, it also tries name-based matching with type compatibility.
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
	var unpairedFrom []schema.Column

	_ = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colPairMap[tag] = columnPair{&col, nil}
		unionTags = append(unionTags, tag)
		unpairedFrom = append(unpairedFrom, col)

		return false, nil
	})

	var unpairedTo []schema.Column
	_ = toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if pair, ok := colPairMap[tag]; ok {
			pair[1] = &col
			colPairMap[tag] = pair
			// Remove from unpaired list since it found a tag match
			for i, fromCol := range unpairedFrom {
				if fromCol.Tag == tag {
					unpairedFrom = append(unpairedFrom[:i], unpairedFrom[i+1:]...)
					break
				}
			}
		} else {
			colPairMap[tag] = columnPair{nil, &col}
			unionTags = append(unionTags, tag)
			unpairedTo = append(unpairedTo, col)
		}

		return false, nil
	})

	// Try name-based pairing for remaining unmatched columns with compatible types
	if len(unpairedFrom) > 0 && len(unpairedTo) > 0 {
		checker := typecompatibility.NewTypeCompatabilityCheckerForStorageFormat(storetypes.Format_Default)

		for _, fromCol := range unpairedFrom {
			for _, toCol := range unpairedTo {
				if fromCol.Name == toCol.Name {
					compatInfo := checker.IsTypeChangeCompatible(fromCol.TypeInfo, toCol.TypeInfo)

					if compatInfo.Compatible {
						// Remove the unpaired entries
						delete(colPairMap, fromCol.Tag)
						delete(colPairMap, toCol.Tag)

						// Create a new paired entry using the fromCol tag (preserves original position)
						colPairMap[fromCol.Tag] = columnPair{&fromCol, &toCol}

						// Remove the toCol tag from unionTags since we're using fromCol tag
						for i, tag := range unionTags {
							if tag == toCol.Tag {
								unionTags = append(unionTags[:i], unionTags[i+1:]...)
								break
							}
						}
						break
					}
				}
			}
		}
	}

	return colPairMap, unionTags
}

type IndexDifference struct {
	DiffType SchemaChangeType
	From     schema.Index
	To       schema.Index
}

// DiffSchIndexes matches two sets of Indexes based on column definitions.
// It returns matched and unmatched Indexes as a slice of IndexDifferences.
func DiffSchIndexes(fromSch, toSch schema.Schema) (diffs []IndexDifference) {
	_ = fromSch.Indexes().Iter(func(fromIdx schema.Index) (stop bool, err error) {
		// Find matching index by comprehensive equality check
		candidateIndexes := toSch.Indexes().GetIndexesByTags(fromIdx.IndexedColumnTags()...)
		var toIdx schema.Index
		for _, candidate := range candidateIndexes {
			if fromIdx.Equals(candidate) {
				toIdx = candidate
				break
			}
		}

		if toIdx == nil {
			diffs = append(diffs, IndexDifference{
				DiffType: SchDiffRemoved,
				From:     fromIdx,
			})
			return false, nil
		}

		diffs = append(diffs, IndexDifference{
			DiffType: SchDiffNone,
			From:     fromIdx,
			To:       toIdx,
		})

		return false, nil
	})

	_ = toSch.Indexes().Iter(func(toIdx schema.Index) (stop bool, err error) {
		// if we've seen this index, skip
		for _, d := range diffs {
			if d.To != nil && d.To.Equals(toIdx) {
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

// DiffForeignKeys matches two sets of ForeignKeys based on column definitions.
// It returns matched and unmatched ForeignKeys as a slice of ForeignKeyDifferences.
func DiffForeignKeys(fromFks, toFKs []doltdb.ForeignKey) (diffs []ForeignKeyDifference) {
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

				if from.DeepEquals(to) {
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
			if d.To.DeepEquals(to) {
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
