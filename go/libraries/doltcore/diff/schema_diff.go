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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

type SchemaChangeType int

const (
	// SchDiffNone is the SchemaChangeType for two columns with the same tag that are identical
	SchDiffNone SchemaChangeType = iota
	// SchDiffAdded is the SchemaChangeType when a column is in the new schema but not the old
	SchDiffColAdded
	// SchDiffRemoved is the SchemaChangeType when a column is in the old schema but not the new
	SchDiffColRemoved
	// SchDiffModified is the SchemaChangeType for two columns with the same tag that are different
	SchDiffColModified
)

// SchemaDifference is the result of comparing two columns from two schemas.
type SchemaDifference struct {
	DiffType SchemaChangeType
	Tag      uint64
	Old      *schema.Column
	New      *schema.Column
}

type columnPair [2]*schema.Column

// DiffSchemas compares two schemas by looking at columns with the same tag.
func DiffSchemas(sch1, sch2 schema.Schema) (map[uint64]SchemaDifference, []uint64) {
	colPairMap, unionTags := pairColumns(sch1, sch2)

	diffs := make(map[uint64]SchemaDifference)
	for _, tag := range unionTags {
		colPair := colPairMap[tag]
		if colPair[0] == nil {
			diffs[tag] = SchemaDifference{SchDiffColAdded, tag, nil, colPair[1]}
		} else if colPair[1] == nil {
			diffs[tag] = SchemaDifference{SchDiffColRemoved, tag, colPair[0], nil}
		} else if !colPair[0].Equals(*colPair[1]) {
			diffs[tag] = SchemaDifference{SchDiffColModified, tag, colPair[0], colPair[1]}
		} else {
			diffs[tag] = SchemaDifference{SchDiffNone, tag, colPair[0], colPair[1]}
		}
	}

	return diffs, unionTags
}

// pairColumns loops over both sets of columns pairing columns with the same tag.
func pairColumns(sch1, sch2 schema.Schema) (map[uint64]columnPair, []uint64) {
	// collect the tag union of the two schemas, ordering sch1 before sch2
	var unionTags []uint64
	colPairMap := make(map[uint64]columnPair)

	_ = sch1.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colPairMap[tag] = columnPair{&col, nil}
		unionTags = append(unionTags, tag)

		return false, nil
	})

	_ = sch2.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
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
