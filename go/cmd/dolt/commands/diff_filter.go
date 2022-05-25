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

package commands

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/store/types"
)

type DiffFilterTransform struct {
	Pipeline     *pipeline.Pipeline
	filterBy     string
	newColsCount int
}

func NewDiffFilterTrans(filterBy string, newColsCount int) *DiffFilterTransform {
	return &DiffFilterTransform{filterBy: filterBy, newColsCount: newColsCount}
}

func (df *DiffFilterTransform) FilterDiffs(inRow row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {

	taggedVals, err := inRow.TaggedValues()
	if err != nil {
		return nil, ""
	}

	joinedRowAsMap := map[uint64]types.Value(taggedVals)

	v, ok := joinedRowAsMap[0]
	if v != nil && ok {
		if df.newColsCount < len(joinedRowAsMap) {
			if df.filterBy != FilterUpdates {
				return nil, ""
			}
		} else {
			if df.filterBy != FilterDeletes {
				return nil, ""
			}
		}
	} else {
		if df.filterBy != FilterInserts {
			return nil, ""
		}
	}

	return []*pipeline.TransformedRowResult{{RowData: inRow, PropertyUpdates: nil}}, ""
}
