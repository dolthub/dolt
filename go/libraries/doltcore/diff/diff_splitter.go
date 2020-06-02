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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/valutil"
)

const (
	// DiffTypeProp is the name of a property added to each split row which tells if its added, dropped, the modified
	// old value, or the new value after modification
	DiffTypeProp = "difftype"

	// CollChangesProp is the name of a property added to each modified row which is a map from collumn name to the
	// type of change.
	CollChangesProp = "collchanges"
)

// DiffChType is an enum that represents the type of change
type DiffChType int

const (
	// DiffAdded is the DiffTypeProp value for a row that was newly added (In new, but not in old)
	DiffAdded DiffChType = iota

	// DiffRemoved is the DiffTypeProp value for a row that was newly deleted (In old, but not in new)
	DiffRemoved

	// DiffModifiedOld is the DiffTypeProp value for the row which represents the old value of the row before it was changed.
	DiffModifiedOld

	// DiffModifiedNew is the DiffTypeProp value for the row which represents the new value of the row after it was changed.
	DiffModifiedNew
)

// DiffTyped is an interface for an object that has a DiffChType
type DiffTyped interface {
	// DiffType gets the DiffChType of an object
	DiffType() DiffChType
}

// DiffRow is a row.Row with a change type associated with it.
type DiffRow struct {
	row.Row
	diffType DiffChType
}

// DiffType gets the DiffChType for the row.
func (dr *DiffRow) DiffType() DiffChType {
	return dr.diffType
}

// DiffSplitter is a struct that can take a diff which is represented by a row with a column for every field in the old
// version, and a column for every field in the new version and split it into two rows with properties which annotate
// what each row is.  This is used to show diffs as 2 lines, instead of 1.
type DiffSplitter struct {
	joiner  *rowconv.Joiner
	oldConv *rowconv.RowConverter
	newConv *rowconv.RowConverter
}

// NewDiffSplitter creates a DiffSplitter
func NewDiffSplitter(joiner *rowconv.Joiner, oldConv, newConv *rowconv.RowConverter) *DiffSplitter {
	return &DiffSplitter{joiner, oldConv, newConv}
}

func convertNamedRow(rows map[string]row.Row, name string, rc *rowconv.RowConverter) (row.Row, error) {
	r, ok := rows[name]

	if !ok || r == nil {
		return nil, nil
	}

	return rc.Convert(r)
}

// SplitDiffIntoOldAndNew is a pipeline.TransformRowFunc which can be used in a pipeline to split single row diffs,
// into 2 row diffs.
func (ds *DiffSplitter) SplitDiffIntoOldAndNew(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	rows, err := ds.joiner.Split(inRow)
	mappedOld, err := convertNamedRow(rows, From, ds.oldConv)

	if err != nil {
		return nil, err.Error()
	}

	mappedNew, err := convertNamedRow(rows, To, ds.newConv)

	if err != nil {
		return nil, err.Error()
	}

	originalNewSch := ds.joiner.SchemaForName(From)
	originalOldSch := ds.joiner.SchemaForName(To)

	var oldProps = map[string]interface{}{DiffTypeProp: DiffRemoved}
	var newProps = map[string]interface{}{DiffTypeProp: DiffAdded}
	if mappedOld != nil && mappedNew != nil {
		oldColDiffs := make(map[string]DiffChType)
		newColDiffs := make(map[string]DiffChType)

		outSch := ds.newConv.DestSch
		outCols := outSch.GetAllCols()
		err := outCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			oldVal, _ := mappedOld.GetColVal(tag)
			newVal, _ := mappedNew.GetColVal(tag)

			_, inOld := originalOldSch.GetAllCols().GetByTag(tag)
			_, inNew := originalNewSch.GetAllCols().GetByTag(tag)

			if inOld && inNew {
				if !valutil.NilSafeEqCheck(oldVal, newVal) {
					newColDiffs[col.Name] = DiffModifiedNew
					oldColDiffs[col.Name] = DiffModifiedOld
				}
			} else if inOld {
				oldColDiffs[col.Name] = DiffRemoved
			} else {
				newColDiffs[col.Name] = DiffAdded
			}

			return false, nil
		})

		if err != nil {
			return nil, err.Error()
		}

		oldProps = map[string]interface{}{DiffTypeProp: DiffModifiedOld, CollChangesProp: oldColDiffs}
		newProps = map[string]interface{}{DiffTypeProp: DiffModifiedNew, CollChangesProp: newColDiffs}
	}

	var results []*pipeline.TransformedRowResult
	if mappedOld != nil {
		results = append(results, &pipeline.TransformedRowResult{RowData: mappedOld, PropertyUpdates: oldProps})
	}

	if mappedNew != nil {
		results = append(results, &pipeline.TransformedRowResult{RowData: mappedNew, PropertyUpdates: newProps})
	}

	return results, ""
}
