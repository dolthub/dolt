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
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
)

const (
	// DiffTypeProp is the name of a property added to each split row which tells if its added, removed, the modified
	// old value, or the new value after modification
	DiffTypeProp = "difftype"

	// ColChangesProp is the name of a property added to each modified row which is a map from column name to the
	// type of change.
	ColChangesProp = "colchanges"
)

// ChangeType is an enum that represents the type of change in a diff
type ChangeType int

const (
	// Inserted is the DiffTypeProp value for a row that was newly added (In new, but not in old)
	Inserted ChangeType = iota

	// Deleted is the DiffTypeProp value for a row that was newly deleted (In old, but not in new)
	Deleted

	// ModifiedOld is the DiffTypeProp value for the row which represents the old value of the row before it was changed.
	ModifiedOld

	// ModifiedNew is the DiffTypeProp value for the row which represents the new value of the row after it was changed.
	ModifiedNew
)

// Row is a row.Row with a change type associated with it.
type Row struct {
	row.Row
	diffType ChangeType
}

// DiffType gets the ChangeType for the row.
func (dr *Row) DiffType() ChangeType {
	return dr.diffType
}

// RowSplitter is a function that takes a diff result row and splits it into [old, new] rows. Either may be nil.
type RowSplitter func(row.Row) ([]row.Row, error)

// Splitter knows how to split a diff row into two, one for the old values and one for the new values. This is used
// when printing a diff result on the command line.
type Splitter struct {
	splitter     RowSplitter
	targetSchema schema.Schema
}

// NewDiffSplitter creates a DiffSplitter
func NewDiffSplitter(joiner RowSplitter, targetSchema schema.Schema) *Splitter {
	return &Splitter{joiner, targetSchema}
}

// SplitDiffIntoOldAndNew is a pipeline.TransformRowFunc which can be used in a pipeline to split single row diffs,
// into 2 row diffs.
func (ds *Splitter) SplitDiffIntoOldAndNew(inRow row.Row, _ pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	rows, err := ds.splitter(inRow)
	if err != nil {
		return nil, err.Error()
	}

	old, new := rows[0], rows[1]

	var oldProps = map[string]interface{}{DiffTypeProp: Deleted}
	var newProps = map[string]interface{}{DiffTypeProp: Inserted}
	if old != nil && new != nil {
		oldColDiffs := make(map[string]ChangeType)
		newColDiffs := make(map[string]ChangeType)

		outCols := ds.targetSchema.GetAllCols()
		err := outCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			newColDiffs[col.Name] = ModifiedNew
			oldColDiffs[col.Name] = ModifiedOld
			return false, nil
		})

		if err != nil {
			return nil, err.Error()
		}

		oldProps = map[string]interface{}{DiffTypeProp: ModifiedOld, ColChangesProp: oldColDiffs}
		newProps = map[string]interface{}{DiffTypeProp: ModifiedNew, ColChangesProp: newColDiffs}
	}

	var results []*pipeline.TransformedRowResult
	if old != nil {
		results = append(results, &pipeline.TransformedRowResult{RowData: old, PropertyUpdates: oldProps})
	}

	if new != nil {
		results = append(results, &pipeline.TransformedRowResult{RowData: new, PropertyUpdates: newProps})
	}

	return results, ""
}
