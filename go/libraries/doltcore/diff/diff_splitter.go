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
	DiffTypeProp    = "difftype"
	CollChangesProp = "collchanges"
)

type DiffChType int

const (
	DiffAdded DiffChType = iota
	DiffRemoved
	DiffModifiedOld
	DiffModifiedNew
)

type DiffTyped interface {
	DiffType() DiffChType
}

type DiffRow struct {
	row.Row
	diffType DiffChType
}

func (dr *DiffRow) DiffType() DiffChType {
	return dr.diffType
}

type DiffSplitter struct {
	joiner  *rowconv.Joiner
	oldConv *rowconv.RowConverter
	newConv *rowconv.RowConverter
}

func NewDiffSplitter(joiner *rowconv.Joiner, oldConv, newConv *rowconv.RowConverter) *DiffSplitter {
	return &DiffSplitter{joiner, oldConv, newConv}
}

func a(rows map[string]row.Row, name string, rc *rowconv.RowConverter) (row.Row, error) {
	r, ok := rows[name]

	if !ok || r == nil {
		return nil, nil
	}

	return rc.Convert(r)
}

func (ds *DiffSplitter) SplitDiffIntoOldAndNew(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	rows, err := ds.joiner.Split(inRow)
	mappedOld, err := a(rows, From, ds.oldConv)

	if err != nil {
		return nil, err.Error()
	}

	mappedNew, err := a(rows, To, ds.newConv)

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
