// Copyright 2020 Liquidata, Inc.
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

package merge

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type MergeVersion int

const (
	BaseVersion MergeVersion = iota
	OurVersion
	TheirVersion
	Blank // for display only
)

var TypeToMergeVersion = map[string]MergeVersion{
	oursStr:   OurVersion,
	theirsStr: TheirVersion,
	baseStr:   BaseVersion,
}

type ConflictSplitter struct {
	joiner     *rowconv.Joiner
	sch        schema.Schema
	converters map[string]*rowconv.RowConverter
}

func NewConflictSplitter(joiner *rowconv.Joiner) (ConflictSplitter, error) {
	baseSch := joiner.SchemaForName(baseStr)
	ourSch := joiner.SchemaForName(baseStr)
	theirSch := joiner.SchemaForName(theirsStr)

	sch, err := untyped.UntypedSchemaUnion(baseSch, ourSch, theirSch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters := make(map[string]*rowconv.RowConverter)
	converters[oursStr], err = tagMappingConverter(ourSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters[theirsStr], err = tagMappingConverter(theirSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters[baseStr], err = tagMappingConverter(baseSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	return ConflictSplitter{joiner: joiner, sch: sch, converters: converters}, nil
}

func (ds ConflictSplitter) GetSchema() schema.Schema {
	return ds.sch
}

func (ds ConflictSplitter) SplitConflicts(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	rows, err := ds.joiner.Split(inRow)
	if err != nil {
		return nil, err.Error()
	}

	var baseRow row.Row
	has := make(map[string]bool)
	baseRow, has[baseStr] = rows[baseStr]
	_, has[oursStr] = rows[oursStr]
	_, has[theirsStr] = rows[theirsStr]

	if has[baseStr] {
		baseRow, err = ds.converters[baseStr].Convert(baseRow)

		if err != nil {
			return nil, err.Error()
		}
	}

	rowData = make([]*pipeline.TransformedRowResult, 0, 3)
	for _, rowType := range []string{baseStr, oursStr, theirsStr} {
		row, ok := rows[rowType]
		props := map[string]interface{}{mergeVersionProp: TypeToMergeVersion[rowType]}

		if ok {
			converted, err := ds.converters[rowType].Convert(row)

			if err != nil {
				return nil, err.Error()
			}

			if !has[baseStr] {
				props[mergeRowOperation] = types.DiffChangeAdded
			} else {
				props[mergeRowOperation] = types.DiffChangeModified
			}

			rowData = append(rowData, &pipeline.TransformedRowResult{RowData: converted, PropertyUpdates: props})
		} else if rowType != baseStr {
			props[mergeRowOperation] = types.DiffChangeRemoved
			rowData = append(rowData, &pipeline.TransformedRowResult{RowData: baseRow, PropertyUpdates: props})
		}
	}

	return rowData, ""
}
