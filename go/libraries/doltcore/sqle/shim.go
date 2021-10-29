// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"context"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type sqlRowIter struct {
	ctx  context.Context
	iter prolly.MapIter

	rowLen  int
	keyDesc val.TupleDesc
	keyProj []int
	valDesc val.TupleDesc
	valProj []int
}

var _ sql.RowIter = sqlRowIter{}

func newKeyedRowIter(ctx context.Context, tbl *doltdb.Table, projectedCols []string, partition *doltTablePartition) (sql.RowIter, error) {

	rows := partition.rowData
	rng := prolly.IndexRange{
		Low:  partition.start,
		High: partition.end - 1,
	}
	if partition.end == NoUpperBound {
		rng.Low, rng.High = 0, rows.Count()-1
	}

	iter, err := rows.IterIndexRange(ctx, rng)
	if err != nil {
		return nil, err
	}
	kd, vd := rows.TupleDescriptors()

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	keyProj, valProj := projectionMappings(sch, projectedCols)

	return sqlRowIter{
		ctx:     ctx,
		iter:    iter,
		rowLen:  len(projectedCols),
		keyDesc: kd,
		valDesc: vd,
		keyProj: keyProj,
		valProj: valProj,
	}, nil
}

func projectionMappings(sch schema.Schema, projectedCols []string) (keyMap, valMap []int) {
	keyMap = make([]int, sch.GetPKCols().Size())
	for idx := range keyMap {
		keyMap[idx] = -1
		idxCol := sch.GetPKCols().GetAtIndex(idx)
		for j, proj := range projectedCols {
			if strings.ToLower(idxCol.Name) == strings.ToLower(proj) {
				keyMap[idx] = j
				break
			}
		}
	}

	valMap = make([]int, sch.GetNonPKCols().Size())
	for idx := range valMap {
		valMap[idx] = -1
		idxCol := sch.GetNonPKCols().GetAtIndex(idx)
		for j, proj := range projectedCols {
			if strings.ToLower(idxCol.Name) == strings.ToLower(proj) {
				valMap[idx] = j
				break
			}
		}
	}

	return
}

func (it sqlRowIter) Next() (sql.Row, error) {
	key, value, err := it.iter.Next(it.ctx)
	if err != nil {
		return nil, err
	}

	row := make(sql.Row, it.rowLen)

	for keyIdx, rowIdx := range it.keyProj {
		if rowIdx == -1 {
			continue
		}
		row[rowIdx] = it.keyDesc.GetField(keyIdx, key)
	}
	for valIdx, rowIdx := range it.valProj {
		if rowIdx == -1 {
			continue
		}
		row[rowIdx] = it.valDesc.GetField(valIdx, value)
	}

	return row, nil
}

func (it sqlRowIter) Close(ctx *sql.Context) error {
	return nil
}
