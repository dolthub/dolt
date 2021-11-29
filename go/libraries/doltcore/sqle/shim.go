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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type sqlRowIter struct {
	ctx  context.Context
	iter prolly.MapRangeIter

	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	keyProj []int
	valProj []int
	rowLen  int
}

var _ sql.RowIter = sqlRowIter{}

func newKeyedRowIter(ctx context.Context, tbl *doltdb.Table, projections []string, partition *doltTablePartition) (sql.RowIter, error) {
	rows := partition.rowData

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := rows.IterRange(ctx, partition.rowRange)
	if err != nil {
		return nil, err
	}

	return rowIterFromMapIter(ctx, sch, rows, iter, projections)
}

func rowIterFromMapIter(
	ctx context.Context,
	sch schema.Schema,
	m prolly.Map,
	iter prolly.MapRangeIter,
	projections []string,
) (sql.RowIter, error) {

	if projections == nil {
		projections = sch.GetAllCols().GetColumnNames()
	}
	keyProj, valProj := projectionMappings(sch, projections)

	kd, vd := m.Descriptors()

	return sqlRowIter{
		ctx:     ctx,
		iter:    iter,
		keyDesc: kd,
		valDesc: vd,
		keyProj: keyProj,
		valProj: valProj,
		rowLen:  len(projections),
	}, nil
}

func projectionMappings(sch schema.Schema, projs []string) (keyMap, valMap []int) {
	keyMap = make([]int, sch.GetPKCols().Size())
	for idx := range keyMap {
		keyMap[idx] = -1
		idxCol := sch.GetPKCols().GetAtIndex(idx)
		for j, proj := range projs {
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
		for j, proj := range projs {
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

var shimPool = pool.NewBuffPool()

func NewAutoThing(current interface{}) (at *autoThing) {
	at = &autoThing{
		current: coerceInt64(current),
		mu:      sync.Mutex{},
	}
	return
}

type autoThing struct {
	current int64
	mu      sync.Mutex
}

var _ sql.AutoIncrementSetter = &autoThing{}

func (at *autoThing) SetAutoIncrementValue(_ *sql.Context, value interface{}) (err error) {
	at.Set(value)
	return
}

func (at *autoThing) Close(*sql.Context) (err error) {
	return
}

func (at *autoThing) Next(passed interface{}) int64 {
	at.mu.Lock()
	defer at.mu.Unlock()

	var value int64
	if passed != nil {
		coerceInt64(passed)
	}
	if value > at.current {
		at.current = value
	}

	current := at.current
	at.current++
	return current
}

func (at *autoThing) Peek() int64 {
	at.mu.Lock()
	defer at.mu.Unlock()
	return at.current
}

func (at *autoThing) Set(value interface{}) {
	at.current = coerceInt64(value)
}

func coerceInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	default:
		panic(value)
	}
}
