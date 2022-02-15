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

package index

import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"

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
var _ sql.RowIter2 = sqlRowIter{}

func NewProllyRowIter(ctx context.Context, sch schema.Schema, rows prolly.Map, rng prolly.Range, projections []string) (sql.RowIter, error) {
	if schema.IsKeyless(sch) {
		return nil, errors.New("format __DOLT_1__ does not support keyless tables")
	}

	iter, err := rows.IterRange(ctx, rng)
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

	// todo(andy): NomsRangeReader seemingly ignores projections
	//if projections == nil {
	//	projections = sch.GetAllCols().GetColumnNames()
	//}

	projections = sch.GetAllCols().GetColumnNames()
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

func projectionMappings(sch schema.Schema, projs []string) (keyMap, valMap val.OrdinalMapping) {
	keyMap = make(val.OrdinalMapping, sch.GetPKCols().Size())
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

	valMap = make(val.OrdinalMapping, sch.GetNonPKCols().Size())
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

func (it sqlRowIter) Next(ctx *sql.Context) (sql.Row, error) {
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

func (it sqlRowIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	key, value, err := it.iter.Next(it.ctx)
	if err != nil {
		return err
	}

	for keyIdx, rowIdx := range it.keyProj {
		if rowIdx == -1 {
			continue
		}

		appendToRowFrame(key, it.keyDesc.Types[keyIdx], keyIdx, frame)
	}

	for valIdx, rowIdx := range it.valProj {
		if rowIdx == -1 {
			continue
		}

		appendToRowFrame(value, it.valDesc.Types[valIdx], valIdx, frame)
	}

	return nil
}

func appendToRowFrame(tuple val.Tuple, typ val.Type, idx int, frame *sql.RowFrame) {
	switch typ.Enc {
	case val.Int8Enc:
		frame.Append(sql.Value{
			Typ: query.Type_INT8,
			Val: tuple.GetField(idx),
		})
	case val.Uint8Enc:
		frame.Append(sql.Value{
			Typ: query.Type_UINT8,
			Val: tuple.GetField(idx),
		})
	case val.Int16Enc:
		frame.Append(sql.Value{
			Typ: query.Type_INT16,
			Val: tuple.GetField(idx),
		})
	case val.Uint16Enc:
		frame.Append(sql.Value{
			Typ: query.Type_UINT16,
			Val: tuple.GetField(idx),
		})
	case val.Int32Enc:
		frame.Append(sql.Value{
			Typ: query.Type_INT32,
			Val: tuple.GetField(idx),
		})
	case val.Uint32Enc:
		frame.Append(sql.Value{
			Typ: query.Type_UINT32,
			Val: tuple.GetField(idx),
		})
	case val.Int64Enc:
		frame.Append(sql.Value{
			Typ: query.Type_INT64,
			Val: tuple.GetField(idx),
		})
	case val.Uint64Enc:
		frame.Append(sql.Value{
			Typ: query.Type_UINT64,
			Val: tuple.GetField(idx),
		})
	case val.Float32Enc:
		frame.Append(sql.Value{
			Typ: query.Type_FLOAT32,
			Val: tuple.GetField(idx),
		})
	case val.Float64Enc:
		frame.Append(sql.Value{
			Typ: query.Type_FLOAT64,
			Val: tuple.GetField(idx),
		})
	case val.DecimalEnc:
		frame.Append(sql.Value{
			Typ: query.Type_DECIMAL,
			Val: tuple.GetField(idx),
		})
	case val.TimeEnc:
		frame.Append(sql.Value{
			Typ: query.Type_TIME,
			Val: tuple.GetField(idx),
		})
	case val.YearEnc:
		frame.Append(sql.Value{
			Typ: query.Type_YEAR,
			Val: tuple.GetField(idx),
		})
	case val.TimestampEnc, val.DateEnc, val.DatetimeEnc:
		frame.Append(sql.Value{
			Typ: query.Type_TIMESTAMP,
			Val: tuple.GetField(idx),
		})
	case val.StringEnc:
		frame.Append(sql.Value{
			Typ: query.Type_VARCHAR,
			Val: tuple.GetField(idx),
		})
	case val.BytesEnc:
		frame.Append(sql.Value{
			Typ: query.Type_VARBINARY,
			Val: tuple.GetField(idx),
		})
	case val.JSONEnc:
		frame.Append(sql.Value{
			Typ: query.Type_JSON,
			Val: tuple.GetField(idx),
		})
	default:
		panic("unknown encoding")
	}
}


func (it sqlRowIter) Close(ctx *sql.Context) error {
	return nil
}

var shimPool = pool.NewBuffPool()
