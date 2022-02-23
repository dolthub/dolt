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
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func init() {
	encodingToType[val.Int8Enc] = query.Type_INT8
	encodingToType[val.Uint8Enc] = query.Type_UINT8
	encodingToType[val.Int16Enc] = query.Type_INT16
	encodingToType[val.Uint16Enc] = query.Type_UINT16
	encodingToType[val.Int32Enc] = query.Type_INT32
	encodingToType[val.Uint32Enc] = query.Type_UINT32
	encodingToType[val.Int64Enc] = query.Type_INT64
	encodingToType[val.Uint64Enc] = query.Type_UINT64
	encodingToType[val.Float32Enc] = query.Type_FLOAT32
	encodingToType[val.Float64Enc] = query.Type_FLOAT64
	encodingToType[val.DecimalEnc] = query.Type_DECIMAL
	encodingToType[val.TimeEnc] = query.Type_TIME
	encodingToType[val.YearEnc] = query.Type_YEAR
	encodingToType[val.TimestampEnc] = query.Type_TIMESTAMP
	encodingToType[val.DateEnc] = query.Type_TIMESTAMP
	encodingToType[val.DatetimeEnc] = query.Type_TIMESTAMP
	encodingToType[val.StringEnc] = query.Type_VARCHAR
	encodingToType[val.ByteStringEnc] = query.Type_VARBINARY
	encodingToType[val.JSONEnc] = query.Type_JSON
}

var encodingToType [256]query.Type

type prollyRowIter struct {
	ctx  context.Context
	iter prolly.MapRangeIter

	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	keyProj []int
	valProj []int
	rowLen  int
}

var _ sql.RowIter = prollyRowIter{}
var _ sql.RowIter2 = prollyRowIter{}

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

	return prollyRowIter{
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

func (it prollyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	key, value, err := it.iter.Next(it.ctx)
	if err != nil {
		return nil, err
	}

	row := make(sql.Row, it.rowLen)

	for keyIdx, rowIdx := range it.keyProj {
		if rowIdx == -1 {
			continue
		}
		row[rowIdx], err = GetField(it.keyDesc, keyIdx, key)
		if err != nil {
			return nil, err
		}
	}
	for valIdx, rowIdx := range it.valProj {
		if rowIdx == -1 {
			continue
		}
		row[rowIdx], err = GetField(it.valDesc, valIdx, value)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (it prollyRowIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	key, value, err := it.iter.Next(it.ctx)
	if err != nil {
		return err
	}

	// TODO: handle out of order projections
	for keyIdx, rowIdx := range it.keyProj {
		if rowIdx == -1 {
			continue
		}

		enc := it.keyDesc.Types[keyIdx].Enc

		frame.Append(sql.Value{
			Typ: encodingToType[enc],
			Val: it.keyDesc.GetField(keyIdx, key),
		})
	}

	for valIdx, rowIdx := range it.valProj {
		if rowIdx == -1 {
			continue
		}

		enc := it.valDesc.Types[valIdx].Enc

		frame.Append(sql.Value{
			Typ: encodingToType[enc],
			Val: it.valDesc.GetField(valIdx, value),
		})
	}

	return nil
}

func (it prollyRowIter) Close(ctx *sql.Context) error {
	return nil
}
