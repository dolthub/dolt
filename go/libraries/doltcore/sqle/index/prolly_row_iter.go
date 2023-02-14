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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func init() {
	// todo: multiple query types can map to a single encoding
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
	encodingToType[val.YearEnc] = query.Type_YEAR
	encodingToType[val.DateEnc] = query.Type_TIMESTAMP
	encodingToType[val.DatetimeEnc] = query.Type_TIMESTAMP
	encodingToType[val.StringEnc] = query.Type_VARCHAR
	encodingToType[val.ByteStringEnc] = query.Type_VARBINARY
	encodingToType[val.JSONEnc] = query.Type_JSON
}

var encodingToType [256]query.Type

type prollyRowIter struct {
	iter prolly.MapIter
	ns   tree.NodeStore

	sqlSch  sql.Schema
	keyDesc val.TupleDesc
	valDesc val.TupleDesc

	keyProj []int
	valProj []int
	// orjProj is a concatenated list of output ordinals for |keyProj| and |valProj|
	ordProj []int
	rowLen  int
}

var _ sql.RowIter = prollyRowIter{}
var _ sql.RowIter2 = prollyRowIter{}

func NewProllyRowIter(sch schema.Schema, sqlSch sql.Schema, rows prolly.Map, iter prolly.MapIter, projections []uint64) (sql.RowIter, error) {
	if projections == nil {
		projections = sch.GetAllCols().Tags
	}

	keyProj, valProj, ordProj := projectionMappings(sch, projections)
	kd, vd := rows.Descriptors()

	if schema.IsKeyless(sch) {
		return &prollyKeylessIter{
			iter:    iter,
			valDesc: vd,
			valProj: valProj,
			ordProj: ordProj,
			rowLen:  len(projections),
			ns:      rows.NodeStore(),
		}, nil
	}

	return prollyRowIter{
		iter:    iter,
		sqlSch:  sqlSch,
		keyDesc: kd,
		valDesc: vd,
		keyProj: keyProj,
		valProj: valProj,
		ordProj: ordProj,
		rowLen:  len(projections),
		ns:      rows.NodeStore(),
	}, nil
}

// projectionMappings returns data structures that specify 1) which fields we read
// from key and value tuples, and 2) the position of those fields in the output row.
func projectionMappings(sch schema.Schema, projections []uint64) (keyMap, valMap, ordMap val.OrdinalMapping) {
	pks := sch.GetPKCols()
	nonPks := sch.GetNonPKCols()

	allMap := make([]int, 2*len(projections))
	i := 0
	j := len(projections) - 1
	for k, t := range projections {
		if idx, ok := pks.TagToIdx[t]; ok {
			allMap[len(projections)+i] = k
			allMap[i] = idx
			i++
		} else if idx, ok := nonPks.TagToIdx[t]; ok {
			allMap[j] = idx
			allMap[len(projections)+j] = k
			j--
		}
	}
	keyMap = allMap[:i]
	valMap = allMap[i:len(projections)]
	ordMap = allMap[len(projections):]
	if schema.IsKeyless(sch) {
		// skip the cardinality value, increment every index
		for i := range keyMap {
			keyMap[i]++
		}
		for i := range valMap {
			valMap[i]++
		}
	}
	return
}

func (it prollyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	key, value, err := it.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	row := make(sql.Row, it.rowLen)
	for i, idx := range it.keyProj {
		outputIdx := it.ordProj[i]
		row[outputIdx], err = GetField(ctx, it.keyDesc, idx, key, it.ns)
		if err != nil {
			return nil, err
		}
	}
	for i, idx := range it.valProj {
		outputIdx := it.ordProj[len(it.keyProj)+i]
		row[outputIdx], err = GetField(ctx, it.valDesc, idx, value, it.ns)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (it prollyRowIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	key, value, err := it.iter.Next(ctx)
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

type prollyKeylessIter struct {
	iter prolly.MapIter
	ns   tree.NodeStore

	valDesc val.TupleDesc
	valProj []int
	ordProj []int
	rowLen  int

	curr sql.Row
	card uint64
}

var _ sql.RowIter = &prollyKeylessIter{}

//var _ sql.RowIter2 = prollyKeylessIter{}

func (it *prollyKeylessIter) Next(ctx *sql.Context) (sql.Row, error) {
	if it.card == 0 {
		if err := it.nextTuple(ctx); err != nil {
			return nil, err
		}
	}

	it.card--

	return it.curr, nil
}

func (it *prollyKeylessIter) nextTuple(ctx *sql.Context) error {
	_, value, err := it.iter.Next(ctx)
	if err != nil {
		return err
	}

	it.card = val.ReadKeylessCardinality(value)
	it.curr = make(sql.Row, it.rowLen)

	for i, idx := range it.valProj {
		outputIdx := it.ordProj[i]
		it.curr[outputIdx], err = GetField(ctx, it.valDesc, idx, value, it.ns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (it *prollyKeylessIter) Close(ctx *sql.Context) error {
	return nil
}
