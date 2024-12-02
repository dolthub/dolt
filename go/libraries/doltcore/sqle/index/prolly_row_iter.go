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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyRowIter struct {
	iter prolly.MapIter
	ns   tree.NodeStore

	keyDesc val.TupleDesc
	valDesc val.TupleDesc

	keyProj []int
	valProj []int
	// ordProj is a concatenated list of output ordinals for |keyProj| and |valProj|
	ordProj []int
	rowLen  int
}

var _ sql.RowIter = prollyRowIter{}

func NewProllyRowIterForMap(sch schema.Schema, rows prolly.Map, iter prolly.MapIter, projections []uint64) (sql.RowIter, error) {
	if projections == nil {
		projections = sch.GetAllCols().Tags
	}

	kd, vd := rows.Descriptors()
	ns := rows.NodeStore()

	return NewProllyRowIterForSchema(sch, iter, kd, vd, projections, ns)
}

func NewProllyRowIterForSchema(sch schema.Schema, iter prolly.MapIter, kd val.TupleDesc, vd val.TupleDesc, projections []uint64, ns tree.NodeStore) (sql.RowIter, error) {
	if schema.IsKeyless(sch) {
		return NewKeylessProllyRowIter(sch, iter, vd, projections, ns), nil
	}

	return NewKeyedProllyRowIter(sch, iter, kd, vd, projections, ns)
}

func NewKeyedProllyRowIter(sch schema.Schema, iter prolly.MapIter, kd val.TupleDesc, vd val.TupleDesc, projections []uint64, ns tree.NodeStore) (sql.RowIter, error) {
	//keyProj, valProj, ordProj := projectionMappings(sch, projections)

	ordMap, err := ProjectionMappingsForIndex2(sch, projections)
	if err != nil {
		return prollyRowIter{}, err
	}
	return prollyRowIter{
		iter:    iter,
		keyDesc: kd,
		valDesc: vd,
		//keyProj: keyProj,
		//valProj: valProj,
		ordProj: ordMap,
		rowLen:  len(projections),
		ns:      ns,
	}, nil
}

func NewKeylessProllyRowIter(
	sch schema.Schema,
	iter prolly.MapIter,
	vd val.TupleDesc,
	projections []uint64,
	ns tree.NodeStore,
) sql.RowIter {
	_, valProj, ordProj := projectionMappings(sch, projections)

	return &prollyKeylessIter{
		iter:    iter,
		valDesc: vd,
		valProj: valProj,
		ordProj: ordProj,
		rowLen:  len(projections),
		ns:      ns,
	}
}

// projectionMappings returns data structures that specify 1) which fields we read
// from key and value tuples, and 2) the position of those fields in the output row.
func projectionMappings(sch schema.Schema, projections []uint64) (keyMap, valMap, ordMap val.OrdinalMapping) {
	keyMap, valMap, ordMap = ProjectionMappingsForIndex(sch, projections)
	adjustOffsetsForKeylessTable(sch, keyMap, valMap)
	return keyMap, valMap, ordMap
}

func adjustOffsetsForKeylessTable(sch schema.Schema, keyMap val.OrdinalMapping, valMap val.OrdinalMapping) {
	if schema.IsKeyless(sch) {
		// skip the cardinality value, increment every index
		for i := range keyMap {
			keyMap[i]++
		}
		for i := range valMap {
			valMap[i]++
		}
	}
}

func ProjectionMappingsForIndex(sch schema.Schema, projections []uint64) (keyMap, valMap, ordMap val.OrdinalMapping) {
	pks := sch.GetPKCols()
	nonPks := sch.GetNonPKCols()

	numPhysicalColumns := len(projections)
	if schema.IsVirtual(sch) {
		numPhysicalColumns = 0
		for _, t := range projections {
			if idx, ok := sch.GetAllCols().TagToIdx[t]; ok && !sch.GetAllCols().GetByIndex(idx).Virtual {
				numPhysicalColumns++
			}
		}
	}

	// Build a slice of positional values. For a set of P projections, for K key columns and N=P-K non-key columns,
	// we'll generate a slice 2P long structured as follows:
	// [K key projections, // list of tuple indexes to read for key columns
	//  N non-key projections, // list of tuple indexes to read for non-key columns, ordered backward from end
	//  P output ordinals]  // list of output column ordinals for each projection
	// Afterward we slice this into three separate mappings to return.
	allMap := make([]int, 2*numPhysicalColumns)
	keyIdx := 0
	nonKeyIdx := numPhysicalColumns - 1
	for projNum, tag := range projections {
		if idx, ok := pks.StoredIndexByTag(tag); ok && !pks.GetByStoredIndex(idx).Virtual {
			allMap[keyIdx] = idx
			allMap[numPhysicalColumns+keyIdx] = projNum
			keyIdx++
		} else if idx, ok := nonPks.StoredIndexByTag(tag); ok && !nonPks.GetByStoredIndex(idx).Virtual {
			allMap[nonKeyIdx] = idx
			allMap[numPhysicalColumns+nonKeyIdx] = projNum
			nonKeyIdx--
		}
	}

	keyMap = allMap[:keyIdx]
	valMap = allMap[keyIdx:numPhysicalColumns]
	ordMap = allMap[numPhysicalColumns:]
	return keyMap, valMap, ordMap
}

func ProjectionMappingsForIndex2(sch schema.Schema, projections []uint64) (val.OrdinalMapping, error) {
	pks := sch.GetPKCols()
	nonPks := sch.GetNonPKCols()
	ords := make(val.OrdinalMapping, len(projections))
	for i, tag := range projections {
		if idx, ok := pks.StoredIndexByTag(tag); ok {
			if pks.GetByStoredIndex(idx).Virtual {
				ords[i] = -1
			} else {
				ords[i] = idx
			}
		} else if idx, ok := nonPks.StoredIndexByTag(tag); ok {
			if nonPks.GetByStoredIndex(idx).Virtual {
				ords[i] = -1
			} else {
				ords[i] = pks.Size() + idx
			}
		} else {
			return nil, fmt.Errorf("tag not found in schema: %d", tag)
		}
	}
	return ords, nil
}

func (it prollyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	key, value, err := it.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	// TODO

	return NewProllyRow(key, value, it.keyDesc, it.valDesc, it.ordProj, it.ns), nil
	//row := make(sql.UntypedSqlRow, it.rowLen)
	//for i, idx := range it.keyProj {
	//	outputIdx := it.ordProj[i]
	//	row[outputIdx], err = tree.GetField(ctx, it.keyDesc, idx, key, it.ns)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//for i, idx := range it.valProj {
	//	outputIdx := it.ordProj[len(it.keyProj)+i]
	//	row[outputIdx], err = tree.GetField(ctx, it.valDesc, idx, value, it.ns)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//return row, nil
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
	it.curr = make(sql.UntypedSqlRow, it.rowLen)

	for i, idx := range it.valProj {
		outputIdx := it.ordProj[i]
		v, err := tree.GetField(ctx, it.valDesc, idx, value, it.ns)
		if err != nil {
			return err
		}
		it.curr.SetValue(outputIdx, v)
	}
	return nil
}

func (it *prollyKeylessIter) Close(ctx *sql.Context) error {
	return nil
}
