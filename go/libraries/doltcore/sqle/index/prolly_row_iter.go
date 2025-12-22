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
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyRowIter struct {
	iter prolly.MapIter
	ns   tree.NodeStore

	keyDesc *val.TupleDesc
	valDesc *val.TupleDesc

	keyProj []int
	valProj []int
	// ordProj is a concatenated list of output ordinals for |keyProj| and |valProj|
	ordProj []int
	rowLen  int
}

var _ sql.RowIter = prollyRowIter{}

func NewProllyRowIterForMap(sch schema.Schema, rows prolly.MapInterface, iter prolly.MapIter, projections []uint64) sql.RowIter {
	if projections == nil {
		projections = sch.GetAllCols().Tags
	}

	kd, vd := rows.Descriptors()
	ns := rows.NodeStore()

	return NewProllyRowIterForSchema(sch, iter, kd, vd, projections, ns)
}

func NewProllyRowIterForSchema(
	sch schema.Schema,
	iter prolly.MapIter,
	kd *val.TupleDesc,
	vd *val.TupleDesc,
	projections []uint64,
	ns tree.NodeStore,
) sql.RowIter {
	if schema.IsKeyless(sch) {
		return NewKeylessProllyRowIter(sch, iter, vd, projections, ns)
	}

	return NewKeyedProllyRowIter(sch, iter, kd, vd, projections, ns)
}

func NewKeyedProllyRowIter(
	sch schema.Schema,
	iter prolly.MapIter,
	kd *val.TupleDesc,
	vd *val.TupleDesc,
	projections []uint64,
	ns tree.NodeStore,
) sql.RowIter {
	keyProj, valProj, ordProj := projectionMappings(sch, projections)

	// TODO: create worker pool here?
	// TODO: ideally we'd create a global one for all iters right?

	return prollyRowIter{
		iter:    iter,
		keyDesc: kd,
		valDesc: vd,
		keyProj: keyProj,
		valProj: valProj,
		ordProj: ordProj,
		rowLen:  len(projections),
		ns:      ns,
	}
}

func NewKeylessProllyRowIter(
	sch schema.Schema,
	iter prolly.MapIter,
	vd *val.TupleDesc,
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

func (it prollyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	key, value, err := it.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Send GetField requests as a job to the worker pool, wait for all results
	row := make(sql.Row, it.rowLen)
	for i, idx := range it.keyProj {
		outputIdx := it.ordProj[i]
		row[outputIdx], err = tree.GetField(ctx, it.keyDesc, idx, key, it.ns)
		if err != nil {
			return nil, err
		}
	}
	for i, idx := range it.valProj {
		outputIdx := it.ordProj[len(it.keyProj)+i]
		row[outputIdx], err = tree.GetField(ctx, it.valDesc, idx, value, it.ns)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

// NextValueRow implements the sql.ValueRowIter interface.
func (it prollyRowIter) NextValueRow(ctx *sql.Context) (sql.ValueRow, error) {
	key, value, err := it.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: use a worker pool? limit number of go routines?
	eg, subCtx := errgroup.WithContext(ctx)
	row := make(sql.ValueRow, it.rowLen)
	for i, idx := range it.keyProj {
		eg.Go(func() (err error) {
			outIdx := it.ordProj[i]
			row[outIdx], err = tree.GetFieldValue(subCtx, it.keyDesc, idx, key, it.ns)
			if err != nil {
				return err
			}
			return nil
		})
	}

	for i, idx := range it.valProj {
		eg.Go(func() (err error) {
			outIdx := it.ordProj[len(it.keyProj)+i]
			row[outIdx], err = tree.GetFieldValue(ctx, it.valDesc, idx, value, it.ns)
			if err != nil {
				return err
			}
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return row, nil
}

// IsValueRowIter implements the sql.ValueRowIter interface.
func (it prollyRowIter) IsValueRowIter(ctx *sql.Context) bool {
	for _, typ := range it.keyDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	for _, typ := range it.valDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	return true
}

func (it prollyRowIter) Close(ctx *sql.Context) error {
	return nil
}

type prollyKeylessIter struct {
	iter       prolly.MapIter
	ns         tree.NodeStore
	valDesc    *val.TupleDesc
	valProj    []int
	ordProj    []int
	curr       sql.Row
	currValRow sql.ValueRow
	rowLen     int
	card       uint64
}

var _ sql.RowIter = &prollyKeylessIter{}
var _ sql.ValueRowIter = &prollyKeylessIter{}

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
		it.curr[outputIdx], err = tree.GetField(ctx, it.valDesc, idx, value, it.ns)
		if err != nil {
			return err
		}
	}
	return nil
}

// NextValueRow implements the sql.ValueRowIter interface.
func (it *prollyKeylessIter) NextValueRow(ctx *sql.Context) (sql.ValueRow, error) {
	if it.card == 0 {
		_, value, err := it.iter.Next(ctx)
		if err != nil {
			return nil, err
		}

		it.card = val.ReadKeylessCardinality(value)
		it.currValRow = make(sql.ValueRow, it.rowLen)
		for i, idx := range it.valProj {
			outputIdx := it.ordProj[i]
			it.currValRow[outputIdx], err = tree.GetFieldValue(ctx, it.valDesc, idx, value, it.ns)
			if err != nil {
				return nil, err
			}
		}
	}
	it.card--
	return it.currValRow, nil
}

// IsValueRowIter implements the sql.ValueRowIter interface.
func (it *prollyKeylessIter) IsValueRowIter(ctx *sql.Context) bool {
	for _, typ := range it.valDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	return true
}

func (it *prollyKeylessIter) Close(ctx *sql.Context) error {
	return nil
}
