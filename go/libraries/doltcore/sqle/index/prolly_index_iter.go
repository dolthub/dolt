// Copyright 2020 Dolthub, Inc.
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
	"io"
	"slices"
	"sync"

	"github.com/dolthub/go-mysql-server/errguard"
	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const indexLookupBufSize = 8

type prollyIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	primary   prolly.MapReader

	// pkMap transforms indexRows index keys
	// into primary index keys
	pkMap val.OrdinalMapping
	pkBld *val.TupleBuilder

	// keyMap and valMap transform tuples from
	// primary row storage into sql.Row's
	keyMap, valMap val.OrdinalMapping
	// ordMap are output ordinals for |keyMap| and |valMap| concatenated
	ordMap      val.OrdinalMapping
	projections []uint64
	sqlSch      sql.Schema

	columnIds   sql.ColSet
	columnIdMap []sql.ColumnId
}

var _ sql.ValueRowWriter = prollyIndexIter{}

// NewProllyIndexIter returns a new prollyIndexIter.
func newProllyIndexIter(
	ctx *sql.Context,
	idx DoltIndex,
	rng prolly.Range,
	pkSch sql.PrimaryKeySchema,
	projections []uint64,
	primary, secondary prolly.MapReader,
	columnIds sql.ColSet,
) (prollyIndexIter, error) {
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyIndexIter{}, err
	}

	kd, _ := primary.Descriptors()
	pkBld := val.NewTupleBuilder(kd, primary.NodeStore())
	pkMap := OrdinalMappingFromIndex(idx)
	keyProj, valProj, ordProj := projectionMappings(idx.Schema(), projections)
	columnIdMap := columnIdMapping(idx.Schema(), projections, columnIds)

	iter := prollyIndexIter{
		idx:         idx,
		indexIter:   indexIter,
		primary:     primary,
		pkBld:       pkBld,
		pkMap:       pkMap,
		keyMap:      keyProj,
		valMap:      valProj,
		ordMap:      ordProj,
		projections: projections,
		sqlSch:      pkSch.Schema,
		columnIds:   columnIds,
		columnIdMap: columnIdMap,
	}

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	idxKey, _, err := p.indexIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	for to := range p.pkMap {
		from := p.pkMap.MapOrdinal(to)
		p.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	pk, err := p.pkBld.Build(ctx, sharePool)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, len(p.projections))
	err = p.primary.Get(ctx, pk, func(key, value val.Tuple) error {
		return p.rowFromTuples(ctx, key, value, r)
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NextValueRow implements the sql.ValueRowIter interface.
func (p prollyIndexIter) WriteNextValueRow(ctx *sql.Context, row sql.ValueRow) error {
	idxKey, _, err := p.indexIter.Next(ctx)
	if err != nil {
		return err
	}
	for to := range p.pkMap {
		from := p.pkMap.MapOrdinal(to)
		p.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	pk, err := p.pkBld.Build(ctx, sharePool)
	if err != nil {
		return err
	}

	err = p.primary.Get(ctx, pk, func(key, value val.Tuple) error {
		keyDesc, valDesc := p.primary.Descriptors()
		for i, idx := range p.keyMap {
			columnId := p.columnIdMap[p.ordMap[i]]
			row[columnId], err = tree.GetFieldValue(ctx, keyDesc, idx, key, p.primary.NodeStore())
			if err != nil {
				return err
			}
		}
		for i, idx := range p.valMap {
			columnId := p.columnIdMap[p.ordMap[len(p.keyMap)+i]]
			row[columnId], err = tree.GetFieldValue(ctx, valDesc, idx, value, p.primary.NodeStore())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// IsValueRowWriter implements the sql.ValueRowWriter interface.
func (p prollyIndexIter) IsValueRowWriter(ctx *sql.Context) bool {
	if p.columnIds.Empty() {
		return false
	}
	keyDesc, valDesc := p.primary.Descriptors()
	for _, typ := range keyDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	for _, typ := range valDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	return true
}

func (p prollyIndexIter) Columns() sql.ColSet {
	return p.columnIds
}

func (p prollyIndexIter) rowFromTuples(ctx context.Context, key, value val.Tuple, r sql.Row) (err error) {
	keyDesc, valDesc := p.primary.Descriptors()

	for i, idx := range p.keyMap {
		outputIdx := p.ordMap[i]
		r[outputIdx], err = tree.GetField(ctx, keyDesc, idx, key, p.primary.NodeStore())
		if err != nil {
			return err
		}
	}

	for i, idx := range p.valMap {
		outputIdx := p.ordMap[len(p.keyMap)+i]
		r[outputIdx], err = tree.GetField(ctx, valDesc, idx, value, p.primary.NodeStore())
		if err != nil {
			return err
		}
	}

	return
}

func (p prollyIndexIter) Close(*sql.Context) error {
	return nil
}

func OrdinalMappingFromIndex(idx DoltIndex) (m val.OrdinalMapping) {
	def := idx.Schema().Indexes().GetByName(idx.ID())
	return schema.PrimaryIndexOrdinalToSecondaryIndexOrdinal(def)
}

type prollyCoveringIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	keyDesc   *val.TupleDesc
	valDesc   *val.TupleDesc

	ns tree.NodeStore

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.

	// |keyMap| and |valMap| are both of len ==
	keyMap, valMap, ordMap val.OrdinalMapping
	projections            []uint64
	sqlSch                 sql.Schema
	columnIds              sql.ColSet
	columnIdMap            []sql.ColumnId
}

var _ sql.RowIter = prollyCoveringIndexIter{}
var _ sql.ValueRowWriter = prollyCoveringIndexIter{}

func newProllyCoveringIndexIter(
	ctx *sql.Context,
	idx DoltIndex,
	rng prolly.Range,
	pkSch sql.PrimaryKeySchema,
	projections []uint64,
	secondary prolly.MapReader,
	columnIds sql.ColSet,
) (prollyCoveringIndexIter, error) {
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyCoveringIndexIter{}, err
	}
	keyDesc, valDesc := secondary.Descriptors()

	var keyMap, valMap, ordMap val.OrdinalMapping
	if idx.IsPrimaryKey() {
		keyMap, valMap, ordMap = primaryIndexMapping(idx, projections)
	} else {
		keyMap, ordMap = coveringIndexMapping(idx, projections)
	}
	columnIdMap := columnIdMapping(idx.Schema(), projections, columnIds)

	return prollyCoveringIndexIter{
		idx:         idx,
		indexIter:   indexIter,
		keyDesc:     keyDesc,
		valDesc:     valDesc,
		keyMap:      keyMap,
		valMap:      valMap,
		ordMap:      ordMap,
		sqlSch:      pkSch.Schema,
		projections: projections,
		ns:          secondary.NodeStore(),
		columnIds:   columnIds,
		columnIdMap: columnIdMap,
	}, nil
}

// Next returns the next row from the iterator.
func (p prollyCoveringIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, v, err := p.indexIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, len(p.projections))
	if err := p.writeRowFromTuples(ctx, k, v, r); err != nil {
		return nil, err
	}

	return r, nil
}

// WriteNextValueRow implements the sql.ValueRowWriter interface.
func (p prollyCoveringIndexIter) WriteNextValueRow(ctx *sql.Context, row sql.ValueRow) error {
	k, v, err := p.indexIter.Next(ctx)
	if err != nil {
		return err
	}

	for i, idx := range p.keyMap {
		columnId := p.columnIdMap[p.ordMap[i]]
		row[columnId], err = tree.GetFieldValue(ctx, p.keyDesc, idx, k, p.ns)
		if err != nil {
			return err
		}
	}

	for i, idx := range p.valMap {
		columnId := p.columnIdMap[p.ordMap[len(p.keyMap)+i]]
		row[columnId], err = tree.GetFieldValue(ctx, p.valDesc, idx, v, p.ns)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p prollyCoveringIndexIter) Columns() sql.ColSet {
	return p.columnIds
}

func (p prollyCoveringIndexIter) WithColumns(set sql.ColSet) sql.ValueRowWriter {
	panic(1)
}

// IsValueRowWriter implements the sql.ValueRowWriter interface.
func (p prollyCoveringIndexIter) IsValueRowWriter(ctx *sql.Context) bool {
	if p.columnIds.Empty() {
		return false
	}
	for _, typ := range p.keyDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	for _, typ := range p.valDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	return true
}

func (p prollyCoveringIndexIter) writeRowFromTuples(ctx context.Context, key, value val.Tuple, r sql.Row) (err error) {
	for i, idx := range p.keyMap {
		outputIdx := p.ordMap[i]
		r[outputIdx], err = tree.GetField(ctx, p.keyDesc, idx, key, p.ns)
		if err != nil {
			return err
		}
	}

	for i, idx := range p.valMap {
		outputIdx := p.ordMap[len(p.keyMap)+i]
		r[outputIdx], err = tree.GetField(ctx, p.valDesc, idx, value, p.ns)
		if err != nil {
			return err
		}
	}
	return
}

func (p prollyCoveringIndexIter) Close(*sql.Context) error {
	return nil
}

func columnIdMapping(tableSchema schema.Schema, projections []uint64, columnIds sql.ColSet) []sql.ColumnId {
	result := make([]sql.ColumnId, len(projections))
	columnId, _ := columnIds.Next(1)
	for _, tableColumn := range tableSchema.GetAllCols().GetColumns() {
		projectionPosition := slices.Index(projections, tableColumn.Tag)
		if projectionPosition >= 0 {
			result[projectionPosition] = columnId
			columnId, _ = columnIds.Next(columnId + 1)
		}
	}
	return result
}

func coveringIndexMapping(d DoltIndex, projections []uint64) (keyMap, ordMap val.OrdinalMapping) {
	idx := d.IndexSchema().GetAllCols()
	allMap := make(val.OrdinalMapping, len(projections)*2)
	var i int
	for _, p := range projections {
		if idx, ok := idx.StoredIndexByTag(p); ok {
			allMap[i] = idx
			allMap[len(projections)+i] = i
			i++
		}
	}
	keyMap = allMap[:len(projections)]
	ordMap = allMap[len(projections):]
	return
}

func primaryIndexMapping(idx DoltIndex, projections []uint64) (keyProj, valProj, ordProj val.OrdinalMapping) {
	return ProjectionMappingsForIndex(idx.Schema(), projections)
}

type prollyKeylessIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	clustered prolly.MapReader

	// clusteredMap transforms secondary index keys
	// into clustered index keys
	clusteredMap val.OrdinalMapping
	clusteredBld *val.TupleBuilder

	eg        *errgroup.Group
	queueFunc func()
	// cancel stops this iterator's producer without canceling the caller's context.
	cancel  context.CancelFunc
	rowChan chan sql.Row

	valueDesc *val.TupleDesc

	// valueMap transforms tuples from the
	// clustered index into sql.Rows
	valueMap val.OrdinalMapping
	ordMap   val.OrdinalMapping
	sqlSch   sql.Schema
	// projections are the column tags of the output rows. They may include
	// virtual columns, which have an output ordinal but no stored value.
	projections []uint64

	card uint64
	curr sql.ValueRow

	columnIds   sql.ColSet
	columnIdMap []sql.ColumnId
}

var _ sql.ValueRowWriter = &prollyKeylessIndexIter{}

func newProllyKeylessIndexIter(ctx *sql.Context, idx DoltIndex, rng prolly.Range, pkSch sql.PrimaryKeySchema, projections []uint64, clustered, secondary prolly.MapReader, reverse bool, columnIds sql.ColSet) (*prollyKeylessIndexIter, error) {
	var indexIter prolly.MapIter
	var err error
	if reverse {
		indexIter, err = secondary.IterRangeReverse(ctx, rng)
	} else {
		indexIter, err = secondary.IterRange(ctx, rng)
	}
	if err != nil {
		return nil, err
	}

	keyDesc, valDesc := clustered.Descriptors()
	indexMap := OrdinalMappingFromIndex(idx)
	keyBld := val.NewTupleBuilder(keyDesc, clustered.NodeStore())
	sch := idx.Schema()
	_, vm, om := projectionMappings(sch, projections)

	workerCtx, cancel := context.WithCancel(ctx)
	eg, c := errgroup.WithContext(workerCtx)

	columnIdMap := columnIdMapping(idx.Schema(), projections, columnIds)
	iter := prollyKeylessIndexIter{
		idx:          idx,
		indexIter:    indexIter,
		clustered:    clustered,
		clusteredMap: indexMap,
		clusteredBld: keyBld,
		eg:           eg,
		cancel:       cancel,
		rowChan:      make(chan sql.Row, indexLookupBufSize),
		valueMap:     vm,
		ordMap:       om,
		valueDesc:    valDesc,
		sqlSch:       pkSch.Schema,
		projections:  projections,
		columnIds:    columnIds,
		columnIdMap:  columnIdMap,
	}

	// TODO: DO NOT SUBMIT. Consider removing the channel approach entirely.
	// We do this because we only want to create the goroutine if we're not using this as a ValueRowWriter
	iter.queueFunc = sync.OnceFunc(func() {
		errguard.Go(eg, func() error {
			return iter.queueRows(c)
		})
	})

	return &iter, nil
}

// Next returns the next row from the iterator.
func (p prollyKeylessIndexIter) Next(ctx *sql.Context) (sql.Row, error) {

	p.queueFunc()
	r, ok := <-p.rowChan
	if ok {
		return r, nil
	}

	if err := p.eg.Wait(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

// WriteNextValueRow implements the sql.ValueRowWriter interface.
func (p *prollyKeylessIndexIter) WriteNextValueRow(ctx *sql.Context, row sql.ValueRow) error {
	if p.card == 0 {

		idxKey, _, err := p.indexIter.Next(ctx)
		if err != nil {
			return err
		}

		for to := range p.clusteredMap {
			from := p.clusteredMap.MapOrdinal(to)
			p.clusteredBld.PutRaw(to, idxKey.GetField(from))
		}
		pk, err := p.clusteredBld.Build(ctx, sharePool)
		if err != nil {
			return err
		}

		var value val.Tuple
		err = p.clustered.Get(ctx, pk, func(k, v val.Tuple) error {
			value = v
			return nil
		})
		if err != nil {
			return err
		}

		p.card = val.ReadKeylessCardinality(value)

		for i, idx := range p.valueMap {
			outputIdx := p.columnIdMap[p.ordMap[i]]
			row[outputIdx], err = tree.GetFieldValue(ctx, p.valueDesc, idx, value, p.clustered.NodeStore())
			if err != nil {
				return err
			}
		}
	}
	p.card--
	return nil

}

// IsValueRowWriter implements the sql.ValueRowWriter interface.
func (p prollyKeylessIndexIter) IsValueRowWriter(ctx *sql.Context) bool {
	if p.columnIds.Empty() {
		return false
	}
	for _, typ := range p.valueDesc.Types {
		if typ.Enc == val.ExtendedEnc || typ.Enc == val.ExtendedAddrEnc || typ.Enc == val.ExtendedAdaptiveEnc {
			return false
		}
	}
	return true
}

func (p prollyKeylessIndexIter) Columns() sql.ColSet {
	return p.columnIds
}

func (p prollyKeylessIndexIter) queueRows(ctx context.Context) error {
	defer close(p.rowChan)

	for {
		idxKey, _, err := p.indexIter.Next(ctx)
		if err != nil {
			return err
		}

		for to := range p.clusteredMap {
			from := p.clusteredMap.MapOrdinal(to)
			p.clusteredBld.PutRaw(to, idxKey.GetField(from))
		}
		pk, err := p.clusteredBld.Build(ctx, sharePool)
		if err != nil {
			return err
		}

		var value val.Tuple
		err = p.clustered.Get(ctx, pk, func(k, v val.Tuple) error {
			value = v
			return nil
		})
		if err != nil {
			return err
		}

		rows, err := p.keylessRowsFromValueTuple(ctx, p.clustered.NodeStore(), value)
		if err != nil {
			return err
		}

		for i := range rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.rowChan <- rows[i]:
			}
		}
	}
}

func (p prollyKeylessIndexIter) keylessRowsFromValueTuple(ctx context.Context, ns tree.NodeStore, value val.Tuple) (rows []sql.Row, err error) {
	card := val.ReadKeylessCardinality(value)
	rows = make([]sql.Row, card)
	rows[0] = make(sql.Row, len(p.projections))

	for i, idx := range p.valueMap {
		columnId := p.ordMap[i]
		rows[0][columnId], err = tree.GetField(ctx, p.valueDesc, idx, value, ns)
		if err != nil {
			return nil, err
		}
	}

	// duplicate row |card| times
	for i := 1; i < len(rows); i++ {
		rows[i] = rows[0].Copy()
	}
	return
}

// Close stops and joins the keyless index row producer before returning.
func (p prollyKeylessIndexIter) Close(*sql.Context) error {
	p.cancel()
	_ = p.eg.Wait()
	return nil
}
