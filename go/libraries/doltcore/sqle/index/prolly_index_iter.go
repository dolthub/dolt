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

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const indexLookupBufSize = 8

type prollyIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	primary   prolly.Map

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
}

var _ sql.RowIter = prollyIndexIter{}

// NewProllyIndexIter returns a new prollyIndexIter.
func newProllyIndexIter(
	ctx *sql.Context,
	idx DoltIndex,
	rng prolly.Range,
	pkSch sql.PrimaryKeySchema,
	projections []uint64,
	dprimary, dsecondary durable.Index,
) (prollyIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(dsecondary)
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyIndexIter{}, err
	}

	primary := durable.ProllyMapFromIndex(dprimary)
	kd, _ := primary.Descriptors()
	pkBld := val.NewTupleBuilder(kd)
	pkMap := ordinalMappingFromIndex(idx)
	keyProj, valProj, ordProj := projectionMappings(idx.Schema(), projections)

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
	pk := p.pkBld.Build(sharePool)

	r := make(sql.Row, len(p.projections))
	err = p.primary.Get(ctx, pk, func(key, value val.Tuple) error {
		return p.rowFromTuples(ctx, key, value, r)
	})
	if err != nil {
		return nil, err
	}
	return r, nil
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

func ordinalMappingFromIndex(idx DoltIndex) (m val.OrdinalMapping) {
	def := idx.Schema().Indexes().GetByName(idx.ID())
	pks := def.PrimaryKeyTags()
	if len(pks) == 0 { // keyless index
		m = make(val.OrdinalMapping, 1)
		m[0] = len(def.AllTags())
		return m
	}

	m = make(val.OrdinalMapping, len(pks))
	for i, pk := range pks {
		for j, tag := range def.AllTags() {
			if tag == pk {
				m[i] = j
				break
			}
		}
	}
	return
}

type prollyCoveringIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	keyDesc   val.TupleDesc
	valDesc   val.TupleDesc

	ns tree.NodeStore

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.

	// |keyMap| and |valMap| are both of len ==
	keyMap, valMap, ordMap val.OrdinalMapping
	projections            []uint64
	sqlSch                 sql.Schema
}

var _ sql.RowIter = prollyCoveringIndexIter{}

func newProllyCoveringIndexIter(
	ctx *sql.Context,
	idx DoltIndex,
	rng prolly.Range,
	pkSch sql.PrimaryKeySchema,
	projections []uint64,
	indexdata durable.Index,
) (prollyCoveringIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(indexdata)
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
	clustered prolly.Map

	// clusteredMap transforms secondary index keys
	// into clustered index keys
	clusteredMap val.OrdinalMapping
	clusteredBld *val.TupleBuilder

	eg      *errgroup.Group
	rowChan chan sql.Row

	// valueMap transforms tuples from the
	// clustered index into sql.Rows
	valueMap  val.OrdinalMapping
	ordMap    val.OrdinalMapping
	valueDesc val.TupleDesc
	sqlSch    sql.Schema
}

var _ sql.RowIter = prollyKeylessIndexIter{}

func newProllyKeylessIndexIter(ctx *sql.Context, idx DoltIndex, rng prolly.Range, doltgresRange *DoltgresRange, pkSch sql.PrimaryKeySchema, projections []uint64, rows, dsecondary durable.Index, reverse bool) (prollyKeylessIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(dsecondary)
	var indexIter prolly.MapIter
	var err error
	if doltgresRange == nil {
		if reverse {
			indexIter, err = secondary.IterRangeReverse(ctx, rng)
		} else {
			indexIter, err = secondary.IterRange(ctx, rng)
		}
		if err != nil {
			return prollyKeylessIndexIter{}, err
		}
	} else {
		indexIter, err = doltgresProllyMapIterator(ctx, secondary.KeyDesc(), secondary.NodeStore(), secondary.Tuples().Root, *doltgresRange)
		if err != nil {
			return prollyKeylessIndexIter{}, err
		}
	}

	clustered := durable.ProllyMapFromIndex(rows)
	keyDesc, valDesc := clustered.Descriptors()
	indexMap := ordinalMappingFromIndex(idx)
	keyBld := val.NewTupleBuilder(keyDesc)
	sch := idx.Schema()
	_, vm, om := projectionMappings(sch, projections)

	eg, c := errgroup.WithContext(ctx)

	iter := prollyKeylessIndexIter{
		idx:          idx,
		indexIter:    indexIter,
		clustered:    clustered,
		clusteredMap: indexMap,
		clusteredBld: keyBld,
		eg:           eg,
		rowChan:      make(chan sql.Row, indexLookupBufSize),
		valueMap:     vm,
		ordMap:       om,
		valueDesc:    valDesc,
		sqlSch:       pkSch.Schema,
	}

	eg.Go(func() error {
		return iter.queueRows(c)
	})

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyKeylessIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	r, ok := <-p.rowChan
	if ok {
		return r, nil
	}

	if err := p.eg.Wait(); err != nil {
		return nil, err
	}

	return nil, io.EOF
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
		pk := p.clusteredBld.Build(sharePool)

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
	rows[0] = make(sql.Row, len(p.valueMap))

	for i, idx := range p.valueMap {
		outputIdx := p.ordMap[i]
		rows[0][outputIdx], err = tree.GetField(ctx, p.valueDesc, idx, value, ns)
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

func (p prollyKeylessIndexIter) Close(*sql.Context) error {
	return nil
}
