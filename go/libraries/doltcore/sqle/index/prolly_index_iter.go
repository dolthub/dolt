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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

const indexLookupBufSize = 1024

type prollyIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	primary   prolly.Map
	keyless   bool

	// pkMap transforms indexRows index keys
	// into primary index keys
	pkMap val.OrdinalMapping
	pkBld *val.TupleBuilder

	eg      *errgroup.Group
	rowChan chan sql.Row

	// keyMap and valMap transform tuples from
	// primary row storage into sql.Row's
	keyMap, valMap val.OrdinalMapping
	sqlSch         sql.Schema
}

var _ sql.RowIter = prollyIndexIter{}
var _ sql.RowIter2 = prollyIndexIter{}

// NewProllyIndexIter returns a new prollyIndexIter.
func newProllyIndexIter(
	ctx *sql.Context,
	idx DoltIndex,
	rng prolly.Range,
	pkSch sql.PrimaryKeySchema,
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
	sch := idx.Schema()
	km, vm := projectionMappings(sch, sch.GetAllCols().GetColumnNames())

	eg, c := errgroup.WithContext(ctx)

	iter := prollyIndexIter{
		idx:       idx,
		indexIter: indexIter,
		primary:   primary,
		keyless:   schema.IsKeyless(sch),
		pkBld:     pkBld,
		pkMap:     pkMap,
		eg:        eg,
		rowChan:   make(chan sql.Row, indexLookupBufSize),
		keyMap:    km,
		valMap:    vm,
		sqlSch:    pkSch.Schema,
	}

	eg.Go(func() error {
		return iter.queueRows(c)
	})

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyIndexIter) Next(ctx *sql.Context) (r sql.Row, err error) {
	for {
		var ok bool
		select {
		case r, ok = <-p.rowChan:
			if ok {
				return r, nil
			}
		}
		if !ok {
			break
		}
	}

	if err = p.eg.Wait(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

func (p prollyIndexIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	panic("unimplemented")
}

func (p prollyIndexIter) queueRows(ctx context.Context) error {
	defer close(p.rowChan)

	// Keyless rows have hash and cardinality values which will not be included, but are a part of the keyMap/valMap
	rLen := len(p.keyMap) + len(p.valMap)
	if p.keyless {
		rLen -= 2
	}

	for {
		idxKey, _, err := p.indexIter.Next(ctx)
		if err != nil {
			return err
		}

		for to := range p.pkMap {
			from := p.pkMap.MapOrdinal(to)
			p.pkBld.PutRaw(to, idxKey.GetField(from))
		}
		pk := p.pkBld.Build(sharePool)

		r := make(sql.Row, rLen)
		err = p.primary.Get(ctx, pk, func(key, value val.Tuple) error {
			return p.rowFromTuples(key, value, r)
		})
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.rowChan <- r:
		}
	}
}

func (p prollyIndexIter) rowFromTuples(key, value val.Tuple, r sql.Row) (err error) {
	keyDesc, valDesc := p.primary.Descriptors()

	for keyIdx, rowIdx := range p.keyMap {
		if rowIdx == -1 {
			continue
		}
		r[rowIdx], err = GetField(keyDesc, keyIdx, key)
		if err != nil {
			return err
		}
	}
	for valIdx, rowIdx := range p.valMap {
		if rowIdx == -1 {
			continue
		}
		r[rowIdx], err = GetField(valDesc, valIdx, value)
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
	if idx.ID() == "PRIMARY" {
		// todo(andy)
		m = make(val.OrdinalMapping, idx.Schema().GetPKCols().Size())
		for i := range m {
			m[i] = i
		}
		return m
	}

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
	return m
}

type prollyCoveringIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapIter
	keyDesc   val.TupleDesc
	valDesc   val.TupleDesc

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.

	// |keyMap| and |valMap| are both of len ==
	keyMap, valMap val.OrdinalMapping
	sqlSch         sql.Schema
}

var _ sql.RowIter = prollyCoveringIndexIter{}
var _ sql.RowIter2 = prollyCoveringIndexIter{}

func newProllyCoveringIndexIter(ctx *sql.Context, idx DoltIndex, rng prolly.Range, pkSch sql.PrimaryKeySchema, indexdata durable.Index) (prollyCoveringIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(indexdata)
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyCoveringIndexIter{}, err
	}
	keyDesc, valDesc := secondary.Descriptors()

	var keyMap []int
	var valMap []int

	if idx.ID() == "PRIMARY" {
		keyMap, valMap = primaryIndexMapping(idx, pkSch)
	} else {
		keyMap = coveringIndexMapping(idx, pkSch)
	}

	iter := prollyCoveringIndexIter{
		idx:       idx,
		indexIter: indexIter,
		keyDesc:   keyDesc,
		valDesc:   valDesc,
		keyMap:    keyMap,
		valMap:    valMap,
		sqlSch:    pkSch.Schema,
	}

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyCoveringIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, v, err := p.indexIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, len(p.keyMap))
	if err := p.writeRowFromTuples(k, v, r); err != nil {
		return nil, err
	}

	return r, nil
}

func (p prollyCoveringIndexIter) Next2(ctx *sql.Context, f *sql.RowFrame) error {
	k, v, err := p.indexIter.Next(ctx)
	if err != nil {
		return err
	}

	return p.writeRow2FromTuples(k, v, f)
}

func (p prollyCoveringIndexIter) writeRowFromTuples(key, value val.Tuple, r sql.Row) (err error) {
	for to := range p.keyMap {
		from := p.keyMap.MapOrdinal(to)
		if from == -1 {
			continue
		}
		r[to], err = GetField(p.keyDesc, from, key)
		if err != nil {
			return err
		}
	}

	for to := range p.valMap {
		from := p.valMap.MapOrdinal(to)
		if from == -1 {
			continue
		}
		r[to], err = GetField(p.valDesc, from, value)
		if err != nil {
			return err
		}
	}

	return
}

func (p prollyCoveringIndexIter) writeRow2FromTuples(key, value val.Tuple, f *sql.RowFrame) (err error) {

	// TODO: handle out of order projections
	for to := range p.keyMap {
		from := p.keyMap.MapOrdinal(to)
		if from == -1 {
			continue
		}

		enc := p.keyDesc.Types[from].Enc
		f.Append(sql.Value{
			Typ: encodingToType[enc],
			Val: p.keyDesc.GetField(from, key),
		})
	}

	for to := range p.valMap {
		from := p.valMap.MapOrdinal(to)
		if from == -1 {
			continue
		}

		enc := p.valDesc.Types[from].Enc
		f.Append(sql.Value{
			Typ: encodingToType[enc],
			Val: p.valDesc.GetField(from, value),
		})
	}

	return
}

func (p prollyCoveringIndexIter) Close(*sql.Context) error {
	return nil
}

func coveringIndexMapping(idx DoltIndex, pkSch sql.PrimaryKeySchema) (keyMap val.OrdinalMapping) {
	allCols := idx.Schema().GetAllCols()
	idxCols := idx.IndexSchema().GetAllCols()

	keyMap = make(val.OrdinalMapping, allCols.Size())
	for i, col := range allCols.GetColumns() {
		j, ok := idxCols.TagToIdx[col.Tag]
		if ok {
			keyMap[i] = j
		} else {
			keyMap[i] = -1
		}
	}

	return
}

func primaryIndexMapping(idx DoltIndex, pkSch sql.PrimaryKeySchema) (keyMap, valMap val.OrdinalMapping) {
	sch := idx.Schema()

	keyMap = make(val.OrdinalMapping, len(pkSch.Schema))
	for i, col := range pkSch.Schema {
		// if |col.Name| not found, IndexOf returns -1
		keyMap[i] = sch.GetPKCols().IndexOf(col.Name)
	}

	valMap = make(val.OrdinalMapping, len(pkSch.Schema))
	for i, col := range pkSch.Schema {
		// if |col.Name| not found, IndexOf returns -1
		valMap[i] = sch.GetNonPKCols().IndexOf(col.Name)
	}

	return
}
