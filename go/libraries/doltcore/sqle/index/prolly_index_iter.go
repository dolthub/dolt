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
	"github.com/dolthub/dolt/go/store/val"
)

const indexLookupBufSize = 1024

type prollyIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapRangeIter
	primary   prolly.Map

	// pkMap transforms indexRows index keys
	// into primary index keys
	pkMap columnMapping
	pkBld *val.TupleBuilder

	eg     *errgroup.Group
	pkChan chan val.Tuple

	// keyMap and valMap transform tuples from
	// primary row storage into sql.Row's
	keyMap, valMap columnMapping
}

var _ sql.RowIter = prollyIndexIter{}

// todo(andy): consolidate definitions of columnMapping
type columnMapping []int

// NewProllyIndexIter returns a new prollyIndexIter.
func newProllyIndexIter(ctx *sql.Context, idx DoltIndex, rng prolly.Range, projection []string) (prollyIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(idx.IndexRowData())
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyIndexIter{}, err
	}

	primary := durable.ProllyMapFromIndex(idx.TableData())
	kd, _ := primary.Descriptors()
	pkBld := val.NewTupleBuilder(kd)
	pkMap := columnMappingFromIndex(idx)
	km, vm := projectionMappings(idx.Schema(), idx.Schema().GetAllCols().GetColumnNames())

	eg, c := errgroup.WithContext(ctx)

	iter := prollyIndexIter{
		idx:       idx,
		indexIter: indexIter,
		primary:   primary,
		pkBld:     pkBld,
		pkMap:     pkMap,
		eg:        eg,
		pkChan:    make(chan val.Tuple, indexLookupBufSize),
		keyMap:    columnMapping(km),
		valMap:    columnMapping(vm),
	}

	eg.Go(func() error {
		return iter.queuePrimaryKeys(c)
	})

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	var err error
	for {
		var ok bool
		var pk val.Tuple
		select {
		case pk, ok = <-p.pkChan:
		}

		if !ok {
			break
		}

		r := make(sql.Row, len(p.keyMap)+len(p.valMap))
		err = p.primary.Get(ctx, pk, func(key, value val.Tuple) (err error) {
			p.rowFromTuples(key, value, r)
			return
		})
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	if err = p.eg.Wait(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

func (p prollyIndexIter) queuePrimaryKeys(ctx context.Context) error {
	defer close(p.pkChan)

	for {
		idxKey, _, err := p.indexIter.Next(ctx)
		if err == io.EOF {
			// ignore io.EOF here, and return it in Next()
			// after |p.pkChan| has been drained
			return nil
		}
		if err != nil {
			return err
		}

		for i, j := range p.pkMap {
			p.pkBld.PutRaw(i, idxKey.GetField(j))
		}
		pk := p.pkBld.Build(sharePool)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.pkChan <- pk:
		}
	}
}

func (p prollyIndexIter) rowFromTuples(key, value val.Tuple, r sql.Row) {
	keyDesc, valDesc := p.primary.Descriptors()

	for keyIdx, rowIdx := range p.keyMap {
		if rowIdx == -1 {
			continue
		}
		r[rowIdx] = keyDesc.GetField(keyIdx, key)
	}
	for valIdx, rowIdx := range p.valMap {
		if rowIdx == -1 {
			continue
		}
		r[rowIdx] = valDesc.GetField(valIdx, value)
	}

	return
}

func (p prollyIndexIter) Close(*sql.Context) error {
	return nil
}

func columnMappingFromIndex(idx DoltIndex) (m columnMapping) {
	if idx.ID() == "PRIMARY" {
		// todo(andy)
		m = make(columnMapping, idx.Schema().GetPKCols().Size())
		for i := range m {
			m[i] = i
		}
		return m
	}

	def := idx.Schema().Indexes().GetByName(idx.ID())
	pks := def.PrimaryKeyTags()
	m = make(columnMapping, len(pks))

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
	indexIter prolly.MapRangeIter
	keyDesc   val.TupleDesc
	valDesc   val.TupleDesc

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.
	// todo(andy): shore up this mapping concept, different semantics different places
	keyMap, valMap []int
}

var _ sql.RowIter = prollyCoveringIndexIter{}

func newProllyCoveringIndexIter(ctx *sql.Context, idx DoltIndex, rng prolly.Range) (prollyCoveringIndexIter, error) {
	secondary := durable.ProllyMapFromIndex(idx.IndexRowData())
	indexIter, err := secondary.IterRange(ctx, rng)
	if err != nil {
		return prollyCoveringIndexIter{}, err
	}
	keyDesc, valDesc := secondary.Descriptors()

	var keyMap []int
	var valMap []int

	if idx.ID() == "PRIMARY" {
		keyMap, valMap = primaryIndexMapping(idx)
	} else {
		keyMap = coveringIndexMapping(idx)
	}

	iter := prollyCoveringIndexIter{
		idx:       idx,
		indexIter: indexIter,
		keyDesc:   keyDesc,
		valDesc:   valDesc,
		keyMap:    keyMap,
		valMap:    valMap,
	}

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyCoveringIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		k, v, err := p.indexIter.Next(ctx)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		r := make(sql.Row, len(p.keyMap)+len(p.valMap))
		p.rowFromTuples(k, v, r)

		return r, nil
	}
}

func (p prollyCoveringIndexIter) rowFromTuples(key, value val.Tuple, r sql.Row) {
	for to, from := range p.keyMap {
		if from == -1 {
			continue
		}
		r[to] = p.keyDesc.GetField(from, key)
	}

	for to, from := range p.valMap {
		if from == -1 {
			continue
		}
		r[to] = p.valDesc.GetField(from, value)
	}

	return
}

func (p prollyCoveringIndexIter) Close(*sql.Context) error {
	return nil
}

// todo(andy): there are multiple column mapping concepts with different semantics
func coveringIndexMapping(idx DoltIndex) (keyMap []int) {
	allCols := idx.Schema().GetAllCols()
	idxCols := idx.IndexSchema().GetAllCols()

	keyMap = make([]int, allCols.Size())
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

func primaryIndexMapping(idx DoltIndex) (keyMap, valMap []int) {
	keyMap = make([]int, idx.Schema().GetPKCols().Size())
	for i := range keyMap {
		keyMap[i] = i
	}

	valMap = make([]int, idx.Schema().GetNonPKCols().Size())
	for i := range valMap {
		valMap[i] = i
	}

	return
}
