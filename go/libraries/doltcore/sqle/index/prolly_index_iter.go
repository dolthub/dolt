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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyIndexIter struct {
	idx       DoltIndex
	indexIter prolly.MapRangeIter
	primary   prolly.Map

	// pkMap transforms secondary index keys
	// into primary index keys
	pkMap columnMapping
	pkBld *val.TupleBuilder

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

	iter := prollyIndexIter{
		idx:       idx,
		indexIter: indexIter,
		primary:   primary,
		pkBld:     pkBld,
		pkMap:     pkMap,
		keyMap:    columnMapping(km),
		valMap:    columnMapping(vm),
	}

	return iter, nil
}

// Next returns the next row from the iterator.
func (p prollyIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		idxKey, _, err := p.indexIter.Next(ctx)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		for i, j := range p.pkMap {
			p.pkBld.PutRaw(i, idxKey.GetField(j))
		}
		pk := p.pkBld.Build(sharePool)

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

// todo(andy)
//type prollyCoveringIndexIter struct {
//	idx       DoltIndex
//	indexIter   nomsKeyIter
//	conv      *KVToSqlRowConverter
//	ctx       *sql.Context
//	pkCols    *schema.ColCollection
//	nonPKCols *schema.ColCollection
//	nbf       *types.NomsBinFormat
//}
//
//func NewProllyCoveringIndexIter(ctx *sql.Context, idx DoltIndex, indexIter nomsKeyIter, resultCols []string) *prollyCoveringIndexIter {
//	idxCols := idx.IndexSchema().GetPKCols()
//	tblPKCols := idx.Schema().GetPKCols()
//	sch := idx.Schema()
//	cols := sch.GetAllCols().GetColumns()
//	tagToSqlColIdx := make(map[uint64]int)
//
//	resultColSet := set.NewCaseInsensitiveStrSet(resultCols)
//	for i, col := range cols {
//		_, partOfIdxKey := idxCols.GetByNameCaseInsensitive(col.Name)
//		if partOfIdxKey && (len(resultCols) == 0 || resultColSet.Contains(col.Name)) {
//			tagToSqlColIdx[col.Tag] = i
//		}
//	}
//
//	for i, col := range cols {
//		_, partOfIndexKey := idxCols.GetByTag(col.Tag)
//		_, partOfTableKeys := tblPKCols.GetByTag(col.Tag)
//		if partOfIndexKey != partOfTableKeys {
//			cols[i], _ = schema.NewColumnWithTypeInfo(col.Name, col.Tag, col.TypeInfo, partOfIndexKey, col.Default, col.AutoIncrement, col.Comment, col.Constraints...)
//		}
//	}
//
//	return &prollyCoveringIndexIter{
//		idx:       idx,
//		indexIter:   indexIter,
//		conv:      NewKVToSqlRowConverter(idx.Format(), tagToSqlColIdx, cols, len(cols)),
//		ctx:       ctx,
//		pkCols:    sch.GetPKCols(),
//		nonPKCols: sch.GetNonPKCols(),
//		nbf:       idx.Format(),
//	}
//}
//
//// Next returns the next row from the iterator.
//func (ci *prollyCoveringIndexIter) Next(ctx *sql.Context) (sql.Row, error) {
//	key, err := ci.indexIter.ReadKey(ctx)
//
//	if err != nil {
//		return nil, err
//	}
//
//	return ci.conv.ConvertKVTuplesToSqlRow(key, types.Tuple{})
//}
//
//func (ci *prollyCoveringIndexIter) Close(*sql.Context) error {
//	return nil
//}
