// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

func NewSecondaryKeyBuilder(sch schema.Schema, def schema.Index, idxDesc val.TupleDesc, p pool.BuffPool) (b SecondaryKeyBuilder) {
	b.builder = val.NewTupleBuilder(idxDesc)
	b.pool = p

	keyless := schema.IsKeyless(sch)
	if keyless {
		// the only key is the hash of the values
		b.split = 1
	} else {
		b.split = sch.GetPKCols().Size()
	}

	b.mapping = make(val.OrdinalMapping, len(def.AllTags()))
	for i, tag := range def.AllTags() {
		j, ok := sch.GetPKCols().TagToIdx[tag]
		if !ok {
			if keyless {
				// Skip cardinality column
				j = b.split + 1 + sch.GetNonPKCols().TagToIdx[tag]
			} else {
				j = b.split + sch.GetNonPKCols().TagToIdx[tag]
			}
		}
		b.mapping[i] = j
	}

	if keyless {
		// last key in index is hash which is the only column in the key
		b.mapping = append(b.mapping, 0)
	}
	return
}

type SecondaryKeyBuilder struct {
	mapping val.OrdinalMapping
	split   int
	builder *val.TupleBuilder
	pool    pool.BuffPool
}

// SecondaryKeyFromRow builds a secondary index key from a clustered index row.
func (b SecondaryKeyBuilder) SecondaryKeyFromRow(k, v val.Tuple) val.Tuple {
	for to := range b.mapping {
		from := b.mapping.MapOrdinal(to)
		if from < b.split {
			b.builder.PutRaw(to, k.GetField(from))
		} else {
			from -= b.split
			buf := v.GetField(from)
			if b.builder.Desc.Types[to].Enc == val.CellEnc {
				// convert from WKB to z-order encoding
				cell := ZCell(deserializeGeometry(buf).(types.GeometryValue))
				buf = cell[:]
			}
			b.builder.PutRaw(to, buf)
		}
	}
	return b.builder.Build(b.pool)
}

func NewClusteredKeyBuilder(def schema.Index, sch schema.Schema, keyDesc val.TupleDesc, p pool.BuffPool) (b ClusteredKeyBuilder) {
	b.pool = p
	if schema.IsKeyless(sch) {
		// [16]byte hash key is always final key field
		b.mapping = val.OrdinalMapping{def.Count() - 1}
		b.builder = val.NewTupleBuilder(val.KeylessTupleDesc)
		return
	}

	// secondary indexes contain all clustered key cols, in some order
	tagToOrdinal := make(map[uint64]int, len(def.AllTags()))
	for ord, tag := range def.AllTags() {
		tagToOrdinal[tag] = ord
	}

	b.builder = val.NewTupleBuilder(keyDesc)
	b.mapping = make(val.OrdinalMapping, keyDesc.Count())
	for i, col := range sch.GetPKCols().GetColumns() {
		b.mapping[i] = tagToOrdinal[col.Tag]
	}
	return
}

type ClusteredKeyBuilder struct {
	mapping val.OrdinalMapping
	builder *val.TupleBuilder
	pool    pool.BuffPool
}

// ClusteredKeyFromIndexKey builds a clustered index key from a secondary index key.
func (b ClusteredKeyBuilder) ClusteredKeyFromIndexKey(k val.Tuple) val.Tuple {
	for to, from := range b.mapping {
		b.builder.PutRaw(to, k.GetField(from))
	}
	return b.builder.Build(b.pool)
}
