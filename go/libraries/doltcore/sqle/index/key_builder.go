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
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expranalysis"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// NewSecondaryKeyBuilder creates a new SecondaryKeyBuilder instance that can build keys for the secondary index |def|.
// The schema of the source table is defined in |sch|, and |idxDesc| describes the tuple layout for the index's keys
// (index value tuples are not used).
func NewSecondaryKeyBuilder(ctx *sql.Context, tableName string, sch schema.Schema, def schema.Index, idxDesc val.TupleDesc, p pool.BuffPool, nodeStore tree.NodeStore) (SecondaryKeyBuilder, error) {
	b := SecondaryKeyBuilder{
		builder:   val.NewTupleBuilder(idxDesc, nodeStore),
		pool:      p,
		nodeStore: nodeStore,
		sch:       sch,
		indexDef:  def,
	}

	keyless := schema.IsKeyless(sch)
	if keyless {
		// the only key is the hash of the values
		b.split = 1
	} else {
		b.split = sch.GetPKCols().Size()
	}

	b.mapping = make(val.OrdinalMapping, len(def.AllTags()))
	var virtualExpressions []sql.Expression
	for i, tag := range def.AllTags() {
		j, ok := sch.GetPKCols().TagToIdx[tag]
		if !ok {
			col := sch.GetNonPKCols().TagToCol[tag]
			if col.Virtual {
				if len(virtualExpressions) == 0 {
					virtualExpressions = make([]sql.Expression, len(def.AllTags()))
				}

				expr, err := expranalysis.ResolveDefaultExpression(ctx, tableName, sch, col)
				if err != nil {
					return SecondaryKeyBuilder{}, err
				}

				virtualExpressions[i] = expr
				j = -1
			} else if keyless {
				// Skip cardinality column
				j = b.split + 1 + sch.GetNonPKCols().TagToIdx[tag]
			} else {
				j = b.split + sch.GetNonPKCols().TagToIdx[tag]
			}
		}
		b.mapping[i] = j
	}

	b.virtualExpressions = virtualExpressions

	if keyless {
		// last key in index is hash which is the only column in the key
		b.mapping = append(b.mapping, 0)
	}
	return b, nil
}

type SecondaryKeyBuilder struct {
	// sch holds the schema of the table on which the secondary index is created
	sch schema.Schema
	// indexDef holds the definition of the secondary index
	indexDef schema.Index
	// mapping defines how to map fields from the source table's schema to this index's tuple layout
	mapping val.OrdinalMapping
	// virtualExpressions holds the expressions for virtual columns in the index, nil for non-virtual indexes
	virtualExpressions []sql.Expression
	// split marks the index in the secondary index's key tuple that splits the main table's
	// key fields from the main table's value fields.
	split     int
	builder   *val.TupleBuilder
	pool      pool.BuffPool
	nodeStore tree.NodeStore
}

// SecondaryKeyFromRow builds a secondary index key from a clustered index row.
func (b SecondaryKeyBuilder) SecondaryKeyFromRow(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	for to := range b.mapping {
		from := b.mapping.MapOrdinal(to)
		if from == -1 {
			// the "from" field is a virtual column
			expr := b.virtualExpressions[to]
			sqlCtx, ok := ctx.(*sql.Context)
			if !ok {
				sqlCtx = sql.NewContext(ctx)
			}

			sqlRow, err := BuildRow(sqlCtx, k, v, b.sch, b.nodeStore)
			if err != nil {
				return nil, err
			}

			value, err := expr.Eval(sqlCtx, sqlRow)
			if err != nil {
				return nil, err
			}

			// TODO: type conversion
			err = tree.PutField(ctx, b.nodeStore, b.builder, to, value)
			if err != nil {
				return nil, err
			}
		} else if from < b.split {
			// the "from" field comes from the key tuple fields
			// NOTE: Because we are using Tuple.GetField and TupleBuilder.PutRaw, we are not
			//       interpreting the tuple data at all and just copying the bytes. This should work
			//       for primary keys since they are always represented in the secondary index exactly
			//       as they are in the primary index, but for the value tuple, we need to interpret the
			//       data so that we can transform StringAddrEnc fields from pointers to strings (i.e. for
			//       prefix indexes) as well as custom handling for ZCell geometry fields.
			b.builder.PutRaw(to, k.GetField(from))
		} else {
			// the "from" field comes from the value tuple fields
			from -= b.split

			if b.canCopyRawBytes(to) {
				b.builder.PutRaw(to, v.GetField(from))
			} else {
				value, err := tree.GetField(ctx, b.sch.GetValueDescriptor(b.nodeStore), from, v, b.nodeStore)
				if err != nil {
					return nil, err
				}

				if len(b.indexDef.PrefixLengths()) > to {
					value = val.TrimValueToPrefixLength(value, b.indexDef.PrefixLengths()[to])
				}

				err = tree.PutField(ctx, b.nodeStore, b.builder, to, value)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return b.builder.Build(b.pool), nil
}

// BuildRow returns a sql.Row for the given key/value tuple pair
func BuildRow(ctx *sql.Context, key, value val.Tuple, sch schema.Schema, ns tree.NodeStore) (sql.Row, error) {
	prollyIter := prolly.NewPointLookup(key, value)
	rowIter := NewProllyRowIterForSchema(sch, prollyIter, sch.GetKeyDescriptor(ns), sch.GetValueDescriptor(ns), sch.GetAllCols().Tags, ns)
	return rowIter.Next(ctx)
}

// canCopyRawBytes returns true if the bytes for |idxField| can
// be copied directly. This is a faster way to populate an index
// but requires that no data transformation is needed. For example,
// prefix indexes have to manipulate the data to extract a prefix
// before the data can be populated in the index, so if an index
// field is a prefix index, this function will return false.
func (b SecondaryKeyBuilder) canCopyRawBytes(idxField int) bool {
	if b.builder.Desc.Types[idxField].Enc == val.CellEnc {
		return false
	} else if len(b.indexDef.PrefixLengths()) > idxField && b.indexDef.PrefixLengths()[idxField] > 0 {
		return false
	}

	return true
}

func NewClusteredKeyBuilder(def schema.Index, sch schema.Schema, keyDesc val.TupleDesc, p pool.BuffPool, ns tree.NodeStore) (b ClusteredKeyBuilder) {
	b.pool = p
	if schema.IsKeyless(sch) {
		// [16]byte hash key is always final key field
		b.mapping = val.OrdinalMapping{def.Count()}
		b.builder = val.NewTupleBuilder(val.KeylessTupleDesc, ns)
		return
	}

	// secondary indexes contain all clustered key cols, in some order
	tagToOrdinal := make(map[uint64]int, len(def.AllTags()))
	for ord, tag := range def.AllTags() {
		tagToOrdinal[tag] = ord
	}

	b.builder = val.NewTupleBuilder(keyDesc, ns)
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
