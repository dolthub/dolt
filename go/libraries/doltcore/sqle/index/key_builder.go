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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
)

// NewSecondaryKeyBuilder creates a new SecondaryKeyBuilder instance that can build keys for the secondary index |def|.
// The schema of the source table is defined in |sch|, and |idxDesc| describes the tuple layout for the index's keys
// (index value tuples are not used).
func NewSecondaryKeyBuilder(ctx context.Context, tableName string, sch schema.Schema, def schema.Index, idxDesc val.TupleDesc, p pool.BuffPool, nodeStore tree.NodeStore) (SecondaryKeyBuilder, error) {
	b := SecondaryKeyBuilder{
		builder:   val.NewTupleBuilder(idxDesc),
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
				sqlCtx, ok := ctx.(*sql.Context)
				if !ok {
					sqlCtx = sql.NewContext(ctx)
				}

				expr, err := resolveDefaultExpression(sqlCtx, col, sch, tableName)
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

// resolveDefaultExpression returns an sql.Expression for the column default or generated expression for the 
// column provided
func resolveDefaultExpression(ctx *sql.Context, col schema.Column, sch schema.Schema, tableName string) (sql.Expression, error) {
	createTable, err := sqlfmt.GenerateCreateTableStatement(tableName, sch, nil, nil)
	if err != nil {
		return nil, err
	}
	
	query := createTable
	sqlSch, err := sqlutil.FromDoltSchema("", tableName, sch)
	if err != nil {
		return nil, err
	}
	
	mockDatabase := memory.NewDatabase("mydb")
	mockTable := memory.NewLocalTable(mockDatabase.BaseDatabase, tableName, sqlSch, nil)
	mockDatabase.AddTable(tableName, mockTable)
	mockProvider := memory.NewDBProvider(mockDatabase)
	catalog := analyzer.NewCatalog(mockProvider)

	pseudoAnalyzedQuery, err := planbuilder.Parse(ctx, catalog, query)
	if err != nil {
		return nil, err
	}

	ct, ok := pseudoAnalyzedQuery.(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("expected a *plan.CreateTable node, but got %T", pseudoAnalyzedQuery)
	}

	colIdx := ct.CreateSchema.Schema.IndexOfColName(col.Name)
	if colIdx == -1 {
		return nil, fmt.Errorf("unable to find column %s in analyzed query", col.Name)
	}
	
	sqlCol := ct.CreateSchema.Schema[colIdx]
	expr := sqlCol.Default
	if expr == nil || expr.Expr == nil {
		expr = sqlCol.Generated
	}
	
	if expr == nil || expr.Expr == nil {
		return nil, fmt.Errorf("unable to find default or generated expression")
	}
	
	return expr.Expr, nil
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

			sqlRow, err := buildRow(sqlCtx, k, v, b.sch, b.nodeStore)
			if err != nil {
				return nil, err
			}
			
			value, err := expr.Eval(sqlCtx, sqlRow)
			if err != nil {
				return nil, err
			}
			
			// TODO: type conversion
			err = PutField(ctx, b.nodeStore, b.builder, to, value)
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
				value, err := GetField(ctx, b.sch.GetValueDescriptor(), from, v, b.nodeStore)
				if err != nil {
					return nil, err
				}

				if len(b.indexDef.PrefixLengths()) > to {
					value = val.TrimValueToPrefixLength(value, b.indexDef.PrefixLengths()[to])
				}

				err = PutField(ctx, b.nodeStore, b.builder, to, value)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return b.builder.Build(b.pool), nil
}

// TODO: dedupe
func buildRow(ctx *sql.Context, key, value val.Tuple, sch schema.Schema, ns tree.NodeStore) (sql.Row, error) {
	pkCols := sch.GetPKCols()
	valueCols := sch.GetNonPKCols()
	allCols := sch.GetAllCols()

	row := make(sql.Row, allCols.Size())

	// Skip adding the key tuple if we're working with a keyless table, since the table row data is
	// always all contained in the value tuple for keyless tables.
	if !schema.IsKeyless(sch) {
		keyDesc := sch.GetKeyDescriptor()
		for i := range keyDesc.Types {
			value, err := GetField(ctx, keyDesc, i, key, ns)
			if err != nil {
				return nil, err
			}

			pkCol := pkCols.GetColumns()[i]
			row[allCols.TagToIdx[pkCol.Tag]+1] = value
		}
	}

	valueColIndex := 0
	valueDescriptor := sch.GetValueDescriptor()
	for valueTupleIndex := range valueDescriptor.Types {
		// Skip processing the first value in the value tuple for keyless tables, since that field
		// always holds the cardinality of the row and shouldn't be passed in to an expression.
		if schema.IsKeyless(sch) && valueTupleIndex == 0 {
			continue
		}

		value, err := GetField(ctx, valueDescriptor, valueTupleIndex, value, ns)
		if err != nil {
			return nil, err
		}

		col := valueCols.GetColumns()[valueColIndex]
		row[allCols.TagToIdx[col.Tag]] = value
		valueColIndex += 1
	}

	return row, nil
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

func NewClusteredKeyBuilder(def schema.Index, sch schema.Schema, keyDesc val.TupleDesc, p pool.BuffPool) (b ClusteredKeyBuilder) {
	b.pool = p
	if schema.IsKeyless(sch) {
		// [16]byte hash key is always final key field
		b.mapping = val.OrdinalMapping{def.Count()}
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
