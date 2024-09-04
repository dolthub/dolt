// Copyright 2024 Dolthub, Inc.
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

package kvexec

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type Builder struct{}

var _ sql.NodeExecBuilder = (*Builder)(nil)

func (b Builder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	switch n := n.(type) {
	case *plan.JoinNode:
		if n.Op.IsLookup() && !n.Op.IsPartial() {
			if ita, ok := getIta(n.Right()); ok && len(r) == 0 && simpleLookupExpressions(ita.Expressions()) {
				if _, _, dstIter, _, dstTags, dstFilter, err := getSourceKv(ctx, n.Right(), false); err == nil && dstIter != nil {
					if srcMap, srcIter, _, srcSchema, srcTags, srcFilter, err := getSourceKv(ctx, n.Left(), true); err == nil && srcSchema != nil {
						if keyLookupMapper := newLookupKeyMapping(ctx, srcSchema, dstIter.InputKeyDesc(), ita.Expressions(), srcMap.NodeStore()); keyLookupMapper.valid() {
							// conditions:
							// (1) lookup or left lookup join
							// (2) left-side is something we read KVs from (table or indexscan, ex: no subqueries)
							// (3) right-side is an index lookup, by definition
							// (4) the key expressions for the lookup are literals or columns (ex: no arithmetic yet)
							split := len(srcTags)
							projections := append(srcTags, dstTags...)
							rowJoiner := newRowJoiner([]schema.Schema{srcSchema, dstIter.Schema()}, []int{split}, projections, dstIter.NodeStore())
							return rowIterTableLookupJoin(srcIter, dstIter, keyLookupMapper, rowJoiner, srcFilter, dstFilter, n.Filter, n.Op.IsLeftOuter(), n.Op.IsExcludeNulls())
						}
					}
				}
			}
		}
	case *plan.GroupBy:
		if len(n.GroupByExprs) == 0 && len(n.SelectedExprs) == 1 {
			if cnt, ok := n.SelectedExprs[0].(*aggregation.Count); ok {
				if _, srcIter, _, srcSchema, _, srcFilter, err := getSourceKv(ctx, n.Child, true); err == nil && srcSchema != nil && srcFilter == nil {
					iter, ok, err := newCountAggregationKvIter(srcIter, srcSchema, cnt.Child)
					if ok && err == nil {
						// (1) no grouping expressions (returns one row)
						// (2) only one COUNT expression with a literal or field reference
						// (3) table or ita as child (no filters)
						return iter, nil
					}
				}
			}
		}
	default:
	}
	return nil, nil
}

func getIta(n sql.Node) (*plan.IndexedTableAccess, bool) {
	switch n := n.(type) {
	case *plan.TableAlias:
		return getIta(n.Child)
	case *plan.Filter:
		return getIta(n.Child)
	case *plan.IndexedTableAccess:
		return n, true
	default:
		return nil, false
	}
}

// simpleLookupExpressions returns true if |keyExprs| includes only field
// references and literals
func simpleLookupExpressions(keyExprs []sql.Expression) bool {
	for _, e := range keyExprs {
		switch e.(type) {
		case *expression.Literal, *expression.GetField:
		default:
			return false
		}
	}
	return true
}

// prollyToSqlJoiner converts a list of KV pairs into a sql.Row
type prollyToSqlJoiner struct {
	ns tree.NodeStore
	// kvSplits are offsets between consecutive kv pairs
	kvSplits    []int
	desc        []kvDesc
	ordMappings []int
}

type kvDesc struct {
	keyDesc     val.TupleDesc
	valDesc     val.TupleDesc
	keyMappings []int
	valMappings []int
}

func newRowJoiner(schemas []schema.Schema, splits []int, projections []uint64, ns tree.NodeStore) *prollyToSqlJoiner {
	numPhysicalColumns := getPhysicalColCount(schemas, splits, projections)

	// last kv pair can safely look ahead for its end range
	splits = append(splits, len(projections))

	// | k1 | v1 | k2 | v2 | ... | ords |
	// refer to more detailed comment below
	// todo: is it worth refactoring from a two-phase to one-phase mapping?
	allMap := make([]int, 2*numPhysicalColumns)
	var tupleDesc []kvDesc

	nextKeyIdx := 0
	nextValIdx := splits[0] - 1
	sch := schemas[0]
	keylessOff := 0
	if schema.IsKeyless(sch) {
		keylessOff = 1
	}
	keyCols := sch.GetPKCols()
	valCols := sch.GetNonPKCols()
	splitIdx := 0
	for i := 0; i <= len(projections); i++ {
		// We will fill the map from table sources incrementally. Each source will have
		// a keyMapping, valueMapping, and ordinal mappings related to converting from
		// storage order->schema order->projection order. allMap is a shared underlying
		// storage for all of these mappings. Split indexes refers to a K/V segmentation
		// of columns from a table. We increment the key mapping positions and decrement
		// the value mapping positions, so the split index will be where the key and value
		// indexes converge after processing a table source's fields.
		if i == splits[splitIdx] {
			var mappingStartIdx int
			if splitIdx > 0 {
				mappingStartIdx = splits[splitIdx-1]
			}
			tupleDesc = append(tupleDesc, kvDesc{
				keyDesc:     sch.GetKeyDescriptor(),
				valDesc:     sch.GetValueDescriptor(),
				keyMappings: allMap[mappingStartIdx:nextKeyIdx],  // prev kv partition -> last key of this partition
				valMappings: allMap[nextKeyIdx:splits[splitIdx]], // first val of partition -> next kv partition
			})
			if i == len(projections) {
				break
			}
			nextKeyIdx = splits[splitIdx]
			splitIdx++
			nextValIdx = splits[splitIdx] - 1
			sch = schemas[splitIdx]

			keylessOff = 0
			if schema.IsKeyless(sch) {
				keylessOff = 1
			}
			keyCols = sch.GetPKCols()
			valCols = sch.GetNonPKCols()
		}
		tag := projections[i]
		if idx, ok := keyCols.StoredIndexByTag(tag); ok && !keyCols.GetByStoredIndex(idx).Virtual {
			allMap[nextKeyIdx] = idx
			allMap[numPhysicalColumns+nextKeyIdx] = i
			nextKeyIdx++
		} else if idx, ok := valCols.StoredIndexByTag(tag); ok && !valCols.GetByStoredIndex(idx).Virtual {
			allMap[nextValIdx] = idx + keylessOff
			allMap[numPhysicalColumns+nextValIdx] = i
			nextValIdx--
		}
	}

	return &prollyToSqlJoiner{
		kvSplits:    splits,
		desc:        tupleDesc,
		ordMappings: allMap[numPhysicalColumns:],
		ns:          ns,
	}
}

func (m *prollyToSqlJoiner) buildRow(ctx context.Context, tuples ...val.Tuple) (sql.Row, error) {
	if len(tuples) != 2*len(m.desc) {
		panic("invalid KV count for prollyToSqlJoiner")
	}
	row := make(sql.Row, len(m.ordMappings))
	split := 0
	var err error
	var tup val.Tuple
	for i, desc := range m.desc {
		tup = tuples[2*i]
		if tup == nil {
			// nullified row
			split = m.kvSplits[i]
			continue
		}
		if i > 0 {
			split = m.kvSplits[i-1]
		}
		for j, idx := range desc.keyMappings {
			outputIdx := m.ordMappings[split+j]
			row[outputIdx], err = tree.GetField(ctx, desc.keyDesc, idx, tup, m.ns)
			if err != nil {
				return nil, err
			}
		}
		tup = tuples[2*i+1]
		for j, idx := range desc.valMappings {
			outputIdx := m.ordMappings[split+len(desc.keyMappings)+j]
			row[outputIdx], err = tree.GetField(ctx, desc.valDesc, idx, tup, m.ns)
			if err != nil {
				return nil, err
			}
		}
	}
	return row, nil
}

func getPhysicalColCount(schemas []schema.Schema, splits []int, projections []uint64) int {
	var virtual bool
	for _, sch := range schemas {
		if schema.IsVirtual(sch) {
			virtual = true
		}
	}

	if !virtual {
		return len(projections)
	}

	numPhysicalColumns := 0
	sch := schemas[0]
	splitIdx := 0
	for i := 0; i < len(projections); i++ {
		if i == splits[splitIdx] {
			splitIdx++
			sch = schemas[splitIdx]
		}
		tag := projections[i]
		if idx, ok := sch.GetAllCols().TagToIdx[tag]; ok && !sch.GetAllCols().GetByIndex(idx).Virtual {
			numPhysicalColumns++
		}
	}
	return numPhysicalColumns
}

// getSourceKv extracts prolly table and index specific structures needed
// to implement a lookup join. We return either |srcIter| or |dstIter|
// depending on whether |isSrc| is true.
func getSourceKv(ctx *sql.Context, n sql.Node, isSrc bool) (prolly.Map, prolly.MapIter, index.SecondaryLookupIterGen, schema.Schema, []uint64, sql.Expression, error) {
	var table *doltdb.Table
	var tags []uint64
	var err error
	var indexMap prolly.Map
	var srcIter prolly.MapIter
	var dstIter index.SecondaryLookupIterGen
	var sch schema.Schema
	switch n := n.(type) {
	case *plan.TableAlias:
		return getSourceKv(ctx, n.Child, isSrc)
	case *plan.Filter:
		m, mIter, destIter, s, t, _, err := getSourceKv(ctx, n.Child, isSrc)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}
		return m, mIter, destIter, s, t, n.Expression, nil
	case *plan.IndexedTableAccess:
		var lb index.IndexScanBuilder
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableIndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}
			lb, err = dt.LookupBuilder(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}
		case *sqle.IndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}
			lb, err = dt.LookupBuilder(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}
		//case *dtables.DiffTable:
		// TODO: add interface to include system tables
		default:
			return prolly.Map{}, nil, nil, nil, nil, nil, nil
		}

		rowData, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}
		indexMap = durable.ProllyMapFromIndex(rowData)

		sch = lb.OutputSchema()

		if isSrc {
			l, err := n.GetLookup(ctx, nil)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}

			prollyRanges, err := index.ProllyRangesForIndex(ctx, l.Index, l.Ranges)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}

			srcIter, err = index.NewSequenceRangeIter(ctx, lb, prollyRanges, l.IsReverse)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, nil, err
			}
		} else {
			dstIter = lb.NewSecondaryIter(n.IsStrictLookup(), len(n.Expressions()), n.NullMask())
		}

	case *plan.ResolvedTable:
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
		case *sqle.AlterableDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
		case *sqle.DoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable(ctx)
		default:
			return prolly.Map{}, nil, nil, nil, nil, nil, nil
		}
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}

		sch, err = table.GetSchema(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}

		priIndex, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}
		indexMap = durable.ProllyMapFromIndex(priIndex)

		srcIter, err = indexMap.IterAll(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}

		if schema.IsKeyless(sch) {
			srcIter = index.NewKeylessCardedMapIter(srcIter)
		}

	default:
		return prolly.Map{}, nil, nil, nil, nil, nil, nil
	}
	if err != nil {
		return prolly.Map{}, nil, nil, nil, nil, nil, err
	}

	if sch == nil && table != nil {
		sch, err = table.GetSchema(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, err
		}
	}

	return indexMap, srcIter, dstIter, sch, tags, nil, nil
}
