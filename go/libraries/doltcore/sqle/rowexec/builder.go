package rowexec

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly"
)

type Builder struct{}

var _ sql.NodeExecBuilder = (*Builder)(nil)

func (b Builder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
	switch n := n.(type) {
	case *plan.JoinNode:
		if n.Op.IsLookup() {
			if ita, ok := getIta(n.Right()); ok {
				if dstMap, _, dstSchema, dstTags, dstFilter, err := getSourceKv(ctx, n.Right(), false); err == nil && dstSchema != nil {
					if srcMap, srcIter, srcSchema, srcTags, srcFilter, err := getSourceKv(ctx, n.Left(), true); err == nil && srcSchema != nil {
						return rowIterTableLookupJoin(ctx, srcIter, srcMap, dstMap, srcSchema, dstSchema, srcTags, dstTags, ita.Expressions(), srcFilter, dstFilter)
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
	case *plan.IndexedTableAccess:
		return n, true
	case *plan.Filter:
		return getIta(n.Child)
	default:
		return nil, false
	}
}

type sqlRowJoiner struct {
	// first |split| are from source
	srcSplit int
	ns       tree.NodeStore

	srcKd val.TupleDesc
	srcVd val.TupleDesc
	tgtKd val.TupleDesc
	tgtVd val.TupleDesc

	ordMappings    []int
	srcKeyMappings []int
	srcValMappings []int
	tgtKeyMappings []int
	tgtValMappings []int
}

func (m *sqlRowJoiner) buildRow(ctx context.Context, srcKey, srcVal, tgtKey, tgtVal val.Tuple) (sql.Row, error) {
	row := make(sql.Row, len(m.ordMappings))
	var err error
	for i, idx := range m.srcKeyMappings {
		outputIdx := m.ordMappings[i]
		row[outputIdx], err = tree.GetField(ctx, m.srcKd, idx, srcKey, m.ns)
		if err != nil {
			return nil, err
		}
	}
	for i, idx := range m.srcValMappings {
		outputIdx := m.ordMappings[len(m.srcKeyMappings)+i]
		row[outputIdx], err = tree.GetField(ctx, m.srcVd, idx, srcVal, m.ns)
		if err != nil {
			return nil, err
		}
	}
	for i, idx := range m.tgtKeyMappings {
		outputIdx := m.ordMappings[m.srcSplit+i]
		row[outputIdx], err = tree.GetField(ctx, m.tgtKd, idx, tgtKey, m.ns)
		if err != nil {
			return nil, err
		}
	}
	for i, idx := range m.tgtValMappings {
		outputIdx := m.ordMappings[m.srcSplit+len(m.tgtKeyMappings)+i]
		row[outputIdx], err = tree.GetField(ctx, m.tgtVd, idx, tgtVal, m.ns)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func getPrimaryLookupRowJoiner(src, tgt schema.Schema, srcSplit int, projections []uint64) *sqlRowJoiner {
	numPhysicalColumns := len(projections)
	if schema.IsVirtual(src) {
		numPhysicalColumns = 0
		for i, t := range projections {
			if idx, ok := src.GetAllCols().TagToIdx[t]; ok && !src.GetAllCols().GetByIndex(idx).Virtual {
				numPhysicalColumns++
				srcSplit = i
			}
			if idx, ok := tgt.GetAllCols().TagToIdx[t]; ok && !tgt.GetAllCols().GetByIndex(idx).Virtual {
				numPhysicalColumns++
			}
		}
	}

	allMap := make([]int, 2*numPhysicalColumns)
	// | srcKey | srcVal | trgKey | trg val | ords |

	keyIdx := 0
	nonKeyIdx := srcSplit - 1
	keyCols := src.GetPKCols()
	valCols := src.GetNonPKCols()
	var firstPkSplit int
	for projNum, tag := range projections {
		if projNum == srcSplit {
			firstPkSplit = keyIdx
			keyIdx = srcSplit
			nonKeyIdx = len(projections) - 1
			keyCols = tgt.GetPKCols()
			valCols = tgt.GetNonPKCols()
		}
		if idx, ok := keyCols.StoredIndexByTag(tag); ok && !keyCols.GetByStoredIndex(idx).Virtual {
			allMap[keyIdx] = idx
			allMap[numPhysicalColumns+keyIdx] = projNum
			keyIdx++
		} else if idx, ok := valCols.StoredIndexByTag(tag); ok && !valCols.GetByStoredIndex(idx).Virtual {
			allMap[nonKeyIdx] = idx
			allMap[numPhysicalColumns+nonKeyIdx] = projNum
			nonKeyIdx--
		}
	}

	return &sqlRowJoiner{
		srcSplit:       srcSplit,
		srcKeyMappings: allMap[:firstPkSplit],
		srcValMappings: allMap[firstPkSplit:srcSplit],
		tgtKeyMappings: allMap[srcSplit:keyIdx],
		tgtValMappings: allMap[keyIdx:numPhysicalColumns],
		ordMappings:    allMap[numPhysicalColumns:],
		srcKd:          src.GetKeyDescriptor(),
		srcVd:          src.GetValueDescriptor(),
		tgtKd:          tgt.GetKeyDescriptor(),
		tgtVd:          tgt.GetValueDescriptor(),
	}
}

func getMap(ctx *sql.Context, dt *sqle.DoltTable) (prolly.Map, schema.Schema, error) {
	table, err := dt.DoltTable(ctx)
	if err != nil {
		return prolly.Map{}, nil, err
	}

	priIndex, err := table.GetRowData(ctx)
	if err != nil {
		return prolly.Map{}, nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return prolly.Map{}, nil, err
	}

	return durable.ProllyMapFromIndex(priIndex), sch, nil
}

func getSourceKv(ctx *sql.Context, n sql.Node, iter bool) (prolly.Map, prolly.MapIter, schema.Schema, []uint64, sql.Expression, error) {
	var table *doltdb.Table
	var tags []uint64
	var err error
	var indexMap prolly.Map
	var mapIter prolly.MapIter
	switch n := n.(type) {
	case *plan.TableAlias:
		return getSourceKv(ctx, n.Child, iter)
	case *plan.Filter:
		m, mIter, s, t, _, err := getSourceKv(ctx, n.Child, iter)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, err
		}
		return m, mIter, s, t, n.Expression, nil
	case *plan.IndexedTableAccess:
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableIndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, err
			}

			var rowData durable.Index
			switch strings.ToLower(dt.Index().ID()) {
			case "primary":
				rowData, err = table.GetRowData(ctx)
			default:
				rowData, err = table.GetIndexRowData(ctx, dt.Index().ID())
			}
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, err
			}
			indexMap = durable.ProllyMapFromIndex(rowData)
			if iter {
				// LOOKUP_BUILDER was intended for join-left, but we can abuse it for
				l, err := n.GetLookup(ctx, nil)
				if err != nil {
					return prolly.Map{}, nil, nil, nil, nil, err
				}
				prollyRanges, err := index.ProllyRangesForIndex(ctx, l.Index, l.Ranges)
				if err != nil {
					return prolly.Map{}, nil, nil, nil, nil, err
				}

				lb, err := dt.LookupBuilder(ctx)
				if err != nil {
					return prolly.Map{}, nil, nil, nil, nil, err
				}

				mapIter, err = index.NewSequenceMapIter(ctx, lb, prollyRanges, l.IsReverse)
				if err != nil {
					return prolly.Map{}, nil, nil, nil, nil, err
				}
			}

		case *sqle.IndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, err
			}
			rowData, err := table.GetIndexRowData(ctx, dt.Index().ID())
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, err
			}
			indexMap = durable.ProllyMapFromIndex(rowData)
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
			return prolly.Map{}, nil, nil, nil, nil, nil
		}
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, err
		}
		priIndex, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, err
		}
		indexMap = durable.ProllyMapFromIndex(priIndex)

		if iter {
			mapIter, err = indexMap.IterAll(ctx)
			if err != nil {
				return prolly.Map{}, nil, nil, nil, nil, err
			}
		}
	default:
		return prolly.Map{}, nil, nil, nil, nil, nil
	}
	if err != nil {
		return prolly.Map{}, nil, nil, nil, nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return prolly.Map{}, nil, nil, nil, nil, err
	}

	return indexMap, mapIter, sch, tags, nil, nil
}
