// Copyright 2022 Dolthub, Inc.
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

package sqle

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	// CommitHashCol is the name of the column containing the commit hash in the result set
	CommitHashCol = "commit_hash"

	// CommitterCol is the name of the column containing the committer in the result set
	CommitterCol = "committer"

	// CommitDateCol is the name of the column containing the commit date in the result set
	CommitDateCol = "commit_date"
)

var (
	// CommitHashColType is the sql type of the commit hash column
	CommitHashColType = types.MustCreateString(sqltypes.Char, 32, sql.Collation_ascii_bin)

	// CommitterColType is the sql type of the committer column
	CommitterColType = types.MustCreateString(sqltypes.VarChar, 1024, sql.Collation_ascii_bin)
)

var _ sql.Table = (*HistoryTable)(nil)
var _ sql.FilteredTable = (*HistoryTable)(nil)
var _ sql.IndexAddressableTable = (*HistoryTable)(nil)
var _ sql.IndexedTable = (*HistoryTable)(nil)

// HistoryTable is a system table that shows the history of rows over time
type HistoryTable struct {
	doltTable     *DoltTable
	commitFilters []sql.Expression
	cmItr         doltdb.CommitItr
	commitCheck   doltdb.CommitFilter
	indexLookup   sql.IndexLookup
	projectedCols []uint64
}

func (ht *HistoryTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	tbl, err := ht.doltTable.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	// For index pushdown to work, we need to represent the indexes from the underlying table as belonging to this one
	// Our results will also not be ordered, so we need to declare them as such
	return index.DoltHistoryIndexesFromTable(ctx, ht.doltTable.db.Name(), ht.Name(), tbl, ht.doltTable.db.DbData().Ddb)
}

func (ht *HistoryTable) IndexedAccess(_ sql.IndexLookup) sql.IndexedTable {
	ret := *ht
	return &ret
}

func (ht *HistoryTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit hash lookup: %s", sql.DebugString(lookup.Ranges))
		}

		var hashes []hash.Hash
		var commits []*doltdb.Commit
		var metas []*datas.CommitMeta
		for _, hs := range hs {
			if hs == doltdb.Working {

			}
			h, ok := hash.MaybeParse(hs)
			if !ok {
				continue
			}
			hashes = append(hashes, h)

			cm, err := doltdb.HashToCommit(ctx, ht.doltTable.db.DbData().Ddb.ValueReadWriter(), ht.doltTable.db.DbData().Ddb.NodeStore(), h)
			if err != nil {
				return nil, err
			}
			commits = append(commits, cm)

			meta, err := cm.GetCommitMeta(ctx)
			if err != nil {
				return nil, err
			}
			metas = append(metas, meta)
		}
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}

		iter, err := ht.filterIter(ctx, doltdb.NewCommitSliceIter(commits, hashes))
		if err != nil {
			return nil, err
		}
		return &commitPartitioner{cmItr: iter}, nil

	}
	ht.indexLookup = lookup
	return ht.Partitions(ctx)
}

// NewHistoryTable creates a history table
func NewHistoryTable(table *DoltTable, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	cmItr := doltdb.CommitItrForRoots(ddb, head)

	h := &HistoryTable{
		doltTable: table,
		cmItr:     cmItr,
	}
	return h
}

// History table schema returns the corresponding history table schema for the base table given, which consists of
// the table's schema with 3 additional columns
func historyTableSchema(tableName string, table *DoltTable) sql.Schema {
	baseSch := table.Schema().Copy()
	newSch := make(sql.Schema, len(baseSch), len(baseSch)+3)

	for i, col := range baseSch {
		// Returning a schema from a single table with multiple table names can confuse parts of the analyzer
		col.Source = tableName
		newSch[i] = col
	}

	newSch = append(newSch,
		&sql.Column{
			Name:   CommitHashCol,
			Source: tableName,
			Type:   CommitHashColType,
		},
		&sql.Column{
			Name:   CommitterCol,
			Source: tableName,
			Type:   CommitterColType,
		},
		&sql.Column{
			Name:   CommitDateCol,
			Source: tableName,
			Type:   types.Datetime,
		},
	)
	return newSch
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (ht *HistoryTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	ht.commitFilters = dtables.FilterFilters(filters, dtables.ColumnPredicate(historyTableCommitMetaCols))
	return ht.commitFilters
}

// Filters returns the list of filters that are applied to this table.
func (ht *HistoryTable) Filters() []sql.Expression {
	return ht.commitFilters
}

// WithFilters returns a new sql.Table instance with the filters applied. We handle filters on any commit columns.
func (ht *HistoryTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	ret := *ht
	ret.commitFilters = dtables.FilterFilters(filters, dtables.ColumnPredicate(historyTableCommitMetaCols))
	return &ret
}

func (ht *HistoryTable) filterIter(ctx *sql.Context, iter doltdb.CommitItr) (doltdb.CommitItr, error) {
	if len(ht.commitFilters) > 0 {
		r, err := ht.doltTable.db.GetRoot(ctx)
		if err != nil {
			return doltdb.FilteringCommitItr{}, err
		}
		h, err := r.HashOf()
		if err != nil {
			return doltdb.FilteringCommitItr{}, err
		}
		filters := substituteWorkingHash(h, ht.commitFilters)
		check, err := commitFilterForExprs(ctx, filters)
		if err != nil {
			return doltdb.FilteringCommitItr{}, err
		}

		return doltdb.NewFilteringCommitItr(iter, check), nil
	}
	return iter, nil
}

func substituteWorkingHash(h hash.Hash, f []sql.Expression) []sql.Expression {
	ret := make([]sql.Expression, len(f))
	for i, e := range f {
		ret[i], _, _ = transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.Literal:
				if e.Value() == doltdb.Working {
					return expression.NewLiteral(h.String(), e.Type()), transform.NewTree, nil
				}
			default:
			}
			return e, transform.SameTree, nil
		})
	}
	return ret
}

var historyTableCommitMetaCols = set.NewStrSet([]string{CommitHashCol, CommitDateCol, CommitterCol})

func commitFilterForExprs(ctx *sql.Context, filters []sql.Expression) (doltdb.CommitFilter, error) {
	filters = transformFilters(ctx, filters...)

	return func(ctx context.Context, h hash.Hash, cm *doltdb.Commit) (filterOut bool, err error) {
		meta, err := cm.GetCommitMeta(ctx)

		if err != nil {
			return false, err
		}

		sc := sql.NewContext(ctx)
		r := sql.Row{h.String(), meta.Name, meta.Time()}

		for _, filter := range filters {
			res, err := filter.Eval(sc, r)
			if err != nil {
				return false, err
			}
			b, ok := res.(bool)
			if ok && !b {
				return true, nil
			}
		}

		return false, err
	}, nil
}

func transformFilters(ctx *sql.Context, filters ...sql.Expression) []sql.Expression {
	for i := range filters {
		filters[i], _, _ = transform.Expr(filters[i], func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			gf, ok := e.(*expression.GetField)
			if !ok {
				return e, transform.SameTree, nil
			}
			switch gf.Name() {
			case CommitHashCol:
				return gf.WithIndex(0), transform.NewTree, nil
			case CommitterCol:
				return gf.WithIndex(1), transform.NewTree, nil
			case CommitDateCol:
				return gf.WithIndex(2), transform.NewTree, nil
			default:
				return gf, transform.SameTree, nil
			}
		})
	}
	return filters
}

func (ht *HistoryTable) WithProjections(colNames []string) sql.Table {
	nt := *ht
	nt.projectedCols = make([]uint64, len(colNames))
	nonHistoryCols := make([]string, 0)
	cols := ht.doltTable.sch.GetAllCols()
	for i := range colNames {
		col, ok := cols.LowerNameToCol[strings.ToLower(colNames[i])]
		if !ok {
			switch colNames[i] {
			case CommitHashCol:
				nt.projectedCols[i] = schema.HistoryCommitHashTag
			case CommitterCol:
				nt.projectedCols[i] = schema.HistoryCommitterTag
			case CommitDateCol:
				nt.projectedCols[i] = schema.HistoryCommitDateTag
			default:
			}
		} else {
			nt.projectedCols[i] = col.Tag
			nonHistoryCols = append(nonHistoryCols, col.Name)
		}
	}
	projectedTable := ht.doltTable.WithProjections(nonHistoryCols)
	nt.doltTable = projectedTable.(*DoltTable)
	return &nt
}

func (ht *HistoryTable) Projections() []string {
	// The semantics of nil v. zero length is important when displaying explain plans
	if ht.projectedCols == nil {
		return nil
	}

	names := make([]string, len(ht.projectedCols))
	cols := ht.doltTable.sch.GetAllCols()
	for i := range ht.projectedCols {
		if col, ok := cols.TagToCol[ht.projectedCols[i]]; ok {
			names[i] = col.Name

		} else {
			switch ht.projectedCols[i] {
			case schema.HistoryCommitHashTag:
				names[i] = CommitHashCol
			case schema.HistoryCommitterTag:
				names[i] = CommitterCol
			case schema.HistoryCommitDateTag:
				names[i] = CommitDateCol
			default:
			}
		}
	}
	return names
}

func (ht *HistoryTable) ProjectedTags() []uint64 {
	if ht.projectedCols != nil {
		return ht.projectedCols
	}
	// Otherwise (no projection), return the tags for the underlying table with the extra meta tags appended
	return append(ht.doltTable.ProjectedTags(), schema.HistoryCommitHashTag, schema.HistoryCommitterTag, schema.HistoryCommitDateTag)
}

// Name returns the name of the history table
func (ht *HistoryTable) Name() string {
	return doltdb.DoltHistoryTablePrefix + ht.doltTable.Name()
}

// String returns the name of the history table
func (ht *HistoryTable) String() string {
	return doltdb.DoltHistoryTablePrefix + ht.doltTable.Name()
}

// Schema returns the schema for the history table
func (ht *HistoryTable) Schema() sql.Schema {
	sch := historyTableSchema(ht.Name(), ht.doltTable)
	if ht.projectedCols == nil {
		return sch
	}

	projectedSch := make(sql.Schema, len(ht.projectedCols))
	allCols := ht.doltTable.sch.GetAllCols()
	for i, t := range ht.projectedCols {
		if col, ok := allCols.TagToCol[t]; ok {
			idx := sch.IndexOfColName(col.Name)
			projectedSch[i] = sch[idx]
		} else if t == schema.HistoryCommitterTag {
			projectedSch[i] = &sql.Column{
				Name:   CommitterCol,
				Source: ht.Name(),
				Type:   CommitterColType,
			}
		} else if t == schema.HistoryCommitHashTag {
			projectedSch[i] = &sql.Column{
				Name:   CommitHashCol,
				Source: ht.Name(),
				Type:   CommitHashColType,
			}
		} else if t == schema.HistoryCommitDateTag {
			projectedSch[i] = &sql.Column{
				Name:   CommitDateCol,
				Source: ht.Name(),
				Type:   types.Datetime,
			}
		} else {
			panic("column not found")
		}
	}
	return projectedSch
}

// Collation implements the sql.Table interface.
func (ht *HistoryTable) Collation() sql.CollationID {
	return sql.CollationID(ht.doltTable.sch.GetCollation())
}

// Partitions returns a PartitionIter which will be used in getting partitions each of which is used to create RowIter.
func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	iter, err := ht.filterIter(ctx, ht.cmItr)
	if err != nil {
		return nil, err
	}
	return &commitPartitioner{cmItr: iter}, nil
}

// PartitionRows takes a partition and returns a row iterator for that partition
func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*commitPartition)
	return newRowItrForTableAtCommit(ctx, ht.doltTable, cp.h, cp.cm, ht.indexLookup, ht.ProjectedTags())
}

// commitPartition is a single commit
type commitPartition struct {
	h  hash.Hash
	cm *doltdb.Commit
}

// Key returns the hash of the commit for this partition which is used as the partition key
func (cp *commitPartition) Key() []byte {
	return cp.h[:]
}

// commitPartitioner creates partitions from a CommitItr
type commitPartitioner struct {
	cmItr doltdb.CommitItr
}

// Next returns the next partition and nil, io.EOF when complete
func (cp commitPartitioner) Next(ctx *sql.Context) (sql.Partition, error) {
	h, cm, err := cp.cmItr.Next(ctx)

	if err != nil {
		return nil, err
	}

	return &commitPartition{h, cm}, nil
}

// Close closes the partitioner
func (cp commitPartitioner) Close(*sql.Context) error {
	return nil
}

type historyIter struct {
	table            sql.Table
	tablePartitions  sql.PartitionIter
	currPart         sql.RowIter
	rowConverter     func(row sql.Row) sql.Row
	nonExistentTable bool
}

func newRowItrForTableAtCommit(ctx *sql.Context, table *DoltTable, h hash.Hash, cm *doltdb.Commit, lookup sql.IndexLookup, projections []uint64) (*historyIter, error) {
	targetSchema := table.Schema()

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	_, _, ok, err := root.GetTableInsensitive(ctx, table.Name())
	if err != nil {
		return nil, err
	}
	if !ok {
		return &historyIter{nonExistentTable: true}, nil
	}

	table, err = table.LockedToRoot(ctx, root)
	if err != nil {
		return nil, err
	}

	var partIter sql.PartitionIter
	var histTable sql.Table
	if !lookup.IsEmpty() {
		indexes, err := table.GetIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, idx := range indexes {
			if idx.ID() == lookup.Index.ID() {
				histTable = table.IndexedAccess(lookup)
				if histTable != nil {
					newLookup := sql.IndexLookup{Index: idx, Ranges: lookup.Ranges}
					partIter, err = histTable.(sql.IndexedTable).LookupPartitions(ctx, newLookup)
					if err != nil {
						return nil, err
					}
					break
				}
			}
		}
	}
	if histTable == nil {
		histTable = table
		partIter, err = table.Partitions(ctx)
		if err != nil {
			return nil, err
		}
	}

	converter := rowConverter(table.Schema(), targetSchema, h, meta, projections)
	return &historyIter{
		table:           histTable,
		tablePartitions: partIter,
		rowConverter:    converter,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row. After retrieving the last row, Close
// will be automatically closed.
func (i *historyIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.nonExistentTable {
		return nil, io.EOF
	}

	if i.currPart == nil {
		nextPart, err := i.tablePartitions.Next(ctx)
		if err != nil {
			return nil, err
		}

		rowIter, err := i.table.PartitionRows(ctx, nextPart)
		if err != nil {
			return nil, err
		}

		i.currPart = rowIter
		return i.Next(ctx)
	}

	r, err := i.currPart.Next(ctx)
	if err == io.EOF {
		i.currPart = nil
		return i.Next(ctx)
	} else if err != nil {
		return nil, err
	}

	return i.rowConverter(r), nil
}

func (i *historyIter) Close(ctx *sql.Context) error {
	return nil
}

func rowConverter(srcSchema, targetSchema sql.Schema, h hash.Hash, meta *datas.CommitMeta, projections []uint64) func(row sql.Row) sql.Row {
	srcToTarget := make(map[int]int)
	for i, col := range targetSchema {
		srcIdx := srcSchema.IndexOfColName(col.Name)
		if srcIdx >= 0 {
			// only add a conversion if the type is the same
			// TODO: we could do a projection to convert between types in some cases
			if srcSchema[srcIdx].Type.Equals(targetSchema[i].Type) {
				srcToTarget[srcIdx] = i
			}
		}
	}

	return func(row sql.Row) sql.Row {
		r := make(sql.Row, len(projections))
		for i, t := range projections {
			switch t {
			case schema.HistoryCommitterTag:
				r[i] = meta.Name
			case schema.HistoryCommitDateTag:
				r[i] = meta.Time()
			case schema.HistoryCommitHashTag:
				r[i] = h.String()
			default:
				if j, ok := srcToTarget[i]; ok {
					r[j] = row[i]
				}
			}
		}
		return r
	}
}
