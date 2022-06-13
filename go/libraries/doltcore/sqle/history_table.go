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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
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
	CommitHashColType = sql.MustCreateString(sqltypes.Char, 32, sql.Collation_ascii_bin)

	// CommitterColType is the sql type of the committer column
	CommitterColType = sql.MustCreateString(sqltypes.VarChar, 1024, sql.Collation_ascii_bin)
)

var _ sql.Table = (*HistoryTable)(nil)
var _ sql.FilteredTable = (*HistoryTable)(nil)
var _ sql.IndexAddressableTable = (*HistoryTable)(nil)
var _ sql.ParallelizedIndexAddressableTable = (*HistoryTable)(nil)
var _ sql.IndexedTable = (*HistoryTable)(nil)

// HistoryTable is a system table that shows the history of rows over time
type HistoryTable struct {
	doltTable     *DoltTable
	commitFilters []sql.Expression
	cmItr         doltdb.CommitItr
	indexLookup   sql.IndexLookup
}

func (ht *HistoryTable) ShouldParallelizeAccess() bool {
	return true
}

func (ht *HistoryTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	tbl, err := ht.doltTable.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	// For index pushdown to work, we need to represent the indexes from the underlying table as belonging to this one
	// Our results will also not be ordered, so we need to declare them as such
	return index.DoltHistoryIndexesFromTable(ctx, ht.doltTable.db.Name(), ht.Name(), tbl)
}

func (ht HistoryTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	ht.indexLookup = lookup
	return &ht
}

// NewHistoryTable creates a history table
func NewHistoryTable(table *DoltTable, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	cmItr := doltdb.CommitItrForRoots(ddb, head)

	return &HistoryTable{
		doltTable: table,
		cmItr:     cmItr,
	}
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
			Type:   sql.Datetime,
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
func (ht HistoryTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	if ht.commitFilters == nil {
		ht.commitFilters = dtables.FilterFilters(filters, dtables.ColumnPredicate(historyTableCommitMetaCols))
	}

	if len(ht.commitFilters) > 0 {
		commitCheck, err := commitFilterForExprs(ctx, ht.commitFilters)
		if err != nil {
			return sqlutil.NewStaticErrorTable(&ht, err)
		}

		ht.cmItr = doltdb.NewFilteringCommitItr(ht.cmItr, commitCheck)
	}

	return &ht
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

func (ht HistoryTable) WithProjections(colNames []string) sql.Table {
	projectedTable := ht.doltTable.WithProjections(colNames)
	ht.doltTable = projectedTable.(*DoltTable)
	return &ht
}

func (ht *HistoryTable) Projections() []string {
	return ht.doltTable.Projections()
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
	return historyTableSchema(ht.Name(), ht.doltTable)
}

// Partitions returns a PartitionIter which will be used in getting partitions each of which is used to create RowIter.
func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &commitPartitioner{ht.cmItr}, nil
}

// PartitionRows takes a partition and returns a row iterator for that partition
func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*commitPartition)

	return newRowItrForTableAtCommit(ctx, ht.Name(), ht.doltTable, cp.h, cp.cm, ht.indexLookup)
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

func newRowItrForTableAtCommit(ctx *sql.Context, tableName string, table *DoltTable, h hash.Hash, cm *doltdb.Commit, lookup sql.IndexLookup) (*historyIter, error) {
	targetSchema := historyTableSchema(tableName, table)

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

	var sqlTable sql.Table
	sqlTable = table
	if lookup != nil {
		// This revision of the table may not have the index we need (which was determined based on HEAD)
		// Only apply the lookup if the index is there
		hasIndex, err := table.HasIndex(ctx, lookup.Index())
		if err != nil {
			return nil, err
		}

		if hasIndex {
			sqlTable = table.WithIndexLookup(lookup)
		}
	}

	tablePartitions, err := sqlTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	converter := rowConverter(sqlTable.Schema(), targetSchema, h, meta)

	return &historyIter{
		table:           sqlTable,
		tablePartitions: tablePartitions,
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

func rowConverter(srcSchema, targetSchema sql.Schema, h hash.Hash, meta *datas.CommitMeta) func(row sql.Row) sql.Row {
	srcToTarget := make(map[int]int)
	for i, col := range targetSchema[:len(targetSchema)-3] {
		srcIdx := srcSchema.IndexOfColName(col.Name)
		if srcIdx >= 0 {
			// only add a conversion if the type is the same
			// TODO: we could do a projection to convert between types in some cases
			if srcSchema[srcIdx].Type == targetSchema[i].Type {
				srcToTarget[srcIdx] = i
			}
		}
	}

	return func(row sql.Row) sql.Row {
		r := make(sql.Row, len(targetSchema))
		for i := range row {
			if idx, ok := srcToTarget[i]; ok {
				r[idx] = row[i]
			}
		}

		r[len(targetSchema)-3] = h.String()
		r[len(targetSchema)-2] = meta.Name
		r[len(targetSchema)-1] = meta.Time()

		return r
	}
}
