// Copyright 2019 Dolthub, Inc.
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

package dtables

import (
	"context"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// DoltHistoryTablePrefix is the name prefix for each history table

	// CommitHashCol is the name of the column containing the commit hash in the result set
	CommitHashCol = "commit_hash"

	// CommitterCol is the name of the column containing the committer in the result set
	CommitterCol = "committer"

	// CommitDateCol is the name of the column containing the commit date in the result set
	CommitDateCol = "commit_date"
)

var _ sql.Table = (*HistoryTable)(nil)
var _ sql.FilteredTable = (*HistoryTable)(nil)

// HistoryTable is a system table that shows the history of rows over time
type HistoryTable struct {
	name                  string
	ddb                   *doltdb.DoltDB
	commitFilters         []sql.Expression
	rowFilters            []sql.Expression
	cmItr                 doltdb.CommitItr
	readerCreateFuncCache *ThreadSafeCRFuncCache
	sqlSch                sql.PrimaryKeySchema
	targetSch             schema.Schema
}

// NewHistoryTable creates a history table
func NewHistoryTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue, head *doltdb.Commit) (sql.Table, error) {
	table, tblName, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(doltdb.DoltHistoryTablePrefix + tblName)
	}

	currentSch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	sch := schema.MustSchemaFromCols(currentSch.GetAllCols().Append(
		schema.NewColumn(CommitHashCol, schema.HistoryCommitHashTag, types.StringKind, false),
		schema.NewColumn(CommitterCol, schema.HistoryCommitterTag, types.StringKind, false),
		schema.NewColumn(CommitDateCol, schema.HistoryCommitDateTag, types.TimestampKind, false),
	))

	if sch.GetAllCols().Size() <= 3 {
		return nil, sql.ErrTableNotFound.New(doltdb.DoltHistoryTablePrefix + tblName)
	}

	sqlSch, err := sqlutil.FromDoltSchema(doltdb.DoltHistoryTablePrefix+tblName, sch)
	if err != nil {
		return nil, err
	}

	cmItr := doltdb.CommitItrForRoots(ddb, head)
	return &HistoryTable{
		name:                  tblName,
		ddb:                   ddb,
		sqlSch:                sqlSch,
		cmItr:                 cmItr,
		readerCreateFuncCache: NewThreadSafeCRFuncCache(),
		targetSch:             sch,
	}, nil
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (ht *HistoryTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	ht.commitFilters, ht.rowFilters = splitCommitFilters(filters)
	return ht.commitFilters
}

// Filters returns the list of filters that are applied to this table.
func (ht *HistoryTable) Filters() []sql.Expression {
	return ht.commitFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (ht *HistoryTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	if ht.commitFilters == nil {
		ht.commitFilters, ht.rowFilters = splitCommitFilters(filters)
	}

	if len(ht.commitFilters) > 0 {
		commitCheck, err := getCommitFilterFunc(ctx, ht.commitFilters)

		if err != nil {
			return sqlutil.NewStaticErrorTable(ht, err)
		}

		ht.cmItr = doltdb.NewFilteringCommitItr(ht.cmItr, commitCheck)
	}

	return ht
}

var commitFilterCols = set.NewStrSet([]string{CommitHashCol, CommitDateCol, CommitterCol})

func getColumnFilterCheck(colNameSet *set.StrSet) func(sql.Expression) bool {
	return func(filter sql.Expression) bool {
		isCommitFilter := true
		sql.Inspect(filter, func(e sql.Expression) (cont bool) {
			if e == nil {
				return true
			}

			switch val := e.(type) {
			case *expression.GetField:
				if !colNameSet.Contains(strings.ToLower(val.Name())) {
					isCommitFilter = false
					return false
				}
			}

			return true
		})

		return isCommitFilter
	}
}

func splitFilters(filters []sql.Expression, filterCheck func(filter sql.Expression) bool) (matching, notMatching []sql.Expression) {
	matching = make([]sql.Expression, 0, len(filters))
	notMatching = make([]sql.Expression, 0, len(filters))
	for _, f := range filters {
		if filterCheck(f) {
			matching = append(matching, f)
		} else {
			notMatching = append(notMatching, f)
		}
	}
	return matching, notMatching
}

func splitCommitFilters(filters []sql.Expression) (commitFilters, rowFilters []sql.Expression) {
	return splitFilters(filters, getColumnFilterCheck(commitFilterCols))
}

func getCommitFilterFunc(ctx *sql.Context, filters []sql.Expression) (doltdb.CommitFilter, error) {
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

func (ht *HistoryTable) WithProjection(colNames []string) sql.Table {
	return ht
}

func (ht *HistoryTable) Projection() []string {
	return []string{}
}

// Name returns the name of the history table
func (ht *HistoryTable) Name() string {
	return doltdb.DoltHistoryTablePrefix + ht.name
}

// String returns the name of the history table
func (ht *HistoryTable) String() string {
	return doltdb.DoltHistoryTablePrefix + ht.name
}

// Schema returns the schema for the history table
func (ht *HistoryTable) Schema() sql.Schema {
	return ht.sqlSch.Schema
}

// Partitions returns a PartitionIter which will be used in getting partitions each of which is used to create RowIter.
func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &commitPartitioner{ht.cmItr}, nil
}

// PartitionRows takes a partition and returns a row iterator for that partition
func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*commitPartition)

	return newRowItrForTableAtCommit(ctx, cp.h, cp.cm, ht.name, ht.targetSch, ht.rowFilters, ht.readerCreateFuncCache)
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

type rowItrForTableAtCommit struct {
	rd           table.TableReadCloser
	sch          schema.Schema
	rowConverter *rowconv.RowConverter
	extraVals    map[uint64]types.Value
	empty        bool
}

func newRowItrForTableAtCommit(
	ctx context.Context,
	h hash.Hash,
	cm *doltdb.Commit,
	tblName string,
	sch schema.Schema,
	filters []sql.Expression,
	readerCreateFuncCache *ThreadSafeCRFuncCache) (*rowItrForTableAtCommit, error) {
	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	tbl, _, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &rowItrForTableAtCommit{empty: true}, nil
	}

	m, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	schHash, err := tbl.GetSchemaHash(ctx)
	if err != nil {
		return nil, err
	}

	rowConverter, err := rowConvForSchema(ctx, tbl.ValueReadWriter(), sch, tblSch)
	if err != nil {
		return nil, err
	}

	// TODO: ThreadSafeCRFuncCache is an older and suboptimal filtering approach that should be replaced
	//       with the unified indexing path that all other tables use. This logic existed before there was a
	//       reasonable way to apply multiple filter conditions to an indexed table scan.
	createReaderFunc, err := readerCreateFuncCache.GetOrCreate(schHash, tbl.Format(), tblSch, filters)
	if err != nil {
		return nil, err
	}

	rd, err := createReaderFunc(ctx, m)
	if err != nil {
		return nil, err
	}

	hashCol, hashOK := sch.GetAllCols().GetByName(CommitHashCol)
	dateCol, dateOK := sch.GetAllCols().GetByName(CommitDateCol)
	committerCol, committerOK := sch.GetAllCols().GetByName(CommitterCol)

	if !hashOK || !dateOK || !committerOK {
		panic("Bug: History table schema should always have commit_hash")
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	return &rowItrForTableAtCommit{
		rd:           rd,
		sch:          sch,
		rowConverter: rowConverter,
		extraVals: map[uint64]types.Value{
			hashCol.Tag:      types.String(h.String()),
			dateCol.Tag:      types.Timestamp(meta.Time()),
			committerCol.Tag: types.String(meta.Name),
		},
		empty: false,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row. After retrieving the last row, Close
// will be automatically closed.
func (tblItr *rowItrForTableAtCommit) Next(ctx *sql.Context) (sql.Row, error) {
	if tblItr.empty {
		return nil, io.EOF
	}

	r, err := tblItr.rd.ReadRow(ctx)

	if err != nil {
		return nil, err
	}

	r, err = tblItr.rowConverter.Convert(r)

	if err != nil {
		return nil, err
	}

	for tag, val := range tblItr.extraVals {
		r, err = r.SetColVal(tag, val, tblItr.sch)

		if err != nil {
			return nil, err
		}
	}

	return sqlutil.DoltRowToSqlRow(r, tblItr.sch)
}

// Close the iterator.
func (tblItr *rowItrForTableAtCommit) Close(ctx *sql.Context) error {
	if tblItr.rd != nil {
		return tblItr.rd.Close(ctx)
	}

	return nil
}

// rowConvForSchema creates a RowConverter for transforming rows with the given schema to the target schema.
func rowConvForSchema(ctx context.Context, vrw types.ValueReadWriter, targetSch schema.Schema, sch schema.Schema) (*rowconv.RowConverter, error) {
	if schema.SchemasAreEqual(sch, schema.EmptySchema) {
		return rowconv.IdentityConverter, nil
	}

	fm, err := rowconv.TagMappingWithNameFallback(sch, targetSch)
	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(ctx, vrw, fm)
}
