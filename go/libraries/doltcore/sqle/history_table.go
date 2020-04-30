// Copyright 2019 Liquidata, Inc.
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
	"strings"
	"time"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	// DoltHistoryTablePrefix is the name prefix for each history table
	DoltHistoryTablePrefix = "dolt_history_"

	// CommitHashCol is the name of the column containing the commit hash in the result set
	CommitHashCol = "commit_hash"

	// CommitterCol is the name of the column containing the committer in the result set
	CommitterCol = "committer"

	// CommitDateCol is the name of the column containing the commit date in the result set
	CommitDateCol = "commit_date"
)

var _ sql.Table = &HistoryTable{}

// HistoryTable is a system table that shows the history of rows over time
type HistoryTable struct {
	name                  string
	ddb                   *doltdb.DoltDB
	ss                    *schema.SuperSchema
	sqlSch                sql.Schema
	commitFilters         []sql.Expression
	rowFilters            []sql.Expression
	cmItr                 doltdb.CommitItr
	indCmItr              *doltdb.CommitIndexingCommitItr
	readerCreateFuncCache map[hash.Hash]CreateReaderFunc
}

// NewHistoryTable creates a history table
func NewHistoryTable(ctx *sql.Context, dbName, tblName string) (*HistoryTable, error) {
	sess := DSessFromSess(ctx.Session)
	ddb, ok := sess.GetDoltDB(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	cmItr, err := doltdb.CommitItrForAllBranches(ctx, ddb)

	if err != nil {
		return nil, err
	}

	indCmItr := doltdb.NewCommitIndexingCommitItr(ddb, cmItr)

	root, ok := sess.GetRoot(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	ss, err := SuperSchemaForAllBranches(ctx, indCmItr, root, tblName)

	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn(CommitHashCol, doltdb.HistoryCommitHashTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn(CommitterCol, doltdb.HistoryCommitterTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn(CommitDateCol, doltdb.HistoryCommitDateTag, types.TimestampKind, false))

	sch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	if sch.GetAllCols().Size() <= 3 {
		return nil, sql.ErrTableNotFound.New(DoltHistoryTablePrefix + tblName)
	}

	tableName := DoltHistoryTablePrefix + tblName
	sqlSch, err := doltSchemaToSqlSchema(tableName, sch)

	if err != nil {
		return nil, err
	}

	return &HistoryTable{
		name:                  tblName,
		ddb:                   ddb,
		ss:                    ss,
		sqlSch:                sqlSch,
		cmItr:                 indCmItr.Unfiltered(),
		indCmItr:              indCmItr,
		readerCreateFuncCache: make(map[hash.Hash]CreateReaderFunc),
	}, nil
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (ht *HistoryTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	ht.commitFilters, ht.rowFilters = splitFilters(filters)
	return ht.commitFilters
}

// Filters returns the list of filters that are applied to this table.
func (ht *HistoryTable) Filters() []sql.Expression {
	return ht.commitFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (ht *HistoryTable) WithFilters(filters []sql.Expression) sql.Table {
	if ht.commitFilters == nil {
		ht.commitFilters, ht.rowFilters = splitFilters(filters)
	}

	if len(ht.commitFilters) > 0 {
		ctx := context.TODO()
		commitCheck, err := getCommitFilterFunc(ht.ddb.Format(), ht.commitFilters)

		// TODO: fix panic
		if err != nil {
			panic(err)
		}

		ht.cmItr, err = ht.indCmItr.Filter(ctx, commitCheck)

		// TODO: fix panic
		if err != nil {
			panic(err)
		}
	}

	return ht
}

var commitFilterCols = set.NewStrSet([]string{CommitHashCol, CommitDateCol, CommitterCol})

func isCommitFilter(filter sql.Expression) bool {
	isCommitFilter := true
	sql.Inspect(filter, func(e sql.Expression) (cont bool) {
		if e == nil {
			return true
		}

		switch val := e.(type) {
		case *expression.GetField:
			if !commitFilterCols.Contains(strings.ToLower(val.Name())) {
				isCommitFilter = false
				return false
			}
		}

		return true
	})

	return isCommitFilter
}

func splitFilters(filters []sql.Expression) (commitFilters, rowFilters []sql.Expression) {
	commitFilters = make([]sql.Expression, 0, len(filters))
	rowFilters = make([]sql.Expression, 0, len(filters))
	for _, f := range filters {
		if isCommitFilter(f) {
			commitFilters = append(commitFilters, f)
		} else {
			rowFilters = append(rowFilters, f)
		}
	}
	return commitFilters, rowFilters
}

func commitSchema() schema.Schema {
	cols := []schema.Column{
		schema.NewColumn(CommitterCol, doltdb.HistoryCommitterTag, types.StringKind, false),
		schema.NewColumn(CommitHashCol, doltdb.HistoryCommitHashTag, types.StringKind, false),
		schema.NewColumn(CommitDateCol, doltdb.HistoryCommitDateTag, types.TimestampKind, false),
	}

	colColl, _ := schema.NewColCollection(cols...)

	return schema.UnkeyedSchemaFromCols(colColl)
}

var commitSch = commitSchema()

func getCommitFilterFunc(nbf *types.NomsBinFormat, filters []sql.Expression) (doltdb.CommitCheck, error) {
	expFunc, err := expreval.ExpressionFuncFromSQLExpressions(nbf, commitSch, filters)

	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, h hash.Hash, committer string, time time.Time) (bool, error) {
		commitFields := map[uint64]types.Value{
			doltdb.HistoryCommitterTag:  types.String(committer),
			doltdb.HistoryCommitHashTag: types.String(h.String()),
			doltdb.HistoryCommitDateTag: types.Timestamp(time),
		}
		return expFunc(ctx, commitFields)
	}, nil
}

func (ht *HistoryTable) WithProjection(colNames []string) sql.Table {
	return ht
}

func (ht *HistoryTable) Projection() []string {
	return []string{}
}

// Name returns the name of the history table
func (ht *HistoryTable) Name() string {
	return DoltHistoryTablePrefix + ht.name
}

// String returns the name of the history table
func (ht *HistoryTable) String() string {
	return DoltHistoryTablePrefix + ht.name
}

// Schema returns the schema for the history table, which will be the super set of the schemas from the history
func (ht *HistoryTable) Schema() sql.Schema {
	return ht.sqlSch
}

// Partitions returns a PartitionIter which will be used in getting partitions each of which is used to create RowIter.
func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &commitPartitioner{ht.cmItr}, nil
}

// PartitionRows takes a partition and returns a row iterator for that partition
func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*commitPartition)

	return newRowItrForTableAtCommit(ctx, cp.h, cp.cm, ht.name, ht.ss, ht.rowFilters, ht.readerCreateFuncCache)
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
func (cp commitPartitioner) Next() (sql.Partition, error) {
	h, cm, err := cp.cmItr.Next(context.TODO())

	if err != nil {
		return nil, err
	}

	return &commitPartition{h, cm}, nil
}

// Close closes the partitioner
func (cp commitPartitioner) Close() error {
	return nil
}

type rowItrForTableAtCommit struct {
	rd             table.TableReadCloser
	sch            schema.Schema
	toSuperSchConv *rowconv.RowConverter
	extraVals      map[uint64]types.Value
	empty          bool
}

func newRowItrForTableAtCommit(
	ctx context.Context,
	h hash.Hash,
	cm *doltdb.Commit,
	tblName string,
	ss *schema.SuperSchema,
	filters []sql.Expression,
	readerCreateFuncCache map[hash.Hash]CreateReaderFunc) (*rowItrForTableAtCommit, error) {
	root, err := cm.GetRootValue()

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

	m, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	schRef, err := tbl.GetSchemaRef()
	schHash := schRef.TargetHash()

	if err != nil {
		return nil, err
	}

	tblSch, err := doltdb.RefToSchema(ctx, root.VRW(), schRef)

	if err != nil {
		return nil, err
	}

	toSuperSchConv, err := rowConvForSchema(ss, tblSch)

	if err != nil {
		return nil, err
	}

	var createReaderFunc CreateReaderFunc
	if createReaderFunc, ok = readerCreateFuncCache[schHash]; !ok {
		createReaderFunc, err = CreateReaderFuncLimitedByExpressions(tbl.Format(), tblSch, filters)

		if err != nil {
			return nil, err
		}

		readerCreateFuncCache[schHash] = createReaderFunc
	}

	rd, err := createReaderFunc(ctx, m)

	if err != nil {
		return nil, err
	}

	sch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	hashCol, hashOK := sch.GetAllCols().GetByName(CommitHashCol)
	dateCol, dateOK := sch.GetAllCols().GetByName(CommitDateCol)
	committerCol, commiterOK := sch.GetAllCols().GetByName(CommitterCol)

	if !hashOK || !dateOK || !commiterOK {
		panic("Bug: History table super schema should always have commit_hash")
	}

	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	return &rowItrForTableAtCommit{
		rd:             rd,
		sch:            sch,
		toSuperSchConv: toSuperSchConv,
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
func (tblItr *rowItrForTableAtCommit) Next() (sql.Row, error) {
	if tblItr.empty {
		return nil, io.EOF
	}

	r, err := tblItr.rd.ReadRow(context.TODO())

	if err != nil {
		return nil, err
	}

	r, err = tblItr.toSuperSchConv.Convert(r)

	if err != nil {
		return nil, err
	}

	for tag, val := range tblItr.extraVals {
		r, err = r.SetColVal(tag, val, tblItr.sch)

		if err != nil {
			return nil, err
		}
	}

	return doltRowToSqlRow(r, tblItr.sch)
}

// Close the iterator.
func (tblItr *rowItrForTableAtCommit) Close() error {
	if tblItr.rd != nil {
		return tblItr.rd.Close(context.TODO())
	}

	return nil
}
