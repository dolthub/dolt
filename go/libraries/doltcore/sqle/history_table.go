package sqle

import (
	"context"
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
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

// HistoryTable is a system table that show the history of rows over time
type HistoryTable struct {
	name    string
	dEnv    *env.DoltEnv
	ss      rowconv.SuperSchema
	sqlSch  sql.Schema
	filters []sql.Expression
	cmItr   doltdb.CommitItr
}

// NewHistoryTable creates a history table
func NewHistoryTable(ctx context.Context, name string, dEnv *env.DoltEnv) (*HistoryTable, error) {
	ssg := rowconv.NewSuperSchemaGen()

	cmItr, err := doltdb.CommitItrForAllBranches(ctx, dEnv.DoltDB)

	if err != nil {
		return nil, err
	}

	err = ssg.AddHistoryOfCommits(ctx, name, dEnv.DoltDB, cmItr)

	if err != nil {
		return nil, err
	}

	ss, err := ssg.GenerateSuperSchema(
		rowconv.NameKindPair{Name: CommitHashCol, Kind: types.StringKind},
		rowconv.NameKindPair{Name: CommitterCol, Kind: types.StringKind},
		rowconv.NameKindPair{Name: CommitDateCol, Kind: types.TimestampKind})

	if err != nil {
		return nil, err
	}

	sch := ss.GetSchema()

	err = cmItr.Reset(ctx)

	if err != nil {
		return nil, err
	}

	tableName := DoltHistoryTablePrefix + name
	sqlSch, err := doltSchemaToSqlSchema(tableName, sch)

	if err != nil {
		return nil, err
	}

	return &HistoryTable{
		name:   name,
		dEnv:   dEnv,
		ss:     ss,
		sqlSch: sqlSch,
		cmItr:  cmItr,
	}, nil
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

	return newRowItrForTableAtCommit(ctx, cp.h, cp.cm, ht.name, ht.ss)
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

	if cm == nil {
		return nil, io.EOF
	}

	return &commitPartition{h, cm}, nil
}

// Close closes the partitioner
func (cp commitPartitioner) Close() error {
	return nil
}

type rowItrForTableAtCommit struct {
	rd             *noms.NomsMapReader
	sch            schema.Schema
	toSuperSchConv *rowconv.RowConverter
	extraVals      map[uint64]types.Value
	empty          bool
}

func newRowItrForTableAtCommit(ctx context.Context, h hash.Hash, cm *doltdb.Commit, tblName string, ss rowconv.SuperSchema) (*rowItrForTableAtCommit, error) {
	root, err := cm.GetRootValue()

	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, tblName)

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

	tblSch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	toSuperSchConv, err := ss.RowConvForSchema(tblSch)

	if err != nil {
		return nil, err
	}

	rd, err := noms.NewNomsMapReader(ctx, m, tblSch)

	if err != nil {
		return nil, err
	}

	hashCol, hashOK := ss.GetSchema().GetAllCols().GetByName(CommitHashCol)
	dateCol, dateOK := ss.GetSchema().GetAllCols().GetByName(CommitDateCol)
	committerCol, commiterOK := ss.GetSchema().GetAllCols().GetByName(CommitterCol)

	if !hashOK || !dateOK || !commiterOK {
		panic("Bug: History table super schema should always have commit_hash")
	}

	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	return &rowItrForTableAtCommit{
		rd:             rd,
		sch:            ss.GetSchema(),
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
