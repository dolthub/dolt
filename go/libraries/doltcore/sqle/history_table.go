package sqle

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/src-d/go-mysql-server/sql"
	"io"
)

const (
	DoltHistoryTablePrefix = "dolt_history_"
	CommitHashCol          = "commit_hash"
	CommitterCol           = "committer"
	CommitDateCol          = "commit_date"
)

type HistoryTable struct {
	name       string
	dEnv       *env.DoltEnv
	ss         rowconv.SuperSchema
	sqlSch     sql.Schema
	filters    []sql.Expression
	fromCommit *doltdb.Commit
}

func NewHistoryTable(ctx context.Context, name string, dEnv *env.DoltEnv) (*HistoryTable, error) {
	ssg := rowconv.NewSuperSchemaGen()
	err := ssg.AddHistoryOfTable(ctx, name, dEnv.DoltDB)

	if err != nil {
		return nil, err
	}

	ss, err := ssg.GenerateSuperSchema(
		rowconv.NameKindPair{Name: CommitHashCol, Kind: types.StringKind},
		rowconv.NameKindPair{Name: CommitterCol, Kind: types.StringKind},
		rowconv.NameKindPair{Name: CommitDateCol, Kind: types.StringKind})

	if err != nil {
		return nil, err
	}

	sch := ss.GetSchema()

	sqlSch, err := doltSchemaToSqlSchema(DoltHistoryTablePrefix+name, sch)

	if err != nil {
		return nil, err
	}

	cs, err := doltdb.NewCommitSpec("HEAD", dEnv.RepoState.Head.Ref.GetPath())

	if err != nil {
		return nil, err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	return &HistoryTable{
		name:       name,
		dEnv:       dEnv,
		ss:         ss,
		sqlSch:     sqlSch,
		fromCommit: cm,
	}, nil
}

func (ht *HistoryTable) Name() string {
	return DoltHistoryTablePrefix + ht.name
}

func (ht *HistoryTable) String() string {
	return DoltHistoryTablePrefix + ht.name
}

func (ht *HistoryTable) Schema() sql.Schema {
	return ht.sqlSch
}

func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	commits, err := actions.TimeSortedCommits(ctx, ht.dEnv.DoltDB, ht.fromCommit, -1)

	if err != nil {
		return nil, err
	}

	return &diffHistTablePartItr{commits: commits, currIdx: 0}, nil
}

func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return newTableAtCommitItr(ctx, ht.dEnv, hash.New(part.Key()), ht.name, ht.ss)
}

func (ht *HistoryTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return nil
}

func (ht *HistoryTable) WithFilters(filters []sql.Expression) sql.Table {
	return ht
}

func (ht *HistoryTable) Filters() []sql.Expression {
	return ht.filters
}

type diffHistTablePartItr struct {
	commits []*doltdb.Commit
	currIdx int
}

func (itr *diffHistTablePartItr) Next() (sql.Partition, error) {
	if itr.currIdx >= len(itr.commits) {
		return nil, io.EOF
	}

	cm := itr.commits[itr.currIdx]
	itr.currIdx++

	h, err := cm.HashOf()

	if err != nil {
		return nil, err
	}

	return diffHistPart{h}, nil
}

func (itr *diffHistTablePartItr) Close() error {
	return nil
}

type diffHistPart struct {
	commitHash hash.Hash
}

func (dhPart diffHistPart) Key() []byte {
	return dhPart.commitHash[:]
}

type tableAtCommitItr struct {
	rd             *noms.NomsMapReader
	sch            schema.Schema
	toSuperSchConv *rowconv.RowConverter
	extraVals      map[uint64]types.Value
	empty          bool
}

func newTableAtCommitItr(ctx context.Context, dEnv *env.DoltEnv, h hash.Hash, tblName string, ss rowconv.SuperSchema) (*tableAtCommitItr, error) {
	cs, err := doltdb.NewCommitSpec(h.String(), "")

	if err != nil {
		return nil, err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue()

	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, tblName)

	if err != nil {
		return nil, err
	}

	if !ok {
		return &tableAtCommitItr{empty: true}, nil
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

	return &tableAtCommitItr{
		rd:             rd,
		sch:            ss.GetSchema(),
		toSuperSchConv: toSuperSchConv,
		extraVals: map[uint64]types.Value{
			hashCol.Tag:      types.String(h.String()),
			dateCol.Tag:      types.String(meta.FormatTS()),
			committerCol.Tag: types.String(meta.Name),
		},
		empty: false,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (tblItr *tableAtCommitItr) Next() (sql.Row, error) {
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
func (tblItr *tableAtCommitItr) Close() error {
	if tblItr.rd != nil {
		return tblItr.rd.Close(context.TODO())
	}

	return nil
}
