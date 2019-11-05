package sqle

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/src-d/go-mysql-server/sql"
	"io"
)

const (
	logTableName = "__log__"
)

var _ sql.Table = (*LogTable)(nil)

type LogTable struct {
	dEnv *env.DoltEnv
}

func NewLogTable(dEnv *env.DoltEnv) *LogTable {
	return &LogTable{dEnv: dEnv}
}

func (dt *LogTable) Name() string {
	return logTableName
}

func (dt *LogTable) String() string {
	return logTableName
}

func (dt *LogTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: sql.Text, Source: logTableName, PrimaryKey: true},
		{Name: "committer", Type: sql.Text, Source: logTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: logTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Text, Source: logTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: logTableName, PrimaryKey: false},
	}
}

func (dt *LogTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{}, nil
}

func (dt *LogTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewLogItr(sqlCtx, dt.dEnv)
}

type LogItr struct {
	commits []*doltdb.Commit
	idx     int
}

func NewLogItr(sqlCtx *sql.Context, dEnv *env.DoltEnv) (*LogItr, error) {
	ddb := dEnv.DoltDB

	cs, err := doltdb.NewCommitSpec("HEAD", dEnv.RepoState.Head.Ref.GetPath())

	if err != nil {
		return nil, err
	}

	commit, err := ddb.Resolve(sqlCtx, cs)

	if err != nil {
		return nil, err
	}

	commits, err := actions.TimeSortedCommits(sqlCtx, ddb, commit, -1)

	if err != nil {
		return nil, err
	}

	return &LogItr{commits, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *LogItr) Next() (sql.Row, error) {
	if itr.idx >= len(itr.commits) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	cm := itr.commits[itr.idx]
	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	h, err := cm.HashOf()

	if err != nil {
		return nil, err
	}

	return sql.NewRow(h.String(), meta.Name, meta.Email, meta.FormatTS(), meta.Description), nil
}

// Close the iterator.
func (itr *LogItr) Close() error {
	return nil
}
