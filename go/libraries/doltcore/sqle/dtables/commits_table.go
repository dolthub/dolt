// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

var _ sql.Table = (*CommitsTable)(nil)

// CommitsTable is a sql.Table that implements a system table which
// shows the combined commit log for all branches in the repo.
type CommitsTable struct {
	dbName string
	ddb    *doltdb.DoltDB
}

// NewCommitsTable creates a CommitsTable
func NewCommitsTable(_ *sql.Context, ddb *doltdb.DoltDB) sql.Table {
	return &CommitsTable{ddb: ddb}
}

// Name is a sql.Table interface function which returns the name of the table.
func (dt *CommitsTable) Name() string {
	return doltdb.CommitsTableName
}

// String is a sql.Table interface function which returns the name of the table.
func (dt *CommitsTable) String() string {
	return doltdb.CommitsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the commits system table.
func (dt *CommitsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: sql.Text, Source: doltdb.CommitsTableName, PrimaryKey: true},
		{Name: "committer", Type: sql.Text, Source: doltdb.CommitsTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: doltdb.CommitsTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Datetime, Source: doltdb.CommitsTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: doltdb.CommitsTableName, PrimaryKey: false},
	}
}

// Partitions is a sql.Table interface function that returns a partition
// of the data. Currently the data is unpartitioned.
func (dt *CommitsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sqlutil.NewSinglePartitionIter(types.Map{}), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (dt *CommitsTable) PartitionRows(sqlCtx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewCommitsRowItr(sqlCtx, dt.ddb)
}

// CommitsRowItr is a sql.RowItr which iterates over each commit as if it's a row in the table.
type CommitsRowItr struct {
	ctx context.Context
	itr doltdb.CommitItr
}

// NewCommitsRowItr creates a CommitsRowItr from the current environment.
func NewCommitsRowItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB) (CommitsRowItr, error) {
	itr, err := doltdb.CommitItrForAllBranches(sqlCtx, ddb)
	if err != nil {
		return CommitsRowItr{}, err
	}

	return CommitsRowItr{ctx: sqlCtx, itr: itr}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr CommitsRowItr) Next() (sql.Row, error) {
	h, cm, err := itr.itr.Next(itr.ctx)
	if err != nil {
		return nil, err
	}

	meta, err := cm.GetCommitMeta()
	if err != nil {
		return nil, err
	}

	return sql.NewRow(h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
}

// Close closes the iterator.
func (itr CommitsRowItr) Close(*sql.Context) error {
	return nil
}
