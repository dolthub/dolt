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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

var _ sql.Table = (*LogTable)(nil)

var _ sql.StatisticsTable = (*LogTable)(nil)

// LogTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type LogTable struct {
	ddb  *doltdb.DoltDB
	head *doltdb.Commit
}

// NewLogTable creates a LogTable
func NewLogTable(_ *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &LogTable{ddb: ddb, head: head}
}

// DataLength implements sql.StatisticsTable
func (dt *LogTable) DataLength(ctx *sql.Context) (uint64, error) {
	return uint64(4*types.Text.MaxByteLength()*4 + 16), nil
}

// RowCount implements sql.StatisticsTable
func (dt *LogTable) RowCount(ctx *sql.Context) (uint64, error) {
	cc, err := dt.head.GetCommitClosure(ctx)
	if err != nil {
		// TODO: remove this when we deprecate LD
		return 1000, nil
	}
	cnt, err := cc.Count()
	return uint64(cnt + 1), err
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// LogTableName
func (dt *LogTable) Name() string {
	return doltdb.LogTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// LogTableName
func (dt *LogTable) String() string {
	return doltdb.LogTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *LogTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: doltdb.LogTableName, PrimaryKey: true},
		{Name: "committer", Type: types.Text, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "email", Type: types.Text, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "date", Type: types.Datetime, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "message", Type: types.Text, Source: doltdb.LogTableName, PrimaryKey: false},
	}
}

// Collation implements the sql.Table interface.
func (dt *LogTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (dt *LogTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *LogTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewLogItr(ctx, dt.ddb, dt.head)
}

// LogItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type LogItr struct {
	child doltdb.CommitItr
}

// NewLogItr creates a LogItr from the current environment.
func NewLogItr(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit) (*LogItr, error) {
	h, err := head.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{h}, nil)
	if err != nil {
		return nil, err
	}

	return &LogItr{child}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *LogItr) Next(ctx *sql.Context) (sql.Row, error) {
	h, cm, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
}

// Close closes the iterator.
func (itr *LogItr) Close(*sql.Context) error {
	return nil
}
