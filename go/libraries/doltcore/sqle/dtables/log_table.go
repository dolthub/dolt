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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

const logsDefaultRowCount = 100

// LogTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type LogTable struct {
	dbName            string
	ddb               *doltdb.DoltDB
	head              *doltdb.Commit
	headHash          hash.Hash
	headCommitClosure *prolly.CommitClosure
}

var _ sql.Table = (*LogTable)(nil)
var _ sql.StatisticsTable = (*LogTable)(nil)
var _ sql.IndexAddressable = (*LogTable)(nil)

// NewLogTable creates a LogTable
func NewLogTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &LogTable{dbName: dbName, ddb: ddb, head: head}
}

// DataLength implements sql.StatisticsTable
func (dt *LogTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

// RowCount implements sql.StatisticsTable
func (dt *LogTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	cc, err := dt.head.GetCommitClosure(ctx)
	if err != nil {
		// TODO: remove this when we deprecate LD
		return logsDefaultRowCount, false, nil
	}
	if cc.IsEmpty() {
		return 1, true, nil
	}
	cnt, err := cc.Count()
	return uint64(cnt + 1), true, err
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// GetLogTableName()
func (dt *LogTable) Name() string {
	return doltdb.GetLogTableName()
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// GetLogTableName()
func (dt *LogTable) String() string {
	return doltdb.GetLogTableName()
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *LogTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: doltdb.GetLogTableName(), PrimaryKey: true, DatabaseSource: dt.dbName},
		{Name: "committer", Type: types.Text, Source: doltdb.GetLogTableName(), PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "email", Type: types.Text, Source: doltdb.GetLogTableName(), PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "date", Type: types.Datetime, Source: doltdb.GetLogTableName(), PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "message", Type: types.Text, Source: doltdb.GetLogTableName(), PrimaryKey: false, DatabaseSource: dt.dbName},
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
func (dt *LogTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	switch p := p.(type) {
	case *doltdb.CommitPart:
		return sql.RowsToRowIter(sql.NewRow(p.Hash().String(), p.Meta().Name, p.Meta().Email, p.Meta().Time(), p.Meta().Description)), nil
	default:
		return NewLogItr(ctx, dt.ddb, dt.head)
	}
}

func (dt *LogTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(dt.dbName, dt.Name(), dt.ddb, true)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *LogTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *LogTable) PreciseMatch() bool {
	return true
}

func (dt *LogTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		return dt.commitHashPartitionIter(ctx, lookup)
	}

	return dt.Partitions(ctx)
}

func (dt *LogTable) commitHashPartitionIter(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	hashStrs, ok := index.LookupToPointSelectStr(lookup)
	if !ok {
		return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
	}
	hashes, commits, metas := index.HashesToCommits(ctx, dt.ddb, hashStrs, nil, false)
	if len(hashes) == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}
	var partitions []sql.Partition
	for i, h := range hashes {
		height, err := commits[i].Height()
		if err != nil {
			return nil, err
		}

		ok, err = dt.CommitIsInScope(ctx, height, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		partitions = append(partitions, doltdb.NewCommitPart(h, commits[i], metas[i]))

	}
	return sql.PartitionsToPartitionIter(partitions...), nil
}

// CommitIsInScope returns true if a given commit hash is head or is
// visible from the current head's ancestry graph.
func (dt *LogTable) CommitIsInScope(ctx context.Context, height uint64, h hash.Hash) (bool, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return false, err
	}
	if headHash == h {
		return true, nil
	}
	cc, err := dt.HeadCommitClosure(ctx)
	if err != nil {
		return false, err
	}
	return cc.ContainsKey(ctx, h, height)
}

func (dt *LogTable) HeadCommitClosure(ctx context.Context) (*prolly.CommitClosure, error) {
	if dt.headCommitClosure == nil {
		cc, err := dt.head.GetCommitClosure(ctx)
		dt.headCommitClosure = &cc
		if err != nil {
			return nil, err
		}
	}
	return dt.headCommitClosure, nil
}

func (dt *LogTable) HeadHash() (hash.Hash, error) {
	if dt.headHash.IsEmpty() {
		var err error
		dt.headHash, err = dt.head.HashOf()
		if err != nil {
			return hash.Hash{}, err
		}
	}
	return dt.headHash, nil
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
	h, optCmt, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	cm, ok := optCmt.ToCommit()
	if !ok {
		// Should have been caught by the commit walk.
		return nil, doltdb.ErrGhostCommitRuntimeFailure
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
