// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const commitsDefaultRowCount = 10

// CommitsTable is a sql.Table that implements a system table which
// shows the combined commit log for all branches in the repo.
type CommitsTable struct {
	dbName    string
	tableName string
	ddb       *doltdb.DoltDB
}

var _ sql.Table = (*CommitsTable)(nil)
var _ sql.IndexAddressable = (*CommitsTable)(nil)
var _ sql.StatisticsTable = (*CommitsTable)(nil)

// NewCommitsTable creates a CommitsTable
func NewCommitsTable(_ *sql.Context, dbName, tableName string, ddb *doltdb.DoltDB) sql.Table {
	return &CommitsTable{dbName: dbName, tableName: tableName, ddb: ddb}
}

func (ct *CommitsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ct.Schema())
	numRows, _, err := ct.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ct *CommitsTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return commitsDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table.
func (ct *CommitsTable) Name() string {
	return ct.tableName
}

// String is a sql.Table interface function which returns the name of the table.
func (ct *CommitsTable) String() string {
	return ct.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the commits system table.
func (ct *CommitsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: ct.tableName, PrimaryKey: true, DatabaseSource: ct.dbName},
		// Author fields
		{Name: "author", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		{Name: "author_email", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		{Name: "author_date", Type: types.Datetime, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		// Committer fields
		{Name: "committer", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		{Name: "committer_email", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		{Name: "committer_date", Type: types.Datetime, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
		// Backward compatibility columns
		{Name: "email", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},    // Deprecated: use author_email
		{Name: "date", Type: types.Datetime, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName}, // Deprecated: use author_date
		// Message
		{Name: "message", Type: types.Text, Source: ct.tableName, PrimaryKey: false, DatabaseSource: ct.dbName},
	}
}

// Collation implements the sql.Table interface.
func (ct *CommitsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition
// of the data. Currently the data is unpartitioned.
func (ct *CommitsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (ct *CommitsTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	switch p := p.(type) {
	case *doltdb.CommitPart:
		return sql.RowsToRowIter(formatCommitTableRow(p.Hash(), p.Meta())), nil
	default:
		return NewCommitsRowItr(ctx, ct.ddb)
	}
}

// GetIndexes implements sql.IndexAddressable
func (ct *CommitsTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(ct.dbName, ct.Name(), ct.ddb, true)
}

// IndexedAccess implements sql.IndexAddressable
func (ct *CommitsTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	nt := *ct
	return &nt
}

func (ct *CommitsTable) PreciseMatch() bool {
	return true
}

func (ct *CommitsTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		hashStrs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
		}
		hashes, commits, metas := index.HashesToCommits(ctx, ct.ddb, hashStrs, nil, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}

		return doltdb.NewCommitSlicePartitionIter(hashes, commits, metas), nil
	}

	return ct.Partitions(ctx)
}

// CommitsRowItr is a sql.RowItr which iterates over each commit as if it's a row in the table.
type CommitsRowItr struct {
	itr doltdb.CommitItr[*sql.Context]
}

// NewCommitsRowItr creates a CommitsRowItr from the current environment.
func NewCommitsRowItr(ctx *sql.Context, ddb *doltdb.DoltDB) (CommitsRowItr, error) {
	itr, err := doltdb.CommitItrForAllBranches[*sql.Context](ctx, ddb)
	if err != nil {
		return CommitsRowItr{}, err
	}

	return CommitsRowItr{itr: itr}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr CommitsRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	h, optCmt, err := itr.itr.Next(ctx)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, io.EOF
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	return formatCommitTableRow(h, meta), nil
}

// Close closes the iterator.
func (itr CommitsRowItr) Close(*sql.Context) error {
	return nil
}

func formatCommitTableRow(h hash.Hash, meta *datas.CommitMeta) sql.Row {
	return sql.NewRow(
		h.String(),           // commit_hash
		meta.AuthorName,      // author
		meta.AuthorEmail,     // author_email
		meta.AuthorTime(),    // author_date
		meta.CommitterName,   // committer
		meta.CommitterEmail,  // committer_email
		meta.CommitterTime(), // committer_date
		meta.AuthorEmail,     // email (deprecated, for backward compatibility)
		meta.AuthorTime(),    // date (deprecated, for backward compatibility)
		meta.Description,     // message
	)
}
