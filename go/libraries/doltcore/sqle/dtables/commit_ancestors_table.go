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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const commitAncestorsDefaultRowCount = 100

// CommitAncestorsTable is a sql.Table that implements a system table which
// shows (commit, parent_commit) relationships for all commits in the repo.
type CommitAncestorsTable struct {
	dbName string
	ddb    *doltdb.DoltDB
}

var _ sql.Table = (*CommitAncestorsTable)(nil)
var _ sql.IndexAddressable = (*CommitAncestorsTable)(nil)
var _ sql.StatisticsTable = (*CommitAncestorsTable)(nil)

// NewCommitAncestorsTable creates a CommitAncestorsTable
func NewCommitAncestorsTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB) sql.Table {
	return &CommitAncestorsTable{dbName: dbName, ddb: ddb}
}

func (dt *CommitAncestorsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dt *CommitAncestorsTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return commitAncestorsDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table.
func (dt *CommitAncestorsTable) Name() string {
	return doltdb.CommitAncestorsTableName
}

// String is a sql.Table interface function which returns the name of the table.
func (dt *CommitAncestorsTable) String() string {
	return doltdb.CommitAncestorsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the commit_ancestors system table.
func (dt *CommitAncestorsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: types.Text, Source: doltdb.CommitAncestorsTableName, PrimaryKey: true},
		{Name: "parent_hash", Type: types.Text, Source: doltdb.CommitAncestorsTableName, PrimaryKey: true},
		{Name: "parent_index", Type: types.Int32, Source: doltdb.CommitAncestorsTableName, PrimaryKey: true},
	}
}

// Collation implements the sql.Table interface.
func (dt *CommitAncestorsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition
// of the data. Currently the data is unpartitioned.
func (dt *CommitAncestorsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (dt *CommitAncestorsTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	switch p := p.(type) {
	case *doltdb.CommitPart:
		return &CommitAncestorsRowItr{
			itr: doltdb.NewOneCommitIter(p.Commit(), p.Hash(), p.Meta()),
			ddb: dt.ddb,
		}, nil
	default:
		return NewCommitAncestorsRowItr(ctx, dt.ddb)
	}
}

// GetIndexes implements sql.IndexAddressable
func (dt *CommitAncestorsTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltCommitIndexes(dt.dbName, dt.Name(), dt.ddb, true)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *CommitAncestorsTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *CommitAncestorsTable) PreciseMatch() bool {
	return true
}

func (dt *CommitAncestorsTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == index.CommitHashIndexId {
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit hash lookup: %s", sql.DebugString(lookup.Ranges))
		}

		hashes, commits, metas := index.HashesToCommits(ctx, dt.ddb, hs, nil, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}

		return doltdb.NewCommitSlicePartitionIter(hashes, commits, metas), nil
	}

	return dt.Partitions(ctx)
}

// CommitAncestorsRowItr is a sql.RowItr which iterates over each
// (commit, parent_commit) pair as if it's a row in the table.
type CommitAncestorsRowItr struct {
	itr   doltdb.CommitItr
	ddb   *doltdb.DoltDB
	cache []sql.Row
}

// NewCommitAncestorsRowItr creates a CommitAncestorsRowItr from the current environment.
func NewCommitAncestorsRowItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB) (*CommitAncestorsRowItr, error) {
	itr, err := doltdb.CommitItrForAllBranches(sqlCtx, ddb)
	if err != nil {
		return nil, err
	}

	return &CommitAncestorsRowItr{
		itr: itr,
		ddb: ddb,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *CommitAncestorsRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	if len(itr.cache) == 0 {
		ch, cm, err := itr.itr.Next(ctx)
		if err != nil {
			// When complete itr.Next will return io.EOF
			return nil, err
		}

		parents, err := itr.ddb.ResolveAllParents(ctx, cm)
		if err != nil {
			return nil, err
		}

		if len(parents) == 0 {
			// init commit
			return sql.NewRow(ch.String(), nil, 0), nil
		}

		itr.cache = make([]sql.Row, len(parents))
		for i, p := range parents {
			ph, err := p.HashOf()
			if err != nil {
				return nil, err
			}

			itr.cache[i] = sql.NewRow(ch.String(), ph.String(), int32(i))
		}
	}

	r := itr.cache[0]
	itr.cache = itr.cache[1:]
	return r, nil
}

// Close closes the iterator.
func (itr *CommitAncestorsRowItr) Close(*sql.Context) error {
	return nil
}
