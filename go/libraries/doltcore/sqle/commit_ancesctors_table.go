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

package sqle

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

var _ sql.Table = (*CommitAncestorsTable)(nil)

// CommitAncestorsTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type CommitAncestorsTable struct {
	dbName string
	ddb    *doltdb.DoltDB
}

// NewCommitAncestorsTable creates a CommitAncestorsTable
func NewCommitAncestorsTable(ctx *sql.Context, dbName string) (sql.Table, error) {
	ddb, ok := DSessFromSess(ctx.Session).GetDoltDB(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return &CommitAncestorsTable{dbName: dbName, ddb: ddb}, nil
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// CommitAncestorsTableName
func (dt *CommitAncestorsTable) Name() string {
	return doltdb.CommitAncestorsTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// CommitAncestorsTableName
func (dt *CommitAncestorsTable) String() string {
	return doltdb.CommitAncestorsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *CommitAncestorsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: sql.Text, Source: doltdb.CommitAncestorsTableName, PrimaryKey: true},
		{Name: "parent_hash", Type: sql.Text, Source: doltdb.CommitAncestorsTableName, PrimaryKey: true},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data. Currently the data is unpartitioned.
func (dt *CommitAncestorsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return newSinglePartitionIter(), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *CommitAncestorsTable) PartitionRows(sqlCtx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewCommitAncestorsRowItr(sqlCtx, dt.ddb)
}

type hashPair struct {
	commitHash string
	parentHash string
}

// CommitAncestorsRowItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type CommitAncestorsRowItr struct {
	ctx    context.Context
	itr    doltdb.CommitItr
	ddb    *doltdb.DoltDB
	cached []hashPair
}

// NewCommitAncestorsRowItr creates a CommitAncestorsRowItr from the current environment.
func NewCommitAncestorsRowItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB) (*CommitAncestorsRowItr, error) {
	itr, err := doltdb.CommitItrForAllBranches(sqlCtx, ddb)
	if err != nil {
		return nil, err
	}

	return &CommitAncestorsRowItr{
		ctx: sqlCtx,
		itr: itr,
		ddb: ddb,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *CommitAncestorsRowItr) Next() (sql.Row, error) {
	if len(itr.cached) == 0 {
		ch, cm, err := itr.itr.Next(itr.ctx)
		if err != nil {
			return nil, err
		}

		parents, err := itr.ddb.ResolveAllParents(itr.ctx, cm)
		if err != nil {
			return nil, err
		}

		if len(parents) == 0 {
			// init commit
			return sql.NewRow(ch.String(), nil), nil
		}

		itr.cached = make([]hashPair, len(parents))
		for i, p := range parents {
			ph, err := p.HashOf()
			if err != nil {
				return nil, err
			}

			itr.cached[i] = hashPair{
				commitHash: ch.String(),
				parentHash: ph.String(),
			}
		}
	}

	r := sql.NewRow(itr.cached[0].commitHash, itr.cached[0].parentHash)
	itr.cached = itr.cached[1:]
	return r, nil
}

// Close closes the iterator.
func (itr *CommitAncestorsRowItr) Close() error {
	return nil
}
