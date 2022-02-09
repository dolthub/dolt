// Copyright 2022 Dolthub, Inc.
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
	"io"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// UnscopedDiffTable is a sql.Table implementation of a system table that shows which tables have
// changed in each commit, across all branches.
type UnscopedDiffTable struct {
	ddb  *doltdb.DoltDB
	head *doltdb.Commit
}

// NewUnscopedDiffTable creates an UnscopedDiffTable
func NewUnscopedDiffTable(_ *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &UnscopedDiffTable{ddb: ddb, head: head}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// LogTableName
func (dt *UnscopedDiffTable) Name() string {
	return doltdb.DiffTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// DiffTableName
func (dt *UnscopedDiffTable) String() string {
	return doltdb.DiffTableName
}

// Schema is a sql.Table interface function that returns the sql.Schema for this system table.
func (dt *UnscopedDiffTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "commit_hash", Type: sql.Text, Source: doltdb.LogTableName, PrimaryKey: true},
		{Name: "committer", Type: sql.Text, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Datetime, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: doltdb.LogTableName, PrimaryKey: false},
		{Name: "table_name", Type: sql.Text, Source: doltdb.LogTableName, PrimaryKey: true},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data. Currently data is unpartitioned.
func (dt *UnscopedDiffTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (dt *UnscopedDiffTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewUnscopedDiffTableItr(ctx, dt.ddb, dt.head)
}

// UnscopedDiffTableItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type UnscopedDiffTableItr struct {
	ctx          *sql.Context
	ddb          *doltdb.DoltDB
	commits      []*doltdb.Commit
	commitIdx    int
	tableNames   []string
	tableNameIdx int
}

// NewUnscopedDiffTableItr creates a UnscopedDiffTableItr from the current environment.
func NewUnscopedDiffTableItr(ctx *sql.Context, ddb *doltdb.DoltDB, head *doltdb.Commit) (*UnscopedDiffTableItr, error) {
	commits, err := actions.TimeSortedCommits(ctx, ddb, head, -1)

	if err != nil {
		return nil, err
	}

	return &UnscopedDiffTableItr{ctx, ddb, commits, 0, nil, -1}, nil
}

// HasNext returns true if this UnscopedDiffItr has more elements left.
func (itr *UnscopedDiffTableItr) HasNext() bool {
	// There are more diff records to iterate over if:
	//   1) there is more than one commit left to process, or
	//   2) the tableNames array isn't nilled out and has data to process

	return itr.commitIdx+1 < len(itr.commits) || itr.tableNames != nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *UnscopedDiffTableItr) Next(*sql.Context) (sql.Row, error) {
	if !itr.HasNext() {
		return nil, io.EOF
	}

	defer func() {
		// Increment the table name index, and if it's the end of the table names array,
		// move to the next commit and reset the table name index
		itr.tableNameIdx++
		if itr.tableNameIdx >= len(itr.tableNames) {
			itr.tableNameIdx = -1
			itr.tableNames = nil
			itr.commitIdx++
		}
	}()

	// Load table names if we don't have them for this commit yet
	for itr.tableNames == nil {
		err := itr.loadTableNames(itr.commits[itr.commitIdx])
		if err != nil {
			return nil, err
		}
	}

	commit := itr.commits[itr.commitIdx]
	hash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	meta, err := commit.GetCommitMeta()
	if err != nil {
		return nil, err
	}

	return sql.NewRow(hash.String(), meta.Name, meta.Email, meta.Time(),
		meta.Description, itr.tableNames[itr.tableNameIdx]), nil
}

// loadTableNames loads the set of changed tables for the current commit into this iterator, taking
// care of advancing the iterator if that commit didn't mutate any tables and checking for EOF condition.
func (itr *UnscopedDiffTableItr) loadTableNames(commit *doltdb.Commit) error {
	tableNames, err := itr.calculateChangedTables(commit)
	if err != nil {
		return err
	}

	// If there are no table deltas for this commit (e.g. a "dolt doc" commit),
	// advance to the next commit, checking for EOF condition.
	if len(tableNames) == 0 {
		itr.commitIdx++
		if !itr.HasNext() {
			return io.EOF
		}
	} else {
		itr.tableNames = tableNames
		itr.tableNameIdx = 0
	}

	return nil
}

// calculateChangedTables calculates the tables that changed in the specified commit, by comparing that
// commit with its immediate ancestor commit.
func (itr *UnscopedDiffTableItr) calculateChangedTables(commit *doltdb.Commit) ([]string, error) {
	toRootValue, err := commit.GetRootValue()
	if err != nil {
		return nil, err
	}

	parent, err := itr.ddb.ResolveParent(itr.ctx, commit, 0)
	if err != nil {
		return nil, err
	}

	fromRootValue, err := parent.GetRootValue()
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(itr.ctx, fromRootValue, toRootValue)
	if err != nil {
		return nil, err
	}

	tablesMap := make(map[string]bool)
	for _, delta := range deltas {
		// Use toName by default for the table name by default, but if it's
		// nil (e.g. when dropping a table), check to see if fromName is available.
		tableName := delta.ToName
		if len(tableName) == 0 {
			if len(delta.FromName) > 0 {
				tableName = delta.FromName
			}
		}

		if len(tableName) > 0 {
			tablesMap[tableName] = true
		}
	}

	// Not all commits mutate tables (e.g. empty commits)
	if len(tablesMap) == 0 {
		return nil, nil
	}

	tables := make([]string, len(tablesMap))
	i := 0
	for key := range tablesMap {
		tables[i] = key
		i++
	}
	sort.Strings(tables)

	return tables, nil
}

// Close closes the iterator.
func (itr *UnscopedDiffTableItr) Close(*sql.Context) error {
	return nil
}
