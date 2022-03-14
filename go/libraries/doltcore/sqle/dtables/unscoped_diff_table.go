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
	"errors"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"

	"github.com/dolthub/go-mysql-server/sql"
)

// UnscopedDiffTable is a sql.Table implementation of a system table that shows which tables have
// changed in each commit, across all branches.
type UnscopedDiffTable struct {
	ddb  *doltdb.DoltDB
	head *doltdb.Commit
}

// tableChange is an internal data structure used to hold the results of processing
// a diff.TableDelta structure into the output data for this system table.
type tableChange struct {
	tableName    string
	dataChange   bool
	schemaChange bool
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
		{Name: "commit_hash", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: true},
		{Name: "table_name", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: true},
		{Name: "committer", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "email", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "date", Type: sql.Datetime, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "message", Type: sql.Text, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "data_change", Type: sql.Boolean, Source: doltdb.DiffTableName, PrimaryKey: false},
		{Name: "schema_change", Type: sql.Boolean, Source: doltdb.DiffTableName, PrimaryKey: false},
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
	ctx             *sql.Context
	ddb             *doltdb.DoltDB
	commits         []*doltdb.Commit
	commitIdx       int
	tableChanges    []tableChange
	tableChangesIdx int
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
	//   2) the tableChanges array isn't nilled out and has data left to process
	return itr.commitIdx+1 < len(itr.commits) || itr.tableChanges != nil
}

// incrementIndexes increments the table changes index, and if it's the end of the table changes array, moves
// to the next commit, and resets the table changes index so that it can be populated when Next() is called.
func (itr *UnscopedDiffTableItr) incrementIndexes() {
	itr.tableChangesIdx++
	if itr.tableChangesIdx >= len(itr.tableChanges) {
		itr.tableChangesIdx = -1
		itr.tableChanges = nil
		itr.commitIdx++
	}
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *UnscopedDiffTableItr) Next(*sql.Context) (sql.Row, error) {
	if !itr.HasNext() {
		return nil, io.EOF
	}
	defer itr.incrementIndexes()

	// Load table changes if we don't have them for this commit yet
	for itr.tableChanges == nil {
		err := itr.loadTableChanges(itr.commits[itr.commitIdx])
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

	tableChange := itr.tableChanges[itr.tableChangesIdx]

	return sql.NewRow(hash.String(), tableChange.tableName, meta.Name, meta.Email, meta.Time(),
		meta.Description, tableChange.dataChange, tableChange.schemaChange), nil
}

// loadTableChanges loads the set of table changes for the current commit into this iterator, taking
// care of advancing the iterator if that commit didn't mutate any tables and checking for EOF condition.
func (itr *UnscopedDiffTableItr) loadTableChanges(commit *doltdb.Commit) error {
	tableChanges, err := itr.calculateTableChanges(commit)
	if err != nil {
		return err
	}

	// If there are no table deltas for this commit (e.g. a "dolt doc" commit),
	// advance to the next commit, checking for EOF condition.
	if len(tableChanges) == 0 {
		itr.commitIdx++
		if !itr.HasNext() {
			return io.EOF
		}
	} else {
		itr.tableChanges = tableChanges
		itr.tableChangesIdx = 0
	}

	return nil
}

// calculateTableChanges calculates the tables that changed in the specified commit, by comparing that
// commit with its immediate ancestor commit.
func (itr *UnscopedDiffTableItr) calculateTableChanges(commit *doltdb.Commit) ([]tableChange, error) {
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

	tableChanges := make([]tableChange, len(deltas))
	for i := 0; i < len(deltas); i++ {
		change, err := itr.processTableDelta(deltas[i])
		if err != nil {
			return nil, err
		}

		tableChanges[i] = *change
	}

	// Not all commits mutate tables (e.g. empty commits)
	if len(tableChanges) == 0 {
		return nil, nil
	}

	return tableChanges, nil
}

// processTableDelta processes the specified TableDelta to determine what kind of change it was (i.e. table drop,
// table rename, table create, or data update) and returns a tableChange struct representing the change.
func (itr *UnscopedDiffTableItr) processTableDelta(delta diff.TableDelta) (*tableChange, error) {
	// Dropping a table is always a schema change, and also a data change if the table contained data
	if itr.isTableDropChange(delta) {
		isEmpty, err := itr.isTableDataEmpty(delta.FromTable)
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.FromName,
			dataChange:   !isEmpty,
			schemaChange: true,
		}, nil
	}

	// Renaming a table is always a schema change, and also a data change if the table data differs
	if itr.isRenameChange(delta) {
		dataChanged, err := itr.isTableDataDifferent(delta)
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.ToName,
			dataChange:   dataChanged,
			schemaChange: true,
		}, nil
	}

	// Creating a table is always a schema change, and also a data change if data was inserted
	if itr.isTableCreateChange(delta) {
		isEmpty, err := itr.isTableDataEmpty(delta.ToTable)
		if err != nil {
			return nil, err
		}

		return &tableChange{
			tableName:    delta.ToName,
			dataChange:   !isEmpty,
			schemaChange: true,
		}, nil
	}

	dataChanged, err := itr.isTableDataDifferent(delta)
	if err != nil {
		return nil, err
	}

	schemaChanged, err := itr.isTableSchemaDifferent(delta)
	if err != nil {
		return nil, err
	}

	return &tableChange{
		tableName:    delta.ToName,
		dataChange:   dataChanged,
		schemaChange: schemaChanged,
	}, nil
}

// Close closes the iterator.
func (itr *UnscopedDiffTableItr) Close(*sql.Context) error {
	return nil
}

// isTableDataEmpty return true if the table does not contain any data
func (itr *UnscopedDiffTableItr) isTableDataEmpty(table *doltdb.Table) (bool, error) {
	rowData, err := table.GetRowData(itr.ctx)
	if err != nil {
		return false, err
	}

	return rowData.Empty(), nil
}

// isRenameChange returns true if the specified TableDelta represents a table rename change.
func (itr *UnscopedDiffTableItr) isRenameChange(delta diff.TableDelta) bool {
	return delta.FromTable != nil &&
		delta.ToTable != nil &&
		delta.FromName != delta.ToName
}

// isTableDropChange return true if the specified TableDelta represents a table drop change.
func (itr *UnscopedDiffTableItr) isTableDropChange(delta diff.TableDelta) bool {
	return len(delta.FromName) > 0 && len(delta.ToName) == 0
}

// isTableCreateChange returns true if the specified TableDelta represents a table create change.
func (itr *UnscopedDiffTableItr) isTableCreateChange(delta diff.TableDelta) bool {
	return delta.FromTable == nil && delta.ToTable != nil
}

// isTableDataDifferent returns true if the data in the from and to tables is different. This method
// should only be called with both from and to tables are not nil.
func (itr *UnscopedDiffTableItr) isTableDataDifferent(delta diff.TableDelta) (bool, error) {
	if delta.FromTable == nil || delta.ToTable == nil {
		return false, errors.New("specified FromTable and ToTable should never be nil")
	}

	fromTableHash, err := delta.FromTable.HashOf()
	if err != nil {
		return false, err
	}

	toTableHash, err := delta.ToTable.HashOf()
	if err != nil {
		return false, err
	}

	return fromTableHash != toTableHash, nil
}

// isTableSchemaDifferent returns true if the schema in the from and to tables is different. This method
// should only be called with both from and to tables are not nil.
func (itr *UnscopedDiffTableItr) isTableSchemaDifferent(delta diff.TableDelta) (bool, error) {
	if delta.FromTable == nil || delta.ToTable == nil {
		return false, errors.New("specified FromTable and ToTable should never be nil")
	}

	fromSchemaHash, err := delta.FromTable.GetSchemaHash(itr.ctx)
	if err != nil {
		return false, err
	}

	toSchemaHash, err := delta.ToTable.GetSchemaHash(itr.ctx)
	if err != nil {
		return false, err
	}

	return fromSchemaHash != toSchemaHash, nil
}
