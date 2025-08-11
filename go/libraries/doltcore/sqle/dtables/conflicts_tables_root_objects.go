// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// NewConflictRootObjectTable returns a new conflicts table for root objects.
func NewConflictRootObjectTable(ctx *sql.Context, rootObject doltdb.ConflictRootObject, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	tableName := rootObject.Name()
	return ConflictRootObjectTable{
		tableName:  tableName,
		sqlSch:     rootObject.Schema(tableName.Name),
		root:       root,
		rootObject: rootObject,
		rs:         rs,
	}, nil
}

// ConflictRootObjectTable is a sql.Table implementation that provides access to the conflicts that exist on a root
// object.
type ConflictRootObjectTable struct {
	tableName  doltdb.TableName
	sqlSch     sql.Schema
	root       doltdb.RootValue
	rootObject doltdb.ConflictRootObject
	rs         RootSetter
}

var _ sql.Table = ConflictRootObjectTable{}
var _ sql.DeletableTable = ConflictRootObjectTable{}

// Name implements the interface sql.Table.
func (ct ConflictRootObjectTable) Name() string {
	return ct.tableName.Name
}

// String implements the interface sql.Table.
func (ct ConflictRootObjectTable) String() string {
	return ct.tableName.Name
}

// Schema implements the interface sql.Table.
func (ct ConflictRootObjectTable) Schema() sql.Schema {
	return ct.sqlSch
}

// Collation implements the interface sql.Table.
func (ct ConflictRootObjectTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (ct ConflictRootObjectTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &conflictRootObjectPartitionIter{
		table: ct,
		done:  false,
	}, nil
}

// PartitionRows implements the interface sql.Table.
func (ct ConflictRootObjectTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	conflictPart, ok := part.(conflictRootObjectPartition)
	if !ok {
		return nil, errors.New("unexpected partition type for root object conflicts table")
	}
	return conflictPart.table.rootObject.Rows(ctx)
}

// Deleter implements the interface sql.DeletableTable.
func (ct ConflictRootObjectTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return &conflictRootObjectDeleter{
		ct:        ct,
		deletions: nil,
	}
}

// Updater implements the interface sql.UpdatableTable.
func (ct ConflictRootObjectTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &conflictRootObjectUpdater{
		ct:         ct,
		oldUpdates: nil,
		newUpdates: nil,
	}
}

// conflictRootObjectPartitionIter is a partition iterator for ConflictRootObjectTable.
type conflictRootObjectPartitionIter struct {
	table ConflictRootObjectTable
	done  bool
}

var _ sql.PartitionIter = (*conflictRootObjectPartitionIter)(nil)

// Next implements the interface sql.PartitionIter.
func (itr *conflictRootObjectPartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if !itr.done {
		itr.done = true
		return conflictRootObjectPartition{itr.table}, nil
	}
	return nil, io.EOF
}

// Close implements the interface sql.PartitionIter.
func (itr *conflictRootObjectPartitionIter) Close(*sql.Context) error {
	return nil
}

// conflictRootObjectPartition is a partition for conflictRootObjectPartitionIter.
type conflictRootObjectPartition struct {
	table ConflictRootObjectTable
}

var _ sql.Partition = conflictRootObjectPartition{}

// Key implements the interface sql.Partition.
func (c conflictRootObjectPartition) Key() []byte {
	return []byte{1}
}

// conflictRootObjectDeleter handles the deletion of fields for root objects.
type conflictRootObjectDeleter struct {
	ct        ConflictRootObjectTable
	deletions []doltdb.RootObjectDiff
}

var _ sql.RowDeleter = &conflictRootObjectDeleter{}

// Delete deletes the given row. Delete will be called once for each row to process for the delete operation, which may
// involve many rows. After all rows have been processed, Close is called.
func (cd *conflictRootObjectDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	toDelete, err := doltdb.RootObjectDiffFromRow(ctx, cd.ct.rootObject, r)
	if err != nil {
		return err
	}
	for _, diff := range cd.deletions {
		c, err := diff.CompareIds(ctx, toDelete)
		if err != nil {
			return err
		}
		if c == 0 {
			return errors.New("cannot delete row") // TODO: real error message
		}
	}
	cd.deletions = append(cd.deletions, toDelete)
	return nil
}

// StatementBegin implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	cd.deletions = nil
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (cd *conflictRootObjectDeleter) Close(ctx *sql.Context) error {
	if len(cd.deletions) == 0 {
		return nil
	}
	newRootObj, err := cd.ct.rootObject.RemoveDiffs(ctx, cd.deletions)
	if err != nil {
		return err
	}
	newRoot, err := cd.ct.root.PutRootObject(ctx, cd.ct.tableName, newRootObj)
	if err != nil {
		return err
	}
	return cd.ct.rs.SetRoot(ctx, newRoot)
}

// conflictRootObjectUpdater handles updating fields for root objects.
type conflictRootObjectUpdater struct {
	ct         ConflictRootObjectTable
	oldUpdates []doltdb.RootObjectDiff
	newUpdates []doltdb.RootObjectDiff
}

var _ sql.RowUpdater = &conflictRootObjectUpdater{}

// Update updates the "our" value in the conflict root object. After all rows have been processed, Close is called.
func (cu *conflictRootObjectUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	oldDiff, err := doltdb.RootObjectDiffFromRow(ctx, cu.ct.rootObject, oldRow)
	if err != nil {
		return err
	}
	newDiff, err := doltdb.RootObjectDiffFromRow(ctx, cu.ct.rootObject, newRow)
	if err != nil {
		return err
	}
	for i, diff := range cu.oldUpdates {
		c, err := diff.CompareIds(ctx, oldDiff)
		if err != nil {
			return err
		}
		if c == 0 {
			cu.oldUpdates[i] = oldDiff
			cu.newUpdates[i] = newDiff
			return nil
		}
	}
	cu.oldUpdates = append(cu.oldUpdates, oldDiff)
	cu.newUpdates = append(cu.newUpdates, newDiff)
	return nil
}

// StatementBegin implements the interface sql.TableEditor.
func (cu *conflictRootObjectUpdater) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor.
func (cu *conflictRootObjectUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	cu.oldUpdates = nil
	cu.newUpdates = nil
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (cu *conflictRootObjectUpdater) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (cu *conflictRootObjectUpdater) Close(ctx *sql.Context) (err error) {
	if len(cu.oldUpdates) == 0 {
		return nil
	}
	newRootObj := cu.ct.rootObject
	for i := range cu.oldUpdates {
		newRootObj, err = newRootObj.UpdateField(ctx, cu.oldUpdates[i], cu.newUpdates[i])
		if err != nil {
			return err
		}
	}
	newRoot, err := cu.ct.root.PutRootObject(ctx, cu.ct.tableName, newRootObj)
	if err != nil {
		return err
	}
	return cu.ct.rs.SetRoot(ctx, newRoot)
}
