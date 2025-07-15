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
	"io"
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// NewConflictsTableRootObject returns a new conflicts table for root objects.
func NewConflictsTableRootObject(ctx *sql.Context, baseTableName string, rootObject doltdb.ConflictRootObject, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	return ConflictsTableRootObject{
		baseTableName: baseTableName,
		sqlSch:        rootObject.Schema(baseTableName),
		root:          root,
		rootObject:    rootObject,
		rs:            rs,
	}, nil
}

// ConflictsTableRootObject is a sql.Table implementation that provides access to the conflicts that exist on a root
// object.
type ConflictsTableRootObject struct {
	baseTableName string
	sqlSch        sql.Schema
	root          doltdb.RootValue
	rootObject    doltdb.ConflictRootObject
	rs            RootSetter
}

var _ sql.Table = ConflictsTableRootObject{}
var _ sql.DeletableTable = ConflictsTableRootObject{}

// Name implements the interface sql.Table.
func (ct ConflictsTableRootObject) Name() string {
	return ct.baseTableName
}

// String implements the interface sql.Table.
func (ct ConflictsTableRootObject) String() string {
	return ct.baseTableName
}

// Schema implements the interface sql.Table.
func (ct ConflictsTableRootObject) Schema() sql.Schema {
	return ct.sqlSch
}

// Collation implements the interface sql.Table.
func (ct ConflictsTableRootObject) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (ct ConflictsTableRootObject) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &conflictRootObjectPartitionIter{
		table: ct,
		done:  false,
	}, nil
}

// PartitionRows implements the interface sql.Table.
func (ct ConflictsTableRootObject) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	conflictPart, ok := part.(conflictRootObjectPartition)
	if !ok {
		return nil, errors.New("unexpected partition type for root object conflicts table")
	}
	return conflictPart.table.rootObject.Rows(ctx)
}

// Deleter implements the interface sql.DeletableTable.
func (ct ConflictsTableRootObject) Deleter(ctx *sql.Context) sql.RowDeleter {
	return &conflictRootObjectDeleter{}
}

// conflictRootObjectPartitionIter is a partition iterator for ConflictsTableRootObject.
type conflictRootObjectPartitionIter struct {
	table ConflictsTableRootObject
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
	table ConflictsTableRootObject
}

var _ sql.Partition = conflictRootObjectPartition{}

// Key implements the interface sql.Partition.
func (c conflictRootObjectPartition) Key() []byte {
	return []byte{1}
}

type conflictRootObjectDeleter struct {
	ct ConflictsTable
	rs RootSetter
}

var _ sql.RowDeleter = &conflictRootObjectDeleter{}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (cd *conflictRootObjectDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	return errors.New("deletion logic not yet implemented")
}

// StatementBegin implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (cd *conflictRootObjectDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (cd *conflictRootObjectDeleter) Close(ctx *sql.Context) error {
	// TODO: finalize the changes made
	return nil
}
