package dtables

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

import (
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

// NewConflictsTable returns a new ConflictsTable instance
func NewConflictsTable(ctx *sql.Context, tblName string, srcTbl sql.Table, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	resolvedTableName, tbl, ok, err := resolve.Table(ctx, root, tblName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(tblName)
	}

	if types.IsFormat_DOLT(tbl.Format()) {
		upd, ok := srcTbl.(sql.UpdatableTable)
		if !ok {
			return nil, fmt.Errorf("%s can not have conflicts because it is not updateable", tblName)
		}
		return newProllyConflictsTable(ctx, tbl, upd, resolvedTableName, root, rs)
	}

	return newNomsConflictsTable(ctx, tbl, resolvedTableName.Name, root, rs)
}

func newNomsConflictsTable(ctx *sql.Context, tbl *doltdb.Table, tblName string, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	rd, err := merge.NewConflictReader(ctx, tbl, doltdb.TableName{Name: tblName})
	if err != nil {
		return nil, err
	}
	confSch := rd.GetSchema()

	sqlSch, err := sqlutil.FromDoltSchema("", doltdb.DoltConfTablePrefix+tblName, confSch)
	if err != nil {
		return nil, err
	}

	return ConflictsTable{
		tblName: tblName,
		sqlSch:  sqlSch,
		root:    root,
		tbl:     tbl,
		rd:      rd,
		rs:      rs,
	}, nil
}

var _ sql.Table = ConflictsTable{}
var _ sql.DeletableTable = ConflictsTable{}

// ConflictsTable is a sql.Table implementation that provides access to the conflicts that exist for a user table
type ConflictsTable struct {
	tblName string
	sqlSch  sql.PrimaryKeySchema
	root    doltdb.RootValue
	tbl     *doltdb.Table
	rd      *merge.ConflictReader
	rs      RootSetter
}

type RootSetter interface {
	SetRoot(ctx *sql.Context, root doltdb.RootValue) error
}

// Name returns the name of the table
func (ct ConflictsTable) Name() string {
	return doltdb.DoltConfTablePrefix + ct.tblName
}

// String returns a string identifying the table
func (ct ConflictsTable) String() string {
	return doltdb.DoltConfTablePrefix + ct.tblName
}

// Schema returns the sql.Schema of the table
func (ct ConflictsTable) Schema() sql.Schema {
	return ct.sqlSch.Schema
}

// Collation implements the sql.Table interface.
func (ct ConflictsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions returns a PartitionIter which can be used to get all the data partitions
func (ct ConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows returns a RowIter for the given partition
func (ct ConflictsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	// conflict reader must be reset each time partitionRows is called.
	// TODO: schema name
	rd, err := merge.NewConflictReader(ctx, ct.tbl, doltdb.TableName{Name: ct.tblName})
	if err != nil {
		return nil, err
	}
	return conflictRowIter{rd}, nil
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (ct ConflictsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return &conflictDeleter{ct: ct, rs: ct.rs}
}

type conflictRowIter struct {
	rd *merge.ConflictReader
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr conflictRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	cnf, err := itr.rd.NextConflict(ctx)

	if err != nil {
		return nil, err
	}

	return sqlutil.DoltRowToSqlRow(cnf, itr.rd.GetSchema())
}

// Close the iterator.
func (itr conflictRowIter) Close(*sql.Context) error {
	return itr.rd.Close()
}

type conflictDeleter struct {
	ct  ConflictsTable
	rs  RootSetter
	pks []types.Value
}

var _ sql.RowDeleter = &conflictDeleter{}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (cd *conflictDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	cnfSch := cd.ct.rd.GetSchema()
	// We could use a test VRW, but as any values which use VRWs will already exist, we can potentially save on memory usage
	cnfRow, err := sqlutil.SqlRowToDoltRow(ctx, cd.ct.tbl.ValueReadWriter(), r, cnfSch)

	if err != nil {
		return err
	}

	pkVal, err := cd.ct.rd.GetKeyForConflict(ctx, cnfRow)

	if err != nil {
		return err
	}

	cd.pks = append(cd.pks, pkVal)
	return nil
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (cd *conflictDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (cd *conflictDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (cd *conflictDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (cd *conflictDeleter) Close(ctx *sql.Context) error {
	_, _, updatedTbl, err := cd.ct.tbl.ResolveConflicts(ctx, cd.pks)

	if err != nil {
		if errors.Is(err, doltdb.ErrNoConflictsResolved) {
			return nil
		}

		return err
	}

	updatedRoot, err := cd.ct.root.PutTable(ctx, doltdb.TableName{Name: cd.ct.tblName}, updatedTbl)

	if err != nil {
		return err
	}

	return cd.rs.SetRoot(ctx, updatedRoot)
}
