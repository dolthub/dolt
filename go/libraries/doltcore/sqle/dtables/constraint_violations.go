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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

// NewConstraintViolationsTable returns a sql.Table that lists constraint violations.
func NewConstraintViolationsTable(ctx *sql.Context, tblName doltdb.TableName, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	if root.VRW().Format() == types.Format_DOLT {
		return newProllyCVTable(ctx, tblName, root, rs)
	}

	return newNomsCVTable(ctx, tblName, root, rs)
}

func newNomsCVTable(ctx *sql.Context, tblName doltdb.TableName, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	var tbl *doltdb.Table
	var err error
	tbl, tblName, err = mustGetTableInsensitive(ctx, root, tblName)
	if err != nil {
		return nil, err
	}
	cvSch, err := tbl.GetConstraintViolationsSchema(ctx)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema("", doltdb.DoltConstViolTablePrefix+tblName.Name, cvSch)
	if err != nil {
		return nil, err
	}

	return &constraintViolationsTable{
		tblName: tblName,
		root:    root,
		cvSch:   cvSch,
		sqlSch:  sqlSch,
		tbl:     tbl,
		rs:      rs,
	}, nil
}

// constraintViolationsTable is a sql.Table implementation that provides access to the constraint violations that exist
// for a user table for the old format.
type constraintViolationsTable struct {
	tblName doltdb.TableName
	root    doltdb.RootValue
	cvSch   schema.Schema
	sqlSch  sql.PrimaryKeySchema
	tbl     *doltdb.Table
	rs      RootSetter
}

var _ sql.Table = (*constraintViolationsTable)(nil)
var _ sql.DeletableTable = (*constraintViolationsTable)(nil)

// Name implements the interface sql.Table.
func (cvt *constraintViolationsTable) Name() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName.Name
}

// String implements the interface sql.Table.
func (cvt *constraintViolationsTable) String() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName.Name
}

// Schema implements the interface sql.Table.
func (cvt *constraintViolationsTable) Schema() sql.Schema {
	return cvt.sqlSch.Schema
}

// Collation implements the interface sql.Table.
func (cvt *constraintViolationsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (cvt *constraintViolationsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (cvt *constraintViolationsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cvMap, err := cvt.tbl.GetConstraintViolations(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := cvMap.Iterator(ctx)
	if err != nil {
		return nil, err
	}
	return &constraintViolationsIter{cvt.cvSch, iter}, nil
}

// Deleter implements the interface sql.DeletableTable.
func (cvt *constraintViolationsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	cvMap, err := cvt.tbl.GetConstraintViolations(ctx)
	if err != nil {
		panic(err)
	}
	return &constraintViolationsDeleter{cvt, cvMap.Edit()}
}

// constraintViolationsIter is the iterator for constraintViolationsTable.
type constraintViolationsIter struct {
	dSch schema.Schema
	iter types.MapIterator
}

var _ sql.RowIter = (*constraintViolationsIter)(nil)

// Next implements the interface sql.RowIter.
func (cvi *constraintViolationsIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, v, err := cvi.iter.NextTuple(ctx)
	if err != nil {
		return nil, err
	}
	dRow, err := row.FromNoms(cvi.dSch, k, v)
	if err != nil {
		return nil, err
	}
	return sqlutil.DoltRowToSqlRow(dRow, cvi.dSch)
}

// Close implements the interface sql.RowIter.
func (cvi *constraintViolationsIter) Close(*sql.Context) error {
	return nil
}

// constraintViolationsDeleter handles deletions on the generated constraintViolationsTable.
type constraintViolationsDeleter struct {
	cvt    *constraintViolationsTable
	editor *types.MapEditor
}

var _ sql.RowDeleter = (*constraintViolationsDeleter)(nil)

// Delete implements the interface sql.RowDeleter.
func (cvd *constraintViolationsDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, cvd.cvt.tbl.ValueReadWriter(), r, cvd.cvt.cvSch)
	if err != nil {
		return err
	}
	key, err := dRow.NomsMapKey(cvd.cvt.cvSch).Value(ctx)
	if err != nil {
		return err
	}
	cvd.editor.Remove(key)
	return nil
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (cvd *constraintViolationsDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (cvd *constraintViolationsDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (cvd *constraintViolationsDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close implements the interface sql.RowDeleter.
func (cvd *constraintViolationsDeleter) Close(ctx *sql.Context) error {
	updatedMap, err := cvd.editor.Map(ctx)
	if err != nil {
		return err
	}
	updatedTbl, err := cvd.cvt.tbl.SetConstraintViolations(ctx, updatedMap)
	if err != nil {
		return err
	}
	updatedRoot, err := cvd.cvt.root.PutTable(ctx, cvd.cvt.tblName, updatedTbl)
	if err != nil {
		return err
	}
	return cvd.cvt.rs.SetRoot(ctx, updatedRoot)
}
