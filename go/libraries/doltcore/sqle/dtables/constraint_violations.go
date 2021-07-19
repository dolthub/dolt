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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

// ConstraintViolationsTable is a sql.Table implementation that provides access to the constraint violations that exist
// for a user table.
type ConstraintViolationsTable struct {
	tblName string
	root    *doltdb.RootValue
	dSch    schema.Schema
	sqlSch  sql.Schema
	tbl     *doltdb.Table
	rs      RootSetter
}

const varcharMaxByteLength = 65535

var _ sql.Table = (*ConstraintViolationsTable)(nil)
var _ sql.DeletableTable = (*ConstraintViolationsTable)(nil)

// NewConstraintViolationsTable returns a new ConstraintViolationsTable.
func NewConstraintViolationsTable(ctx *sql.Context, tblName string, root *doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	tbl, ok, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(tblName)
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	typeType, err := typeinfo.FromSqlType(
		sql.MustCreateEnumType([]string{"foreign key", "unique index", "check constraint"}, sql.Collation_Default))
	if err != nil {
		return nil, err
	}
	typeCol, err := schema.NewColumnWithTypeInfo("violation_type", schema.DoltConstraintViolationsTypeTag, typeType, true, "", false, "")
	if err != nil {
		return nil, err
	}
	infoCol, err := schema.NewColumnWithTypeInfo("violation_info", schema.DoltConstraintViolationsInfoTag, typeinfo.JSONType, false, "", false, "")
	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection()
	colColl = colColl.Append(typeCol)
	for _, col := range sch.GetAllCols().GetColumns() {
		col.IsPartOfPK = true
		colColl = colColl.Append(col)
	}
	colColl = colColl.Append(infoCol)
	dSch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema(doltdb.DoltConstViolTablePrefix+tblName, dSch)
	if err != nil {
		return nil, err
	}
	return &ConstraintViolationsTable{
		tblName: tblName,
		root:    root,
		dSch:    dSch,
		sqlSch:  sqlSch,
		tbl:     tbl,
		rs:      rs,
	}, nil
}

// Name implements the interface sql.Table.
func (cvt *ConstraintViolationsTable) Name() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName
}

// String implements the interface sql.Table.
func (cvt *ConstraintViolationsTable) String() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName
}

// Schema implements the interface sql.Table.
func (cvt *ConstraintViolationsTable) Schema() sql.Schema {
	return cvt.sqlSch
}

// Partitions implements the interface sql.Table.
func (cvt *ConstraintViolationsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sqlutil.NewSinglePartitionIter(types.EmptyMap), nil
}

// PartitionRows implements the interface sql.Table.
func (cvt *ConstraintViolationsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cvMap, err := cvt.tbl.GetConstraintViolations(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := cvMap.Iterator(ctx)
	if err != nil {
		return nil, err
	}
	return &constraintViolationsIter{ctx, cvt.dSch, iter}, nil
}

// Deleter implements the interface sql.DeletableTable.
func (cvt *ConstraintViolationsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	cvMap, err := cvt.tbl.GetConstraintViolations(ctx)
	if err != nil {
		panic(err)
	}
	return &constraintViolationsDeleter{cvt, cvMap.Edit()}
}

// constraintViolationsIter is the iterator for ConstraintViolationsTable.
type constraintViolationsIter struct {
	ctx  *sql.Context
	dSch schema.Schema
	iter types.MapIterator
}

var _ sql.RowIter = (*constraintViolationsIter)(nil)

// Next implements the interface sql.RowIter.
func (cvi *constraintViolationsIter) Next() (sql.Row, error) {
	k, v, err := cvi.iter.NextTuple(cvi.ctx)
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

// constraintViolationsDeleter handles deletions on the generated ConstraintViolationsTable.
type constraintViolationsDeleter struct {
	cvt    *ConstraintViolationsTable
	editor *types.MapEditor
}

var _ sql.RowDeleter = (*constraintViolationsDeleter)(nil)

// Delete implements the interface sql.RowDeleter.
func (cvd *constraintViolationsDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, cvd.cvt.tbl.ValueReadWriter(), r, cvd.cvt.dSch)
	if err != nil {
		return err
	}
	key, err := dRow.NomsMapKey(cvd.cvt.dSch).Value(ctx)
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
