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
	"context"
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func getDoltConstraintViolationsBaseSqlSchema() sql.Schema {
	return []*sql.Column{
		{Name: "from_root_ish", Type: gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 1023), PrimaryKey: false, Nullable: true},
		{Name: "violation_type", Type: gmstypes.MustCreateEnumType([]string{"foreign key", "unique index", "check constraint", "not null"}, sql.Collation_Default), PrimaryKey: true},
	}
}

// GetDoltConstraintViolationsBaseSqlSchema returns the base schema for the dolt_constraint_violations_* system table.
// This is used by Doltgres to update the dolt_constraint_violations_* schema using Doltgres types.
var GetDoltConstraintViolationsBaseSqlSchema = getDoltConstraintViolationsBaseSqlSchema

// getConstraintViolationsSchema returns a table's dolt_constraint_violations system table schema.
func getConstraintViolationsSchema(ctx context.Context, t *doltdb.Table, tn doltdb.TableName, root doltdb.RootValue) (schema.Schema, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	baseSch := sql.NewPrimaryKeySchema(GetDoltConstraintViolationsBaseSqlSchema())
	baseDoltSch, err := sqlutil.ToDoltSchema(ctx, root, tn, baseSch, root, sql.Collation_Default)
	if err != nil {
		return nil, err
	}
	baseColColl := baseDoltSch.GetAllCols()
	baseCols := baseColColl.GetColumns()

	schSize := sch.GetAllCols().Size()
	if schema.IsKeyless(sch) {
		// Keyless tables have an additional dolt_row_hash column
		schSize += 1
	}

	cols := make([]schema.Column, 0, baseColColl.Size()+schSize)
	cols = append(cols, baseCols[0:2]...)
	infoCol, err := schema.NewColumnWithTypeInfo("violation_info", schema.DoltConstraintViolationsInfoTag, typeinfo.JSONType, false, "", false, "")
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		// If this is a keyless table, we need to add a new column for the keyless table's generated row hash.
		// We need to add this internal row hash value, in order to guarantee a unique primary key in the
		// constraint violations table.
		cols = append(cols, schema.NewColumn("dolt_row_hash", 0, types.BlobKind, true))
	} else {
		cols = append(cols, sch.GetPKCols().GetColumns()...)
	}
	cols = append(cols, sch.GetNonPKCols().GetColumns()...)
	cols = append(cols, infoCol)

	return schema.NewSchema(schema.NewColCollection(cols...), nil, schema.Collation_Default, nil, nil)
}

func newProllyCVTable(ctx *sql.Context, tblName doltdb.TableName, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	var tbl *doltdb.Table
	var err error
	tbl, tblName, err = getTableInsensitiveOrError(ctx, root, tblName)
	if err != nil {
		return nil, err
	}
	cvSch, err := getConstraintViolationsSchema(ctx, tbl, tblName, root)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema("", doltdb.DoltConstViolTablePrefix+tblName.Name, cvSch)
	if err != nil {
		return nil, err
	}

	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(arts)
	return &prollyConstraintViolationsTable{
		tblName: tblName,
		root:    root,
		sqlSch:  sqlSch,
		tbl:     tbl,
		rs:      rs,
		artM:    m,
	}, nil
}

// prollyConstraintViolationsTable is a sql.Table implementation that provides access to the constraint violations that exist
// for a user table for the v1 format.
type prollyConstraintViolationsTable struct {
	tblName doltdb.TableName
	root    doltdb.RootValue
	sqlSch  sql.PrimaryKeySchema
	tbl     *doltdb.Table
	rs      RootSetter
	artM    prolly.ArtifactMap
}

var _ sql.Table = (*prollyConstraintViolationsTable)(nil)
var _ sql.DeletableTable = (*prollyConstraintViolationsTable)(nil)

// Name implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Name() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName.Name
}

// String implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) String() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName.Name
}

// Schema implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Schema() sql.Schema {
	return cvt.sqlSch.Schema
}

// Collation implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (cvt *prollyConstraintViolationsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	idx, err := cvt.tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(idx)
	itr, err := m.IterAllCVs(ctx)
	if err != nil {
		return nil, err
	}
	sch, err := cvt.tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	kd, vd := sch.GetMapDescriptors()

	// value tuples encoded in ConstraintViolationMeta may
	// violate the not null constraints assumed by fixed access
	kd = kd.WithoutFixedAccess()
	vd = vd.WithoutFixedAccess()

	return prollyCVIter{
		itr: itr,
		sch: sch,
		kd:  kd,
		vd:  vd,
		ns:  cvt.artM.NodeStore(),
	}, nil
}

func (cvt *prollyConstraintViolationsTable) Deleter(context *sql.Context) sql.RowDeleter {
	ed := cvt.artM.Editor()
	p := cvt.artM.Pool()
	kd, _ := cvt.artM.Descriptors()
	kb := val.NewTupleBuilder(kd)

	return &prollyCVDeleter{
		kd:   kd,
		kb:   kb,
		ed:   ed,
		pool: p,
		cvt:  cvt,
	}
}

type prollyCVIter struct {
	itr    prolly.ArtifactIter
	sch    schema.Schema
	kd, vd val.TupleDesc
	ns     tree.NodeStore
}

func (itr prollyCVIter) Next(ctx *sql.Context) (sql.Row, error) {
	art, err := itr.itr.Next(ctx)
	if err != nil {
		return nil, err
	}

	// In addition to the table's columns, the constraint violations table adds
	// three more columns: from_root_ish, violation_type, and violation_info
	additionalColumns := 3
	if schema.IsKeyless(itr.sch) {
		// If this is for a keyless table, then there is no PK in the schema, so we
		// add one additional column for the generated hash. This is necessary for
		// being able to uniquely identify rows in the constraint violations table.
		additionalColumns++
	}

	r := make(sql.UntypedSqlRow, itr.sch.GetAllCols().Size()+additionalColumns)
	r.SetValue(0, art.SourceRootish.String())
	r.SetValue(1, merge.MapCVType(art.ArtType))

	var meta prolly.ConstraintViolationMeta
	err = json.Unmarshal(art.Metadata, &meta)
	if err != nil {
		return nil, err
	}

	var v interface{}
	o := 2
	if !schema.IsKeyless(itr.sch) {
		for i := 0; i < itr.kd.Count(); i++ {
			v, err = tree.GetField(ctx, itr.kd, i, art.SourceKey, itr.ns)
			if err != nil {
				return nil, err
			}
			r.SetValue(o+i, v)
		}
		o += itr.kd.Count()

		for i := 0; i < itr.vd.Count(); i++ {
			v, err = tree.GetField(ctx, itr.vd, i, meta.Value, itr.ns)
			if err != nil {
				return nil, err
			}
			r.SetValue(o+i, v)
		}
		o += itr.vd.Count()
	} else {
		// For a keyless table, we still need a key to uniquely identify the row in the constraint
		// violation table, so we add in the unique hash for the row.
		v, err = tree.GetField(ctx, itr.kd, 0, art.SourceKey, itr.ns)
		if err != nil {
			return nil, err
		}
		r.SetValue(o, v)
		o += 1

		for i := 0; i < itr.vd.Count()-1; i++ {
			v, err = tree.GetField(ctx, itr.vd, i+1, meta.Value, itr.ns)
			if err != nil {
				return nil, err
			}
			r.SetValue(o+i, v)
		}
		o += itr.vd.Count() - 1
	}

	switch art.ArtType {
	case prolly.ArtifactTypeForeignKeyViol:
		var m merge.FkCVMeta
		err = json.Unmarshal(meta.VInfo, &m)
		if err != nil {
			return nil, err
		}
		r.SetValue(o, m)
	case prolly.ArtifactTypeUniqueKeyViol:
		var m merge.UniqCVMeta
		err = json.Unmarshal(meta.VInfo, &m)
		if err != nil {
			return nil, err
		}
		r.SetValue(o, m)
	case prolly.ArtifactTypeNullViol:
		var m merge.NullViolationMeta
		err = json.Unmarshal(meta.VInfo, &m)
		if err != nil {
			return nil, err
		}
		r.SetValue(o, m)
	case prolly.ArtifactTypeChkConsViol:
		var m merge.CheckCVMeta
		err = json.Unmarshal(meta.VInfo, &m)
		if err != nil {
			return nil, err
		}
		r.SetValue(o, m)
	default:
		panic("json not implemented for artifact type")
	}

	return r, nil
}

type prollyCVDeleter struct {
	kd   val.TupleDesc
	kb   *val.TupleBuilder
	pool pool.BuffPool
	ed   *prolly.ArtifactsEditor
	cvt  *prollyConstraintViolationsTable
}

var _ sql.RowDeleter = (*prollyCVDeleter)(nil)

// Delete implements the interface sql.RowDeleter.
func (d *prollyCVDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	// When we delete a row, we need to build the primary key from the row data.
	// The PK has 3+ fields: from_root_ish, violation_type, plus all PK fields from the source table.
	// If the source table is keyless and has no PK, then we use the unique row hash provided by keyless tables.
	for i := 0; i < d.kd.Count()-2; i++ {
		err := tree.PutField(ctx, d.cvt.artM.NodeStore(), d.kb, i, r.GetValue(i+2))
		if err != nil {
			return err
		}
	}

	// then the hash
	h := hash.Parse(r.GetValue(0).(string))
	d.kb.PutCommitAddr(d.kd.Count()-2, h)

	// Finally the artifact type
	artType := merge.UnmapCVType(merge.CvType(r.GetValue(1).(uint64)))
	d.kb.PutUint8(d.kd.Count()-1, uint8(artType))

	key := d.kb.Build(d.pool)
	err := d.ed.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close implements the interface sql.RowDeleter.
func (d *prollyCVDeleter) Close(ctx *sql.Context) error {
	arts, err := d.ed.Flush(ctx)
	if err != nil {
		return err
	}

	// TODO: We can delete from more than one table in a single statement. Root
	// updates should be restricted to write session and not individual table
	// editors.

	updatedTbl, err := d.cvt.tbl.SetArtifacts(ctx, durable.ArtifactIndexFromProllyMap(arts))
	if err != nil {
		return err
	}

	updatedRoot, err := d.cvt.root.PutTable(ctx, d.cvt.tblName, updatedTbl)
	if err != nil {
		return err
	}

	return d.cvt.rs.SetRoot(ctx, updatedRoot)
}

func (itr prollyCVIter) Close(ctx *sql.Context) error {
	return nil
}
