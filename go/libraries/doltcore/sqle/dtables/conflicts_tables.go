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
	"context"
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var _ sql.Table = ConflictsTable{}

// ConflictsTable is a sql.Table implementation that provides access to the conflicts that exist for a user table
type ConflictsTable struct {
	tblName string
	sqlSch  sql.PrimaryKeySchema
	root    *doltdb.RootValue
	tbl     *doltdb.Table
	rd      *merge.ConflictReader
	confIdx durable.ConflictIndex
	confSch conflict.ConflictSchema
	rs      RootSetter
}

type RootSetter interface {
	SetRoot(ctx *sql.Context, root *doltdb.RootValue) error
}

// NewConflictsTable returns a new ConflictsTableTable instance
func NewConflictsTable(ctx *sql.Context, tblName string, root *doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	tbl, tblName, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(tblName)
	}

	schs, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}
	if schs.Base == nil || schs.Schema == nil || schs.MergeSchema == nil {
		schs.Base, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		schs.Schema, schs.MergeSchema = schs.Base, schs.Base
	}

	var rd *merge.ConflictReader
	var confSch schema.Schema
	if tbl.Format() == types.Format_DOLT_1 {
		confSch, err = CalculateConflictSchema(schs.Base, schs.Schema, schs.MergeSchema)
		if err != nil {
			return nil, err
		}
	} else {
		rd, err = merge.NewConflictReader(ctx, tbl)
		if err != nil {
			return nil, err
		}
		confSch = rd.GetSchema()
	}

	sqlSch, err := sqlutil.FromDoltSchema(doltdb.DoltConfTablePrefix+tblName, confSch)
	if err != nil {
		return nil, err
	}

	return ConflictsTable{
		tblName: tblName,
		sqlSch:  sqlSch,
		root:    root,
		tbl:     tbl,
		rd:      rd,
		confIdx: confIdx,
		confSch: schs,
		rs:      rs,
	}, nil
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

// Partitions returns a PartitionIter which can be used to get all the data partitions
func (ct ConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows returns a RowIter for the given partition
func (ct ConflictsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if ct.tbl.Format() == types.Format_DOLT_1 {
		return newProllyConflictRowIter(ctx, durable.ProllyMapFromConflictIndex(ct.confIdx), ct.confSch.Base, ct.confSch.Schema, ct.confSch.MergeSchema)
	}

	return conflictRowIter{ct.rd}, nil
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (ct ConflictsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	if ct.tbl.Format() == types.Format_DOLT_1 {
		return newProllyConflictDeleter(ct)
	} else {
		return &conflictDeleter{ct: ct, rs: ct.rs}
	}
}

type prollyConflictRowIter struct {
	confItr                  prolly.ConflictIter
	kd                       val.TupleDesc
	baseVD, oursVD, theirsVD val.TupleDesc
	// offsets for each version
	b, o, t int
	n       int
}

func newProllyConflictRowIter(ctx context.Context, conflictMap prolly.ConflictMap, baseSch, ourSch, theirSch schema.Schema) (prollyConflictRowIter, error) {
	iter, err := conflictMap.IterAll(ctx)
	if err != nil {
		return prollyConflictRowIter{}, err
	}

	kd := shim.KeyDescriptorFromSchema(baseSch)
	baseVD := shim.ValueDescriptorFromSchema(baseSch)
	oursVD := shim.ValueDescriptorFromSchema(ourSch)
	theirsVD := shim.ValueDescriptorFromSchema(theirSch)

	b := 0
	o := kd.Count() + baseVD.Count()
	t := o + kd.Count() + oursVD.Count()
	n := o + t + kd.Count() + theirsVD.Count()

	return prollyConflictRowIter{
		confItr:  iter,
		kd:       kd,
		baseVD:   baseVD,
		oursVD:   oursVD,
		theirsVD: theirsVD,
		b:        b,
		o:        o,
		t:        t,
		n:        n,
	}, nil
}

var _ sql.RowIter = prollyConflictRowIter{}

func (itr prollyConflictRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, v, err := itr.confItr.Next(ctx)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, itr.n)

	for i := 0; i < itr.kd.Count(); i++ {
		f, err := index.GetField(itr.kd, i, k)
		if err != nil {
			return nil, err
		}
		r[itr.b+i], r[itr.o+i], r[itr.t+i] = f, f, f
	}

	tup := v.BaseValue()
	for i := 0; i < itr.baseVD.Count(); i++ {
		f, err := index.GetField(itr.baseVD, i, tup)
		if err != nil {
			return nil, err
		}
		r[itr.b+itr.kd.Count()+i] = f
	}
	tup = v.OurValue()
	for i := 0; i < itr.oursVD.Count(); i++ {
		f, err := index.GetField(itr.oursVD, i, tup)
		if err != nil {
			return nil, err
		}
		r[itr.o+itr.kd.Count()+i] = f
	}
	tup = v.TheirValue()
	for i := 0; i < itr.theirsVD.Count(); i++ {
		f, err := index.GetField(itr.theirsVD, i, tup)
		if err != nil {
			return nil, err
		}
		r[itr.t+itr.kd.Count()+i] = f
	}

	return r, nil
}

func (itr prollyConflictRowIter) Close(ctx *sql.Context) error {
	return nil
}

type prollyConflictDeleter struct {
	kd             val.TupleDesc
	kB             *val.TupleBuilder
	pool           pool.BuffPool
	ed             prolly.ConflictEditor
	ct             ConflictsTable
	rs             RootSetter
	conflictSchema conflict.ConflictSchema
}

func newProllyConflictDeleter(ct ConflictsTable) *prollyConflictDeleter {
	conflictMap := durable.ProllyMapFromConflictIndex(ct.confIdx)
	kd, _, _, _ := conflictMap.Descriptors()
	ed := conflictMap.Editor()
	kB := val.NewTupleBuilder(kd)
	p := conflictMap.Pool()
	return &prollyConflictDeleter{
		kd:             kd,
		kB:             kB,
		pool:           p,
		ed:             ed,
		ct:             ct,
		conflictSchema: ct.confSch,
	}
}

func (cd *prollyConflictDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	// first columns are the keys
	for i := 0; i < cd.kd.Count(); i++ {
		err := index.PutField(cd.kB, i, r[i])
		if err != nil {
			return err
		}
	}

	key := cd.kB.Build(cd.pool)
	err := cd.ed.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (cd *prollyConflictDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (cd *prollyConflictDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (cd *prollyConflictDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (cd *prollyConflictDeleter) Close(ctx *sql.Context) error {
	conflicts, err := cd.ed.Flush(ctx)
	if err != nil {
		return err
	}

	// TODO: We can delete from more than one table in a single statement. Root
	// updates should be restricted to write session and not individual table
	// editors.

	// TODO (dhruv): move this code into some kind of ResolveConflicts function
	var updatedTbl *doltdb.Table
	if conflicts.Count() == 0 {
		updatedTbl, err = cd.ct.tbl.ClearConflicts(ctx)
		if err != nil {
			return err
		}
	} else {
		updatedTbl, err = cd.ct.tbl.SetConflicts(ctx, cd.conflictSchema, durable.ConflictIndexFromProllyMap(conflicts))
		if err != nil {
			return err
		}
	}

	updatedRoot, err := cd.ct.root.PutTable(ctx, cd.ct.tblName, updatedTbl)
	if err != nil {
		return err
	}

	return cd.ct.rs.SetRoot(ctx, updatedRoot)
}

type conflictRowIter struct {
	rd *merge.ConflictReader
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr conflictRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	cnf, _, err := itr.rd.NextConflict(ctx)

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

	updatedRoot, err := cd.ct.root.PutTable(ctx, cd.ct.tblName, updatedTbl)

	if err != nil {
		return err
	}

	return cd.rs.SetRoot(ctx, updatedRoot)
}

func CalculateConflictSchema(base, ours, theirs schema.Schema) (schema.Schema, error) {
	cols := make([]schema.Column, ours.GetAllCols().Size()+theirs.GetAllCols().Size()+base.GetAllCols().Size())

	i := 0
	putWithPrefix := func(prefix string, sch schema.Schema) error {
		err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			c, err := schema.NewColumnWithTypeInfo(prefix+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
			if err != nil {
				return true, err
			}
			cols[i] = c
			i++
			return false, nil
		})
		if err != nil {
			return err
		}
		err = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			c, err := schema.NewColumnWithTypeInfo(prefix+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
			if err != nil {
				return true, err
			}
			cols[i] = c
			i++
			return false, nil
		})
		return err
	}

	err := putWithPrefix("base_", base)
	if err != nil {
		return nil, err
	}
	err = putWithPrefix("ours_", ours)
	if err != nil {
		return nil, err
	}
	err = putWithPrefix("theirs_", theirs)
	if err != nil {
		return nil, err
	}

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
