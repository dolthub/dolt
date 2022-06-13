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
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func newProllyConflictsTable(ctx *sql.Context, tbl *doltdb.Table, tblName string, root *doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(arts)

	baseSch, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx)
	if err != nil {
		return nil, err
	}
	confSch, err := CalculateConflictSchema(baseSch, ourSch, theirSch)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema(doltdb.DoltConfTablePrefix+tblName, confSch)
	if err != nil {
		return nil, err
	}

	return ProllyConflictsTable{
		tblName:  tblName,
		sqlSch:   sqlSch,
		baseSch:  baseSch,
		ourSch:   ourSch,
		theirSch: theirSch,
		root:     root,
		tbl:      tbl,
		rs:       rs,
		artM:     m,
	}, nil
}

// ProllyConflictsTable is a sql.Table implementation that uses the merge
// artifacts table to persist and read conflicts.
type ProllyConflictsTable struct {
	tblName                   string
	sqlSch                    sql.PrimaryKeySchema
	baseSch, ourSch, theirSch schema.Schema
	root                      *doltdb.RootValue
	tbl                       *doltdb.Table
	rs                        RootSetter
	artM                      prolly.ArtifactMap
}

func (ct ProllyConflictsTable) Name() string {
	return doltdb.DoltConfTablePrefix + ct.tblName
}

func (ct ProllyConflictsTable) String() string {
	return doltdb.DoltConfTablePrefix + ct.tblName
}

func (ct ProllyConflictsTable) Schema() sql.Schema {
	return ct.sqlSch.Schema
}

func (ct ProllyConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (ct ProllyConflictsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return newProllyConflictRowIter(ctx, ct)
}

func (ct ProllyConflictsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return newProllyConflictDeleter(ct)
}

type prollyConflictRowIter struct {
	itr     prolly.ConflictArtifactIter
	vrw     types.ValueReadWriter
	ourRows prolly.Map

	baseHash, theirHash []byte
	baseRows            prolly.Map
	theirRows           prolly.Map

	kd                       val.TupleDesc
	baseVD, oursVD, theirsVD val.TupleDesc
	// offsets for each version
	b, o, t int
	n       int
}

var _ sql.RowIter = &prollyConflictRowIter{}

func newProllyConflictRowIter(ctx *sql.Context, ct ProllyConflictsTable) (*prollyConflictRowIter, error) {
	idx, err := ct.tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ourRows := durable.ProllyMapFromIndex(idx)

	itr, err := ct.artM.IterAllConflicts(ctx)
	if err != nil {
		return nil, err
	}

	kd := shim.KeyDescriptorFromSchema(ct.baseSch)
	baseVD := shim.ValueDescriptorFromSchema(ct.baseSch)
	oursVD := shim.ValueDescriptorFromSchema(ct.ourSch)
	theirsVD := shim.ValueDescriptorFromSchema(ct.theirSch)

	b := 1
	o := b + kd.Count() + baseVD.Count()
	t := o + kd.Count() + oursVD.Count()
	n := t + kd.Count() + theirsVD.Count()

	return &prollyConflictRowIter{
		itr:      itr,
		vrw:      ct.tbl.ValueReadWriter(),
		ourRows:  ourRows,
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

func (itr *prollyConflictRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	c, err := itr.nextConflictVals(ctx)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, itr.n)
	r[0] = c.h.String()

	for i := 0; i < itr.kd.Count(); i++ {
		f, err := index.GetField(itr.kd, i, c.k)
		if err != nil {
			return nil, err
		}
		if c.bV != nil {
			r[itr.b+i] = f
		}
		if c.oV != nil {
			r[itr.o+i] = f
		}
		if c.tV != nil {
			r[itr.t+i] = f
		}
	}

	if c.bV != nil {
		for i := 0; i < itr.baseVD.Count(); i++ {
			f, err := index.GetField(itr.baseVD, i, c.bV)
			if err != nil {
				return nil, err
			}
			r[itr.b+itr.kd.Count()+i] = f
		}
	}

	if c.oV != nil {
		for i := 0; i < itr.oursVD.Count(); i++ {
			f, err := index.GetField(itr.oursVD, i, c.oV)
			if err != nil {
				return nil, err
			}
			r[itr.o+itr.kd.Count()+i] = f
		}
	}

	if c.tV != nil {
		for i := 0; i < itr.theirsVD.Count(); i++ {
			f, err := index.GetField(itr.theirsVD, i, c.tV)
			if err != nil {
				return nil, err
			}
			r[itr.t+itr.kd.Count()+i] = f
		}
	}

	return r, nil
}

type conf struct {
	k, bV, oV, tV val.Tuple
	h             hash.Hash
}

func (itr *prollyConflictRowIter) nextConflictVals(ctx *sql.Context) (c conf, err error) {
	ca, err := itr.itr.Next(ctx)
	if err != nil {
		return conf{}, err
	}
	c.k = ca.Key
	c.h = hash.New(ca.HeadCmHash)

	err = itr.loadTableMaps(ctx, ca.Metadata)
	if err != nil {
		return conf{}, err
	}

	err = itr.baseRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.bV = v
		return nil
	})
	if err != nil {
		return conf{}, err
	}
	err = itr.ourRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.oV = v
		return nil
	})
	if err != nil {
		return conf{}, err
	}
	err = itr.theirRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.tV = v
		return nil
	})
	if err != nil {
		return conf{}, err
	}

	return c, nil
}

// loadTableMaps loads the maps specified in the metadata if they are different from
// the currently loaded maps.
func (itr *prollyConflictRowIter) loadTableMaps(ctx context.Context, meta prolly.ConflictMetadata) error {
	if bytes.Compare(itr.baseHash, meta.BaseTblHash) != 0 {
		baseTbl, err := durable.TableFromAddr(ctx, itr.vrw, hash.New(meta.BaseTblHash))
		if err != nil {
			return err
		}
		idx, err := baseTbl.GetTableRows(ctx)
		if err != nil {
			return err
		}
		itr.baseRows = durable.ProllyMapFromIndex(idx)
		itr.baseHash = meta.BaseTblHash
	}

	if bytes.Compare(itr.theirHash, meta.TheirTblHash) != 0 {
		theirTbl, err := durable.TableFromAddr(ctx, itr.vrw, hash.New(meta.TheirTblHash))
		if err != nil {
			return err
		}
		idx, err := theirTbl.GetTableRows(ctx)
		if err != nil {
			return err
		}
		itr.theirRows = durable.ProllyMapFromIndex(idx)
		itr.theirHash = meta.TheirTblHash
	}

	return nil
}

func (itr *prollyConflictRowIter) Close(ctx *sql.Context) error {
	return nil
}

type prollyConflictDeleter struct {
	kd                        val.TupleDesc
	kB                        *val.TupleBuilder
	pool                      pool.BuffPool
	ed                        prolly.ArtifactsEditor
	ct                        ProllyConflictsTable
	rs                        RootSetter
	baseSch, ourSch, theirSch schema.Schema
}

func newProllyConflictDeleter(ct ProllyConflictsTable) *prollyConflictDeleter {
	kd, _ := ct.artM.Descriptors()
	ed := ct.artM.Editor()
	kB := val.NewTupleBuilder(kd)
	p := ct.artM.Pool()
	return &prollyConflictDeleter{
		kd:   kd,
		kB:   kB,
		pool: p,
		ed:   ed,
		ct:   ct,
	}
}

func (cd *prollyConflictDeleter) Delete(ctx *sql.Context, r sql.Row) error {

	// get keys from either base, ours, or theirs
	o := func() int {
		if o := 1; r[o] != nil {
			return o
		} else if o = 1 + cd.kd.Count(); r[o] != nil {
			return o
		} else if o = 1 + cd.kd.Count()*2; r[o] != nil {
			return o
		} else {
			panic("neither base, ours, or theirs had a key")
		}
	}()

	// first part of the artifact key is the keys of the source table
	for i := 0; i < cd.kd.Count()-2; i++ {
		err := index.PutField(cd.kB, i, r[o+i])
		if err != nil {
			return err
		}
	}

	// then the hash follows. It is the first column of the row and the second to last in the key
	h := hash.Parse(r[0].(string))
	cd.kB.PutAddress(cd.kd.Count()-2, h[:])

	// Finally the artifact type which is always a conflict
	cd.kB.PutUint8(cd.kd.Count()-1, uint8(prolly.ArtifactTypeConflict))

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
	arts, err := cd.ed.Flush(ctx)
	if err != nil {
		return err
	}

	// TODO: We can delete from more than one table in a single statement. Root
	// updates should be restricted to write session and not individual table
	// editors.

	updatedTbl, err := cd.ct.tbl.SetArtifacts(ctx, durable.ArtifactIndexFromProllyMap(arts))
	if err != nil {
		return err
	}

	updatedRoot, err := cd.ct.root.PutTable(ctx, cd.ct.tblName, updatedTbl)
	if err != nil {
		return err
	}

	return cd.ct.rs.SetRoot(ctx, updatedRoot)
}

func CalculateConflictSchema(base, ours, theirs schema.Schema) (schema.Schema, error) {
	cols := make([]schema.Column, 1+ours.GetAllCols().Size()+theirs.GetAllCols().Size()+base.GetAllCols().Size())

	// the commit hash of the left branch's head when the conflict was created
	cols[0] = schema.NewColumn("created_at_cm", 0, types.StringKind, false)

	i := 1
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
	err = putWithPrefix("our_", ours)
	if err != nil {
		return nil, err
	}
	err = putWithPrefix("their_", theirs)
	if err != nil {
		return nil, err
	}

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
