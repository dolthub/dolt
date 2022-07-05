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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
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

	baseSch, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
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
	tblName string
	vrw     types.ValueReadWriter
	ourRows prolly.Map
	keyless bool

	kd                       val.TupleDesc
	baseVD, oursVD, theirsVD val.TupleDesc
	// offsets for each version
	b, o, t int
	n       int

	baseHash, theirHash hash.Hash
	baseRows            prolly.Map
	theirRows           prolly.Map
}

var _ sql.RowIter = &prollyConflictRowIter{}

// base_cols, our_cols, our_diff_type, their_cols, their_diff_type
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

	keyless := schema.IsKeyless(ct.ourSch)

	kd := shim.KeyDescriptorFromSchema(ct.baseSch)
	baseVD := shim.ValueDescriptorFromSchema(ct.baseSch)
	oursVD := shim.ValueDescriptorFromSchema(ct.ourSch)
	theirsVD := shim.ValueDescriptorFromSchema(ct.theirSch)

	b := 1
	var o, t, n int
	if !keyless {
		o = b + kd.Count() + baseVD.Count()
		t = o + kd.Count() + oursVD.Count() + 1
		n = t + kd.Count() + theirsVD.Count() + 1
	} else {
		o = b + baseVD.Count() - 1
		t = o + oursVD.Count()
		n = t + theirsVD.Count()
	}

	return &prollyConflictRowIter{
		itr:      itr,
		tblName:  ct.tblName,
		vrw:      ct.tbl.ValueReadWriter(),
		ourRows:  ourRows,
		keyless:  keyless,
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

	if !itr.keyless {
		for i := 0; i < itr.kd.Count(); i++ {
			f, err := index.GetField(ctx, itr.kd, i, c.k, itr.baseRows.NodeStore())
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

		err = itr.putConflictRowVals(ctx, c, r)
		if err != nil {
			return nil, err
		}
	} else {
		err = itr.putKeylessConflictRowVals(ctx, c, r)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (itr *prollyConflictRowIter) putConflictRowVals(ctx *sql.Context, c conf, r sql.Row) error {
	if c.bV != nil {
		for i := 0; i < itr.baseVD.Count(); i++ {
			f, err := index.GetField(ctx, itr.baseVD, i, c.bV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.b+itr.kd.Count()+i] = f
		}
	}

	if c.oV != nil {
		for i := 0; i < itr.oursVD.Count(); i++ {
			f, err := index.GetField(ctx, itr.oursVD, i, c.oV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.o+itr.kd.Count()+i] = f
		}
	}
	r[itr.o+itr.kd.Count()+itr.oursVD.Count()] = getDiffType(c.bV, c.oV)

	if c.tV != nil {
		for i := 0; i < itr.theirsVD.Count(); i++ {
			f, err := index.GetField(ctx, itr.theirsVD, i, c.tV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.t+itr.kd.Count()+i] = f
		}
	}
	r[itr.t+itr.kd.Count()+itr.theirsVD.Count()] = getDiffType(c.bV, c.tV)

	return nil
}

func getDiffType(base val.Tuple, other val.Tuple) string {
	if base == nil {
		return merge.ConflictDiffTypeAdded
	} else if other == nil {
		return merge.ConflictDiffTypeRemoved
	}

	// There has to be some edit, otherwise it wouldn't be a conflict...
	return merge.ConflictDiffTypeModified
}

func (itr *prollyConflictRowIter) putKeylessConflictRowVals(ctx *sql.Context, c conf, r sql.Row) error {
	if c.bV != nil {
		for i := 0; i < itr.baseVD.Count()-1; i++ {
			f, err := index.GetField(ctx, itr.baseVD, i+1, c.bV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.b+i] = f
		}
	}

	if c.oV != nil {
		for i := 0; i < itr.oursVD.Count()-1; i++ {
			f, err := index.GetField(ctx, itr.oursVD, i+1, c.oV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.o+i] = f
		}
	}
	r[itr.o+itr.oursVD.Count()-1] = getDiffType(c.bV, c.oV)

	if c.tV != nil {
		for i := 0; i < itr.theirsVD.Count()-1; i++ {
			f, err := index.GetField(ctx, itr.theirsVD, i+1, c.tV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.t+i] = f
		}
	}
	r[itr.t+itr.theirsVD.Count()-1] = getDiffType(c.bV, c.tV)

	return nil
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
	c.h = ca.TheirRootIsh

	err = itr.loadTableMaps(ctx, ca.Metadata.BaseRootIsh, ca.TheirRootIsh)
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
// the currently loaded maps. |baseHash| and |theirHash| are table hashes.
func (itr *prollyConflictRowIter) loadTableMaps(ctx context.Context, baseHash, theirHash hash.Hash) error {
	if itr.baseHash.Compare(baseHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, baseHash)
		if err != nil {
			return err
		}
		baseTbl, ok, err := rv.GetTable(ctx, itr.tblName)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to find table %s in base root value", itr.tblName)
		}

		idx, err := baseTbl.GetRowData(ctx)
		if err != nil {
			return err
		}
		itr.baseRows = durable.ProllyMapFromIndex(idx)
		itr.baseHash = baseHash
	}

	if itr.theirHash.Compare(theirHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, theirHash)
		if err != nil {
			return err
		}
		theirTbl, ok, err := rv.GetTable(ctx, itr.tblName)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to find table %s in right root value", itr.tblName)
		}

		idx, err := theirTbl.GetRowData(ctx)
		if err != nil {
			return err
		}
		itr.theirRows = durable.ProllyMapFromIndex(idx)
		itr.theirHash = theirHash
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
	if schema.IsKeyless(cd.ct.ourSch) {
		panic("conflict deleter for keyless tables not implemented")
	}

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
		err := index.PutField(ctx, cd.ed.Mut.NodeStore(), cd.kB, i, r[o+i])

		if err != nil {
			return err
		}
	}

	// then the hash follows. It is the first column of the row and the second to last in the key
	h := hash.Parse(r[0].(string))
	cd.kB.PutCommitAddr(cd.kd.Count()-2, h)

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
	cols := make([]schema.Column, 3+ours.GetAllCols().Size()+theirs.GetAllCols().Size()+base.GetAllCols().Size())

	// the commit hash or working set hash of the right side during merge
	cols[0] = schema.NewColumn("from_root_ish", 0, types.StringKind, false)

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
	cols[i] = schema.NewColumn("our_diff_type", uint64(i), types.StringKind, false)
	i++
	err = putWithPrefix("their_", theirs)
	if err != nil {
		return nil, err
	}
	cols[i] = schema.NewColumn("their_diff_type", uint64(i), types.StringKind, false)

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
