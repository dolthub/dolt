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
	"encoding/base64"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/zeebo/xxh3"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func newProllyConflictsTable(
	ctx *sql.Context,
	tbl *doltdb.Table,
	sourceUpdatableTbl sql.UpdatableTable,
	tblName doltdb.TableName,
	root doltdb.RootValue,
	rs RootSetter,
) (sql.Table, error) {
	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(arts)

	baseSch, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
	if err != nil {
		return nil, err
	}
	confSch, versionMappings, err := calculateConflictSchema(baseSch, ourSch, theirSch)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema("", doltdb.DoltConfTablePrefix+tblName.Name, confSch)
	if err != nil {
		return nil, err
	}

	return ProllyConflictsTable{
		tblName:         tblName,
		sqlSch:          sqlSch,
		baseSch:         baseSch,
		ourSch:          ourSch,
		theirSch:        theirSch,
		root:            root,
		tbl:             tbl,
		rs:              rs,
		artM:            m,
		sqlTable:        sourceUpdatableTbl,
		versionMappings: versionMappings,
	}, nil
}

// ProllyConflictsTable is a sql.Table implementation that uses the merge
// artifacts table to persist and read conflicts.
type ProllyConflictsTable struct {
	tblName                   doltdb.TableName
	sqlSch                    sql.PrimaryKeySchema
	baseSch, ourSch, theirSch schema.Schema
	root                      doltdb.RootValue
	tbl                       *doltdb.Table
	rs                        RootSetter
	artM                      prolly.ArtifactMap
	sqlTable                  sql.UpdatableTable
	versionMappings           *versionMappings
}

var _ sql.UpdatableTable = ProllyConflictsTable{}
var _ sql.DeletableTable = ProllyConflictsTable{}

func (ct ProllyConflictsTable) Name() string {
	return doltdb.DoltConfTablePrefix + ct.tblName.Name
}

func (ct ProllyConflictsTable) String() string {
	return doltdb.DoltConfTablePrefix + ct.tblName.Name
}

func (ct ProllyConflictsTable) Schema() sql.Schema {
	return ct.sqlSch.Schema
}

func (ct ProllyConflictsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (ct ProllyConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (ct ProllyConflictsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return newProllyConflictRowIter(ctx, ct)
}

func (ct ProllyConflictsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	ourUpdater := ct.sqlTable.Updater(ctx)
	return newProllyConflictOurTableUpdater(ourUpdater, ct.versionMappings, ct.baseSch, ct.ourSch, ct.theirSch)
}

func (ct ProllyConflictsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return newProllyConflictDeleter(ct)
}

type prollyConflictRowIter struct {
	itr     prolly.ConflictArtifactIter
	tblName doltdb.TableName
	vrw     types.ValueReadWriter
	ns      tree.NodeStore
	ourRows prolly.Map
	keyless bool
	ourSch  schema.Schema

	kd                       val.TupleDesc
	baseVD, oursVD, theirsVD val.TupleDesc
	// offsets for each version
	b, o, t int
	n       int

	baseHash, theirHash hash.Hash
	baseRows            prolly.Map
	theirRows           prolly.Map
}

var _ sql.RowIter = (*prollyConflictRowIter)(nil)

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

	kd := ct.baseSch.GetKeyDescriptor()
	baseVD := ct.baseSch.GetValueDescriptor()
	oursVD := ct.ourSch.GetValueDescriptor()
	theirsVD := ct.theirSch.GetValueDescriptor()

	b := 1
	var o, t, n int
	if !keyless {
		o = b + kd.Count() + baseVD.Count()
		t = o + kd.Count() + oursVD.Count() + 1
		n = t + kd.Count() + theirsVD.Count() + 2
	} else {
		o = b + baseVD.Count() - 1
		t = o + oursVD.Count()
		n = t + theirsVD.Count() + 4
	}

	return &prollyConflictRowIter{
		itr:      itr,
		tblName:  ct.tblName,
		vrw:      ct.tbl.ValueReadWriter(),
		ns:       ct.tbl.NodeStore(),
		ourRows:  ourRows,
		keyless:  keyless,
		ourSch:   ct.ourSch,
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
			f, err := tree.GetField(ctx, itr.kd, i, c.k, itr.baseRows.NodeStore())
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
			f, err := tree.GetField(ctx, itr.baseVD, i, c.bV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.b+itr.kd.Count()+i] = f
		}
	}

	if c.oV != nil {
		for i := 0; i < itr.oursVD.Count(); i++ {
			f, err := tree.GetField(ctx, itr.oursVD, i, c.oV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.o+itr.kd.Count()+i] = f
		}
	}
	r[itr.o+itr.kd.Count()+itr.oursVD.Count()] = getDiffType(c.bV, c.oV)

	if c.tV != nil {
		for i := 0; i < itr.theirsVD.Count(); i++ {
			f, err := tree.GetField(ctx, itr.theirsVD, i, c.tV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.t+itr.kd.Count()+i] = f
		}
	}
	r[itr.t+itr.kd.Count()+itr.theirsVD.Count()] = getDiffType(c.bV, c.tV)
	r[itr.t+itr.kd.Count()+itr.theirsVD.Count()+1] = c.id

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

func (itr *prollyConflictRowIter) putKeylessConflictRowVals(ctx *sql.Context, c conf, r sql.Row) (err error) {
	ns := itr.baseRows.NodeStore()

	if c.bV != nil {
		// Cardinality
		r[itr.n-3], err = tree.GetField(ctx, itr.baseVD, 0, c.bV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.baseVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.baseVD, i+1, c.bV, ns)
			if err != nil {
				return err
			}
			r[itr.b+i] = f
		}
	} else {
		r[itr.n-3] = uint64(0)
	}

	if c.oV != nil {
		r[itr.n-2], err = tree.GetField(ctx, itr.oursVD, 0, c.oV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.oursVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.oursVD, i+1, c.oV, ns)
			if err != nil {
				return err
			}
			r[itr.o+i] = f
		}
	} else {
		r[itr.n-2] = uint64(0)
	}

	r[itr.o+itr.oursVD.Count()-1] = getDiffType(c.bV, c.oV)

	if c.tV != nil {
		r[itr.n-1], err = tree.GetField(ctx, itr.theirsVD, 0, c.tV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.theirsVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.theirsVD, i+1, c.tV, ns)
			if err != nil {
				return err
			}
			r[itr.t+i] = f
		}
	} else {
		r[itr.n-1] = uint64(0)
	}

	o := itr.t + itr.theirsVD.Count() - 1
	r[o] = getDiffType(c.bV, c.tV)
	r[itr.n-4] = c.id

	return nil
}

type conf struct {
	k, bV, oV, tV val.Tuple
	h             hash.Hash
	id            string
}

func (itr *prollyConflictRowIter) nextConflictVals(ctx *sql.Context) (c conf, err error) {
	ca, err := itr.itr.Next(ctx)
	if err != nil {
		return conf{}, err
	}
	c.k = ca.Key
	c.h = ca.TheirRootIsh

	// To ensure that the conflict id is unique, we hash both TheirRootIsh and the key of the table.
	b := xxh3.Hash128(append(ca.Key, c.h[:]...)).Bytes()
	c.id = base64.RawStdEncoding.EncodeToString(b[:])

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
func (itr *prollyConflictRowIter) loadTableMaps(ctx *sql.Context, baseHash, theirHash hash.Hash) error {
	if itr.baseHash.Compare(baseHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, itr.ns, baseHash)
		if err != nil {
			return err
		}
		baseTbl, ok, err := rv.GetTable(ctx, itr.tblName)
		if err != nil {
			return err
		}

		var idx durable.Index
		if !ok {
			idx, err = durable.NewEmptyIndex(ctx, itr.vrw, itr.ns, itr.ourSch, false)
		} else {
			idx, err = baseTbl.GetRowData(ctx)
		}

		if err != nil {
			return err
		}

		itr.baseRows = durable.ProllyMapFromIndex(idx)
		itr.baseHash = baseHash
	}

	if itr.theirHash.Compare(theirHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, itr.ns, theirHash)
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

// prollyConflictOurTableUpdater allows users to update the "our table" by
// modifying rows in the conflict table. Any updates to the conflict table our
// columns are applied on the source table.
type prollyConflictOurTableUpdater struct {
	baseSch, ourSch, theirSch schema.Schema
	srcUpdater                sql.RowUpdater
	versionMappings           *versionMappings
	pkOrdinals                []int
	schemaOK                  bool
}

func newProllyConflictOurTableUpdater(ourUpdater sql.RowUpdater, versionMappings *versionMappings, baseSch, ourSch, theirSch schema.Schema) *prollyConflictOurTableUpdater {
	return &prollyConflictOurTableUpdater{
		srcUpdater:      ourUpdater,
		versionMappings: versionMappings,
		pkOrdinals:      ourSch.GetPkOrdinals(),
	}
}

// Update implements sql.RowUpdater. It translates updates on the conflict table to the source table.
func (cu *prollyConflictOurTableUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {

	// Apply updates to columns prefixed with our_
	// Updates to other columns are no-ops.
	ourOldRow := make(sql.Row, len(cu.versionMappings.ourMapping))
	ourNewRow := make(sql.Row, len(cu.versionMappings.ourMapping))
	for i, j := range cu.versionMappings.ourMapping {
		ourOldRow[i] = oldRow[j]
	}
	for i, j := range cu.versionMappings.ourMapping {
		ourNewRow[i] = newRow[j]
	}

	return cu.srcUpdater.Update(ctx, ourOldRow, ourNewRow)
}

// StatementBegin implements sql.RowUpdater.
func (cu *prollyConflictOurTableUpdater) StatementBegin(ctx *sql.Context) {
	cu.srcUpdater.StatementBegin(ctx)
}

// DiscardChanges implements sql.RowUpdater.
func (cu *prollyConflictOurTableUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return cu.srcUpdater.DiscardChanges(ctx, errorEncountered)
}

// StatementComplete implements sql.RowUpdater.
func (cu *prollyConflictOurTableUpdater) StatementComplete(ctx *sql.Context) error {
	return cu.srcUpdater.StatementComplete(ctx)
}

// Close implements sql.RowUpdater.
func (cu *prollyConflictOurTableUpdater) Close(c *sql.Context) error {
	return cu.srcUpdater.Close(c)
}

type prollyConflictDeleter struct {
	kd, vd         val.TupleDesc
	kB, vB         *val.TupleBuilder
	pool           pool.BuffPool
	ed             *prolly.ArtifactsEditor
	ct             ProllyConflictsTable
	rs             RootSetter
	ourDiffTypeIdx int
	baseColSize    int
	ourColSize     int
}

func newProllyConflictDeleter(ct ProllyConflictsTable) *prollyConflictDeleter {
	kd, _ := ct.artM.Descriptors()
	ed := ct.artM.Editor()
	kB := val.NewTupleBuilder(kd)

	vd := ct.ourSch.GetValueDescriptor()
	vB := val.NewTupleBuilder(vd)
	p := ct.artM.Pool()

	baseColSize := ct.baseSch.GetAllCols().Size()
	ourColSize := ct.ourSch.GetAllCols().Size()
	// root_ish, base_cols..., our_cols, our_diff_type
	ourDiffTypeIdx := 1 + baseColSize + ourColSize

	return &prollyConflictDeleter{
		kd:             kd,
		vd:             vd,
		kB:             kB,
		vB:             vB,
		pool:           p,
		ed:             ed,
		ct:             ct,
		ourDiffTypeIdx: ourDiffTypeIdx,
		baseColSize:    baseColSize,
		ourColSize:     ourColSize,
	}
}

func (cd *prollyConflictDeleter) Delete(ctx *sql.Context, r sql.Row) (err error) {
	// first part of the artifact key is the keys of the source table
	if !schema.IsKeyless(cd.ct.ourSch) {
		err = cd.putPrimaryKeys(ctx, r)
	} else {
		err = cd.putKeylessHash(ctx, r)
	}
	if err != nil {
		return err
	}

	// then the hash follows. It is the first column of the row and the second to last in the key
	h := hash.Parse(r[0].(string))
	cd.kB.PutCommitAddr(cd.kd.Count()-2, h)

	// Finally the artifact type which is always a conflict
	cd.kB.PutUint8(cd.kd.Count()-1, uint8(prolly.ArtifactTypeConflict))

	key := cd.kB.Build(cd.pool)
	err = cd.ed.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func (cd *prollyConflictDeleter) putPrimaryKeys(ctx *sql.Context, r sql.Row) error {
	// get keys from either base, ours, or theirs
	o := func() int {
		if o := 1; r[o] != nil {
			return o
		} else if o = 1 + cd.kd.Count() - 2 + cd.vd.Count(); r[o] != nil {
			return o
		} else if o = 1 + (cd.kd.Count()-2+cd.vd.Count())*2 + 1; r[o] != nil {
			return o
		} else {
			panic("neither base, ours, or theirs had a key")
		}
	}()

	for i := 0; i < cd.kd.Count()-2; i++ {
		err := tree.PutField(ctx, cd.ed.NodeStore(), cd.kB, i, r[o+i])

		if err != nil {
			return err
		}
	}

	return nil
}

func (cd *prollyConflictDeleter) putKeylessHash(ctx *sql.Context, r sql.Row) error {
	var rowVals sql.Row
	if r[cd.ourDiffTypeIdx] == merge.ConflictDiffTypeAdded {
		// use our cols
		rowVals = r[1+cd.baseColSize : 1+cd.baseColSize+cd.ourColSize]
	} else {
		// use base cols
		rowVals = r[1 : 1+cd.baseColSize]
	}

	// init cardinality to 0
	cd.vB.PutUint64(0, 0)
	for i, v := range rowVals {
		err := tree.PutField(ctx, cd.ed.NodeStore(), cd.vB, i+1, v)
		if err != nil {
			return err
		}
	}

	v := cd.vB.Build(cd.pool)
	k := val.HashTupleFromValue(cd.pool, v)
	cd.kB.PutHash128(0, k.GetField(0))
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

type versionMappings struct {
	ourMapping, theirMapping, baseMapping val.OrdinalMapping
}

// returns the schema of the rows returned by the conflicts table and a mappings between each version and the source table.
func calculateConflictSchema(base, ours, theirs schema.Schema) (schema.Schema, *versionMappings, error) {
	keyless := schema.IsKeyless(ours)
	n := 4 + ours.GetAllCols().Size() + theirs.GetAllCols().Size() + base.GetAllCols().Size()
	if keyless {
		n += 3
	}

	cols := make([]schema.Column, n)

	// the commit hash or working set hash of the right side during merge
	cols[0] = schema.NewColumn("from_root_ish", 0, types.StringKind, false)

	i := 1
	putWithPrefix := func(prefix string, sch schema.Schema, stripConstraints bool) (val.OrdinalMapping, error) {
		allCols := sch.GetAllCols()
		mapping := make(val.OrdinalMapping, allCols.Size())
		err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			var cons []schema.ColConstraint
			if !stripConstraints {
				cons = col.Constraints
			}
			c, err := schema.NewColumnWithTypeInfo(prefix+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment, cons...)
			if err != nil {
				return true, err
			}
			cols[i] = c
			mapping[allCols.TagToIdx[tag]] = i
			i++
			return false, nil
		})
		if err != nil {
			return nil, err
		}
		err = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			var cons []schema.ColConstraint
			if !stripConstraints {
				cons = col.Constraints
			}
			c, err := schema.NewColumnWithTypeInfo(prefix+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment, cons...)
			if err != nil {
				return true, err
			}
			cols[i] = c
			mapping[allCols.TagToIdx[tag]] = i
			i++
			return false, nil
		})
		return mapping, err
	}

	baseColMapping, err := putWithPrefix("base_", base, true)
	if err != nil {
		return nil, nil, err
	}
	ourColMapping, err := putWithPrefix("our_", ours, false)
	if err != nil {
		return nil, nil, err
	}
	cols[i] = schema.NewColumn("our_diff_type", uint64(i), types.StringKind, false)
	i++
	theirColMapping, err := putWithPrefix("their_", theirs, true)
	if err != nil {
		return nil, nil, err
	}
	cols[i] = schema.NewColumn("their_diff_type", uint64(i), types.StringKind, false)
	i++

	cols[i] = schema.NewColumn("dolt_conflict_id", uint64(i), types.StringKind, false)
	i++

	if keyless {
		cols[i] = schema.NewColumn("base_cardinality", uint64(i), types.UintKind, false)
		i++
		cols[i] = schema.NewColumn("our_cardinality", uint64(i), types.UintKind, false)
		i++
		cols[i] = schema.NewColumn("their_cardinality", uint64(i), types.UintKind, false)
		i++
	}

	sch, err := schema.NewSchema(schema.NewColCollection(cols...), nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	return sch,
		&versionMappings{
			ourMapping:   ourColMapping,
			theirMapping: theirColMapping,
			baseMapping:  baseColMapping},
		nil
}
