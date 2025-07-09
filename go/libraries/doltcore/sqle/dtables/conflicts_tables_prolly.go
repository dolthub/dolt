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
	confSch, versionMappings, err := CalculateConflictSchema(baseSch, ourSch, theirSch)
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
	return newProllyConflictOurTableUpdater(ctx, ct)
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

	cds     ConflictDescriptors
	offsets ConflictOffsets

	baseHash, theirHash hash.Hash
	baseRows            prolly.Map
	theirRows           prolly.Map
}

// ConflictOffsets holds the offsets of the columns in a conflict row. The
// offsets are used to put the values in the correct place in the row.
// Base is the offset of the first base column, Ours is the offset of the first
// ours column, and Theirs is the offset of the first theirs column.
type ConflictOffsets struct {
	Base, Ours, Theirs int
	ColCount           int
}

// ConflictDescriptors holds the descriptors for the key and base, ours, and theirs values for a row.
type ConflictDescriptors struct {
	BaseVal, OurVal, TheirVal val.TupleDesc
	Key                       val.TupleDesc
}

// GetConflictOffsets returns the offsets of the columns in a conflict row.
//
// For keyed tables, the conflict row structure is:
// [from_root_ish] [base_key...] [base_vals...] [our_key...] [our_vals...] [our_diff_type] [their_key...] [their_vals...] [their_diff_type] [dolt_conflict_id]
//
// For keyless tables, the conflict row structure is:
// [from_root_ish] [base_vals...] [our_vals...] [our_diff_type] [their_vals...] [their_diff_type] [dolt_conflict_id] [base_cardinality] [our_cardinality] [their_cardinality]
func GetConflictOffsets(keyless bool, cds ConflictDescriptors) ConflictOffsets {
	// Skip index 0 which is always from_root_ish
	baseOffset := 1
	var ourOffset, theirOffset, colCount int
	if !keyless {
		// Base section: base key columns + base value columns
		ourOffset = baseOffset + cds.Key.Count() + cds.BaseVal.Count()
		// +1 for our_diff_type column that follows the ours section
		theirOffset = ourOffset + cds.Key.Count() + cds.OurVal.Count() + 1
		// +2 for their_diff_type and dolt_conflict_id columns at the end
		colCount = theirOffset + cds.Key.Count() + cds.TheirVal.Count() + 2
	} else {
		// For keyless: base values (excluding cardinality which comes at the end)
		ourOffset = baseOffset + cds.BaseVal.Count() - 1
		// Ours section: our value columns (excluding cardinality)
		theirOffset = ourOffset + cds.OurVal.Count()
		// +4 for our_diff_type, their_diff_type, dolt_conflict_id, and 3 cardinality columns
		colCount = theirOffset + cds.TheirVal.Count() + 4
	}

	return ConflictOffsets{
		Base:     baseOffset,
		Ours:     ourOffset,
		Theirs:   theirOffset,
		ColCount: colCount,
	}
}

// GetConflictDescriptors returns the descriptors for the key and base, ours, and theirs
// values.
func GetConflictDescriptors(baseSch, ourSch, theirSch schema.Schema, ns tree.NodeStore) ConflictDescriptors {
	key := baseSch.GetKeyDescriptor(ns)
	baseVD := baseSch.GetValueDescriptor(ns)
	oursVD := ourSch.GetValueDescriptor(ns)
	theirsVD := theirSch.GetValueDescriptor(ns)
	return ConflictDescriptors{
		BaseVal:  baseVD,
		OurVal:   oursVD,
		TheirVal: theirsVD,
		Key:      key,
	}
}

var _ sql.RowIter = (*prollyConflictRowIter)(nil)

func newProllyConflictRowIter(ctx *sql.Context, ct ProllyConflictsTable) (*prollyConflictRowIter, error) {
	idx, err := ct.tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ourRows, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return nil, err
	}

	itr, err := ct.artM.IterAllConflicts(ctx)
	if err != nil {
		return nil, err
	}

	keyless := schema.IsKeyless(ct.ourSch)
	cds := GetConflictDescriptors(ct.baseSch, ct.ourSch, ct.theirSch, ct.root.NodeStore())
	offsets := GetConflictOffsets(keyless, cds)

	return &prollyConflictRowIter{
		itr:     itr,
		tblName: ct.tblName,
		vrw:     ct.tbl.ValueReadWriter(),
		ns:      ct.tbl.NodeStore(),
		ourRows: ourRows,
		keyless: keyless,
		ourSch:  ct.ourSch,
		cds:     cds,
		offsets: offsets,
	}, nil
}

func (itr *prollyConflictRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	confVal, err := itr.nextConflictVals(ctx)
	if err != nil {
		return nil, err
	}

	row := make(sql.Row, itr.offsets.ColCount)
	row[0] = confVal.Hash.String() // from_root_ish

	if !itr.keyless {
		err = itr.putConflictRowVals(ctx, confVal, row)
		if err != nil {
			return nil, err
		}
	} else {
		err = itr.putKeylessConflictRowVals(ctx, confVal, row)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

// PutConflictRowVals puts the values of the conflict row into the given row.
func PutConflictRowVals(ctx *sql.Context, confVal ConflictVal, row sql.Row, offsets ConflictOffsets, cds ConflictDescriptors, ns tree.NodeStore) error {
	// Sets key columns for the conflict row.
	for i := 0; i < cds.Key.Count(); i++ {
		f, err := tree.GetField(ctx, cds.Key, i, confVal.Key, ns)
		if err != nil {
			return err
		}
		if confVal.Base != nil {
			row[offsets.Base+i] = f
		}
		if confVal.Ours != nil {
			row[offsets.Ours+i] = f
		}
		if confVal.Theirs != nil {
			row[offsets.Theirs+i] = f
		}
	}

	if confVal.Base != nil {
		for i := 0; i < cds.BaseVal.Count(); i++ {
			f, err := tree.GetField(ctx, cds.BaseVal, i, confVal.Base, ns)
			if err != nil {
				return err
			}
			baseColOffset := offsets.Base + cds.Key.Count() + i
			row[baseColOffset] = f // base_[col]
		}
	}

	if confVal.Ours != nil {
		for i := 0; i < cds.OurVal.Count(); i++ {
			f, err := tree.GetField(ctx, cds.OurVal, i, confVal.Ours, ns)
			if err != nil {
				return err
			}
			ourColOffset := offsets.Ours + cds.Key.Count() + i
			row[ourColOffset] = f // our_[col]
		}
	}

	ourDiffTypeOffset := offsets.Ours + cds.Key.Count() + cds.OurVal.Count()
	row[ourDiffTypeOffset] = getConflictDiffType(confVal.Base, confVal.Ours) // our_diff_type

	if confVal.Theirs != nil {
		for i := 0; i < cds.TheirVal.Count(); i++ {
			f, err := tree.GetField(ctx, cds.TheirVal, i, confVal.Theirs, ns)
			if err != nil {
				return err
			}
			theirColOffset := offsets.Theirs + cds.Key.Count() + i
			row[theirColOffset] = f // their_[col]
		}
	}

	theirDiffTypeOffset := offsets.Theirs + cds.Key.Count() + cds.TheirVal.Count()
	row[theirDiffTypeOffset] = getConflictDiffType(confVal.Base, confVal.Theirs) // their_diff_type

	conflictIdOffset := theirDiffTypeOffset + 1
	row[conflictIdOffset] = confVal.Id // dolt_conflict_id

	return nil
}

func (itr *prollyConflictRowIter) putConflictRowVals(ctx *sql.Context, confVal ConflictVal, row sql.Row) error {
	ns := itr.baseRows.NodeStore()
	return PutConflictRowVals(ctx, confVal, row, itr.offsets, itr.cds, ns)
}

func getConflictDiffType(base val.Tuple, other val.Tuple) string {
	if base == nil {
		return merge.ConflictDiffTypeAdded
	} else if other == nil {
		return merge.ConflictDiffTypeRemoved
	}

	// There has to be some edit, otherwise it wouldn't be a conflict...
	return merge.ConflictDiffTypeModified
}

// PutKeylessConflictRowVals puts the values of the keyless conflict row into the given row.
func PutKeylessConflictRowVals(ctx *sql.Context, confVal ConflictVal, row sql.Row, offsets ConflictOffsets, cds ConflictDescriptors, ns tree.NodeStore) (err error) {
	if confVal.Base != nil {
		f, err := tree.GetField(ctx, cds.BaseVal, 0, confVal.Base, ns)
		if err != nil {
			return err
		}
		row[offsets.ColCount-3] = f // base_cardinality

		for i := 0; i < cds.BaseVal.Count()-1; i++ {
			f, err := tree.GetField(ctx, cds.BaseVal, i+1, confVal.Base, ns)
			if err != nil {
				return err
			}
			baseColOffset := offsets.Base + i
			row[baseColOffset] = f // base_[col]
		}
	} else {
		row[offsets.ColCount-3] = uint64(0) // base_cardinality
	}

	if confVal.Ours != nil {
		f, err := tree.GetField(ctx, cds.OurVal, 0, confVal.Ours, ns)
		if err != nil {
			return err
		}
		row[offsets.ColCount-2] = f // our_cardinality

		for i := 0; i < cds.OurVal.Count()-1; i++ {
			f, err := tree.GetField(ctx, cds.OurVal, i+1, confVal.Ours, ns)
			if err != nil {
				return err
			}
			row[offsets.Ours+i] = f // our_[col]
		}
	} else {
		row[offsets.ColCount-2] = uint64(0) // our_cardinality
	}

	ourDiffTypeOffset := offsets.Ours + cds.OurVal.Count() - 1
	row[ourDiffTypeOffset] = getConflictDiffType(confVal.Base, confVal.Ours) // our_diff_type

	if confVal.Theirs != nil {
		f, err := tree.GetField(ctx, cds.TheirVal, 0, confVal.Theirs, ns)
		if err != nil {
			return err
		}
		row[offsets.ColCount-1] = f // their_cardinality

		for i := 0; i < cds.TheirVal.Count()-1; i++ {
			f, err := tree.GetField(ctx, cds.TheirVal, i+1, confVal.Theirs, ns)
			if err != nil {
				return err
			}
			row[offsets.Theirs+i] = f // their_[col]
		}
	} else {
		row[offsets.ColCount-1] = uint64(0) // their_cardinality
	}

	theirDiffTypeOffset := offsets.Theirs + cds.TheirVal.Count() - 1
	row[theirDiffTypeOffset] = getConflictDiffType(confVal.Base, confVal.Theirs) // their_diff_type

	row[offsets.ColCount-4] = confVal.Id // dolt_conflict_id

	return nil
}

func (itr *prollyConflictRowIter) putKeylessConflictRowVals(ctx *sql.Context, confVal ConflictVal, row sql.Row) (err error) {
	ns := itr.baseRows.NodeStore()
	return PutKeylessConflictRowVals(ctx, confVal, row, itr.offsets, itr.cds, ns)
}

type ConflictVal struct {
	Key, Base, Ours, Theirs val.Tuple
	Hash                    hash.Hash
	Id                      string
}

// GetConflictId gets the conflict ID, ensuring that it is unique by hashing both theirRootish and the key of the table.
func GetConflictId(key val.Tuple, confHash hash.Hash) string {
	b := xxh3.Hash128(append(key, confHash[:]...)).Bytes()
	return base64.RawStdEncoding.EncodeToString(b[:])
}

func (itr *prollyConflictRowIter) nextConflictVals(ctx *sql.Context) (confVal ConflictVal, err error) {
	ca, err := itr.itr.Next(ctx)
	if err != nil {
		return ConflictVal{}, err
	}

	confVal.Key = ca.Key
	confVal.Hash = ca.TheirRootIsh
	confVal.Id = GetConflictId(ca.Key, confVal.Hash)

	err = itr.loadTableMaps(ctx, ca.Metadata.BaseRootIsh, ca.TheirRootIsh)
	if err != nil {
		return ConflictVal{}, err
	}

	err = itr.baseRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		confVal.Base = v
		return nil
	})
	if err != nil {
		return ConflictVal{}, err
	}
	err = itr.ourRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		confVal.Ours = v
		return nil
	})
	if err != nil {
		return ConflictVal{}, err
	}
	err = itr.theirRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		confVal.Theirs = v
		return nil
	})
	if err != nil {
		return ConflictVal{}, err
	}

	return confVal, nil
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
			idx, err = durable.NewEmptyPrimaryIndex(ctx, itr.vrw, itr.ns, itr.ourSch)
		} else {
			idx, err = baseTbl.GetRowData(ctx)
		}

		if err != nil {
			return err
		}

		itr.baseRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}

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
		itr.theirRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}
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

func newProllyConflictOurTableUpdater(ctx *sql.Context, ct ProllyConflictsTable) *prollyConflictOurTableUpdater {
	ourUpdater := ct.sqlTable.Updater(ctx)
	return &prollyConflictOurTableUpdater{
		srcUpdater:      ourUpdater,
		versionMappings: ct.versionMappings,
		pkOrdinals:      ct.ourSch.GetPkOrdinals(),
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
	kB := val.NewTupleBuilder(kd, ct.artM.NodeStore())

	vd := ct.ourSch.GetValueDescriptor(ct.root.NodeStore())
	vB := val.NewTupleBuilder(vd, ct.artM.NodeStore())
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

	key, err := cd.kB.Build(cd.pool)
	if err != nil {
		return err
	}
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
		} else if o = 1 + cd.baseColSize; r[o] != nil {
			return o
		} else if o = 1 + cd.baseColSize + cd.ourColSize + 1; r[o] != nil { // advance past from_root_ish, base cols, our cols, and our_diff_type
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

	v, err := cd.vB.Build(cd.pool)
	if err != nil {
		return err
	}
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
func CalculateConflictSchema(base, ours, theirs schema.Schema) (schema.Schema, *versionMappings, error) {
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
