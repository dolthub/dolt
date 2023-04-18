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

package doltdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey        = "tables"
	foreignKeyKey    = "foreign_key"
	featureVersKey   = "feature_ver"
	rootCollationKey = "root_collation_key"

	// deprecated
	superSchemasKey = "super_schemas"
)

type FeatureVersion int64

// DoltFeatureVersion is described in feature_version.md.
// only variable for testing.
var DoltFeatureVersion FeatureVersion = 3 // last bumped when storing creation time for triggers

// RootValue is the value of the Database and is the committed value in every Dolt commit.
type RootValue struct {
	vrw  types.ValueReadWriter
	ns   tree.NodeStore
	st   rvStorage
	fkc  *ForeignKeyCollection // cache the first load
	hash hash.Hash             // cache first load
}

func (root *RootValue) ResolveRootValue(ctx context.Context) (*RootValue, error) {
	return root, nil
}

var _ Rootish = &RootValue{}

type tableEdit struct {
	name string
	ref  *types.Ref

	// Used for rename.
	old_name string
}

type tableMap interface {
	Get(ctx context.Context, name string) (hash.Hash, error)
	Iter(ctx context.Context, cb func(name string, addr hash.Hash) (bool, error)) error
}

func tmIterAll(ctx context.Context, tm tableMap, cb func(name string, addr hash.Hash)) error {
	return tm.Iter(ctx, func(name string, addr hash.Hash) (bool, error) {
		cb(name, addr)
		return false, nil
	})
}

type rvStorage interface {
	GetFeatureVersion() (FeatureVersion, bool, error)

	GetTablesMap(ctx context.Context, vr types.ValueReadWriter, ns tree.NodeStore) (tableMap, error)
	GetForeignKeys(ctx context.Context, vr types.ValueReader) (types.Value, bool, error)
	GetCollation(ctx context.Context) (schema.Collation, error)

	SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, m types.Value) (rvStorage, error)
	SetFeatureVersion(v FeatureVersion) (rvStorage, error)
	SetCollation(ctx context.Context, collation schema.Collation) (rvStorage, error)

	EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rvStorage, error)

	DebugString(ctx context.Context) string
	nomsValue() types.Value
}

type nomsRvStorage struct {
	valueSt types.Struct
}

func (r nomsRvStorage) GetFeatureVersion() (FeatureVersion, bool, error) {
	v, ok, err := r.valueSt.MaybeGet(featureVersKey)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return FeatureVersion(v.(types.Int)), true, nil
	} else {
		return 0, false, nil
	}
}

func (r nomsRvStorage) GetTablesMap(context.Context, types.ValueReadWriter, tree.NodeStore) (tableMap, error) {
	v, found, err := r.valueSt.MaybeGet(tablesKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nomsTableMap{types.EmptyMap}, nil
	}
	return nomsTableMap{v.(types.Map)}, nil
}

type nomsTableMap struct {
	types.Map
}

func (ntm nomsTableMap) Get(ctx context.Context, name string) (hash.Hash, error) {
	v, f, err := ntm.MaybeGet(ctx, types.String(name))
	if err != nil {
		return hash.Hash{}, err
	}
	if !f {
		return hash.Hash{}, nil
	}
	return v.(types.Ref).TargetHash(), nil
}

func (ntm nomsTableMap) Iter(ctx context.Context, cb func(name string, addr hash.Hash) (bool, error)) error {
	return ntm.Map.Iter(ctx, func(k, v types.Value) (bool, error) {
		name := string(k.(types.String))
		addr := v.(types.Ref).TargetHash()
		return cb(name, addr)
	})
}

func (r nomsRvStorage) GetForeignKeys(context.Context, types.ValueReader) (types.Value, bool, error) {
	v, found, err := r.valueSt.MaybeGet(foreignKeyKey)
	if err != nil {
		return types.Map{}, false, err
	}
	if !found {
		return types.Map{}, false, err
	}
	return v.(types.Map), true, nil
}

func (r nomsRvStorage) GetCollation(ctx context.Context) (schema.Collation, error) {
	v, found, err := r.valueSt.MaybeGet(rootCollationKey)
	if err != nil {
		return schema.Collation_Unspecified, err
	}
	if !found {
		return schema.Collation_Default, nil
	}
	return schema.Collation(v.(types.Uint)), nil
}

func (r nomsRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rvStorage, error) {
	m, err := r.GetTablesMap(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}
	nm := m.(nomsTableMap).Map

	me := nm.Edit()
	for _, e := range edits {
		if e.old_name != "" {
			old, f, err := nm.MaybeGet(ctx, types.String(e.old_name))
			if err != nil {
				return nil, err
			}
			if !f {
				return nil, ErrTableNotFound
			}
			_, f, err = nm.MaybeGet(ctx, types.String(e.name))
			if err != nil {
				return nil, err
			}
			if f {
				return nil, ErrTableExists
			}
			me = me.Remove(types.String(e.old_name)).Set(types.String(e.name), old)
		} else {
			if e.ref == nil {
				me = me.Remove(types.String(e.name))
			} else {
				me = me.Set(types.String(e.name), *e.ref)
			}
		}
	}

	nm, err = me.Map(ctx)
	if err != nil {
		return nil, err
	}

	st, err := r.valueSt.Set(tablesKey, nm)
	if err != nil {
		return nil, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (rvStorage, error) {
	st, err := r.valueSt.Set(foreignKeyKey, v)
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetFeatureVersion(v FeatureVersion) (rvStorage, error) {
	st, err := r.valueSt.Set(featureVersKey, types.Int(v))
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetCollation(ctx context.Context, collation schema.Collation) (rvStorage, error) {
	st, err := r.valueSt.Set(rootCollationKey, types.Uint(collation))
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) DebugString(ctx context.Context) string {
	var buf bytes.Buffer
	err := types.WriteEncodedValue(ctx, &buf, r.valueSt)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (r nomsRvStorage) nomsValue() types.Value {
	return r.valueSt
}

func newRootValue(vrw types.ValueReadWriter, ns tree.NodeStore, v types.Value) (*RootValue, error) {
	var storage rvStorage

	if vrw.Format().UsesFlatbuffers() {
		srv, err := serial.TryGetRootAsRootValue([]byte(v.(types.SerialMessage)), serial.MessagePrefixSz)
		if err != nil {
			return nil, err
		}
		storage = fbRvStorage{srv}
	} else {
		st, ok := v.(types.Struct)
		if !ok {
			return nil, errors.New("invalid value passed to newRootValue")
		}

		storage = nomsRvStorage{st}
	}
	ver, ok, err := storage.GetFeatureVersion()
	if err != nil {
		return nil, err
	}
	if ok {
		if DoltFeatureVersion < ver {
			return nil, ErrClientOutOfDate{
				ClientVer: DoltFeatureVersion,
				RepoVer:   ver,
			}
		}
	}

	return &RootValue{vrw, ns, storage, nil, hash.Hash{}}, nil
}

// LoadRootValueFromRootIshAddr takes the hash of the commit or the hash of a
// working set and returns the corresponding RootValue.
func LoadRootValueFromRootIshAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, h hash.Hash) (*RootValue, error) {
	val, err := datas.LoadRootNomsValueFromRootIshAddr(ctx, vrw, h)
	if err != nil {
		return nil, err
	}
	return decodeRootNomsValue(vrw, ns, val)
}

func decodeRootNomsValue(vrw types.ValueReadWriter, ns tree.NodeStore, val types.Value) (*RootValue, error) {
	if val == nil {
		return nil, ErrNoRootValAtHash
	}

	if !isRootValue(vrw.Format(), val) {
		return nil, ErrNoRootValAtHash
	}

	return newRootValue(vrw, ns, val)
}

func isRootValue(nbf *types.NomsBinFormat, val types.Value) bool {
	if nbf.UsesFlatbuffers() {
		if sm, ok := val.(types.SerialMessage); ok {
			return string(serial.GetFileID(sm)) == serial.RootValueFileID
		}
	} else {
		if st, ok := val.(types.Struct); ok {
			return st.Name() == ddbRootStructName
		}
	}
	return false
}

func EmptyRootValue(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore) (*RootValue, error) {
	if vrw.Format().UsesFlatbuffers() {
		builder := flatbuffers.NewBuilder(80)

		emptyam, err := prolly.NewEmptyAddressMap(ns)
		if err != nil {
			return nil, err
		}
		ambytes := []byte(tree.ValueFromNode(emptyam.Node()).(types.SerialMessage))
		tablesoff := builder.CreateByteVector(ambytes)

		var empty hash.Hash
		fkoff := builder.CreateByteVector(empty[:])
		serial.RootValueStart(builder)
		serial.RootValueAddFeatureVersion(builder, int64(DoltFeatureVersion))
		serial.RootValueAddCollation(builder, serial.Collationutf8mb4_0900_bin)
		serial.RootValueAddTables(builder, tablesoff)
		serial.RootValueAddForeignKeyAddr(builder, fkoff)
		bs := serial.FinishMessage(builder, serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
		return newRootValue(vrw, ns, types.SerialMessage(bs))
	}

	empty, err := types.NewMap(ctx, vrw)
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		tablesKey:       empty,
		superSchemasKey: empty,
		foreignKeyKey:   empty,
		featureVersKey:  types.Int(DoltFeatureVersion),
	}

	st, err := types.NewStruct(vrw.Format(), ddbRootStructName, sd)
	if err != nil {
		return nil, err
	}

	return newRootValue(vrw, ns, st)
}

func (root *RootValue) VRW() types.ValueReadWriter {
	return root.vrw
}

func (root *RootValue) NodeStore() tree.NodeStore {
	return root.ns
}

// GetFeatureVersion returns the feature version of this root, if one is written
func (root *RootValue) GetFeatureVersion(ctx context.Context) (ver FeatureVersion, ok bool, err error) {
	return root.st.GetFeatureVersion()
}

func (root *RootValue) setFeatureVersion(v FeatureVersion) (*RootValue, error) {
	newStorage, err := root.st.SetFeatureVersion(v)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *RootValue) GetCollation(ctx context.Context) (schema.Collation, error) {
	return root.st.GetCollation(ctx)
}

func (root *RootValue) SetCollation(ctx context.Context, collation schema.Collation) (*RootValue, error) {
	newStorage, err := root.st.SetCollation(ctx, collation)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *RootValue) HasTable(ctx context.Context, tName string) (bool, error) {
	tableMap, err := root.st.GetTablesMap(ctx, root.vrw, root.ns)
	if err != nil {
		return false, err
	}
	a, err := tableMap.Get(ctx, tName)
	if err != nil {
		return false, err
	}
	return !a.IsEmpty(), nil
}

func (root *RootValue) GenerateTagsForNewColColl(ctx context.Context, tableName string, cc *schema.ColCollection) (*schema.ColCollection, error) {
	newColNames := make([]string, 0, cc.Size())
	newColKinds := make([]types.NomsKind, 0, cc.Size())
	_ = cc.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		newColNames = append(newColNames, col.Name)
		newColKinds = append(newColKinds, col.Kind)
		return false, nil
	})

	newTags, err := root.GenerateTagsForNewColumns(ctx, tableName, newColNames, newColKinds, nil)
	if err != nil {
		return nil, err
	}

	idx := 0
	return schema.MapColCollection(cc, func(col schema.Column) schema.Column {
		col.Tag = newTags[idx]
		idx++
		return col
	}), nil
}

// GenerateTagsForNewColumns deterministically generates a slice of new tags that are unique within the history of this root. The names and NomsKinds of
// the new columns are used to see the tag generator.
func (root *RootValue) GenerateTagsForNewColumns(
	ctx context.Context,
	tableName string,
	newColNames []string,
	newColKinds []types.NomsKind,
	headRoot *RootValue,
) ([]uint64, error) {
	if len(newColNames) != len(newColKinds) {
		return nil, fmt.Errorf("error generating tags, newColNames and newColKinds must be of equal length")
	}

	newTags := make([]*uint64, len(newColNames))

	// Get existing columns from the current root, or the head root if the table doesn't exist in the current root. The
	// latter case is to support reusing table tags in the case of drop / create in the same session, which is common
	// during import.
	existingCols, err := getExistingColumns(ctx, root, headRoot, tableName, newColNames, newColKinds)
	if err != nil {
		return nil, err
	}

	// If we found any existing columns set them in the newTags list.
	for _, col := range existingCols {
		col := col
		for i := range newColNames {
			// Only re-use tags if the noms kind didn't change
			// TODO: revisit this when new storage format is further along
			if strings.ToLower(newColNames[i]) == strings.ToLower(col.Name) &&
				newColKinds[i] == col.TypeInfo.NomsKind() {
				newTags[i] = &col.Tag
				break
			}
		}
	}

	var existingColKinds []types.NomsKind
	for _, col := range existingCols {
		existingColKinds = append(existingColKinds, col.Kind)
	}

	existingTags, err := GetAllTagsForRoots(ctx, headRoot, root)
	if err != nil {
		return nil, err
	}

	outputTags := make([]uint64, len(newTags))
	for i := range newTags {
		if newTags[i] != nil {
			outputTags[i] = *newTags[i]
			continue
		}

		outputTags[i] = schema.AutoGenerateTag(existingTags, tableName, existingColKinds, newColNames[i], newColKinds[i])
		existingColKinds = append(existingColKinds, newColKinds[i])
		existingTags.Add(outputTags[i], tableName)
	}

	return outputTags, nil
}

func getExistingColumns(
	ctx context.Context,
	root, headRoot *RootValue,
	tableName string,
	newColNames []string,
	newColKinds []types.NomsKind,
) ([]schema.Column, error) {

	var existingCols []schema.Column
	tbl, found, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if found {
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			existingCols = append(existingCols, col)
			return false, nil
		})
	} else if headRoot != nil {
		tbl, found, err := headRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}

		if found {
			sch, err := tbl.GetSchema(ctx)
			if err != nil {
				return nil, err
			}

			existingCols = schema.GetSharedCols(sch, newColNames, newColKinds)
		}
	}

	return existingCols, nil
}

func (root *RootValue) GetAllSchemas(ctx context.Context) (map[string]schema.Schema, error) {
	m := make(map[string]schema.Schema)
	err := root.IterTables(ctx, func(name string, table *Table, sch schema.Schema) (stop bool, err error) {
		m[name] = sch
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return m, nil
}

func (root *RootValue) GetTableHash(ctx context.Context, tName string) (hash.Hash, bool, error) {
	tableMap, err := root.getTableMap(ctx)
	if err != nil {
		return hash.Hash{}, false, err
	}

	tVal, err := tableMap.Get(ctx, tName)
	if err != nil {
		return hash.Hash{}, false, err
	}

	return tVal, !tVal.IsEmpty(), nil
}

func (root *RootValue) SetTableHash(ctx context.Context, tName string, h hash.Hash) (*RootValue, error) {
	val, err := root.vrw.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	ref, err := types.NewRef(val, root.vrw.Format())

	if err != nil {
		return nil, err
	}

	return putTable(ctx, root, tName, ref)
}

// ResolveTableName resolves a case-insensitive name to the exact name as stored in Dolt. Returns false if no matching
// name was found.
func (root *RootValue) ResolveTableName(ctx context.Context, tName string) (string, bool, error) {
	tableMap, err := root.getTableMap(ctx)
	if err != nil {
		return "", false, err
	}

	a, err := tableMap.Get(ctx, tName)
	if err != nil {
		return "", false, err
	}
	if !a.IsEmpty() {
		return tName, true, nil
	}

	found := false
	lwrName := strings.ToLower(tName)
	err = tmIterAll(ctx, tableMap, func(name string, addr hash.Hash) {
		if found == false && lwrName == strings.ToLower(name) {
			tName = name
			found = true
		}
	})
	if err != nil {
		return "", false, nil
	}
	return tName, found, nil
}

// GetTable will retrieve a table by its case-sensitive name.
func (root *RootValue) GetTable(ctx context.Context, tName string) (*Table, bool, error) {
	tableMap, err := root.getTableMap(ctx)

	if err != nil {
		return nil, false, err
	}

	addr, err := tableMap.Get(ctx, tName)
	if err != nil {
		return nil, false, err
	}
	if addr.IsEmpty() {
		return nil, false, nil
	}

	table, err := durable.TableFromAddr(ctx, root.VRW(), root.ns, addr)
	if err != nil {
		return nil, false, err
	}

	return &Table{table: table}, true, err
}

// GetTableInsensitive will retrieve a table by its case-insensitive name.
func (root *RootValue) GetTableInsensitive(ctx context.Context, tName string) (*Table, string, bool, error) {
	resolvedName, ok, err := root.ResolveTableName(ctx, tName)
	if err != nil {
		return nil, "", false, err
	}
	if !ok {
		return nil, "", false, nil
	}
	tbl, ok, err := root.GetTable(ctx, resolvedName)
	if err != nil {
		return nil, "", false, err
	}
	return tbl, resolvedName, ok, nil
}

// GetTableByColTag looks for the table containing the given column tag.
func (root *RootValue) GetTableByColTag(ctx context.Context, tag uint64) (tbl *Table, name string, found bool, err error) {
	err = root.IterTables(ctx, func(tn string, t *Table, s schema.Schema) (bool, error) {
		_, found = s.GetAllCols().GetByTag(tag)
		if found {
			name, tbl = tn, t
		}

		return found, nil
	})

	if err != nil {
		return nil, "", false, err
	}

	return tbl, name, found, nil
}

// GetTableNames retrieves the lists of all tables for a RootValue
func (root *RootValue) GetTableNames(ctx context.Context) ([]string, error) {
	tableMap, err := root.getTableMap(ctx)
	if err != nil {
		return nil, err
	}

	var names []string
	err = tmIterAll(ctx, tableMap, func(name string, _ hash.Hash) {
		names = append(names, name)
	})
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (root *RootValue) getTableMap(ctx context.Context) (tableMap, error) {
	return root.st.GetTablesMap(ctx, root.vrw, root.ns)
}

func (root *RootValue) TablesInConflict(ctx context.Context) ([]string, error) {
	names, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	conflicted := make([]string, 0, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}

		ok, err := tbl.HasConflicts(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			conflicted = append(conflicted, name)
		}
	}

	return conflicted, nil
}

// TablesWithConstraintViolations returns all tables that have constraint violations.
func (root *RootValue) TablesWithConstraintViolations(ctx context.Context) ([]string, error) {
	names, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	violating := make([]string, 0, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}

		n, err := tbl.NumConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}

		if n > 0 {
			violating = append(violating, name)
		}
	}

	return violating, nil
}

func (root *RootValue) HasConflicts(ctx context.Context) (bool, error) {
	cnfTbls, err := root.TablesInConflict(ctx)

	if err != nil {
		return false, err
	}

	return len(cnfTbls) > 0, nil
}

// HasConstraintViolations returns whether any tables have constraint violations.
func (root *RootValue) HasConstraintViolations(ctx context.Context) (bool, error) {
	tbls, err := root.TablesWithConstraintViolations(ctx)
	if err != nil {
		return false, err
	}
	return len(tbls) > 0, nil
}

// IterTables calls the callback function cb on each table in this RootValue.
func (root *RootValue) IterTables(ctx context.Context, cb func(name string, table *Table, sch schema.Schema) (stop bool, err error)) error {
	tm, err := root.getTableMap(ctx)
	if err != nil {
		return err
	}

	return tm.Iter(ctx, func(name string, addr hash.Hash) (bool, error) {
		nt, err := durable.TableFromAddr(ctx, root.VRW(), root.ns, addr)
		if err != nil {
			return true, err
		}
		tbl := &Table{table: nt}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		return cb(name, tbl, sch)
	})
}

func (root *RootValue) withStorage(st rvStorage) *RootValue {
	return &RootValue{root.vrw, root.ns, st, nil, hash.Hash{}}
}

func (root *RootValue) nomsValue() types.Value {
	return root.st.nomsValue()
}

// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func (root *RootValue) PutTable(ctx context.Context, tName string, table *Table) (*RootValue, error) {
	err := validateTagUniqueness(ctx, root, tName, table)
	if err != nil {
		return nil, err
	}

	tableRef, err := durable.RefFromNomsTable(ctx, table.table)
	if err != nil {
		return nil, err
	}

	return putTable(ctx, root, tName, tableRef)
}

func putTable(ctx context.Context, root *RootValue, tName string, ref types.Ref) (*RootValue, error) {
	if !IsValidTableName(tName) {
		panic("Don't attempt to put a table with a name that fails the IsValidTableName check")
	}

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, root.ns, []tableEdit{{name: tName, ref: &ref}})
	if err != nil {
		return nil, err
	}

	return root.withStorage(newStorage), nil
}

// CreateEmptyTable creates an empty table in this root with the name and schema given, returning the new root value.
func (root *RootValue) CreateEmptyTable(ctx context.Context, tName string, sch schema.Schema) (*RootValue, error) {
	empty, err := durable.NewEmptyIndex(ctx, root.vrw, root.ns, sch)
	if err != nil {
		return nil, err
	}

	indexes, err := durable.NewIndexSet(ctx, root.VRW(), root.ns)
	if err != nil {
		return nil, err
	}
	err = sch.Indexes().Iter(func(index schema.Index) (stop bool, err error) {
		// create an empty map for every index
		indexes, err = indexes.PutIndex(ctx, index.Name(), empty)
		return
	})
	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(ctx, root.VRW(), root.ns, sch, empty, indexes, nil)
	if err != nil {
		return nil, err
	}

	newRoot, err := root.PutTable(ctx, tName, tbl)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
}

// HashOf gets the hash of the root value
func (root *RootValue) HashOf() (hash.Hash, error) {
	if root.hash.IsEmpty() {
		var err error
		root.hash, err = root.st.nomsValue().Hash(root.vrw.Format())
		if err != nil {
			return hash.Hash{}, nil
		}
	}
	return root.hash, nil
}

// RenameTable renames a table by changing its string key in the RootValue's table map. In order to preserve
// column tag information, use this method instead of a table drop + add.
func (root *RootValue) RenameTable(ctx context.Context, oldName, newName string) (*RootValue, error) {
	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, root.ns, []tableEdit{{old_name: oldName, name: newName}})
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *RootValue) RemoveTables(ctx context.Context, skipFKHandling bool, allowDroppingFKReferenced bool, tables ...string) (*RootValue, error) {
	tableMap, err := root.getTableMap(ctx)
	if err != nil {
		return nil, err
	}

	edits := make([]tableEdit, len(tables))
	for i, name := range tables {
		a, err := tableMap.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		if a.IsEmpty() {
			return nil, fmt.Errorf("%w: '%s'", ErrTableNotFound, name)
		}
		edits[i].name = name
	}

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, root.ns, edits)
	if err != nil {
		return nil, err
	}

	newRoot := root.withStorage(newStorage)
	if skipFKHandling {
		return newRoot, nil
	}

	fkc, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	if allowDroppingFKReferenced {
		err = fkc.RemoveAndUnresolveTables(ctx, root, tables...)
	} else {
		err = fkc.RemoveTables(ctx, tables...)
	}

	if err != nil {
		return nil, err
	}

	return newRoot.PutForeignKeyCollection(ctx, fkc)
}

// GetForeignKeyCollection returns the ForeignKeyCollection for this root. As collections are meant to be modified
// in-place, each returned collection may freely be altered without affecting future returned collections from this root.
func (root *RootValue) GetForeignKeyCollection(ctx context.Context) (*ForeignKeyCollection, error) {
	if root.fkc == nil {
		fkMap, ok, err := root.st.GetForeignKeys(ctx, root.vrw)
		if err != nil {
			return nil, err
		}
		if !ok {
			fkc := &ForeignKeyCollection{
				foreignKeys: map[string]ForeignKey{},
			}
			return fkc, nil
		}

		root.fkc, err = DeserializeForeignKeys(ctx, root.vrw.Format(), fkMap)
		if err != nil {
			return nil, err
		}
	}
	return root.fkc.copy(), nil
}

// PutForeignKeyCollection returns a new root with the given foreign key collection.
func (root *RootValue) PutForeignKeyCollection(ctx context.Context, fkc *ForeignKeyCollection) (*RootValue, error) {
	value, err := SerializeForeignKeys(ctx, root.vrw, fkc)
	if err != nil {
		return nil, err
	}
	newStorage, err := root.st.SetForeignKeyMap(ctx, root.vrw, value)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

// ValidateForeignKeysOnSchemas ensures that all foreign keys' tables are present, removing any foreign keys where the declared
// table is missing, and returning an error if a key is in an invalid state or a referenced table is missing. Does not
// check any tables' row data.
func (root *RootValue) ValidateForeignKeysOnSchemas(ctx context.Context) (*RootValue, error) {
	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	allTablesSlice, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	allTablesSet := make(map[string]schema.Schema)
	for _, tableName := range allTablesSlice {
		tbl, ok, err := root.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("found table `%s` in staging but could not load for foreign key check", tableName)
		}
		tblSch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		allTablesSet[tableName] = tblSch
	}

	// some of these checks are sanity checks and should never happen
	allForeignKeys := fkCollection.AllKeys()
	for _, foreignKey := range allForeignKeys {
		tblSch, existsInRoot := allTablesSet[foreignKey.TableName]
		if existsInRoot {
			if err := foreignKey.ValidateTableSchema(tblSch); err != nil {
				return nil, err
			}
			parentSch, existsInRoot := allTablesSet[foreignKey.ReferencedTableName]
			if !existsInRoot {
				return nil, fmt.Errorf("foreign key `%s` requires the referenced table `%s`", foreignKey.Name, foreignKey.ReferencedTableName)
			}
			if err := foreignKey.ValidateReferencedTableSchema(parentSch); err != nil {
				return nil, err
			}
		} else {
			if !fkCollection.RemoveKeyByName(foreignKey.Name) {
				return nil, fmt.Errorf("`%s` does not exist as a foreign key", foreignKey.Name)
			}
		}
	}

	return root.PutForeignKeyCollection(ctx, fkCollection)
}

// GetAllTagsForRoots gets all tags for |roots|.
func GetAllTagsForRoots(ctx context.Context, roots ...*RootValue) (tags schema.TagMapping, err error) {
	tags = make(schema.TagMapping)
	for _, root := range roots {
		if root == nil {
			continue
		}
		err = root.IterTables(ctx, func(tblName string, _ *Table, sch schema.Schema) (stop bool, err error) {
			for _, t := range sch.GetAllCols().Tags {
				tags.Add(t, tblName)
			}
			return
		})
		if err != nil {
			break
		}
	}
	return
}

// UnionTableNames returns an array of all table names in all roots passed as params.
// The table names are in order of the RootValues passed in.
func UnionTableNames(ctx context.Context, roots ...*RootValue) ([]string, error) {
	seenTblNamesMap := make(map[string]bool)
	tblNames := []string{}
	for _, root := range roots {
		rootTblNames, err := root.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}
		for _, tn := range rootTblNames {
			if _, ok := seenTblNamesMap[tn]; !ok {
				seenTblNamesMap[tn] = true
				tblNames = append(tblNames, tn)
			}
		}
	}

	return tblNames, nil
}

// validateTagUniqueness checks for tag collisions between the given table and the set of tables in then given root.
func validateTagUniqueness(ctx context.Context, root *RootValue, tableName string, table *Table) error {
	prev, ok, err := root.GetTable(ctx, tableName)
	if err != nil {
		return err
	}
	if ok {
		prevHash, err := prev.GetSchemaHash(ctx)
		if err != nil {
			return err
		}

		newHash, err := table.GetSchemaHash(ctx)
		if err != nil {
			return err
		}

		// short-circuit if schema unchanged
		if prevHash == newHash {
			return nil
		}
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	existing, err := GetAllTagsForRoots(ctx, root)
	if err != nil {
		return err
	}

	var ee []string
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		name, ok := existing.Get(tag)
		if ok && name != tableName {
			ee = append(ee, schema.ErrTagPrevUsed(tag, col.Name, name).Error())
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	if len(ee) > 0 {
		return fmt.Errorf(strings.Join(ee, "\n"))
	}
	return nil
}

type debugStringer interface {
	DebugString(ctx context.Context) string
}

// DebugString returns a human readable string with the contents of this root. If |transitive| is true, row data from
// all tables is also included. This method is very expensive for large root values, so |transitive| should only be used
// when debugging tests.
func (root *RootValue) DebugString(ctx context.Context, transitive bool) string {
	var buf bytes.Buffer
	buf.WriteString(root.st.DebugString(ctx))

	if transitive {
		buf.WriteString("\nTables:")
		root.IterTables(ctx, func(name string, table *Table, sch schema.Schema) (stop bool, err error) {
			buf.WriteString("\nTable ")
			buf.WriteString(name)
			buf.WriteString(":\n")

			buf.WriteString(table.DebugString(ctx))

			return false, nil
		})
	}

	return buf.String()
}

// MapTableHashes returns a map of each table name and hash.
func (root *RootValue) MapTableHashes(ctx context.Context) (map[string]hash.Hash, error) {
	names, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	nameToHash := make(map[string]hash.Hash)
	for _, name := range names {
		h, ok, err := root.GetTableHash(ctx, name)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("root found a table with name '%s' but no hash", name)
		} else {
			nameToHash[name] = h
		}
	}
	return nameToHash, nil
}

type fbRvStorage struct {
	srv *serial.RootValue
}

func (r fbRvStorage) GetFeatureVersion() (FeatureVersion, bool, error) {
	return FeatureVersion(r.srv.FeatureVersion()), true, nil
}

func (r fbRvStorage) getAddressMap(vrw types.ValueReadWriter, ns tree.NodeStore) (prolly.AddressMap, error) {
	tbytes := r.srv.TablesBytes()
	node, err := shim.NodeFromValue(types.SerialMessage(tbytes))
	if err != nil {
		return prolly.AddressMap{}, err
	}
	return prolly.NewAddressMap(node, ns)
}

func (r fbRvStorage) GetTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore) (tableMap, error) {
	am, err := r.getAddressMap(vrw, ns)
	if err != nil {
		return nil, err
	}
	return fbTableMap{am}, nil
}

type fbTableMap struct {
	prolly.AddressMap
}

func (m fbTableMap) Get(ctx context.Context, name string) (hash.Hash, error) {
	return m.AddressMap.Get(ctx, name)
}

func (m fbTableMap) Iter(ctx context.Context, cb func(string, hash.Hash) (bool, error)) error {
	var stop bool
	return m.AddressMap.IterAll(ctx, func(n string, a hash.Hash) error {
		if !stop {
			var err error
			stop, err = cb(n, a)
			return err
		}
		return nil
	})
}

func (r fbRvStorage) GetForeignKeys(ctx context.Context, vr types.ValueReader) (types.Value, bool, error) {
	addr := hash.New(r.srv.ForeignKeyAddrBytes())
	if addr.IsEmpty() {
		return types.SerialMessage{}, false, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return types.SerialMessage{}, false, err
	}
	return v.(types.SerialMessage), true, nil
}

func (r fbRvStorage) GetCollation(ctx context.Context) (schema.Collation, error) {
	collation := r.srv.Collation()
	// Pre-existing repositories will return invalid here
	if collation == serial.Collationinvalid {
		return schema.Collation_Default, nil
	}
	return schema.Collation(collation), nil
}

func (r fbRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rvStorage, error) {
	builder := flatbuffers.NewBuilder(80)

	am, err := r.getAddressMap(vrw, ns)
	if err != nil {
		return nil, err
	}
	ae := am.Editor()
	for _, e := range edits {
		if e.old_name != "" {
			oldaddr, err := am.Get(ctx, e.old_name)
			if err != nil {
				return nil, err
			}
			newaddr, err := am.Get(ctx, e.name)
			if err != nil {
				return nil, err
			}
			if oldaddr.IsEmpty() {
				return nil, ErrTableNotFound
			}
			if !newaddr.IsEmpty() {
				return nil, ErrTableExists
			}
			err = ae.Delete(ctx, e.old_name)
			if err != nil {
				return nil, err
			}
			err = ae.Update(ctx, e.name, oldaddr)
			if err != nil {
				return nil, err
			}
		} else {
			if e.ref == nil {
				err := ae.Delete(ctx, e.name)
				if err != nil {
					return nil, err
				}
			} else {
				err := ae.Update(ctx, e.name, e.ref.TargetHash())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	am, err = ae.Flush(ctx)
	if err != nil {
		return nil, err
	}

	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	tablesoff := builder.CreateByteVector(ambytes)

	fkoff := builder.CreateByteVector(r.srv.ForeignKeyAddrBytes())
	serial.RootValueStart(builder)
	serial.RootValueAddFeatureVersion(builder, r.srv.FeatureVersion())
	serial.RootValueAddCollation(builder, r.srv.Collation())
	serial.RootValueAddTables(builder, tablesoff)
	serial.RootValueAddForeignKeyAddr(builder, fkoff)

	bs := serial.FinishMessage(builder, serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
	msg, err := serial.TryGetRootAsRootValue(bs, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	return fbRvStorage{msg}, nil
}

func (r fbRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (rvStorage, error) {
	var h hash.Hash
	isempty, err := emptyForeignKeyCollection(v.(types.SerialMessage))
	if err != nil {
		return nil, err
	}
	if !isempty {
		ref, err := vrw.WriteValue(ctx, v)
		if err != nil {
			return nil, err
		}
		h = ref.TargetHash()
	}
	ret := r.clone()
	copy(ret.srv.ForeignKeyAddrBytes(), h[:])
	return ret, nil
}

func (r fbRvStorage) SetFeatureVersion(v FeatureVersion) (rvStorage, error) {
	ret := r.clone()
	ret.srv.MutateFeatureVersion(int64(v))
	return ret, nil
}

func (r fbRvStorage) SetCollation(ctx context.Context, collation schema.Collation) (rvStorage, error) {
	ret := r.clone()
	ret.srv.MutateCollation(serial.Collation(collation))
	return ret, nil
}

func (r fbRvStorage) clone() fbRvStorage {
	bs := make([]byte, len(r.srv.Table().Bytes))
	copy(bs, r.srv.Table().Bytes)
	var ret serial.RootValue
	ret.Init(bs, r.srv.Table().Pos)
	return fbRvStorage{&ret}
}

func (r fbRvStorage) DebugString(ctx context.Context) string {
	return fmt.Sprintf("fbRvStorage[%d, %s, %s]",
		r.srv.FeatureVersion(),
		"...", // TODO: Print out tables map
		hash.New(r.srv.ForeignKeyAddrBytes()).String())
}

func (r fbRvStorage) nomsValue() types.Value {
	return types.SerialMessage(r.srv.Table().Bytes)
}

type DataCacheKey struct {
	hash.Hash
}

func NewDataCacheKey(rv *RootValue) (DataCacheKey, error) {
	hash, err := rv.HashOf()
	if err != nil {
		return DataCacheKey{}, err
	}

	return DataCacheKey{hash}, nil
}

// HackNomsValuesFromRootValues unwraps a RootVal to a noms Value.
// Deprecated: only for use in dolt migrate.
func HackNomsValuesFromRootValues(root *RootValue) types.Value {
	return root.nomsValue()
}
