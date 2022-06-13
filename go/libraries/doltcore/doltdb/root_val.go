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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey       = "tables"
	superSchemasKey = "super_schemas"
	foreignKeyKey   = "foreign_key"
	featureVersKey  = "feature_ver"
)

type FeatureVersion int64

// DoltFeatureVersion is described in feature_version.md.
// only variable for testing.
var DoltFeatureVersion FeatureVersion = 3 // last bumped when storing creation time for triggers

// RootValue is the value of the Database and is the committed value in every Dolt commit.
type RootValue struct {
	vrw types.ValueReadWriter
	st  rvStorage
	fkc *ForeignKeyCollection // cache the first load
}

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

	GetTablesMap(ctx context.Context, vr types.ValueReadWriter) (tableMap, error)
	GetSuperSchemaMap(ctx context.Context, vr types.ValueReader) (types.Map, bool, error)
	GetForeignKeyMap(ctx context.Context, vr types.ValueReader) (types.Map, bool, error)

	SetSuperSchemaMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error)
	SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error)
	SetFeatureVersion(v FeatureVersion) (rvStorage, error)

	EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, edits []tableEdit) (rvStorage, error)

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

func (r nomsRvStorage) GetTablesMap(context.Context, types.ValueReadWriter) (tableMap, error) {
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

func (r nomsRvStorage) GetSuperSchemaMap(context.Context, types.ValueReader) (types.Map, bool, error) {
	v, found, err := r.valueSt.MaybeGet(superSchemasKey)
	if err != nil {
		return types.Map{}, false, err
	}
	if !found {
		return types.Map{}, false, nil
	}
	return v.(types.Map), true, nil
}

func (r nomsRvStorage) GetForeignKeyMap(context.Context, types.ValueReader) (types.Map, bool, error) {
	v, found, err := r.valueSt.MaybeGet(foreignKeyKey)
	if err != nil {
		return types.Map{}, false, err
	}
	if !found {
		return types.Map{}, false, err
	}
	return v.(types.Map), true, nil
}

func (r nomsRvStorage) SetSuperSchemaMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error) {
	st, err := r.valueSt.Set(superSchemasKey, m)
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, edits []tableEdit) (rvStorage, error) {
	m, err := r.GetTablesMap(ctx, vrw)
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

func (r nomsRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error) {
	st, err := r.valueSt.Set(foreignKeyKey, m)
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

func newRootValue(vrw types.ValueReadWriter, v types.Value) (*RootValue, error) {
	var storage rvStorage

	if vrw.Format().UsesFlatbuffers() {
		srv := serial.GetRootAsRootValue([]byte(v.(types.SerialMessage)), 0)
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

	return &RootValue{vrw, storage, nil}, nil
}

func isRootValue(nbf *types.NomsBinFormat, val types.Value) bool {
	if nbf.UsesFlatbuffers() {
		if sm, ok := val.(types.SerialMessage); ok {
			return string(serial.GetFileID([]byte(sm))) == serial.RootValueFileID
		}
	} else {
		if st, ok := val.(types.Struct); ok {
			return st.Name() == ddbRootStructName
		}
	}
	return false
}

func EmptyRootValue(ctx context.Context, vrw types.ValueReadWriter) (*RootValue, error) {
	if vrw.Format().UsesFlatbuffers() {
		builder := flatbuffers.NewBuilder(80)

		ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(vrw))
		emptyam := prolly.NewEmptyAddressMap(ns)
		ambytes := []byte(tree.ValueFromNode(emptyam.Node()).(types.TupleRowStorage))
		tablesoff := builder.CreateByteVector(ambytes)

		var empty hash.Hash
		fkoff := builder.CreateByteVector(empty[:])
		ssoff := builder.CreateByteVector(empty[:])
		serial.RootValueStart(builder)
		serial.RootValueAddFeatureVersion(builder, int64(DoltFeatureVersion))
		serial.RootValueAddTables(builder, tablesoff)
		serial.RootValueAddForeignKeyAddr(builder, fkoff)
		serial.RootValueAddSuperSchemasAddr(builder, ssoff)
		builder.FinishWithFileIdentifier(serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
		bs := builder.FinishedBytes()
		return newRootValue(vrw, types.SerialMessage(bs))
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

	return newRootValue(vrw, st)
}

func (root *RootValue) VRW() types.ValueReadWriter {
	return root.vrw
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

func (root *RootValue) HasTable(ctx context.Context, tName string) (bool, error) {
	tableMap, err := root.st.GetTablesMap(ctx, root.vrw)
	if err != nil {
		return false, err
	}
	a, err := tableMap.Get(ctx, tName)
	if err != nil {
		return false, err
	}
	return !a.IsEmpty(), nil
}

// GetSuperSchema returns the SuperSchema for the table name specified if that table exists.
func (root *RootValue) GetSuperSchema(ctx context.Context, tName string) (*schema.SuperSchema, bool, error) {
	// SuperSchema is only persisted on Commit()
	ss, found, err := root.getSuperSchemaAtLastCommit(ctx, tName)

	if err != nil {
		return nil, false, err
	}
	if !found {
		ss, _ = schema.NewSuperSchema()
	}

	t, tblFound, err := root.GetTable(ctx, tName)
	if err != nil {
		return nil, false, err
	}

	if !found && !tblFound {
		// table doesn't exist in current commit or in history
		return nil, false, nil
	}

	if tblFound {
		sch, err := t.GetSchema(ctx)
		if err != nil {
			return nil, false, err
		}

		err = ss.AddSchemas(sch)
		if err != nil {
			return nil, false, err
		}
	}

	return ss, true, err
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

	var existingCols []schema.Column
	newTags := make([]uint64, len(newColNames))

	// Get existing columns from the current root, or the head root if the table doesn't exist in the current root. The
	// latter case is to support reusing table tags in the case of drop / create in the same session, which is common
	// during import.
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

	// If we found any existing columns set them in the newTags list.
	// We only do this if we want to reuse columns from a previous existing table with the same name
	if headRoot != nil {
		for _, col := range existingCols {
			for i := range newColNames {
				// Only re-use tags if the noms kind didn't change
				// TODO: revisit this when new storage format is further along
				if strings.ToLower(newColNames[i]) == strings.ToLower(col.Name) &&
					newColKinds[i] == col.TypeInfo.NomsKind() {
					newTags[i] = col.Tag
					break
				}
			}
		}
	}

	var existingColKinds []types.NomsKind
	for _, col := range existingCols {
		existingColKinds = append(existingColKinds, col.Kind)
	}

	rootSuperSchema, err := GetRootValueSuperSchema(ctx, root)
	if err != nil {
		return nil, err
	}

	existingTags := set.NewUint64Set(rootSuperSchema.AllTags())
	for i := range newTags {
		if newTags[i] > 0 {
			continue
		}

		newTags[i] = schema.AutoGenerateTag(existingTags, tableName, existingColKinds, newColNames[i], newColKinds[i])
		existingColKinds = append(existingColKinds, newColKinds[i])
		existingTags.Add(newTags[i])
	}

	return newTags, nil
}

// GerSuperSchemaMap returns the Noms map that tracks SuperSchemas, used to create new RootValues on checkout branch.
func (root *RootValue) GetSuperSchemaMap(ctx context.Context) (types.Map, error) {
	return root.getOrCreateSuperSchemaMap(ctx)
}

// SuperSchemas are only persisted on commit.
func (root *RootValue) getSuperSchemaAtLastCommit(ctx context.Context, tName string) (*schema.SuperSchema, bool, error) {
	ssm, err := root.getOrCreateSuperSchemaMap(ctx)
	if err != nil {
		return nil, false, err
	}

	v, found, err := ssm.MaybeGet(ctx, types.String(tName))
	if err != nil {
		return nil, false, err
	}
	if !found {
		// Super Schema doesn't exist for new or nonexistent table
		return nil, false, nil
	}

	ssValRef := v.(types.Ref)
	ssVal, err := ssValRef.TargetValue(ctx, root.vrw)
	if err != nil {
		return nil, false, err
	}

	ss, err := encoding.UnmarshalSuperSchemaNomsValue(ctx, root.vrw.Format(), ssVal)
	if err != nil {
		return nil, false, err
	}

	return ss, true, nil
}

func (root *RootValue) getOrCreateSuperSchemaMap(ctx context.Context) (types.Map, error) {
	m, found, err := root.st.GetSuperSchemaMap(ctx, root.vrw)
	if err != nil {
		return types.Map{}, err
	}
	if found {
		return m, nil
	}

	return types.NewMap(ctx, root.vrw)
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

	table, err := durable.TableFromAddr(ctx, root.VRW(), addr)
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

// GetTableByColTag looks for the table containing the given column tag. It returns false if no table exists in the history.
// If the table containing the given tag previously existed and was deleted, it will return its name and a nil pointer.
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

	err = root.iterSuperSchemas(ctx, func(tn string, ss *schema.SuperSchema) (bool, error) {
		_, found = ss.GetByTag(tag)
		if found {
			name = tn
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
	return root.st.GetTablesMap(ctx, root.vrw)
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

		cv, err := tbl.GetConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}

		if cv.Len() > 0 {
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
		nt, err := durable.TableFromAddr(ctx, root.VRW(), addr)
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

func (root *RootValue) iterSuperSchemas(ctx context.Context, cb func(name string, ss *schema.SuperSchema) (stop bool, err error)) error {
	m, err := root.getOrCreateSuperSchemaMap(ctx)
	if err != nil {
		return err
	}

	return m.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		name := string(key.(types.String))

		// use GetSuperSchema() to pickup uncommitted SuperSchemas
		ss, _, err := root.GetSuperSchema(ctx, name)
		if err != nil {
			return false, err
		}

		return cb(name, ss)
	})
}

// PutSuperSchema writes a new map entry for the table name and super schema supplied, it will overwrite an existing entry.
func (root *RootValue) PutSuperSchema(ctx context.Context, tName string, ss *schema.SuperSchema) (*RootValue, error) {
	ssm, err := root.getOrCreateSuperSchemaMap(ctx)

	if err != nil {
		return nil, err
	}

	ssVal, err := encoding.MarshalSuperSchemaAsNomsValue(ctx, root.VRW(), ss)

	if err != nil {
		return nil, err
	}

	ssRef, err := WriteValAndGetRef(ctx, root.VRW(), ssVal)

	if err != nil {
		return nil, err
	}

	m, err := ssm.Edit().Set(types.String(tName), ssRef).Map(ctx)

	if err != nil {
		return nil, err
	}

	newStorage, err := root.st.SetSuperSchemaMap(ctx, root.vrw, m)
	if err != nil {
		return nil, err
	}

	return root.withStorage(newStorage), nil
}

func (root *RootValue) withStorage(st rvStorage) *RootValue {
	return &RootValue{root.vrw, st, nil}
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

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, []tableEdit{{name: tName, ref: &ref}})
	if err != nil {
		return nil, err
	}

	return root.withStorage(newStorage), nil
}

// CreateEmptyTable creates an empty table in this root with the name and schema given, returning the new root value.
func (root *RootValue) CreateEmptyTable(ctx context.Context, tName string, sch schema.Schema) (*RootValue, error) {
	empty, err := durable.NewEmptyIndex(ctx, root.vrw, sch)
	if err != nil {
		return nil, err
	}

	indexes := durable.NewIndexSet(ctx, root.VRW())
	err = sch.Indexes().Iter(func(index schema.Index) (stop bool, err error) {
		// create an empty map for every index
		indexes, err = indexes.PutIndex(ctx, index.Name(), empty)
		return
	})
	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(ctx, root.VRW(), sch, empty, indexes, nil)
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
	return root.st.nomsValue().Hash(root.vrw.Format())
}

// UpdateSuperSchemasFromOther updates SuperSchemas of tblNames using SuperSchemas from other.
func (root *RootValue) UpdateSuperSchemasFromOther(ctx context.Context, tblNames []string, other *RootValue) (*RootValue, error) {
	ssm, err := root.getOrCreateSuperSchemaMap(ctx)

	if err != nil {
		return nil, err
	}

	sse := ssm.Edit()

	for _, tn := range tblNames {

		ss, found, err := root.GetSuperSchema(ctx, tn)

		if err != nil {
			return nil, err
		}

		oss, foundOther, err := other.GetSuperSchema(ctx, tn)

		if err != nil {
			return nil, err
		}

		var newSS *schema.SuperSchema
		if found && foundOther {
			newSS, err = schema.SuperSchemaUnion(ss, oss)
		} else if found {
			newSS = ss
		} else if foundOther {
			newSS = oss
		} else {
			h, _ := root.HashOf()
			oh, _ := other.HashOf()
			return nil, errors.New(fmt.Sprintf("table %s does not exist in root %s or root %s", tn, h.String(), oh.String()))
		}

		if err != nil {
			return nil, err
		}

		ssVal, err := encoding.MarshalSuperSchemaAsNomsValue(ctx, root.VRW(), newSS)

		if err != nil {
			return nil, err
		}

		ssRef, err := WriteValAndGetRef(ctx, root.VRW(), ssVal)

		if err != nil {
			return nil, err
		}

		sse = sse.Set(types.String(tn), ssRef)
	}

	m, err := sse.Map(ctx)

	if err != nil {
		return nil, err
	}

	newStorage, err := root.st.SetSuperSchemaMap(ctx, root.vrw, m)
	if err != nil {
		return nil, err
	}

	return root.withStorage(newStorage), nil
}

// RenameTable renames a table by changing its string key in the RootValue's table map. In order to preserve
// column tag information, use this method instead of a table drop + add.
func (root *RootValue) RenameTable(ctx context.Context, oldName, newName string) (*RootValue, error) {

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, []tableEdit{{old_name: oldName, name: newName}})
	if err != nil {
		return nil, err
	}

	ssMap, err := root.getOrCreateSuperSchemaMap(ctx)
	if err != nil {
		return nil, err
	}

	ssv, found, err := ssMap.MaybeGet(ctx, types.String(oldName))
	if err != nil {
		return nil, err
	}
	if found {
		ssme := ssMap.Edit().Remove(types.String(oldName))
		ssme = ssme.Set(types.String(newName), ssv)
		ssMap, err = ssme.Map(ctx)
		if err != nil {
			return nil, err
		}

		newStorage, err = newStorage.SetSuperSchemaMap(ctx, root.vrw, ssMap)
		if err != nil {
			return nil, err
		}
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

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, edits)
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
		fkMap, err := root.GetForeignKeyCollectionMap(ctx)
		if err != nil {
			return nil, err
		}
		root.fkc, err = LoadForeignKeyCollection(ctx, fkMap)
		if err != nil {
			return nil, err
		}
	}
	return root.fkc.copy(), nil
}

// GetForeignKeyCollectionMap returns the persisted noms Map of the foreign key collection on this root. If the intent
// is to retrieve a ForeignKeyCollection in particular, it is advised to call GetForeignKeyCollection as it caches the
// result for performance.
func (root *RootValue) GetForeignKeyCollectionMap(ctx context.Context) (types.Map, error) {
	fkMap, found, err := root.st.GetForeignKeyMap(ctx, root.vrw)
	if err != nil {
		return types.Map{}, err
	}

	if !found {
		fkMap, err = types.NewMap(ctx, root.vrw)
		if err != nil {
			return types.Map{}, err
		}
	}

	return fkMap, nil
}

// PutForeignKeyCollection returns a new root with the given foreign key collection.
func (root *RootValue) PutForeignKeyCollection(ctx context.Context, fkc *ForeignKeyCollection) (*RootValue, error) {
	fkMap, err := fkc.Map(ctx, root.vrw)
	if err != nil {
		return nil, err
	}
	newStorage, err := root.st.SetForeignKeyMap(ctx, root.vrw, fkMap)
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

// GetRootValueSuperSchema creates a SuperSchema with every column in history of root.
func GetRootValueSuperSchema(ctx context.Context, root *RootValue) (*schema.SuperSchema, error) {
	ssMap, err := root.getOrCreateSuperSchemaMap(ctx)

	if err != nil {
		return nil, err
	}

	var sss []*schema.SuperSchema
	err = ssMap.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		ssValRef := value.(types.Ref)
		ssVal, err := ssValRef.TargetValue(ctx, root.vrw)

		if err != nil {
			return true, err
		}

		ss, err := encoding.UnmarshalSuperSchemaNomsValue(ctx, root.vrw.Format(), ssVal)

		if err != nil {
			return true, err
		}

		sss = append(sss, ss) // go get -f parseltongue
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	rootSuperSchema, err := schema.SuperSchemaUnion(sss...)
	if err != nil {
		return nil, err
	}

	// super schemas are only persisted on commit, so add in working schemas
	err = root.IterTables(ctx, func(name string, table *Table, sch schema.Schema) (stop bool, err error) {
		err = rootSuperSchema.AddSchemas(sch)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return rootSuperSchema, nil
}

// UnionTableNames returns an array of all table names in all roots passed as params.
func UnionTableNames(ctx context.Context, roots ...*RootValue) ([]string, error) {
	allTblNames := make([]string, 0, 16)
	for _, root := range roots {
		tblNames, err := root.GetTableNames(ctx)

		if err != nil {
			return nil, err
		}

		allTblNames = append(allTblNames, tblNames...)
	}

	return set.Unique(allTblNames), nil
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

	var ee []string
	err = root.iterSuperSchemas(ctx, func(tn string, ss *schema.SuperSchema) (stop bool, err error) {
		if tn == tableName {
			return false, nil
		}

		err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			_, ok := ss.GetByTag(tag)
			if ok {
				ee = append(ee, schema.ErrTagPrevUsed(tag, col.Name, tn).Error())
			}
			return false, nil
		})
		return false, err
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

func (r fbRvStorage) getAddressMap(vrw types.ValueReadWriter) prolly.AddressMap {
	tbytes := r.srv.TablesBytes()
	node := shim.NodeFromValue(types.TupleRowStorage(tbytes))
	ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(vrw))
	return prolly.NewAddressMap(node, ns)
}

func (r fbRvStorage) GetTablesMap(ctx context.Context, vrw types.ValueReadWriter) (tableMap, error) {
	am := r.getAddressMap(vrw)
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

func (r fbRvStorage) GetSuperSchemaMap(ctx context.Context, vr types.ValueReader) (types.Map, bool, error) {
	addr := hash.New(r.srv.SuperSchemasAddrBytes())
	if addr.IsEmpty() {
		return types.Map{}, false, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return types.Map{}, false, err
	}
	return v.(types.Map), true, nil
}

func (r fbRvStorage) GetForeignKeyMap(ctx context.Context, vr types.ValueReader) (types.Map, bool, error) {
	addr := hash.New(r.srv.ForeignKeyAddrBytes())
	if addr.IsEmpty() {
		return types.Map{}, false, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return types.Map{}, false, err
	}
	return v.(types.Map), true, nil
}

func (r fbRvStorage) SetSuperSchemaMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error) {
	var h hash.Hash
	if !m.Empty() {
		ref, err := vrw.WriteValue(ctx, m)
		if err != nil {
			return nil, err
		}
		h = ref.TargetHash()
	}
	ret := r.clone()
	copy(ret.srv.SuperSchemasAddrBytes(), h[:])
	return ret, nil
}

func (r fbRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, edits []tableEdit) (rvStorage, error) {
	builder := flatbuffers.NewBuilder(80)

	am := r.getAddressMap(vrw)
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
	am, err := ae.Flush(ctx)
	if err != nil {
		return nil, err
	}

	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.TupleRowStorage))
	tablesoff := builder.CreateByteVector(ambytes)

	fkoff := builder.CreateByteVector(r.srv.ForeignKeyAddrBytes())
	ssoff := builder.CreateByteVector(r.srv.SuperSchemasAddrBytes())
	serial.RootValueStart(builder)
	serial.RootValueAddFeatureVersion(builder, r.srv.FeatureVersion())
	serial.RootValueAddTables(builder, tablesoff)
	serial.RootValueAddForeignKeyAddr(builder, fkoff)
	serial.RootValueAddSuperSchemasAddr(builder, ssoff)
	builder.FinishWithFileIdentifier(serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
	bs := builder.FinishedBytes()
	return fbRvStorage{serial.GetRootAsRootValue(bs, 0)}, nil
}

func (r fbRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, m types.Map) (rvStorage, error) {
	var h hash.Hash
	if !m.Empty() {
		ref, err := vrw.WriteValue(ctx, m)
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

func (r fbRvStorage) clone() fbRvStorage {
	bs := make([]byte, len(r.srv.Table().Bytes))
	copy(bs, r.srv.Table().Bytes)
	var ret serial.RootValue
	ret.Init(bs, r.srv.Table().Pos)
	return fbRvStorage{&ret}
}

func (r fbRvStorage) DebugString(ctx context.Context) string {
	return fmt.Sprintf("fbRvStorage[%d, %s, %s, %s]",
		r.srv.FeatureVersion(),
		"...", // TODO: Print out tables map
		hash.New(r.srv.ForeignKeyAddrBytes()).String(),
		hash.New(r.srv.SuperSchemasAddrBytes()).String())
}

func (r fbRvStorage) nomsValue() types.Value {
	return types.SerialMessage(r.srv.Table().Bytes)
}
