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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
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
var DoltFeatureVersion FeatureVersion = 2 // last bumped when changing TEXT types to use noms Blobs

// RootValue defines the structure used inside all Dolthub noms dbs
type RootValue struct {
	vrw     types.ValueReadWriter
	valueSt types.Struct
	fkc     *ForeignKeyCollection // cache the first load
}

func newRootValue(vrw types.ValueReadWriter, st types.Struct) (*RootValue, error) {
	v, ok, err := st.MaybeGet(featureVersKey)
	if err != nil {
		return nil, err
	}
	if ok {
		ver := FeatureVersion(v.(types.Int))
		if DoltFeatureVersion < ver {
			return nil, ErrClientOutOfDate{
				ClientVer: DoltFeatureVersion,
				RepoVer:   ver,
			}
		}
	}

	return &RootValue{vrw, st, nil}, nil
}

func EmptyRootValue(ctx context.Context, vrw types.ValueReadWriter) (*RootValue, error) {
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
	v, ok, err := root.valueSt.MaybeGet(featureVersKey)
	if err != nil || !ok {
		return ver, ok, err
	}
	ver = FeatureVersion(v.(types.Int))
	return ver, ok, err
}

func (root *RootValue) HasTable(ctx context.Context, tName string) (bool, error) {
	val, found, err := root.valueSt.MaybeGet(tablesKey)

	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	tableMap := val.(types.Map)
	return tableMap.Has(ctx, types.String(tName))
}

// TableNameInUse checks if a name can be used to create a new table. The most recent
// names of all current tables and all previously existing tables cannot be used.
func (root *RootValue) TableNameInUse(ctx context.Context, tName string) (bool, error) {
	_, ok, err := root.GetSuperSchema(ctx, tName)
	if err != nil {
		return false, err
	}
	return ok, nil
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
				if strings.ToLower(newColNames[i]) == strings.ToLower(col.Name) {
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
	v, found, err := root.valueSt.MaybeGet(superSchemasKey)

	if err != nil {
		return types.EmptyMap, err
	}

	var ssm types.Map
	if found {
		ssm = v.(types.Map)
	} else {
		ssm, err = types.NewMap(ctx, root.vrw)
	}
	return ssm, err
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
	tableMap, err := root.getTableMap()
	if err != nil {
		return hash.Hash{}, false, err
	}

	tVal, found, err := tableMap.MaybeGet(ctx, types.String(tName))
	if err != nil {
		return hash.Hash{}, false, err
	}

	if tVal == nil || !found {
		return hash.Hash{}, false, nil
	}

	tValRef := tVal.(types.Ref)
	return tValRef.TargetHash(), true, nil
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
	tableMap, err := root.getTableMap()
	if err != nil {
		return "", false, err
	}
	if ok, err := tableMap.Has(ctx, types.String(tName)); err != nil {
		return "", false, err
	} else if ok {
		return tName, true, nil
	}

	found := false
	lwrName := strings.ToLower(tName)
	err = tableMap.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		keyStr := string(key.(types.String))
		if lwrName == strings.ToLower(keyStr) {
			tName = keyStr
			found = true
			return true, nil
		}

		return false, nil
	})
	if err != nil {
		return "", false, nil
	}
	return tName, found, nil
}

// GetTable will retrieve a table by its case-sensitive name.
func (root *RootValue) GetTable(ctx context.Context, tName string) (*Table, bool, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, false, err
	}

	r, found, err := tableMap.MaybeGet(ctx, types.String(tName))
	if err != nil {
		return nil, false, err
	}
	if r == nil || !found {
		return nil, false, nil
	}

	table, err := durable.NomsTableFromRef(ctx, root.VRW(), r.(types.Ref))
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
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	numTables := int(tableMap.Len())
	names := make([]string, 0, numTables)

	err = tableMap.Iter(ctx, func(key, _ types.Value) (stop bool, err error) {
		names = append(names, string(key.(types.String)))
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return names, nil
}

func (root *RootValue) getTableMap() (types.Map, error) {
	val, found, err := root.valueSt.MaybeGet(tablesKey)

	if err != nil {
		return types.EmptyMap, err
	}

	if !found || val == nil {
		return types.EmptyMap, err
	}

	tableMap := val.(types.Map)
	return tableMap, err
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
	tm, err := root.getTableMap()

	if err != nil {
		return err
	}

	itr, err := tm.Iterator(ctx)

	if err != nil {
		return err
	}

	for {
		nm, tableRef, err := itr.Next(ctx)

		if err != nil || nm == nil || tableRef == nil {
			return err
		}

		name := string(nm.(types.String))
		nt, err := durable.NomsTableFromRef(ctx, root.VRW(), tableRef.(types.Ref))
		if err != nil {
			return err
		}
		tbl := &Table{table: nt}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return err
		}

		stop, err := cb(name, tbl, sch)

		if err != nil || stop {
			return err
		}
	}
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
	newRoot := root
	ssm, err := newRoot.getOrCreateSuperSchemaMap(ctx)

	if err != nil {
		return nil, err
	}

	ssVal, err := encoding.MarshalSuperSchemaAsNomsValue(ctx, newRoot.VRW(), ss)

	if err != nil {
		return nil, err
	}

	ssRef, err := WriteValAndGetRef(ctx, newRoot.VRW(), ssVal)

	if err != nil {
		return nil, err
	}

	m, err := ssm.Edit().Set(types.String(tName), ssRef).Map(ctx)

	if err != nil {
		return nil, err
	}

	newRootSt := newRoot.valueSt
	newRootSt, err = newRootSt.Set(superSchemasKey, m)

	if err != nil {
		return nil, err
	}

	return newRootValue(root.vrw, newRootSt)
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

func putTable(ctx context.Context, root *RootValue, tName string, tableRef types.Ref) (*RootValue, error) {
	if !IsValidTableName(tName) {
		panic("Don't attempt to put a table with a name that fails the IsValidTableName check")
	}

	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	tMapEditor := tableMap.Edit()
	tMapEditor = tMapEditor.Set(types.String(tName), tableRef)

	m, err := tMapEditor.Map(ctx)

	if err != nil {
		return nil, err
	}

	rootValSt := root.valueSt
	rootValSt, err = rootValSt.Set(tablesKey, m)

	if err != nil {
		return nil, err
	}

	return newRootValue(root.vrw, rootValSt)
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
	return root.valueSt.Hash(root.vrw.Format())
}

// UpdateSuperSchemasFromOther updates SuperSchemas of tblNames using SuperSchemas from other.
func (root *RootValue) UpdateSuperSchemasFromOther(ctx context.Context, tblNames []string, other *RootValue) (*RootValue, error) {
	newRoot := root
	ssm, err := newRoot.getOrCreateSuperSchemaMap(ctx)

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

		ssVal, err := encoding.MarshalSuperSchemaAsNomsValue(ctx, newRoot.VRW(), newSS)

		if err != nil {
			return nil, err
		}

		ssRef, err := WriteValAndGetRef(ctx, newRoot.VRW(), ssVal)

		if err != nil {
			return nil, err
		}

		sse = sse.Set(types.String(tn), ssRef)
	}

	m, err := sse.Map(ctx)

	if err != nil {
		return nil, err
	}

	newRootSt := newRoot.valueSt
	newRootSt, err = newRootSt.Set(superSchemasKey, m)

	if err != nil {
		return nil, err
	}

	return newRootValue(root.vrw, newRootSt)
}

// RenameTable renames a table by changing its string key in the RootValue's table map. In order to preserve
// column tag information, use this method instead of a table drop + add.
func (root *RootValue) RenameTable(ctx context.Context, oldName, newName string) (*RootValue, error) {
	tableMap, err := root.getTableMap()
	if err != nil {
		return nil, err
	}

	tv, found, err := tableMap.MaybeGet(ctx, types.String(oldName))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrTableNotFound
	}

	_, found, err = tableMap.MaybeGet(ctx, types.String(newName))
	if err != nil {
		return nil, err
	}
	if found {
		return nil, ErrTableExists
	}

	tme := tableMap.Edit().Remove(types.String(oldName))
	tme = tme.Set(types.String(newName), tv)
	tableMap, err = tme.Map(ctx)
	if err != nil {
		return nil, err
	}
	rootValSt := root.valueSt
	rootValSt, err = rootValSt.Set(tablesKey, tableMap)
	if err != nil {
		return nil, err
	}

	foreignKeyCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	foreignKeyCollection.RenameTable(oldName, newName)
	fkMap, err := foreignKeyCollection.Map(ctx, root.vrw)
	if err != nil {
		return nil, err
	}
	rootValSt, err = rootValSt.Set(foreignKeyKey, fkMap)
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

		rootValSt, err = rootValSt.Set(superSchemasKey, ssMap)
		if err != nil {
			return nil, err
		}
		return newRootValue(root.vrw, rootValSt)
	}

	return newRootValue(root.vrw, rootValSt)
}

func (root *RootValue) RemoveTables(ctx context.Context, allowDroppingFKReferenced bool, tables ...string) (*RootValue, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	me := tableMap.Edit()
	for _, tbl := range tables {
		key := types.String(tbl)

		if has, err := tableMap.Has(ctx, key); err != nil {
			return nil, err
		} else if has {
			me = me.Remove(key)
		} else {
			return nil, ErrTableNotFound
		}
	}

	m, err := me.Map(ctx)

	if err != nil {
		return nil, err
	}

	rootValSt, err := root.valueSt.Set(tablesKey, m)

	if err != nil {
		return nil, err
	}

	newRoot, err := newRootValue(root.vrw, rootValSt)
	if err != nil {
		return nil, err
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
	v, found, err := root.valueSt.MaybeGet(foreignKeyKey)
	if err != nil {
		return types.EmptyMap, err
	}

	var fkMap types.Map
	if found {
		fkMap = v.(types.Map)
	} else {
		fkMap, err = types.NewMap(ctx, root.vrw)
		if err != nil {
			return types.EmptyMap, err
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
	rootValSt, err := root.valueSt.Set(foreignKeyKey, fkMap)
	if err != nil {
		return nil, err
	}
	return &RootValue{root.vrw, rootValSt, fkc.copy()}, nil
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
			err := fkCollection.RemoveKeyByName(foreignKey.Name)
			if err != nil {
				return nil, err
			}
		}
	}

	return root.PutForeignKeyCollection(ctx, fkCollection)
}

// RootNeedsUniqueTagsMigration determines if this root needs to be migrated to uniquify its tags.
func RootNeedsUniqueTagsMigration(root *RootValue) (bool, error) {
	// SuperSchemas were added in the same update that required unique tags. If a root does not have a
	// SuperSchema map then it was created before the unique tags constraint was enforced.
	_, found, err := root.valueSt.MaybeGet(superSchemasKey)
	if err != nil {
		return false, err
	}
	needToMigrate := !found
	return needToMigrate, nil
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
	tblMap, err := root.getTableMap()
	if err != nil {
		return nil, err
	}

	err = tblMap.Iter(ctx, func(key, _ types.Value) (stop bool, err error) {
		tbl, _, err := root.GetTable(ctx, string(key.(types.String)))
		if err != nil {
			return true, err
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return true, err
		}
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

// DebugString returns a human readable string with the contents of this root. If |transitive| is true, row data from
// all tables is also included. This method is very expensive for large root values, so |transitive| should only be used
// when debugging tests.
func (root *RootValue) DebugString(ctx context.Context, transitive bool) string {
	var buf bytes.Buffer
	err := types.WriteEncodedValue(ctx, &buf, root.valueSt)
	if err != nil {
		panic(err)
	}

	if transitive {
		buf.WriteString("\nTables:")
		root.IterTables(ctx, func(name string, table *Table, sch schema.Schema) (stop bool, err error) {
			buf.WriteString("\nName:")
			buf.WriteString(name)
			buf.WriteString("\n")

			buf.WriteString("Data:\n")
			data, err := table.GetNomsRowData(ctx)
			if err != nil {
				panic(err)
			}

			err = types.WriteEncodedValue(ctx, &buf, data)
			if err != nil {
				panic(err)
			}
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
