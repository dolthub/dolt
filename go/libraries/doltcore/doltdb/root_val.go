// Copyright 2019 Liquidata, Inc.
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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey       = "tables"
	superSchemasKey = "super_schemas"
)

// RootValue defines the structure used inside all Liquidata noms dbs
type RootValue struct {
	vrw     types.ValueReadWriter
	valueSt types.Struct
}

type DocDetails struct {
	NewerText []byte
	DocPk     string
	Value     types.Value
	File      string
}

func NewRootValue(ctx context.Context, vrw types.ValueReadWriter, tables map[string]hash.Hash, ssMap types.Map) (*RootValue, error) {
	values := make([]types.Value, 2*len(tables))

	index := 0
	for k, v := range tables {
		values[index] = types.String(k)
		valForHash, err := vrw.ReadValue(ctx, v)

		if err != nil {
			return nil, err
		}

		if valForHash == nil {
			return nil, ErrHashNotFound
		}

		values[index+1], err = types.NewRef(valForHash, vrw.Format())

		if err != nil {
			return nil, err
		}

		index += 2
	}

	tblMap, err := types.NewMap(ctx, vrw, values...)

	if err != nil {
		return nil, err
	}

	return newRootFromMaps(vrw, tblMap, ssMap)
}

func newRootValue(vrw types.ValueReadWriter, st types.Struct) *RootValue {
	return &RootValue{vrw, st}
}

func emptyRootValue(ctx context.Context, vrw types.ValueReadWriter) (*RootValue, error) {
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}

	mm, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}

	return newRootFromMaps(vrw, m, mm)
}

func newRootFromMaps(vrw types.ValueReadWriter, tblMap types.Map, ssMap types.Map) (*RootValue, error) {
	sd := types.StructData{
		tablesKey:       tblMap,
		superSchemasKey: ssMap,
	}

	st, err := types.NewStruct(vrw.Format(), ddbRootStructName, sd)

	if err != nil {
		return nil, err
	}

	return newRootValue(vrw, st), err
}

func (root *RootValue) VRW() types.ValueReadWriter {
	return root.vrw
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

// TableNamesForTags returns a map from column tags to the column's table.
func (root *RootValue) TablesNamesForTags(ctx context.Context, tags ...uint64) (tagToTable map[uint64]string, err error) {
	m, err := root.getOrCreateSuperSchemaMap(ctx)

	if err != nil {
		return nil, err
	}

	tagToTable = make(map[uint64]string)
	err = m.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		ssValRef := value.(types.Ref)
		ssVal, err := ssValRef.TargetValue(ctx, root.vrw)

		if err != nil {
			return true, err
		}

		ss, err := encoding.UnmarshalSuperSchemaNomsValue(ctx, root.vrw.Format(), ssVal)

		if err != nil {
			return true, err
		}

		for _, t := range tags {
			_, found := ss.GetByTag(t)

			if found {
				tagToTable[t] = string(key.(types.String))
			}
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	// Now check if the tag exists in the working set of any table
	tNames, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	for _, tn := range tNames {
		t, _, err := root.GetTable(ctx, tn)

		if err != nil {
			return nil, err
		}

		sch, err := t.GetSchema(ctx)

		if err != nil {
			return nil, err
		}

		for _, t := range tags {
			_, found := sch.GetAllCols().GetByTag(t)

			if found {
				tagToTable[t] = tn
			}
		}
	}

	return tagToTable, nil
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

// GenerateTagsForNewColumns deterministically generates a slice of new tags that are unique within the history of this root. The names and NomsKinds of
// the new columns are used to see the tag generator.
func (root *RootValue) GenerateTagsForNewColumns(ctx context.Context, tableName string, newColNames []string, newColKinds []types.NomsKind) ([]uint64, error) {
	if len(newColNames) != len(newColKinds) {
		return nil, fmt.Errorf("error generating tags, newColNames and newColKinds must be of equal length")
	}

	rootSuperSchema, err := GetRootValueSuperSchema(ctx, root)

	if err != nil {
		return nil, err
	}

	var existingColKinds []types.NomsKind
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
			existingColKinds = append(existingColKinds, col.Kind)
			return false, nil
		})
	}

	newTags := make([]uint64, len(newColNames))
	for i := range newTags {
		newTags[i] = schema.AutoGenerateTag(rootSuperSchema, tableName, existingColKinds, newColNames[i], newColKinds[i])
		existingColKinds = append(existingColKinds, newColKinds[i])
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

// TODO: get or create from history of root
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
	return ssm, nil
}

func (root *RootValue) getTableSt(ctx context.Context, tName string) (*types.Struct, bool, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, false, err
	}

	tVal, found, err := tableMap.MaybeGet(ctx, types.String(tName))

	if tVal == nil || !found {
		return nil, false, nil
	}

	tValRef := tVal.(types.Ref)
	val, err := tValRef.TargetValue(ctx, root.vrw)

	if err != nil {
		return nil, false, err
	}

	tableStruct := val.(types.Struct)
	return &tableStruct, true, nil
}

func (root *RootValue) GetTableHash(ctx context.Context, tName string) (hash.Hash, bool, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return hash.Hash{}, false, err
	}

	tVal, found, err := tableMap.MaybeGet(ctx, types.String(tName))

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

// GetTable will retrieve a table by name
func (root *RootValue) GetTable(ctx context.Context, tName string) (*Table, bool, error) {
	if st, ok, err := root.getTableSt(ctx, tName); err != nil {
		return nil, false, err
	} else if ok {
		return &Table{root.vrw, *st}, true, nil
	}

	return nil, false, nil
}

func (root *RootValue) GetTableInsensitive(ctx context.Context, tName string) (*Table, string, bool, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, "", false, err
	}

	var foundKey string
	hasExact, err := tableMap.Has(ctx, types.String(tName))

	if err != nil {
		return nil, "", false, err
	}

	if hasExact {
		foundKey = tName
	} else {
		lwrName := strings.ToLower(tName)
		err = tableMap.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
			keyStr := string(key.(types.String))
			if lwrName == strings.ToLower(keyStr) {
				foundKey = keyStr
				return true, nil
			}

			return false, nil
		})

		if err != nil {
			return nil, "", false, nil
		}
	}

	tbl, ok, err := root.GetTable(ctx, foundKey)

	if err != nil {
		return nil, "", false, err
	}

	return tbl, foundKey, ok, nil
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
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	numTables := int(tableMap.Len())
	names := make([]string, 0, numTables)

	err = tableMap.Iter(ctx, func(key, tblRefVal types.Value) (stop bool, err error) {
		tblVal, err := tblRefVal.(types.Ref).TargetValue(ctx, root.vrw)

		if err != nil {
			return false, err
		}

		tblSt := tblVal.(types.Struct)
		tbl := &Table{root.vrw, tblSt}
		if has, err := tbl.HasConflicts(); err != nil {
			return false, err
		} else if has {
			names = append(names, string(key.(types.String)))
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return names, nil
}

func (root *RootValue) HasConflicts(ctx context.Context) (bool, error) {
	cnfTbls, err := root.TablesInConflict(ctx)

	if err != nil {
		return false, err
	}

	return len(cnfTbls) > 0, nil
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

	ssRef, err := writeValAndGetRef(ctx, newRoot.VRW(), ssVal)

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

	return newRootValue(root.vrw, newRootSt), nil
}

// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func (root *RootValue) PutTable(ctx context.Context, tName string, table *Table) (*RootValue, error) {
	err := validateTagUniqueness(ctx, root, tName, table)

	if err != nil {
		return nil, err
	}

	tableRef, err := writeValAndGetRef(ctx, root.VRW(), table.tableStruct)

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

	return newRootValue(root.vrw, rootValSt), nil
}

// CreateEmptyTable creates an empty table in this root with the name and schema given, returning the new root value.
func (root *RootValue) CreateEmptyTable(ctx context.Context, tName string, sch schema.Schema) (*RootValue, error) {
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, root.VRW(), sch)
	if err != nil {
		return nil, err
	}

	m, err := types.NewMap(ctx, root.VRW())
	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(ctx, root.VRW(), schVal, m)
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

// TableDiff returns the slices of tables added, modified, and removed when compared with another root value.  Tables
// In this instance that are not in the other instance are considered added, and tables in the other instance and not
// this instance are considered removed.
func (root *RootValue) TableDiff(ctx context.Context, other *RootValue) (added, modified, removed []string, err error) {
	added = []string{}
	modified = []string{}
	removed = []string{}

	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, nil, nil, err
	}

	otherMap, err := other.getTableMap()

	if err != nil {
		return nil, nil, nil, err
	}

	itr1, err := tableMap.Iterator(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	itr2, err := otherMap.Iterator(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	pk1, val1, err := itr1.Next(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	pk2, val2, err := itr2.Next(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	for pk1 != nil || pk2 != nil {
		if pk1 == nil || pk2 == nil || !pk1.Equals(pk2) {
			var pk2IsNilOrGreater bool
			if pk1 == nil {
				pk2IsNilOrGreater = false
			} else {
				pk2IsNilOrGreater = pk2 == nil

				if !pk2IsNilOrGreater {
					pk2IsNilOrGreater, err = pk1.Less(root.vrw.Format(), pk2)

					if err != nil {
						return nil, nil, nil, err
					}
				}
			}

			if pk2IsNilOrGreater {
				added = append(added, string(pk1.(types.String)))
				pk1, val1, err = itr1.Next(ctx)

				if err != nil {
					return nil, nil, nil, err
				}
			} else {
				removed = append(removed, string(pk2.(types.String)))
				pk2, val2, err = itr2.Next(ctx)

				if err != nil {
					return nil, nil, nil, err
				}
			}
		} else {

			if !val1.Equals(val2) {
				modified = append(modified, string(pk1.(types.String)))
			}

			pk1, val1, err = itr1.Next(ctx)

			if err != nil {
				return nil, nil, nil, err
			}

			pk2, val2, err = itr2.Next(ctx)

			if err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return added, modified, removed, nil
}

func (root *RootValue) UpdateTablesFromOther(ctx context.Context, tblNames []string, other *RootValue) (*RootValue, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	otherMap, err := other.getTableMap()

	if err != nil {
		return nil, err
	}

	me := tableMap.Edit()
	for _, tblName := range tblNames {
		key := types.String(tblName)
		if val, ok, err := otherMap.MaybeGet(ctx, key); err != nil {
			return nil, err
		} else if ok {
			me = me.Set(key, val)
		} else if _, ok, err := tableMap.MaybeGet(ctx, key); err != nil {
			return nil, err
		} else if ok {
			me = me.Remove(key)
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

	return newRootValue(root.vrw, rootValSt), nil
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

		ssRef, err := writeValAndGetRef(ctx, newRoot.VRW(), ssVal)

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

	return newRootValue(root.vrw, newRootSt), nil
}

func (root *RootValue) RenameTable(ctx context.Context, oldName, newName string) (*RootValue, error) {
	tableMap, err := root.getTableMap()

	if err != nil {
		return nil, err
	}

	v, found, err := tableMap.MaybeGet(ctx, types.String(oldName))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrTableNotFound
	}

	me := tableMap.Edit().Remove(types.String(oldName))
	me = me.Set(types.String(newName), v)
	m, err := me.Map(ctx)

	if err != nil {
		return nil, err
	}

	rootValSt, err := root.valueSt.Set(tablesKey, m)

	if err != nil {
		return nil, err
	}

	return newRootValue(root.vrw, rootValSt), nil
}

func (root *RootValue) RemoveTables(ctx context.Context, tables ...string) (*RootValue, error) {
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

	return newRootValue(root.vrw, rootValSt), nil
}

// DocDiff returns the added, modified and removed docs when comparing a root value with an other (newer) value. If the other value,
// is not provided, then we compare the docs on the root value to the docDetails provided.
func (root *RootValue) DocDiff(ctx context.Context, other *RootValue, docDetails []DocDetails) (added, modified, removed []string, err error) {
	oldTbl, oldTblFound, err := root.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, nil, nil, err
	}
	var oldSch schema.Schema
	if oldTblFound {
		sch, err := oldTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		oldSch = sch
	}

	if other == nil {
		detailsWithValues, err := addValuesToDocs(ctx, oldTbl, &oldSch, docDetails)
		if err != nil {
			return nil, nil, nil, err
		}
		a, m, r := GetDocDiffsFromDocDetails(ctx, detailsWithValues)
		return a, m, r, nil
	}

	newTbl, newTblFound, err := other.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, nil, nil, err
	}

	var newSch schema.Schema
	if newTblFound {
		sch, err := newTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		newSch = sch
	}

	docDetailsBtwnRoots, err := getDocDetailsBtwnRoots(ctx, newTbl, newSch, newTblFound, oldTbl, oldSch, oldTblFound)
	if err != nil {
		return nil, nil, nil, err
	}

	a, m, r := GetDocDiffsFromDocDetails(ctx, docDetailsBtwnRoots)
	return a, m, r, nil
}

func getDocDetailsBtwnRoots(ctx context.Context, newTbl *Table, newSch schema.Schema, newTblFound bool, oldTbl *Table, oldSch schema.Schema, oldTblFound bool) ([]DocDetails, error) {
	var docDetailsBtwnRoots []DocDetails
	if newTblFound {
		newRows, err := newTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = newRows.IterAll(ctx, func(key, val types.Value) error {
			newRow, err := row.FromNoms(newSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := DocDetails{}
			updated, err := addDocPKToDocFromRow(newRow, &doc)
			if err != nil {
				return err
			}
			updated, err = addNewerTextToDocFromRow(ctx, newRow, &updated)
			if err != nil {
				return err
			}
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if oldTblFound {
		oldRows, err := oldTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = oldRows.IterAll(ctx, func(key, val types.Value) error {
			oldRow, err := row.FromNoms(oldSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := DocDetails{}
			updated, err := addDocPKToDocFromRow(oldRow, &doc)
			if err != nil {
				return err
			}
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			updated, err = AddNewerTextToDocFromTbl(ctx, newTbl, &newSch, updated)
			if err != nil {
				return err
			}

			if updated.Value != nil && updated.NewerText == nil {
				docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return docDetailsBtwnRoots, nil
}

func GetDocDiffsFromDocDetails(ctx context.Context, docDetails []DocDetails) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}
	for _, doc := range docDetails {
		added, modified, removed = appendDocDiffs(added, modified, removed, doc.Value, doc.NewerText, doc.DocPk)
	}
	return added, modified, removed
}

func addValuesToDocs(ctx context.Context, tbl *Table, sch *schema.Schema, docDetails []DocDetails) ([]DocDetails, error) {
	if tbl != nil && sch != nil {
		for i, details := range docDetails {
			newDetails, err := AddValueToDocFromTbl(ctx, tbl, sch, details)
			if err != nil {
				return nil, err
			}
			docDetails[i] = newDetails
		}
	}
	return docDetails, nil
}

// AddValueToDocFromTbl updates the Value field of a docDetail using the provided table and schema.
func AddValueToDocFromTbl(ctx context.Context, tbl *Table, sch *schema.Schema, docDetail DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		pkTaggedVal := row.TaggedValues{
			DocNameTag: types.String(docDetail.DocPk),
		}

		docRow, ok, err := tbl.GetRowByPKVals(ctx, pkTaggedVal, *sch)
		if err != nil {
			return DocDetails{}, err
		}

		if ok {
			docValue, _ := docRow.GetColVal(DocTextTag)
			docDetail.Value = docValue
		} else {
			docDetail.Value = nil
		}
	} else {
		docDetail.Value = nil
	}
	return docDetail, nil
}

// AddNewerTextToDocFromTbl updates the NewerText field of a docDetail using the provided table and schema.
func AddNewerTextToDocFromTbl(ctx context.Context, tbl *Table, sch *schema.Schema, doc DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		pkTaggedVal := row.TaggedValues{
			DocNameTag: types.String(doc.DocPk),
		}

		docRow, ok, err := tbl.GetRowByPKVals(ctx, pkTaggedVal, *sch)
		if err != nil {
			return DocDetails{}, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(DocTextTag)
			doc.NewerText = []byte(docValue.(types.String))
		} else {
			doc.NewerText = nil
		}
	} else {
		doc.NewerText = nil
	}
	return doc, nil
}

func addNewerTextToDocFromRow(ctx context.Context, r row.Row, doc *DocDetails) (DocDetails, error) {
	docValue, ok := r.GetColVal(DocTextTag)
	if !ok {
		doc.NewerText = nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.NewerText = []byte(docValStr)
	}
	return *doc, nil
}

func addDocPKToDocFromRow(r row.Row, doc *DocDetails) (DocDetails, error) {
	colVal, _ := r.GetColVal(DocNameTag)
	if colVal == nil {
		doc.DocPk = ""
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.DocPk = docName
	}

	return *doc, nil
}

func appendDocDiffs(added, modified, removed []string, olderVal types.Value, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if olderVal.HumanReadableString() != strconv.Quote(string(newerVal)) {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
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

func validateTagUniqueness(ctx context.Context, root *RootValue, tableName string, table *Table) error {
	sch, err := table.GetSchema(ctx)

	if err != nil {
		return err
	}

	tt, err := root.TablesNamesForTags(ctx, sch.GetAllCols().Tags...)

	if err != nil {
		return err
	}

	var ee []string
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tn, tagExists := tt[tag]
		if tagExists && tn != tableName {
			ee = append(ee, schema.ErrTagCollisionAcrossTables(tag, col.Name, tn).Error())
		}
		return false, nil
	})

	if len(ee) > 0 {
		return fmt.Errorf(strings.Join(ee, "\n"))
	}

	return nil
}

