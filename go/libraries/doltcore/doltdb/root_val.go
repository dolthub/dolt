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

	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey    = "tables"
	docTableName = "dolt_docs"
	licensePk    = "LICENSE.md"
	readmePk     = "README.md"
)

// RootValue defines the structure used inside all Liquidata noms dbs
type RootValue struct {
	vrw     types.ValueReadWriter
	valueSt types.Struct
}

func NewRootValue(ctx context.Context, vrw types.ValueReadWriter, tables map[string]hash.Hash) (*RootValue, error) {
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

	return newRootFromTblMap(vrw, tblMap)
}

func newRootValue(vrw types.ValueReadWriter, st types.Struct) *RootValue {
	return &RootValue{vrw, st}
}

func emptyRootValue(ctx context.Context, vrw types.ValueReadWriter) (*RootValue, error) {
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}
	return newRootFromTblMap(vrw, m)
}

func newRootFromTblMap(vrw types.ValueReadWriter, tblMap types.Map) (*RootValue, error) {
	sd := types.StructData{
		tablesKey: tblMap,
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

// GetTable will retrieve a table by name
func (root *RootValue) GetTable(ctx context.Context, tName string) (*Table, bool, error) {
	if st, ok, err := root.getTableSt(ctx, tName); err != nil {
		return nil, false, err
	} else if ok {
		return &Table{root.vrw, *st}, true, nil
	}

	return nil, false, nil
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

// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func (root *RootValue) PutTable(ctx context.Context, tName string, table *Table) (*RootValue, error) {
	return PutTable(ctx, root, root.VRW(), tName, table)
}

// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func PutTable(ctx context.Context, root *RootValue, vrw types.ValueReadWriter, tName string, table *Table) (*RootValue, error) {
	if !IsValidTableName(tName) {
		panic("Don't attempt to put a table with a name that fails the IsValidTableName check")
	}

	rootValSt := root.valueSt
	tableRef, err := writeValAndGetRef(ctx, vrw, table.tableStruct)

	if err != nil {
		return nil, err
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

	rootValSt, err = rootValSt.Set(tablesKey, m)

	if err != nil {
		return nil, err
	}

	return newRootValue(root.vrw, rootValSt), nil
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
			//tblSt1 := val1.(types.Ref).TargetValue(root.vrw)
			//tblSt2 := val2.(types.Ref).TargetValue(root.vrw)
			//tbl1 := Table{root.vrw, tblSt1.(types.Struct)}
			//tbl2 := Table{root.vrw, tblSt2.(types.Struct)}

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

// DocDiff returns the slices of the dolt docs on the filesystem that are added, modified, and removed when compared with a root value.  Docs
// In this instance that are not in the other instance are considered added, and docs in the other instance and not
// this instance are considered removed.
func (root *RootValue) DocDiff(ctx context.Context, newerLicenseBytes, newerReadmeBytes []byte) (added, modified, removed []string, err error) {
	docTable, found, err := root.GetTable(ctx, docTableName)

	if err != nil {
		return nil, nil, nil, err
	}

	var rowMap types.Map
	var licenseVal types.Value
	var readmeVal types.Value
	if found {
		rowMap, err = docTable.GetRowData(ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		licenseVal, _, err = rowMap.MaybeGet(ctx, types.String(licensePk))
		readmeVal, _, err = rowMap.MaybeGet(ctx, types.String(readmePk))
	}

	added = []string{}
	modified = []string{}
	removed = []string{}
	added, modified, removed = appendDocDiffs(added, modified, removed, licenseVal, newerLicenseBytes, licensePk)
	added, modified, removed = appendDocDiffs(added, modified, removed, readmeVal, newerReadmeBytes, readmePk)

	return added, modified, removed, nil
}

func appendDocDiffs(added, removed, modified []string, olderVal types.Value, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if string(newerVal) != olderVal.HumanReadableString() {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
}
