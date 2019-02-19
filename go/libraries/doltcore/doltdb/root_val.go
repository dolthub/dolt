package doltdb

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey = "tables"
)

// RootValue defines the structure used inside all Liquidata noms dbs
type RootValue struct {
	vrw     types.ValueReadWriter
	valueSt types.Struct
}

func NewRootValue(vrw types.ValueReadWriter, tables map[string]hash.Hash) (*RootValue, error) {
	values := make([]types.Value, 2*len(tables))

	err := pantoerr.PanicToError("unable to read values from noms", func() error {
		index := 0
		for k, v := range tables {
			values[index] = types.String(k)
			valForHash := vrw.ReadValue(v)

			if valForHash == nil {
				return ErrHashNotFound
			}

			values[index+1] = types.NewRef(valForHash)
			index += 2
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	tblMap := types.NewMap(vrw, values...)
	return newRootFromTblMap(vrw, tblMap), nil
}

func newRootValue(vrw types.ValueReadWriter, st types.Struct) *RootValue {
	return &RootValue{vrw, st}
}

func emptyRootValue(vrw types.ValueReadWriter) *RootValue {
	return newRootFromTblMap(vrw, types.NewMap(vrw))
}

func newRootFromTblMap(vrw types.ValueReadWriter, tblMap types.Map) *RootValue {
	sd := types.StructData{
		tablesKey: tblMap,
	}

	st := types.NewStruct(ddbRootStructName, sd)

	return newRootValue(vrw, st)
}

func (root *RootValue) VRW() types.ValueReadWriter {
	return root.vrw
}

func (root *RootValue) HasTable(tName string) bool {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	return tableMap.Has(types.String(tName))
}

func (root *RootValue) getTableSt(tName string) (*types.Struct, bool) {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	tVal := tableMap.Get(types.String(tName))

	if tVal == nil {
		return nil, false
	}

	tValRef := tVal.(types.Ref)
	tableStruct := tValRef.TargetValue(root.vrw).(types.Struct)
	return &tableStruct, true
}

func (root *RootValue) GetTableHash(tName string) (hash.Hash, bool) {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	tVal := tableMap.Get(types.String(tName))

	if tVal == nil {
		return hash.Hash{}, false
	}

	tValRef := tVal.(types.Ref)
	return tValRef.TargetHash(), true
}

// GetTable will retrieve a table by name
func (root *RootValue) GetTable(tName string) (*Table, bool) {
	if st, ok := root.getTableSt(tName); ok {
		return &Table{root.vrw, *st}, true
	}

	return nil, false
}

// GetTableNames retrieves the lists of all tables for a RootValue
func (root *RootValue) GetTableNames() []string {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	numTables := int(tableMap.Len())
	names := make([]string, 0, numTables)

	tableMap.Iter(func(key, _ types.Value) (stop bool) {
		names = append(names, string(key.(types.String)))
		return false
	})

	return names
}

func (root *RootValue) TablesInConflict() []string {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	numTables := int(tableMap.Len())
	names := make([]string, 0, numTables)

	tableMap.Iter(func(key, tblRefVal types.Value) (stop bool) {
		tblVal := tblRefVal.(types.Ref).TargetValue(root.vrw)
		tblSt := tblVal.(types.Struct)
		tbl := &Table{root.vrw, tblSt}
		if tbl.HasConflicts() {
			names = append(names, string(key.(types.String)))
		}

		return false
	})

	return names
}

func (root *RootValue) HasConflicts() bool {
	cnfTbls := root.TablesInConflict()

	return len(cnfTbls) > 0
}

// PutTableToWorking inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func (root *RootValue) PutTable(ddb *DoltDB, tName string, table *Table) *RootValue {
	if !IsValidTableName(tName) {
		panic("Don't attempt to put a table with a name that fails the IsValidTableName check")
	}

	rootValSt := root.valueSt
	tableRef := writeValAndGetRef(ddb.ValueReadWriter(), table.tableStruct)

	tableMap := rootValSt.Get(tablesKey).(types.Map)
	tMapEditor := tableMap.Edit()
	tMapEditor = tMapEditor.Set(types.String(tName), tableRef)

	rootValSt = rootValSt.Set(tablesKey, tMapEditor.Map())
	return newRootValue(root.vrw, rootValSt)
}

// HashOf gets the hash of the root value
func (root *RootValue) HashOf() hash.Hash {
	return root.valueSt.Hash()
}

// TableDiff returns the slices of tables added, modified, and removed when compared with another root value.  Tables
// In this instance that are not in the other instance are considered added, and tables in the other instance and not
// this instance are considered removed.
func (root *RootValue) TableDiff(other *RootValue) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}

	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	otherMap := other.valueSt.Get(tablesKey).(types.Map)

	itr1 := tableMap.Iterator()
	itr2 := otherMap.Iterator()

	pk1, val1 := itr1.Next()
	pk2, val2 := itr2.Next()

	for pk1 != nil || pk2 != nil {
		if pk1 == nil || pk2 == nil || !pk1.Equals(pk2) {
			if pk2 == nil || (pk1 != nil && pk1.Less(pk2)) {
				added = append(added, string(pk1.(types.String)))
				pk1, val1 = itr1.Next()
			} else {
				removed = append(removed, string(pk2.(types.String)))
				pk2, val2 = itr2.Next()
			}
		} else {
			//tblSt1 := val1.(types.Ref).TargetValue(root.vrw)
			//tblSt2 := val2.(types.Ref).TargetValue(root.vrw)
			//tbl1 := Table{root.vrw, tblSt1.(types.Struct)}
			//tbl2 := Table{root.vrw, tblSt2.(types.Struct)}

			if !val1.Equals(val2) {
				modified = append(modified, string(pk1.(types.String)))
			}

			pk1, val1 = itr1.Next()
			pk2, val2 = itr2.Next()
		}
	}

	return added, modified, removed
}

func (root *RootValue) UpdateTablesFromOther(tblNames []string, other *RootValue) *RootValue {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	otherMap := other.valueSt.Get(tablesKey).(types.Map)

	me := tableMap.Edit()
	for _, tblName := range tblNames {
		key := types.String(tblName)
		if val, ok := otherMap.MaybeGet(key); ok {
			me = me.Set(key, val)
		} else if _, ok := tableMap.MaybeGet(key); ok {
			me = me.Remove(key)
		}
	}

	rootValSt := root.valueSt.Set(tablesKey, me.Map())
	return newRootValue(root.vrw, rootValSt)
}

func (root *RootValue) RemoveTables(tables []string) (*RootValue, error) {
	tableMap := root.valueSt.Get(tablesKey).(types.Map)
	me := tableMap.Edit()
	for _, tbl := range tables {
		key := types.String(tbl)

		if me.Has(key) {
			me = me.Remove(key)
		} else {
			return nil, ErrTableNotFound
		}
	}

	rootValSt := root.valueSt.Set(tablesKey, me.Map())
	return newRootValue(root.vrw, rootValSt), nil
}
