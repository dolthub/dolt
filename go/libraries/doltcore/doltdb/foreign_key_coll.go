// Copyright 2020 Liquidata, Inc.
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
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type ForeignKeyCollection struct {
	foreignKeys map[string]*ForeignKey
}

type ForeignKeyReferenceOption byte

const (
	ForeignKeyReferenceOption_DefaultAction ForeignKeyReferenceOption = iota
	ForeignKeyReferenceOption_Cascade
	ForeignKeyReferenceOption_NoAction
	ForeignKeyReferenceOption_Restrict
	ForeignKeyReferenceOption_SetNull
)

// DisplayForeignKey is a representation of a Foreign Key that is meant for display, such as when displaying a schema.
type DisplayForeignKey struct {
	Name                   string
	TableName              string
	TableIndex             string
	TableColumns           []string
	ReferencedTableName    string
	ReferencedTableIndex   string
	ReferencedTableColumns []string
	OnUpdate               ForeignKeyReferenceOption
	OnDelete               ForeignKeyReferenceOption
}

// ForeignKey is the complete, internal representation of a Foreign Key.
type ForeignKey struct {
	Name                   string                    `noms:"name" json:"name"`
	TableName              string                    `noms:"tbl_name" json:"tbl_name"`
	TableIndex             string                    `noms:"tbl_index" json:"tbl_index"`
	TableColumns           []uint64                  `noms:"tbl_cols" json:"tbl_cols"`
	ReferencedTableName    string                    `noms:"ref_tbl_name" json:"ref_tbl_name"`
	ReferencedTableIndex   string                    `noms:"ref_tbl_index" json:"ref_tbl_index"`
	ReferencedTableColumns []uint64                  `noms:"ref_tbl_cols" json:"ref_tbl_cols"`
	OnUpdate               ForeignKeyReferenceOption `noms:"on_update" json:"on_update"`
	OnDelete               ForeignKeyReferenceOption `noms:"on_delete" json:"on_delete"`
}

// LoadForeignKeyCollection returns a new ForeignKeyCollection using the provided map returned previously by GetMap.
func LoadForeignKeyCollection(ctx context.Context, fkMap types.Map) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]*ForeignKey),
	}
	err := fkMap.IterAll(ctx, func(_, value types.Value) error {
		foreignKey := &ForeignKey{}
		err := marshal.Unmarshal(ctx, fkMap.Format(), value, foreignKey)
		if err != nil {
			return err
		}
		fkc.foreignKeys[foreignKey.Name] = foreignKey
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fkc, nil
}

func NewForeignKeyCollection(keys ...*ForeignKey) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]*ForeignKey),
	}
	for _, k := range keys {
		err := fkc.AddKey(k)
		if err != nil {
			return nil, err
		}
	}
	return fkc, nil
}

// AddKey adds the given foreign key to the collection. Checks that the given name is unique in the collection, and that
// both column counts are equal. All other validation should occur before being added to the collection.
func (fkc *ForeignKeyCollection) AddKey(key *ForeignKey) error {
	if key.Name == "" {
		key.Name = fmt.Sprintf("fk_%s_%s_1", key.TableName, key.ReferencedTableName)
		for i := 2; fkc.Contains(key.Name); i++ {
			key.Name = fmt.Sprintf("fk_%s_%s_%d", key.TableName, key.ReferencedTableName, i)
		}
	}

	_, ok := fkc.GetByNameCaseInsensitive(key.Name)
	if ok {
		return fmt.Errorf("a foreign key with the name `%s` already exists", key.Name)
	}

	if len(key.TableColumns) != len(key.ReferencedTableColumns) {
		return fmt.Errorf("foreign keys must have the same number of columns declared and referenced")
	}

	if key.TableName == key.ReferencedTableName {
		return fmt.Errorf("inter-table foreign keys are not yet supported")
	}

	fkc.foreignKeys[key.Name] = key
	return nil
}

// AllKeys returns a slice, sorted by name ascending, containing all of the foreign keys in this collection.
func (fkc *ForeignKeyCollection) AllKeys() []*ForeignKey {
	fks := make([]*ForeignKey, len(fkc.foreignKeys))
	i := 0
	for _, fk := range fkc.foreignKeys {
		fks[i] = fk
		i++
	}
	sort.Slice(fks, func(i, j int) bool {
		return fks[i].Name < fks[j].Name
	})
	return fks
}

// Contains returns whether the given foreign key name already exists for this collection.
func (fkc *ForeignKeyCollection) Contains(foreignKeyName string) bool {
	_, ok := fkc.GetByNameCaseInsensitive(foreignKeyName)
	return ok
}

// Count returns the number of indexes in this collection.
func (fkc *ForeignKeyCollection) Count() int {
	return len(fkc.foreignKeys)
}

// GetByNameCaseInsensitive returns a ForeignKey with a matching case-insensitive name, and whether a match exists.
func (fkc *ForeignKeyCollection) GetByNameCaseInsensitive(foreignKeyName string) (match *ForeignKey, ok bool) {
	for name, fk := range fkc.foreignKeys {
		if strings.ToLower(name) == strings.ToLower(foreignKeyName) {
			return fk, true
		}
	}
	return nil, false
}

// GetByTags gets the Foreign Key defined over the parent and child columns corresponding to tags parameters.
func (fkc *ForeignKeyCollection) GetByTags(parentTags, childTags []uint64) (match *ForeignKey, ok bool) {
	_ = fkc.Iter(func(fk *ForeignKey) (stop bool, err error) {
		if len(fk.ReferencedTableColumns) != len(parentTags) {
			return false, nil
		}
		for i, t := range fk.ReferencedTableColumns {
			if t != parentTags[i] {
				return false, nil
			}
		}

		if len(fk.TableColumns) != len(childTags) {
			return false, nil
		}
		for i, t := range fk.TableColumns {
			if t != childTags[i] {
				return false, nil
			}
		}
		match, ok = fk, true
		return true, nil
	})
	return match, ok
}

func (fkc *ForeignKeyCollection) Iter(cb func(fk *ForeignKey) (stop bool, err error)) error {
	for _, fk := range fkc.foreignKeys {
		stop, err := cb(fk)
		if err != nil {
			return err
		}
		if stop {
			return err
		}
	}
	return nil
}

// KeysForDisplay returns display-ready foreign keys that the given table declares. The results are intended only
// for displaying key information to a user, and SHOULD NOT be used elsewhere. The results are sorted by name ascending.
func (fkc *ForeignKeyCollection) KeysForDisplay(ctx context.Context, tableName string, root *RootValue) ([]*DisplayForeignKey, error) {
	var declaresFk []*DisplayForeignKey
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == tableName {
			tableColumns, ok, err := fkc.columnTagsToNames(ctx, foreignKey.TableName, foreignKey.Name, foreignKey.TableColumns, root)
			if err != nil {
				return nil, err
			}
			if !ok { // root may be in an incomplete state regarding the foreign key, so we skip displaying any invalid keys
				continue
			}
			refTableColumns, ok, err := fkc.columnTagsToNames(ctx, foreignKey.ReferencedTableName, foreignKey.Name, foreignKey.ReferencedTableColumns, root)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			declaresFk = append(declaresFk, &DisplayForeignKey{
				Name:                   foreignKey.Name,
				TableName:              foreignKey.TableName,
				TableIndex:             foreignKey.TableIndex,
				TableColumns:           tableColumns,
				ReferencedTableName:    foreignKey.ReferencedTableName,
				ReferencedTableIndex:   foreignKey.ReferencedTableIndex,
				ReferencedTableColumns: refTableColumns,
				OnUpdate:               foreignKey.OnUpdate,
				OnDelete:               foreignKey.OnDelete,
			})
		}
	}
	sort.Slice(declaresFk, func(i, j int) bool {
		return declaresFk[i].Name < declaresFk[j].Name
	})
	return declaresFk, nil
}

// KeysForTable returns all foreign keys that reference the given table in some capacity. The returned array
// declaredFk contains all foreign keys in which this table declared the foreign key. The array referencedByFk contains
// all foreign keys in which this table is the referenced table. If the table contains a self-referential foreign key,
// it will be present in both declaresFk and referencedByFk. Each array is sorted by name ascending.
func (fkc *ForeignKeyCollection) KeysForTable(tableName string) (declaredFk, referencedByFk []*ForeignKey) {
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == tableName {
			declaredFk = append(declaredFk, foreignKey)
		}
		if foreignKey.ReferencedTableName == tableName {
			referencedByFk = append(referencedByFk, foreignKey)
		}
	}
	sort.Slice(declaredFk, func(i, j int) bool {
		return declaredFk[i].Name < declaredFk[j].Name
	})
	sort.Slice(referencedByFk, func(i, j int) bool {
		return referencedByFk[i].Name < referencedByFk[j].Name
	})
	return
}

// Map returns the collection as a Noms Map for persistence.
func (fkc *ForeignKeyCollection) Map(ctx context.Context, vrw types.ValueReadWriter) (types.Map, error) {
	fkMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	fkMapEditor := fkMap.Edit()
	for _, foreignKey := range fkc.foreignKeys {
		val, err := marshal.Marshal(ctx, vrw, *foreignKey)
		if err != nil {
			return types.EmptyMap, err
		}
		fkMapEditor.Set(types.String(foreignKey.Name), val)
	}
	return fkMapEditor.Map(ctx)
}

// RemoveKey removes a foreign key from the collection. It does not remove the associated indexes from their
// respective tables.
func (fkc *ForeignKeyCollection) RemoveKey(foreignKeyName string) (*ForeignKey, error) {
	fk, ok := fkc.foreignKeys[foreignKeyName]
	if !ok {
		return nil, fmt.Errorf("`%s` does not exist as a foreign key", foreignKeyName)
	}
	delete(fkc.foreignKeys, foreignKeyName)
	return fk, nil
}

// RemoveTables removes all foreign keys associated with the given tables, if permitted. The operation assumes that ALL
// tables to be removed are in a single call, as splitting tables into different calls may result in unintended errors.
func (fkc *ForeignKeyCollection) RemoveTables(ctx context.Context, root *RootValue, tables ...string) (*RootValue, error) {
	tableSet := make(map[string]struct{})
	for _, table := range tables {
		tableSet[table] = struct{}{}
	}
	for _, foreignKey := range fkc.foreignKeys {
		_, declaringTable := tableSet[foreignKey.TableName]
		_, referenceTable := tableSet[foreignKey.ReferencedTableName]
		if referenceTable && !declaringTable {
			return nil, fmt.Errorf("unable to remove `%s` since it is referenced from table `%s`", foreignKey.ReferencedTableName, foreignKey.TableName)
		}
		if declaringTable {
			if !referenceTable {
				tbl, ok, err := root.GetTable(ctx, foreignKey.ReferencedTableName)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, fmt.Errorf("table `%s` not found, unable to remove foreign key `%s`", foreignKey.ReferencedTableName, foreignKey.Name)
				}
				sch, err := tbl.GetSchema(ctx)
				if err != nil {
					return nil, err
				}
				_, err = sch.Indexes().RemoveIndex(foreignKey.TableIndex)
				if err != nil {
					return nil, err
				}
				tbl, err = tbl.UpdateSchema(ctx, sch)
				if err != nil {
					return nil, err
				}
				root, err = root.PutTable(ctx, foreignKey.ReferencedTableName, tbl)
				if err != nil {
					return nil, err
				}
			}
			delete(fkc.foreignKeys, foreignKey.Name)
		}
	}
	return root.PutForeignKeyCollection(ctx, fkc)
}

// RenameTable updates all foreign key entries in the collection with the updated table name. Does not check for name
// collisions.
func (fkc *ForeignKeyCollection) RenameTable(oldTableName, newTableName string) {
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == oldTableName {
			foreignKey.TableName = newTableName
		}
		if foreignKey.ReferencedTableName == oldTableName {
			foreignKey.ReferencedTableName = newTableName
		}
	}
}

// Stage takes the keys to add and remove and updates the current collection. Does not perform any key validation nor
// name uniqueness verification, as this is intended for use in commit staging. Adding a foreign key and updating (such
// as a table rename) an existing one are functionally the same.
func (fkc *ForeignKeyCollection) Stage(ctx context.Context, fksToAdd []*ForeignKey, fksToRemove []*ForeignKey) {
	for _, foreignKeyToAdd := range fksToAdd {
		fkc.foreignKeys[foreignKeyToAdd.Name] = foreignKeyToAdd
	}
	for _, foreignKeyToRemove := range fksToRemove {
		delete(fkc.foreignKeys, foreignKeyToRemove.Name)
	}
}

// ValidateData ensures that the foreign key is valid by comparing the index data from the given table against the index
// data from the referenced table.
func (fk *ForeignKey) ValidateData(ctx context.Context, tableIndexData types.Map, refTableIndex schema.Index, refTableIndexData types.Map) error {
	if fk.ReferencedTableIndex != refTableIndex.Name() {
		return fmt.Errorf("cannot validate data as wrong referenced index was given: expected `%s` but received `%s`",
			fk.ReferencedTableIndex, refTableIndex.Name())
	}
	refTableIndexSch := refTableIndex.Schema()
	err := tableIndexData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		indexTaggedValues, err := row.ParseTaggedValues(key.(types.Tuple))
		if err != nil {
			return true, err
		}
		refIndexKeyVals := make([]types.Value, len(fk.TableColumns)*2)
		for i, colTag := range fk.TableColumns {
			val, ok := indexTaggedValues[colTag]
			if !ok {
				return true, fmt.Errorf("cannot find value for tag `%d` on table `%s`", colTag, fk.TableName)
			}
			newTag := fk.ReferencedTableColumns[i]
			refIndexKeyVals[2*i] = types.Uint(newTag)
			refIndexKeyVals[2*i+1] = val
		}
		refIndexKey, err := types.NewTuple(refTableIndexData.Format(), refIndexKeyVals...)
		if err != nil {
			return true, err
		}

		indexIter := noms.NewNomsRangeReader(refTableIndexSch, refTableIndexData,
			[]*noms.ReadRange{{Start: refIndexKey, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
				return tuple.StartsWith(refIndexKey), nil
			}}},
		)
		_, err = indexIter.ReadRow(ctx)
		if err == nil { // row exists
			return false, nil
		} else if err != io.EOF {
			return true, err
		} else {
			indexKeyStr, _ := types.EncodedValue(ctx, refIndexKey)
			return true, fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", fk.Name, fk.TableName, indexKeyStr)
		}
	})
	return err
}

// Equals returns whether the given foreign key is equivalent to another. As tags are unique, we can compare using those
// and ignore the table names, ensuring equality even through table and column renames.
func (fk *ForeignKey) Equals(other *ForeignKey) bool {
	if len(fk.TableColumns) != len(other.TableColumns) || len(fk.ReferencedTableColumns) != len(other.ReferencedTableColumns) {
		return false
	}
	for i := range fk.TableColumns {
		if fk.TableColumns[i] != other.TableColumns[i] {
			return false
		}
	}
	for i := range fk.ReferencedTableColumns {
		if fk.ReferencedTableColumns[i] != other.ReferencedTableColumns[i] {
			return false
		}
	}
	return fk.Name == other.Name &&
		fk.OnUpdate == other.OnUpdate &&
		fk.OnDelete == other.OnDelete
}

// ValidateReferencedTableSchema verifies that the given schema matches the expectation of the referenced table.
func (fk *ForeignKey) ValidateReferencedTableSchema(sch schema.Schema) error {
	allSchCols := sch.GetAllCols()
	for _, colTag := range fk.ReferencedTableColumns {
		_, ok := allSchCols.GetByTag(colTag)
		if !ok {
			return fmt.Errorf("foreign key `%s` has entered an invalid state, referenced table `%s` has unexpected schema",
				fk.Name, fk.ReferencedTableName)
		}
	}
	if !sch.Indexes().Contains(fk.ReferencedTableIndex) {
		return fmt.Errorf("foreign key `%s` has entered an invalid state, referenced table `%s` is missing the index `%s`",
			fk.Name, fk.ReferencedTableName, fk.ReferencedTableIndex)
	}
	return nil
}

// ValidateTableSchema verifies that the given schema matches the expectation of the declaring table.
func (fk *ForeignKey) ValidateTableSchema(sch schema.Schema) error {
	allSchCols := sch.GetAllCols()
	for _, colTag := range fk.TableColumns {
		_, ok := allSchCols.GetByTag(colTag)
		if !ok {
			return fmt.Errorf("foreign key `%s` has entered an invalid state, table `%s` has unexpected schema", fk.Name, fk.TableName)
		}
	}
	if !sch.Indexes().Contains(fk.TableIndex) {
		return fmt.Errorf("foreign key `%s` has entered an invalid state, table `%s` is missing the index `%s`",
			fk.Name, fk.TableName, fk.TableIndex)
	}
	return nil
}

// String returns the SQL reference option in uppercase.
func (refOp ForeignKeyReferenceOption) String() string {
	switch refOp {
	case ForeignKeyReferenceOption_DefaultAction:
		return "NONE SPECIFIED"
	case ForeignKeyReferenceOption_Cascade:
		return "CASCADE"
	case ForeignKeyReferenceOption_NoAction:
		return "NO ACTION"
	case ForeignKeyReferenceOption_Restrict:
		return "RESTRICT"
	case ForeignKeyReferenceOption_SetNull:
		return "SET NULL"
	default:
		return "INVALID"
	}
}

// columnTagsToNames loads all of the column names for the tags given from the root given.
func (fkc *ForeignKeyCollection) columnTagsToNames(ctx context.Context, tableName string, fkName string, colTags []uint64, root *RootValue) ([]string, bool, error) {
	tbl, ok, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	tableSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	tableColumns := make([]string, len(colTags))
	for i := range colTags {
		col, ok := tableSch.GetAllCols().GetByTag(colTags[i])
		if !ok {
			return nil, false, nil
		}
		tableColumns[i] = col.Name
	}
	return tableColumns, true, nil
}

// copy returns an exact copy of the calling collection. As collections are meant to be modified in-place, this ensures
// that the original collection is not affected by any operations applied to the copied collection.
func (fkc *ForeignKeyCollection) copy() *ForeignKeyCollection {
	copiedForeignKeys := make(map[string]*ForeignKey)
	for _, key := range fkc.foreignKeys {
		valueKey := *key // value types are copied, so this essentially copies all fields (the slices never change so it's okay)
		copiedForeignKeys[valueKey.Name] = &valueKey
	}
	return &ForeignKeyCollection{copiedForeignKeys}
}
