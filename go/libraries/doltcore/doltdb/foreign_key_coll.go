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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type ForeignKeyCollection struct {
	foreignKeys map[string]ForeignKey
}

type ForeignKeyReferenceOption byte

const (
	ForeignKeyReferenceOption_DefaultAction ForeignKeyReferenceOption = iota
	ForeignKeyReferenceOption_Cascade
	ForeignKeyReferenceOption_NoAction
	ForeignKeyReferenceOption_Restrict
	ForeignKeyReferenceOption_SetNull
)

// ForeignKey is the complete, internal representation of a Foreign Key.
type ForeignKey struct {
	// TODO: remove index names and retrieve indexes by column tags
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

// EqualDefs returns whether two foreign keys have the same definition over the same column sets.
// It does not compare table names or foreign key names.
func (fk ForeignKey) EqualDefs(other ForeignKey) bool {
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

// DeepEquals compares all attributes of a foreign key to another, including name and table names.
func (fk ForeignKey) DeepEquals(other ForeignKey) bool {
	if !fk.EqualDefs(other) {
		return false
	}
	return fk.Name == other.Name &&
		fk.TableName == other.TableName &&
		fk.ReferencedTableName == other.ReferencedTableName
}

// HashOf returns the Noms hash of a ForeignKey.
func (fk ForeignKey) HashOf() hash.Hash {
	var bb bytes.Buffer
	bb.Write([]byte(fk.Name))
	bb.Write([]byte(fk.TableName))
	bb.Write([]byte(fk.TableIndex))
	for _, t := range fk.TableColumns {
		_ = binary.Write(&bb, binary.LittleEndian, t)
	}
	bb.Write([]byte(fk.ReferencedTableName))
	bb.Write([]byte(fk.ReferencedTableIndex))
	for _, t := range fk.ReferencedTableColumns {
		_ = binary.Write(&bb, binary.LittleEndian, t)
	}
	bb.Write([]byte{byte(fk.OnUpdate), byte(fk.OnDelete)})

	return hash.Of(bb.Bytes())
}

// ConstraintIsSatisfied ensures that the foreign key is valid by comparing the index data from the given table against the index
// data from the referenced table.
func (fk ForeignKey) ConstraintIsSatisfied(ctx context.Context, childIdx, parentIdx types.Map, childDef, parentDef schema.Index) error {
	if fk.ReferencedTableIndex != parentDef.Name() {
		return fmt.Errorf("cannot validate data as wrong referenced index was given: expected `%s` but received `%s`",
			fk.ReferencedTableIndex, parentDef.Name())
	}

	tagMap := make(map[uint64]uint64, len(fk.TableColumns))
	for i, childTag := range fk.TableColumns {
		tagMap[childTag] = fk.ReferencedTableColumns[i]
	}

	// FieldMappings ignore columns not in the tagMap
	fm, err := rowconv.NewFieldMapping(childDef.Schema(), parentDef.Schema(), tagMap)
	if err != nil {
		return err
	}

	rc, err := rowconv.NewRowConverter(fm)
	if err != nil {
		return err
	}

	rdr, err := noms.NewNomsMapReader(ctx, childIdx, childDef.Schema())
	if err != nil {
		return err
	}

	for {
		childIdxRow, err := rdr.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		parentIdxRow, err := rc.Convert(childIdxRow)
		if err != nil {
			return err
		}
		if row.IsEmpty(parentIdxRow) {
			continue
		}

		partial, err := parentIdxRow.ReduceToIndexPartialKey(parentDef)
		if err != nil {
			return err
		}

		indexIter := noms.NewNomsRangeReader(parentDef.Schema(), parentIdx,
			[]*noms.ReadRange{{Start: partial, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
				return tuple.StartsWith(partial), nil
			}}},
		)

		switch _, err = indexIter.ReadRow(ctx); err {
		case nil:
			continue // parent table contains child key
		case io.EOF:
			indexKeyStr, _ := types.EncodedValue(ctx, partial)
			return fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", fk.Name, fk.TableName, indexKeyStr)
		default:
			return err
		}
	}

	return nil
}

// ValidateReferencedTableSchema verifies that the given schema matches the expectation of the referenced table.
func (fk ForeignKey) ValidateReferencedTableSchema(sch schema.Schema) error {
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
func (fk ForeignKey) ValidateTableSchema(sch schema.Schema) error {
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

// LoadForeignKeyCollection returns a new ForeignKeyCollection using the provided map returned previously by GetMap.
func LoadForeignKeyCollection(ctx context.Context, fkMap types.Map) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]ForeignKey),
	}
	err := fkMap.IterAll(ctx, func(key, value types.Value) error {
		foreignKey := &ForeignKey{}
		err := marshal.Unmarshal(ctx, fkMap.Format(), value, foreignKey)
		if err != nil {
			return err
		}
		fkc.foreignKeys[string(key.(types.String))] = *foreignKey
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fkc, nil
}

func NewForeignKeyCollection(keys ...ForeignKey) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]ForeignKey),
	}
	for _, k := range keys {
		err := fkc.AddKeys(k)
		if err != nil {
			return nil, err
		}
	}
	return fkc, nil
}

// AddKeys adds the given foreign key to the collection. Checks that the given name is unique in the collection, and that
// both column counts are equal. All other validation should occur before being added to the collection.
func (fkc *ForeignKeyCollection) AddKeys(fks ...ForeignKey) error {
	for _, key := range fks {
		if key.Name == "" {
			// assign a name based on the hash
			// 8 char = 5 base32 bytes, should be collision resistant
			key.Name = key.HashOf().String()[:8]
		}

		if _, ok := fkc.GetByTags(key.TableColumns, key.ReferencedTableColumns); ok {
			// this differs from MySQL's logic
			return fmt.Errorf("a foreign key over columns %v and referenced columns %v already exists",
				key.TableColumns, key.ReferencedTableColumns)
		}
		if _, ok := fkc.GetByNameCaseInsensitive(key.Name); ok {
			return fmt.Errorf("a foreign key with the name `%s` already exists", key.Name)
		}
		if len(key.TableColumns) != len(key.ReferencedTableColumns) {
			return fmt.Errorf("foreign keys must have the same number of columns declared and referenced")
		}
		if key.TableName == key.ReferencedTableName {
			return fmt.Errorf("inter-table foreign keys are not yet supported")
		}

		fkc.foreignKeys[key.HashOf().String()] = key
	}
	return nil
}

// AllKeys returns a slice, sorted by name ascending, containing all of the foreign keys in this collection.
func (fkc *ForeignKeyCollection) AllKeys() []ForeignKey {
	fks := make([]ForeignKey, len(fkc.foreignKeys))
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
func (fkc *ForeignKeyCollection) GetByNameCaseInsensitive(foreignKeyName string) (ForeignKey, bool) {
	if foreignKeyName == "" {
		return ForeignKey{}, false
	}
	for _, fk := range fkc.foreignKeys {
		if strings.ToLower(fk.Name) == strings.ToLower(foreignKeyName) {
			return fk, true
		}
	}
	return ForeignKey{}, false
}

// GetByTags gets the Foreign Key defined over the parent and child columns corresponding to tags parameters.
func (fkc *ForeignKeyCollection) GetByTags(childTags, parentTags []uint64) (match ForeignKey, ok bool) {
	_ = fkc.Iter(func(fk ForeignKey) (stop bool, err error) {
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

func (fkc *ForeignKeyCollection) Iter(cb func(fk ForeignKey) (stop bool, err error)) error {
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

// KeysForTable returns all foreign keys that reference the given table in some capacity. The returned array
// declaredFk contains all foreign keys in which this table declared the foreign key. The array referencedByFk contains
// all foreign keys in which this table is the referenced table. If the table contains a self-referential foreign key,
// it will be present in both declaresFk and referencedByFk. Each array is sorted by name ascending.
func (fkc *ForeignKeyCollection) KeysForTable(tableName string) (declaredFk, referencedByFk []ForeignKey) {
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
	for hashOf, foreignKey := range fkc.foreignKeys {
		val, err := marshal.Marshal(ctx, vrw, foreignKey)
		if err != nil {
			return types.EmptyMap, err
		}
		fkMapEditor.Set(types.String(hashOf), val)
	}
	return fkMapEditor.Map(ctx)
}

// RemoveKeys removes any Foreign Keys with matching column set from the collection.
func (fkc *ForeignKeyCollection) RemoveKeys(fks ...ForeignKey) {
	drops := set.NewStrSet(nil)
	for _, outgoing := range fks {
		for k, existing := range fkc.foreignKeys {
			if outgoing.EqualDefs(existing) {
				drops.Add(k)
			}
		}
	}
	for _, k := range drops.AsSlice() {
		delete(fkc.foreignKeys, k)
	}
}

// RemoveKeyByName removes a foreign key from the collection. It does not remove the associated indexes from their
// respective tables.
func (fkc *ForeignKeyCollection) RemoveKeyByName(foreignKeyName string) error {
	var key string
	for k, fk := range fkc.foreignKeys {
		if strings.ToLower(fk.Name) == strings.ToLower(foreignKeyName) {
			key = k
			break
		}
	}
	if key == "" {
		return fmt.Errorf("`%s` does not exist as a foreign key", foreignKeyName)
	}
	delete(fkc.foreignKeys, key)
	return nil
}

// RemoveTables removes all foreign keys associated with the given tables, if permitted. The operation assumes that ALL
// tables to be removed are in a single call, as splitting tables into different calls may result in unintended errors.
func (fkc *ForeignKeyCollection) RemoveTables(ctx context.Context, tables ...string) error {
	outgoing := set.NewStrSet(tables)
	for _, fk := range fkc.foreignKeys {
		dropChild := outgoing.Contains(fk.TableName)
		dropParent := outgoing.Contains(fk.ReferencedTableName)
		if dropParent && !dropChild {
			return fmt.Errorf("unable to remove `%s` since it is referenced from table `%s`", fk.ReferencedTableName, fk.TableName)
		}
		if dropChild {
			delete(fkc.foreignKeys, fk.HashOf().String())
		}
	}
	return nil
}

// RenameTable updates all foreign key entries in the collection with the updated table name. Does not check for name
// collisions.
func (fkc *ForeignKeyCollection) RenameTable(oldTableName, newTableName string) {
	updated := make(map[string]ForeignKey, len(fkc.foreignKeys))
	for _, fk := range fkc.foreignKeys {
		if fk.TableName == oldTableName {
			fk.TableName = newTableName
		}
		if fk.ReferencedTableName == oldTableName {
			fk.ReferencedTableName = newTableName
		}
		updated[fk.HashOf().String()] = fk
	}
	fkc.foreignKeys = updated
}

// Stage takes the keys to add and remove and updates the current collection. Does not perform any key validation nor
// name uniqueness verification, as this is intended for use in commit staging. Adding a foreign key and updating (such
// as a table rename) an existing one are functionally the same.
func (fkc *ForeignKeyCollection) Stage(ctx context.Context, fksToAdd []ForeignKey, fksToRemove []ForeignKey) {
	for _, fk := range fksToAdd {
		fkc.foreignKeys[fk.HashOf().String()] = fk
	}
	for _, fk := range fksToRemove {
		delete(fkc.foreignKeys, fk.HashOf().String())
	}
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

// copy returns an exact copy of the calling collection. As collections are meant to be modified in-place, this ensures
// that the original collection is not affected by any operations applied to the copied collection.
func (fkc *ForeignKeyCollection) copy() *ForeignKeyCollection {
	copiedForeignKeys := make(map[string]ForeignKey)
	for hashOf, key := range fkc.foreignKeys {
		copiedForeignKeys[hashOf] = key
	}
	return &ForeignKeyCollection{copiedForeignKeys}
}
