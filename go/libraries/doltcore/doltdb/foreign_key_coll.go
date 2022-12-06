// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// ForeignKeyCollection represents the collection of foreign keys for a root value.
type ForeignKeyCollection struct {
	foreignKeys map[string]ForeignKey
}

// ForeignKeyViolationError represents a set of foreign key violations for a table.
type ForeignKeyViolationError struct {
	ForeignKey    ForeignKey
	Schema        schema.Schema
	ViolationRows []row.Row
}

// Error implements the interface error.
func (f *ForeignKeyViolationError) Error() string {
	if len(f.ViolationRows) == 0 {
		return "no violations were found, should not be an error"
	}
	sb := strings.Builder{}
	const earlyTerminationLimit = 50
	terminatedEarly := false
	for i := range f.ViolationRows {
		if i >= earlyTerminationLimit {
			terminatedEarly = true
			break
		}
		key, _ := f.ViolationRows[i].NomsMapKey(f.Schema).Value(context.Background())
		val, _ := f.ViolationRows[i].NomsMapValue(f.Schema).Value(context.Background())
		valSlice, _ := val.(types.Tuple).AsSlice()
		all, _ := key.(types.Tuple).Append(valSlice...)
		str, _ := types.EncodedValue(context.Background(), all)
		sb.WriteRune('\n')
		sb.WriteString(str)
	}
	if terminatedEarly {
		return fmt.Sprintf("foreign key violations on `%s`.`%s`:%s\n%d more violations are not being displayed",
			f.ForeignKey.Name, f.ForeignKey.TableName, sb.String(), len(f.ViolationRows)-earlyTerminationLimit)
	} else {
		return fmt.Sprintf("foreign key violations on `%s`.`%s`:%s", f.ForeignKey.Name, f.ForeignKey.TableName, sb.String())
	}
}

var _ error = (*ForeignKeyViolationError)(nil)

type ForeignKeyReferentialAction byte

const (
	ForeignKeyReferentialAction_DefaultAction ForeignKeyReferentialAction = iota
	ForeignKeyReferentialAction_Cascade
	ForeignKeyReferentialAction_NoAction
	ForeignKeyReferentialAction_Restrict
	ForeignKeyReferentialAction_SetNull
)

// ForeignKey is the complete, internal representation of a Foreign Key.
type ForeignKey struct {
	Name                   string                      `noms:"name" json:"name"`
	TableName              string                      `noms:"tbl_name" json:"tbl_name"`
	TableIndex             string                      `noms:"tbl_index" json:"tbl_index"`
	TableColumns           []uint64                    `noms:"tbl_cols" json:"tbl_cols"`
	ReferencedTableName    string                      `noms:"ref_tbl_name" json:"ref_tbl_name"`
	ReferencedTableIndex   string                      `noms:"ref_tbl_index" json:"ref_tbl_index"`
	ReferencedTableColumns []uint64                    `noms:"ref_tbl_cols" json:"ref_tbl_cols"`
	OnUpdate               ForeignKeyReferentialAction `noms:"on_update" json:"on_update"`
	OnDelete               ForeignKeyReferentialAction `noms:"on_delete" json:"on_delete"`
	UnresolvedFKDetails    UnresolvedFKDetails         `noms:"unres_fk,omitempty" json:"unres_fk,omitempty"`
}

// UnresolvedFKDetails contains any details necessary for an unresolved foreign key to resolve to a valid foreign key.
type UnresolvedFKDetails struct {
	TableColumns           []string `noms:"x_tbl_cols" json:"x_tbl_cols"`
	ReferencedTableColumns []string `noms:"x_ref_tbl_cols" json:"x_ref_tbl_cols"`
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
		fk.ReferencedTableName == other.ReferencedTableName &&
		fk.TableIndex == other.TableIndex &&
		fk.ReferencedTableIndex == other.ReferencedTableIndex
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
	for _, col := range fk.UnresolvedFKDetails.TableColumns {
		_ = binary.Write(&bb, binary.LittleEndian, col)
	}
	for _, col := range fk.UnresolvedFKDetails.ReferencedTableColumns {
		_ = binary.Write(&bb, binary.LittleEndian, col)
	}

	return hash.Of(bb.Bytes())
}

// IsSelfReferential returns whether the table declaring the foreign key is also referenced by the foreign key.
func (fk ForeignKey) IsSelfReferential() bool {
	return strings.ToLower(fk.TableName) == strings.ToLower(fk.ReferencedTableName)
}

// IsResolved returns whether the foreign key has been resolved.
func (fk ForeignKey) IsResolved() bool {
	return len(fk.TableColumns) > 0 && len(fk.ReferencedTableColumns) > 0
}

// ValidateReferencedTableSchema verifies that the given schema matches the expectation of the referenced table.
func (fk ForeignKey) ValidateReferencedTableSchema(sch schema.Schema) error {
	// An unresolved foreign key will be validated later, so we don't return an error here.
	if !fk.IsResolved() {
		return nil
	}
	allSchCols := sch.GetAllCols()
	for _, colTag := range fk.ReferencedTableColumns {
		_, ok := allSchCols.GetByTag(colTag)
		if !ok {
			return fmt.Errorf("foreign key `%s` has entered an invalid state, referenced table `%s` has unexpected schema",
				fk.Name, fk.ReferencedTableName)
		}
	}

	if (fk.ReferencedTableIndex != "" && !sch.Indexes().Contains(fk.ReferencedTableIndex)) || (fk.ReferencedTableIndex == "" && sch.GetPKCols().Size() < len(fk.ReferencedTableColumns)) {
		return fmt.Errorf("foreign key `%s` has entered an invalid state, referenced table `%s` is missing the index `%s`",
			fk.Name, fk.ReferencedTableName, fk.ReferencedTableIndex)
	}
	return nil
}

// ValidateTableSchema verifies that the given schema matches the expectation of the declaring table.
func (fk ForeignKey) ValidateTableSchema(sch schema.Schema) error {
	// An unresolved foreign key will be validated later, so we don't return an error here.
	if !fk.IsResolved() {
		return nil
	}
	allSchCols := sch.GetAllCols()
	for _, colTag := range fk.TableColumns {
		_, ok := allSchCols.GetByTag(colTag)
		if !ok {
			return fmt.Errorf("foreign key `%s` has entered an invalid state, table `%s` has unexpected schema", fk.Name, fk.TableName)
		}
	}
	if (fk.TableIndex != "" && !sch.Indexes().Contains(fk.TableIndex)) || (fk.TableIndex == "" && sch.GetPKCols().Size() < len(fk.TableColumns)) {
		return fmt.Errorf("foreign key `%s` has entered an invalid state, table `%s` is missing the index `%s`",
			fk.Name, fk.TableName, fk.TableIndex)
	}
	return nil
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
			// TODO: constraint names should be unique, and this isn't guaranteed to be.
			//  This logic needs to live at the table / DB level.
			key.Name = key.HashOf().String()[:8]
		}

		if _, ok := fkc.GetByNameCaseInsensitive(key.Name); ok {
			return fmt.Errorf("a foreign key with the name `%s` already exists", key.Name)
		}
		if len(key.TableColumns) != len(key.ReferencedTableColumns) {
			return fmt.Errorf("foreign keys must have the same number of columns declared and referenced")
		}
		if key.IsResolved() {
			if _, ok := fkc.GetByTags(key.TableColumns, key.ReferencedTableColumns); ok {
				// this differs from MySQL's logic
				return fmt.Errorf("a foreign key over columns %v and referenced columns %v already exists",
					key.TableColumns, key.ReferencedTableColumns)
			}
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

type FkIndexUpdate struct {
	FkName  string
	FromIdx string
	ToIdx   string
}

// UpdateIndexes updates the indexes used by the foreign keys as outlined by the update structs given. All foreign
// keys updated in this manner must belong to the same table, whose schema is provided.
func (fkc *ForeignKeyCollection) UpdateIndexes(ctx context.Context, tableSchema schema.Schema, updates []FkIndexUpdate) error {
	for _, u := range updates {
		fk, ok := fkc.GetByNameCaseInsensitive(u.FkName)
		if !ok {
			return errors.New("foreign key not found")
		}
		fkc.RemoveKeys(fk)
		fk.ReferencedTableIndex = u.ToIdx

		err := fkc.AddKeys(fk)
		if err != nil {
			return err
		}

		err = fk.ValidateReferencedTableSchema(tableSchema)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByTags gets the ForeignKey defined over the parent and child columns corresponding to their tags.
func (fkc *ForeignKeyCollection) GetByTags(childTags, parentTags []uint64) (ForeignKey, bool) {
	if len(childTags) == 0 || len(parentTags) == 0 {
		return ForeignKey{}, false
	}
OuterLoop:
	for _, fk := range fkc.foreignKeys {
		if len(fk.ReferencedTableColumns) != len(parentTags) {
			continue
		}
		for i, t := range fk.ReferencedTableColumns {
			if t != parentTags[i] {
				continue OuterLoop
			}
		}

		if len(fk.TableColumns) != len(childTags) {
			continue
		}
		for i, t := range fk.TableColumns {
			if t != childTags[i] {
				continue OuterLoop
			}
		}
		return fk, true
	}
	return ForeignKey{}, false
}

// GetMatchingKey gets the ForeignKey defined over the parent and child columns. If the given foreign key is resolved,
// then both resolved and unresolved keys are checked for a match. If the given foreign key is unresolved, then ONLY
// unresolved keys may be found.
//
// This discrepancy is due to the primary uses for this function. It is assumed that the ForeignKeyCollection is an
// ancestor collection compared to the collection that the given key comes from. Therefore, the key found in the
// ancestor will usually be the unresolved version of the given key, hence the comparison is valid. However, if the
// given key is unresolved, it is treated as a new key, which cannot be matched to a resolved key that previously
// existed.
//
// The given schema map is keyed by table name, and is used in the event that the given key is resolved and any keys in
// the collection are unresolved. A "dirty resolution" is performed, which matches the column names to tags, and then a
// standard tag comparison is performed. If a table or column is not in the map, then the foreign key is ignored.
func (fkc *ForeignKeyCollection) GetMatchingKey(fk ForeignKey, allSchemas map[string]schema.Schema) (ForeignKey, bool) {
	if !fk.IsResolved() {
		// The given foreign key is unresolved, so we only look for matches on unresolved keys
	OuterLoopUnresolved:
		for _, existingFk := range fkc.foreignKeys {
			// For unresolved keys, the table name is important (column tags are globally unique, column names are not)
			if existingFk.IsResolved() ||
				fk.TableName != existingFk.TableName ||
				fk.ReferencedTableName != existingFk.ReferencedTableName ||
				len(fk.UnresolvedFKDetails.TableColumns) != len(existingFk.UnresolvedFKDetails.TableColumns) ||
				len(fk.UnresolvedFKDetails.ReferencedTableColumns) != len(existingFk.UnresolvedFKDetails.ReferencedTableColumns) {
				continue
			}
			for i, fkCol := range fk.UnresolvedFKDetails.TableColumns {
				if fkCol != existingFk.UnresolvedFKDetails.TableColumns[i] {
					continue OuterLoopUnresolved
				}
			}
			for i, fkCol := range fk.UnresolvedFKDetails.ReferencedTableColumns {
				if fkCol != existingFk.UnresolvedFKDetails.ReferencedTableColumns[i] {
					continue OuterLoopUnresolved
				}
			}
			return existingFk, true
		}
		return ForeignKey{}, false
	}
	// The given foreign key is resolved, so we may match both resolved and unresolved keys
OuterLoopResolved:
	for _, existingFk := range fkc.foreignKeys {
		if existingFk.IsResolved() {
			// When both are resolved, we do a standard tag comparison
			if len(fk.TableColumns) != len(existingFk.TableColumns) ||
				len(fk.ReferencedTableColumns) != len(existingFk.ReferencedTableColumns) {
				continue
			}
			for i, tag := range fk.TableColumns {
				if tag != existingFk.TableColumns[i] {
					continue OuterLoopResolved
				}
			}
			for i, tag := range fk.ReferencedTableColumns {
				if tag != existingFk.ReferencedTableColumns[i] {
					continue OuterLoopResolved
				}
			}
			return existingFk, true
		} else {
			// Since the existing key is unresolved, we reference the schema map to get tags we can use
			if len(fk.TableColumns) != len(existingFk.UnresolvedFKDetails.TableColumns) ||
				len(fk.ReferencedTableColumns) != len(existingFk.UnresolvedFKDetails.ReferencedTableColumns) {
				continue
			}
			tblSch, ok := allSchemas[existingFk.TableName]
			if !ok {
				continue
			}
			refTblSch, ok := allSchemas[existingFk.ReferencedTableName]
			if !ok {
				continue
			}
			for i, tag := range fk.TableColumns {
				col, ok := tblSch.GetAllCols().GetByNameCaseInsensitive(existingFk.UnresolvedFKDetails.TableColumns[i])
				if !ok || tag != col.Tag {
					continue OuterLoopResolved
				}
			}
			for i, tag := range fk.ReferencedTableColumns {
				col, ok := refTblSch.GetAllCols().GetByNameCaseInsensitive(existingFk.UnresolvedFKDetails.ReferencedTableColumns[i])
				if !ok || tag != col.Tag {
					continue OuterLoopResolved
				}
			}
			return existingFk, true
		}
	}
	return ForeignKey{}, false
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
	lowercaseTblName := strings.ToLower(tableName)
	for _, foreignKey := range fkc.foreignKeys {
		if strings.ToLower(foreignKey.TableName) == lowercaseTblName {
			declaredFk = append(declaredFk, foreignKey)
		}
		if strings.ToLower(foreignKey.ReferencedTableName) == lowercaseTblName {
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
// respective tables. Returns true if the key was successfully removed.
func (fkc *ForeignKeyCollection) RemoveKeyByName(foreignKeyName string) bool {
	var key string
	for k, fk := range fkc.foreignKeys {
		if strings.ToLower(fk.Name) == strings.ToLower(foreignKeyName) {
			key = k
			break
		}
	}
	if key == "" {
		return false
	}
	delete(fkc.foreignKeys, key)
	return true
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

// RemoveAndUnresolveTables removes all foreign keys associated with the given tables. If a parent is dropped without
// its child, then the foreign key goes to an unresolved state. The operation assumes that ALL tables to be removed are
// in a single call, as splitting tables into different calls may result in unintended errors.
func (fkc *ForeignKeyCollection) RemoveAndUnresolveTables(ctx context.Context, root *RootValue, tables ...string) error {
	outgoing := set.NewStrSet(tables)
	for _, fk := range fkc.foreignKeys {
		dropChild := outgoing.Contains(fk.TableName)
		dropParent := outgoing.Contains(fk.ReferencedTableName)
		if dropParent && !dropChild {
			if !fk.IsResolved() {
				continue
			}
			delete(fkc.foreignKeys, fk.HashOf().String())

			fk.UnresolvedFKDetails.TableColumns = make([]string, len(fk.TableColumns))
			fk.UnresolvedFKDetails.ReferencedTableColumns = make([]string, len(fk.ReferencedTableColumns))

			tbl, ok, err := root.GetTable(ctx, fk.TableName)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("table `%s` declares the resolved foreign key `%s` but the table cannot be found",
					fk.TableName, fk.Name)
			}
			sch, err := tbl.GetSchema(ctx)
			if err != nil {
				return err
			}
			for i, tag := range fk.TableColumns {
				col, ok := sch.GetAllCols().GetByTag(tag)
				if !ok {
					return fmt.Errorf("table `%s` uses tag `%d` in foreign key `%s` but no matching column was found",
						fk.TableName, tag, fk.Name)
				}
				fk.UnresolvedFKDetails.TableColumns[i] = col.Name
			}

			refTbl, ok, err := root.GetTable(ctx, fk.ReferencedTableName)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("table `%s` is referenced by the resolved foreign key `%s` but cannot be found",
					fk.ReferencedTableName, fk.Name)
			}
			refSch, err := refTbl.GetSchema(ctx)
			if err != nil {
				return err
			}
			for i, tag := range fk.ReferencedTableColumns {
				col, ok := refSch.GetAllCols().GetByTag(tag)
				if !ok {
					return fmt.Errorf("table `%s` uses tag `%d` in foreign key `%s` but no matching column was found",
						fk.ReferencedTableName, tag, fk.Name)
				}
				fk.UnresolvedFKDetails.ReferencedTableColumns[i] = col.Name
			}

			fk.TableColumns = nil
			fk.ReferencedTableColumns = nil
			fk.TableIndex = ""
			fk.ReferencedTableIndex = ""
			fkc.foreignKeys[fk.HashOf().String()] = fk
		}
		if dropChild {
			delete(fkc.foreignKeys, fk.HashOf().String())
		}
	}
	return nil
}

// RenameTable updates all foreign key entries in the collection with the updated table name. Does not check for name
// collisions. Additionally, any unresolved foreign keys will still update their referenced names as this matches
// MySQL's behavior.
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

// Tables returns the set of all tables that either declare a foreign key or are referenced by a foreign key.
func (fkc *ForeignKeyCollection) Tables() map[string]struct{} {
	tables := make(map[string]struct{})
	for _, fk := range fkc.foreignKeys {
		tables[fk.TableName] = struct{}{}
		tables[fk.ReferencedTableName] = struct{}{}
	}
	return tables
}

// String returns the SQL reference option in uppercase.
func (refOp ForeignKeyReferentialAction) String() string {
	switch refOp {
	case ForeignKeyReferentialAction_DefaultAction:
		return "NONE SPECIFIED"
	case ForeignKeyReferentialAction_Cascade:
		return "CASCADE"
	case ForeignKeyReferentialAction_NoAction:
		return "NO ACTION"
	case ForeignKeyReferentialAction_Restrict:
		return "RESTRICT"
	case ForeignKeyReferentialAction_SetNull:
		return "SET NULL"
	default:
		return "INVALID"
	}
}

// ReducedString returns the SQL reference option in uppercase. All reference options are functionally equivalent to
// either RESTRICT, CASCADE, or SET NULL, therefore only one those three options are returned.
func (refOp ForeignKeyReferentialAction) ReducedString() string {
	switch refOp {
	case ForeignKeyReferentialAction_DefaultAction, ForeignKeyReferentialAction_NoAction, ForeignKeyReferentialAction_Restrict:
		return "RESTRICT"
	case ForeignKeyReferentialAction_Cascade:
		return "CASCADE"
	case ForeignKeyReferentialAction_SetNull:
		return "SET NULL"
	default:
		return "INVALID"
	}
}

// ColumnHasFkRelationship returns a foreign key that uses this tag. Returns n
func (fkc *ForeignKeyCollection) ColumnHasFkRelationship(tag uint64) (ForeignKey, bool) {
	for _, key := range fkc.AllKeys() {
		tags := append(key.TableColumns, key.ReferencedTableColumns...)

		for _, keyTag := range tags {
			if tag == keyTag {
				return key, true
			}
		}
	}

	return ForeignKey{}, false
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
