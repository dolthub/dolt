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
	"maps"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/go-mysql-server/sql"
)

// ForeignKeyCollection represents the collection of foreign keys for a root value.
type ForeignKeyCollection struct {
	// foreignKeys holds every foreign key, keyed by the case-folded name of the table that declares it and then by the
	// case-folded name of the foreign key.
	foreignKeys map[TableName]map[string]ForeignKey
	// indexes is built on first use, since most collections are loaded or copied and then only read by name or by
	// declaring table, which |foreignKeys| answers directly. Once built, it is kept up to date.
	indexes *foreignKeyIndexes
}

// foreignKeyIndexes indexes the keys of a ForeignKeyCollection for KeysForTable and GetMatchingKey.
type foreignKeyIndexes struct {
	// referencedBy indexes the foreign keys by the case-folded name of the table they reference.
	referencedBy map[TableName]map[foreignKeyID]struct{}
	// byChildTag indexes the resolved foreign keys by the tag of their first declared column, and unresolved holds the
	// unresolved foreign keys. GetMatchingKey matches keys by column rather than by table name, since a table may be
	// renamed between the collections it compares.
	byChildTag map[uint64]map[foreignKeyID]struct{}
	unresolved map[foreignKeyID]struct{}
}

func (idx *foreignKeyIndexes) add(id foreignKeyID, key ForeignKey) {
	addToIndex(idx.referencedBy, key.ReferencedTableName.FoldCase(), id)
	if key.IsResolved() {
		addToIndex(idx.byChildTag, key.TableColumns[0], id)
	} else {
		idx.unresolved[id] = struct{}{}
	}
}

func (idx *foreignKeyIndexes) remove(id foreignKeyID, key ForeignKey) {
	removeFromIndex(idx.referencedBy, key.ReferencedTableName.FoldCase(), id)
	if key.IsResolved() {
		removeFromIndex(idx.byChildTag, key.TableColumns[0], id)
	} else {
		delete(idx.unresolved, id)
	}
}

// foreignKeyID identifies a foreign key by the case-folded names of the table that declares it and of the key itself.
type foreignKeyID struct {
	table TableName
	name  string
}

func idOf(fk ForeignKey) foreignKeyID {
	return foreignKeyID{table: fk.TableName.FoldCase(), name: foldString(fk.Name)}
}

type ForeignKeyReferentialAction byte

const (
	ForeignKeyReferentialAction_DefaultAction ForeignKeyReferentialAction = iota
	ForeignKeyReferentialAction_Cascade
	ForeignKeyReferentialAction_NoAction
	ForeignKeyReferentialAction_Restrict
	ForeignKeyReferentialAction_SetNull
	ForeignKeyReferentialAction_SetDefault
)

// ForeignKeyMatchType represents NULL handling semantics for composite FK columns (Doltgres-only feature).
type ForeignKeyMatchType byte

const (
	ForeignKeyMatchType_Simple ForeignKeyMatchType = iota
	ForeignKeyMatchType_Full
	ForeignKeyMatchType_Partial // this is not supported in Postgres
)

// ForeignKey is the complete, internal representation of a Foreign Key.
type ForeignKey struct {
	Name                   string                      `noms:"name" json:"name"`
	TableName              TableName                   `noms:"tbl_name" json:"tbl_name"`
	TableIndex             string                      `noms:"tbl_index" json:"tbl_index"`
	TableColumns           []uint64                    `noms:"tbl_cols" json:"tbl_cols"`
	ReferencedTableName    TableName                   `noms:"ref_tbl_name" json:"ref_tbl_name"`
	ReferencedTableIndex   string                      `noms:"ref_tbl_index" json:"ref_tbl_index"`
	ReferencedTableColumns []uint64                    `noms:"ref_tbl_cols" json:"ref_tbl_cols"`
	OnUpdate               ForeignKeyReferentialAction `noms:"on_update" json:"on_update"`
	OnDelete               ForeignKeyReferentialAction `noms:"on_delete" json:"on_delete"`
	UnresolvedFKDetails    UnresolvedFKDetails         `noms:"unres_fk,omitempty" json:"unres_fk,omitempty"`
	IsNotValid             bool                        `noms:"is_not_valid,omitempty" json:"is_not_valid,omitempty"`
	MatchType              ForeignKeyMatchType         `noms:"match_type,omitempty" json:"match_type,omitempty"`
}

// UnresolvedFKDetails contains any details necessary for an unresolved foreign key to resolve to a valid foreign key.
type UnresolvedFKDetails struct {
	TableColumns           []string `noms:"x_tbl_cols" json:"x_tbl_cols"`
	ReferencedTableColumns []string `noms:"x_ref_tbl_cols" json:"x_ref_tbl_cols"`
}

// EqualDefs returns whether two foreign keys have the same definition over the same column sets.
// It does not compare table names.
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

// Equals compares this ForeignKey to |other| and returns true if they are equal. Foreign keys can either be in
// a "resolved" state, where the referenced columns in the parent and child tables are identified by column tags,
// or in an "unresolved" state where the reference columns in the parent and child are still identified by strings.
// If one foreign key is resolved and one is unresolved, the logic for comparing them requires resolving the string
// column names to column tags, which is why |fkSchemasByName| and |otherSchemasByName| are passed in. Each of these
// is a map of table schemas for |fk| and |other|, where the child table and every parent table referenced in the
// foreign key is present in the map.
func (fk ForeignKey) Equals(other ForeignKey, fkSchemasByName, otherSchemasByName map[TableName]schema.Schema) bool {
	// If both FKs are resolved or unresolved, we can just deeply compare them
	if fk.IsResolved() == other.IsResolved() {
		return fk.DeepEquals(other)
	}

	// Otherwise, one FK is resolved and one is not, so we need to work a little harder
	// to calculate equality since their referenced columns are represented differently.
	// First check the attributes that don't change when an FK is resolved or unresolved.
	if fk.Name != other.Name &&
		fk.TableName != other.TableName &&
		fk.ReferencedTableName != other.ReferencedTableName &&
		fk.TableIndex != other.TableIndex &&
		fk.ReferencedTableIndex != other.ReferencedTableIndex &&
		fk.OnUpdate == other.OnUpdate &&
		fk.OnDelete == other.OnDelete {
		return false
	}

	// Sort out which FK is resolved and which is not
	var resolvedFK, unresolvedFK ForeignKey
	var resolvedSchemasByName map[TableName]schema.Schema
	if fk.IsResolved() {
		resolvedFK, unresolvedFK, resolvedSchemasByName = fk, other, fkSchemasByName
	} else {
		resolvedFK, unresolvedFK, resolvedSchemasByName = other, fk, otherSchemasByName
	}

	// Check the columns on the child table
	if len(resolvedFK.TableColumns) != len(unresolvedFK.UnresolvedFKDetails.TableColumns) {
		return false
	}
	for i, tag := range resolvedFK.TableColumns {
		unresolvedColName := unresolvedFK.UnresolvedFKDetails.TableColumns[i]
		resolvedSch, ok := resolvedSchemasByName[resolvedFK.TableName]
		if !ok {
			return false
		}
		resolvedCol, ok := resolvedSch.GetAllCols().GetByTag(tag)
		if !ok {
			return false
		}
		if resolvedCol.Name != unresolvedColName {
			return false
		}
	}

	// Check the columns on the parent table
	if len(resolvedFK.ReferencedTableColumns) != len(unresolvedFK.UnresolvedFKDetails.ReferencedTableColumns) {
		return false
	}
	for i, tag := range resolvedFK.ReferencedTableColumns {
		unresolvedColName := unresolvedFK.UnresolvedFKDetails.ReferencedTableColumns[i]
		resolvedSch, ok := resolvedSchemasByName[unresolvedFK.ReferencedTableName]
		if !ok {
			return false
		}
		resolvedCol, ok := resolvedSch.GetAllCols().GetByTag(tag)
		if !ok {
			return false
		}
		if resolvedCol.Name != unresolvedColName {
			return false
		}
	}

	return true
}

// DeepEquals compares all attributes of a foreign key to another, including name and
// table names. Note that if one foreign key is resolved and the other is NOT resolved,
// then this function will not calculate equality correctly. When comparing a resolved
// FK with an unresolved FK, the ForeignKey.Equals() function should be used instead.
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

var fkBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// HashOf returns the Noms hash of a ForeignKey.
func (fk ForeignKey) HashOf() (hash.Hash, error) {
	bb := fkBufPool.Get().([]byte)
	defer func() {
		bb = bb[:0]
		fkBufPool.Put(bb)
	}()

	var err error
	bb = append(bb, fk.Name...)
	bb = append(bb, fk.TableName.String()...)
	bb = append(bb, fk.TableIndex...)
	for _, col := range fk.TableColumns {
		bb, err = binary.Append(bb, binary.LittleEndian, col)
		if err != nil {
			return hash.Hash{}, err
		}
	}
	bb = append(bb, fk.ReferencedTableName.String()...)
	bb = append(bb, fk.ReferencedTableIndex...)
	for _, col := range fk.ReferencedTableColumns {
		bb, err = binary.Append(bb, binary.LittleEndian, col)
		if err != nil {
			return hash.Hash{}, err
		}
	}
	bb = append(bb, byte(fk.OnUpdate))
	bb = append(bb, byte(fk.OnDelete))
	for _, col := range fk.UnresolvedFKDetails.TableColumns {
		bb = append(bb, col...)
	}
	for _, col := range fk.UnresolvedFKDetails.ReferencedTableColumns {
		bb = append(bb, col...)
	}
	return hash.Of(bb), nil
}

// CombinedHash returns a combined hash value for all foreign keys in the slice provided.
// An empty slice has a zero hash.
func CombinedHash(fks []ForeignKey) (hash.Hash, error) {
	if len(fks) == 0 {
		return hash.Hash{}, nil
	}

	var bb bytes.Buffer
	for _, fk := range fks {
		h, err := fk.HashOf()
		if err != nil {
			return hash.Hash{}, err
		}
		bb.Write(h[:])
	}

	return hash.Of(bb.Bytes()), nil
}

// IsSelfReferential returns whether the table declaring the foreign key is also referenced by the foreign key.
func (fk ForeignKey) IsSelfReferential() bool {
	return fk.TableName.EqualFold(fk.ReferencedTableName)
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
	fkc := newForeignKeyCollection()
	for _, k := range keys {
		err := fkc.AddKeys(k)
		if err != nil {
			return nil, err
		}
	}
	return fkc, nil
}

func newForeignKeyCollection() *ForeignKeyCollection {
	return &ForeignKeyCollection{
		foreignKeys: make(map[TableName]map[string]ForeignKey),
	}
}

// getIndexes returns the indexes of this collection, building them if this is their first use.
func (fkc *ForeignKeyCollection) getIndexes() *foreignKeyIndexes {
	if fkc.indexes == nil {
		idx := &foreignKeyIndexes{
			referencedBy: make(map[TableName]map[foreignKeyID]struct{}),
			byChildTag:   make(map[uint64]map[foreignKeyID]struct{}),
			unresolved:   make(map[foreignKeyID]struct{}),
		}
		for table, byName := range fkc.foreignKeys {
			for name, key := range byName {
				idx.add(foreignKeyID{table: table, name: name}, key)
			}
		}
		fkc.indexes = idx
	}
	return fkc.indexes
}

// AddKeys adds the given foreign key to the collection. Checks that the given name is unique in the collection, and that
// both column counts are equal. All other validation should occur before being added to the collection.
func (fkc *ForeignKeyCollection) AddKeys(fks ...ForeignKey) error {
	for _, key := range fks {
		if _, ok := fkc.GetByNameCaseInsensitive(key.Name, key.TableName); ok {
			return sql.ErrForeignKeyDuplicateName.New(key.Name)
		}
		if err := fkc.addKey(key); err != nil {
			return err
		}
	}
	return nil
}

// addKey adds the given foreign key to the collection without checking that its name is unique among all the keys
// that GetByNameCaseInsensitive would match. A key of the same name declared on the same table is still an error,
// since one of the two would otherwise be lost.
func (fkc *ForeignKeyCollection) addKey(key ForeignKey) error {
	if len(key.TableColumns) != len(key.ReferencedTableColumns) {
		return fmt.Errorf("foreign keys must have the same number of columns declared and referenced")
	}
	id := idOf(key)
	if _, ok := fkc.get(id); ok {
		return sql.ErrForeignKeyDuplicateName.New(key.Name)
	}

	fkc.put(id, key)
	return nil
}

func (fkc *ForeignKeyCollection) get(id foreignKeyID) (ForeignKey, bool) {
	fk, ok := fkc.foreignKeys[id.table][id.name]
	return fk, ok
}

// put stores |key| under |id|, which must be idOf(key), replacing any key with the same foreignKeyID.
func (fkc *ForeignKeyCollection) put(id foreignKeyID, key ForeignKey) {
	fkc.remove(id)

	byName, ok := fkc.foreignKeys[id.table]
	if !ok {
		byName = make(map[string]ForeignKey)
		fkc.foreignKeys[id.table] = byName
	}
	byName[id.name] = key

	if fkc.indexes != nil {
		fkc.indexes.add(id, key)
	}
}

// remove deletes the foreign key with the given foreignKeyID, if there is one.
func (fkc *ForeignKeyCollection) remove(id foreignKeyID) {
	key, ok := fkc.get(id)
	if !ok {
		return
	}

	delete(fkc.foreignKeys[id.table], id.name)
	if len(fkc.foreignKeys[id.table]) == 0 {
		delete(fkc.foreignKeys, id.table)
	}

	if fkc.indexes != nil {
		fkc.indexes.remove(id, key)
	}
}

func addToIndex[K comparable](index map[K]map[foreignKeyID]struct{}, k K, id foreignKeyID) {
	ids, ok := index[k]
	if !ok {
		ids = make(map[foreignKeyID]struct{})
		index[k] = ids
	}
	ids[id] = struct{}{}
}

func removeFromIndex[K comparable](index map[K]map[foreignKeyID]struct{}, k K, id foreignKeyID) {
	delete(index[k], id)
	if len(index[k]) == 0 {
		delete(index, k)
	}
}

// keysFor returns the foreign keys with the given foreignKeyIDs, in no particular order.
func (fkc *ForeignKeyCollection) keysFor(ids map[foreignKeyID]struct{}) []ForeignKey {
	var fks []ForeignKey
	for id := range ids {
		fk, _ := fkc.get(id)
		fks = append(fks, fk)
	}
	return fks
}

// unsortedKeys returns all of the foreign keys in this collection, in no particular order.
func (fkc *ForeignKeyCollection) unsortedKeys() []ForeignKey {
	fks := make([]ForeignKey, 0, fkc.Count())
	for _, byName := range fkc.foreignKeys {
		for _, fk := range byName {
			fks = append(fks, fk)
		}
	}
	return fks
}

func sortByName(fks []ForeignKey) {
	sort.Slice(fks, func(i, j int) bool {
		return fks[i].Name < fks[j].Name
	})
}

// AllKeys returns a slice, sorted by name ascending, containing all of the foreign keys in this collection.
func (fkc *ForeignKeyCollection) AllKeys() []ForeignKey {
	fks := fkc.unsortedKeys()
	sortByName(fks)
	return fks
}

// Contains returns whether the given foreign key name already exists for this collection.
func (fkc *ForeignKeyCollection) Contains(foreignKeyName string, tableName TableName) bool {
	_, ok := fkc.GetByNameCaseInsensitive(foreignKeyName, tableName)
	return ok
}

// HashOf returns the hash of the serialized form of this foreign key collection
func (fkc *ForeignKeyCollection) HashOf(ctx context.Context, vrw types.ValueReadWriter) (hash.Hash, error) {
	serialized, err := SerializeForeignKeys(ctx, vrw, fkc)
	if err != nil {
		return hash.Hash{}, err
	}

	return serialized.Hash(vrw.Format())
}

// Count returns the number of indexes in this collection.
func (fkc *ForeignKeyCollection) Count() int {
	count := 0
	for _, byName := range fkc.foreignKeys {
		count += len(byName)
	}
	return count
}

// GetByNameCaseInsensitive returns a ForeignKey with a matching case-insensitive name, and whether a match exists. If
// |tableName| has a schema, the foreign key must be declared on a matching table, otherwise it may be declared on any
// table.
func (fkc *ForeignKeyCollection) GetByNameCaseInsensitive(foreignKeyName string, tableName TableName) (ForeignKey, bool) {
	if foreignKeyName == "" {
		return ForeignKey{}, false
	}
	name := foldString(foreignKeyName)
	if tableName.Schema != "" {
		// schema name is empty for Dolt but not in Doltgres. (schema name must be populated for Doltgres)
		fk, ok := fkc.foreignKeys[tableName.FoldCase()][name]
		return fk, ok
	}
	// Dolt foreign key names are unique across all tables, so look in every table.
	for _, byName := range fkc.foreignKeys {
		if fk, ok := byName[name]; ok {
			return fk, true
		}
	}
	return ForeignKey{}, false
}

type FkIndexUpdate struct {
	FkName  string
	Table   TableName
	FromIdx string
	ToIdx   string
}

// UpdateIndexes updates the indexes used by the foreign keys as outlined by the update structs given. All foreign
// keys updated in this manner must belong to the same table, whose schema is provided.
func (fkc *ForeignKeyCollection) UpdateIndexes(ctx context.Context, tableSchema schema.Schema, updates []FkIndexUpdate) error {
	for _, u := range updates {
		fk, ok := fkc.GetByNameCaseInsensitive(u.FkName, u.Table)
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
	for _, fk := range fkc.unsortedKeys() {
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
// then both resolved and unresolved keys are checked for a match. If the given foreign key is unresolved, then it
// will match with unresolved keys, and may also match with resolved keys if |matchUnresolvedKeyToResolvedKey| is
// set to true.
//
// When |matchUnresolvedKeyToResolvedKey| is false, it is assumed that the ForeignKeyCollection is an
// ancestor collection compared to the collection that the given key comes from. Therefore, the key found in the
// ancestor will usually be the unresolved version of the given key, hence the comparison is valid. However, if the
// given key is unresolved, it is treated as a new key, which cannot be matched to a resolved key that previously
// existed.
//
// When the ForeignKeyCollection is NOT an ancestor collection, |matchUnresolvedKeyToResolvedKey| can be set to true
// to enable |fk| to match against both resolved and unresolved keys in the ForeignKeyCollection. This is necessary
// when one session commits a table with resolved foreign keys, but another open session still has an unresolved
// version of the foreign key. When the open session commits, it needs to merge it's root with the root that has
// the resolved foreign key, so it's unresolved foreign key needs to be able to match a resolved key in this case.
//
// The given schema map, |allSchemas|, is keyed by table name, and is used in the event that the given key is resolved
// and any keys in the collection are unresolved. A "dirty resolution" is performed, which matches the column names to
// tags, and then a standard tag comparison is performed. If a table or column is not in the map, then the foreign key
// is ignored.
func (fkc *ForeignKeyCollection) GetMatchingKey(fk ForeignKey, allSchemas map[TableName]schema.Schema, matchUnresolvedKeyToResolvedKey bool) (ForeignKey, bool) {
	if !fk.IsResolved() {
		// The given foreign key is unresolved, so we only look for matches on unresolved keys
		// Only keys declared on the same table can match.
	OuterLoopUnresolved:
		for _, existingFk := range fkc.foreignKeys[fk.TableName.FoldCase()] {
			// For unresolved keys, the table name is important (column tags are globally unique, column names are not)
			if (!matchUnresolvedKeyToResolvedKey && existingFk.IsResolved()) ||
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
	// The given foreign key is resolved, so we may match both resolved and unresolved keys. A resolved key can only
	// match if it has the same first column tag.
	idx := fkc.getIndexes()
	candidates := append(fkc.keysFor(idx.byChildTag[fk.TableColumns[0]]), fkc.keysFor(idx.unresolved)...)
OuterLoopResolved:
	for _, existingFk := range candidates {
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
	for _, fk := range fkc.unsortedKeys() {
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
func (fkc *ForeignKeyCollection) KeysForTable(tableName TableName) (declaredFk, referencedByFk []ForeignKey) {
	table := tableName.FoldCase()
	for _, fk := range fkc.foreignKeys[table] {
		declaredFk = append(declaredFk, fk)
	}
	referencedByFk = fkc.keysFor(fkc.getIndexes().referencedBy[table])
	sortByName(declaredFk)
	sortByName(referencedByFk)
	return declaredFk, referencedByFk
}

// RemoveKeys removes any Foreign Keys with matching column set from the collection.
func (fkc *ForeignKeyCollection) RemoveKeys(fks ...ForeignKey) {
	for _, existing := range fkc.unsortedKeys() {
		for _, outgoing := range fks {
			if outgoing.EqualDefs(existing) {
				fkc.remove(idOf(existing))
				break
			}
		}
	}
}

// RemoveKeyByName removes a foreign key from the collection. It does not remove the associated indexes from their
// respective tables. Returns true if the key was successfully removed.
func (fkc *ForeignKeyCollection) RemoveKeyByName(foreignKeyName string, tableName TableName) bool {
	fk, ok := fkc.GetByNameCaseInsensitive(foreignKeyName, tableName)
	if !ok {
		return false
	}
	fkc.remove(idOf(fk))
	return true
}

// RemoveTables removes all foreign keys associated with the given tables, if permitted. The operation assumes that ALL
// tables to be removed are in a single call, as splitting tables into different calls may result in unintended errors.
func (fkc *ForeignKeyCollection) RemoveTables(ctx context.Context, tables ...TableName) error {
	outgoing := NewTableNameSet(tables)
	for _, fk := range fkc.unsortedKeys() {
		dropChild := outgoing.Contains(fk.TableName)
		dropParent := outgoing.Contains(fk.ReferencedTableName)
		if dropParent && !dropChild {
			return fmt.Errorf("unable to remove `%s` since it is referenced from table `%s`", fk.ReferencedTableName, fk.TableName)
		}
		if dropChild {
			fkc.remove(idOf(fk))
		}
	}
	return nil
}

// RemoveAndUnresolveTables removes all foreign keys associated with the given tables. If a parent is dropped without
// its child, then the foreign key goes to an unresolved state. The operation assumes that ALL tables to be removed are
// in a single call, as splitting tables into different calls may result in unintended errors.
func (fkc *ForeignKeyCollection) RemoveAndUnresolveTables(ctx context.Context, root RootValue, tables ...TableName) error {
	outgoing := NewTableNameSet(tables)
	for _, fk := range fkc.unsortedKeys() {
		dropChild := outgoing.Contains(fk.TableName)
		dropParent := outgoing.Contains(fk.ReferencedTableName)
		if dropParent && !dropChild {
			if !fk.IsResolved() {
				continue
			}

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

			fkc.put(idOf(fk), fk)
		}
		if dropChild {
			fkc.remove(idOf(fk))
		}
	}
	return nil
}

// Tables returns the set of all tables that either declare a foreign key or are referenced by a foreign key.
func (fkc *ForeignKeyCollection) Tables() map[TableName]struct{} {
	tables := make(map[TableName]struct{})
	for _, fk := range fkc.unsortedKeys() {
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
	case ForeignKeyReferentialAction_SetDefault:
		return "SET DEFAULT"
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
	case ForeignKeyReferentialAction_SetDefault:
		return "SET DEFAULT"
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

// Copy returns an exact copy of the calling collection. As collections are meant to be modified in-place, this ensures
// that the original collection is not affected by any operations applied to the copied collection.
func (fkc *ForeignKeyCollection) Copy() *ForeignKeyCollection {
	copied := &ForeignKeyCollection{
		foreignKeys: make(map[TableName]map[string]ForeignKey, len(fkc.foreignKeys)),
	}
	for table, byName := range fkc.foreignKeys {
		copied.foreignKeys[table] = maps.Clone(byName)
	}
	return copied
}
