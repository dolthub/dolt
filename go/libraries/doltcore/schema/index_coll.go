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

package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
)

type IndexCollection interface {
	// AddIndex adds the given index, overwriting any current indexes with the same name or columns.
	// It does not perform any kind of checking, and is intended for schema modifications.
	AddIndex(indexes ...Index)
	// AddIndexByColNames adds an index with the given name and columns (in index order).
	AddIndexByColNames(indexName string, cols []string, prefixLengths []uint16, props IndexProperties) (Index, error)
	// AddIndexByColTags adds an index with the given name and column tags (in index order).
	AddIndexByColTags(indexName string, tags []uint64, prefixLengths []uint16, props IndexProperties) (Index, error)
	// todo: this method is trash, clean up this interface
	UnsafeAddIndexByColTags(indexName string, tags []uint64, prefixLengths []uint16, props IndexProperties) (Index, error)
	// AllIndexes returns a slice containing all of the indexes in this collection.
	AllIndexes() []Index
	// Contains returns whether the given index name already exists for this table.
	Contains(indexName string) bool
	// Count returns the number of indexes in this collection.
	Count() int
	// Equals returns whether this index collection is equivalent to another. Indexes are compared by everything except
	// for their name, the names of all columns, and anything relating to the parent table's primary keys.
	Equals(other IndexCollection) bool
	// GetByName returns the index with the given name, or nil if it does not exist.
	GetByName(indexName string) Index
	// GetByNameCaseInsensitive returns the index with a matching case-insensitive name, the bool return value indicates if a match was found.
	GetByNameCaseInsensitive(indexName string) (Index, bool)
	// GetIndexByColumnNames returns whether the collection contains an index that has this exact collection and ordering of columns.
	GetIndexByColumnNames(cols ...string) (Index, bool)
	// GetIndexByTags returns whether the collection contains an index that has this exact collection and ordering of columns.
	// Note that if an index collection contains multiple indexes that cover the same column tags (e.g. different index
	// types) then this method will return one of them, but it is not guaranteed which one and can easily result in a
	// race condition.
	GetIndexByTags(tags ...uint64) (Index, bool)
	// GetIndexesByTags returns all indexes from this collection that cover the same columns identified by |tags|, in the
	// same order specified. This method is preferred over GetIndexByTags.
	GetIndexesByTags(tags ...uint64) []Index
	// IndexesWithColumn returns all indexes that index the given column.
	IndexesWithColumn(columnName string) []Index
	// IndexesWithTag returns all indexes that index the given tag.
	IndexesWithTag(tag uint64) []Index
	// Iter iterated over the indexes in the collection, calling the cb function on each.
	Iter(cb func(index Index) (stop bool, err error)) error
	// Merge adds the given index if it does not already exist. Indexed columns are referenced by column name,
	// rather than by tag number, which allows an index from a different table to be added as long as they have matching
	// column names. If an index with the same name or column structure already exists, or the index contains different
	// columns, then it is skipped.
	Merge(indexes ...Index)
	// RemoveIndex removes an index from the table metadata.
	RemoveIndex(indexName string) (Index, error)
	// RenameIndex renames an index in the table metadata.
	RenameIndex(oldName, newName string) (Index, error)
	//SetPks changes the pks or pk ordinals
	SetPks([]uint64) error
	// ContainsFullTextIndex returns whether the collection contains at least one Full-Text index.
	ContainsFullTextIndex() bool
	// Copy returns a copy of this index collection that can be modified without affecting the original.
	Copy() IndexCollection
}

type IndexProperties struct {
	IsUnique      bool
	IsSpatial     bool
	IsFullText    bool
	IsUserDefined bool
	Comment       string
	FullTextProperties
	IsVector bool
	VectorProperties
}

type FullTextProperties struct {
	ConfigTable      string
	PositionTable    string
	DocCountTable    string
	GlobalCountTable string
	RowCountTable    string
	KeyType          uint8
	KeyName          string
	KeyPositions     []uint16
}

type VectorProperties struct {
	DistanceType vector.DistanceType
}

type indexCollectionImpl struct {
	colColl       *ColCollection
	indexes       map[string]*indexImpl
	colTagToIndex map[uint64][]*indexImpl
	pks           []uint64
}

func NewIndexCollection(cols *ColCollection, pkCols *ColCollection) IndexCollection {
	ixc := &indexCollectionImpl{
		colColl:       cols,
		indexes:       make(map[string]*indexImpl),
		colTagToIndex: make(map[uint64][]*indexImpl),
	}
	if cols != nil {
		for _, col := range cols.cols {
			ixc.colTagToIndex[col.Tag] = nil
			if col.IsPartOfPK {
				ixc.pks = append(ixc.pks, col.Tag)
			}
		}
	}
	if pkCols != nil {
		for i, col := range pkCols.cols {
			ixc.pks[i] = col.Tag
		}
	}
	return ixc
}

func (ixc *indexCollectionImpl) Copy() IndexCollection {
	// no need to copy the colColl, it's immutable
	nixc := &indexCollectionImpl{
		colColl: ixc.colColl,
	}

	if ixc.pks != nil {
		nixc.pks = make([]uint64, len(ixc.pks))
		copy(nixc.pks, ixc.pks)
	}

	if ixc.indexes != nil {
		nixc.indexes = make(map[string]*indexImpl, len(ixc.indexes))
		for name, index := range ixc.indexes {
			nixc.indexes[name] = index.copy()
		}
	}

	if ixc.colTagToIndex != nil {
		nixc.colTagToIndex = make(map[uint64][]*indexImpl, len(ixc.colTagToIndex))
		for tag, indexes := range ixc.colTagToIndex {
			var indexesCopy []*indexImpl
			if indexes != nil {
				indexesCopy = make([]*indexImpl, len(indexes))
				for i, index := range indexes {
					indexesCopy[i] = index.copy()
				}
			}
			nixc.colTagToIndex[tag] = indexesCopy
		}
	}

	return nixc
}

func (ixc *indexCollectionImpl) AddIndex(indexes ...Index) {
	for _, indexInterface := range indexes {
		index, ok := indexInterface.(*indexImpl)
		if !ok {
			panic(fmt.Errorf("unknown index type: %T", indexInterface))
		}
		index = index.copy()
		index.indexColl = ixc
		index.allTags = combineAllTags(index.tags, ixc.pks)
		lowerName := strings.ToLower(index.name)
		oldNamedIndex, ok := ixc.indexes[lowerName]
		if ok {
			ixc.removeIndex(oldNamedIndex)
		}
		ixc.indexes[lowerName] = index
		for _, tag := range index.tags {
			ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
		}
	}
}

func (ixc *indexCollectionImpl) AddIndexByColNames(indexName string, cols []string, prefixLengths []uint16, props IndexProperties) (Index, error) {
	tags, ok := ixc.columnNamesToTags(cols)
	if !ok {
		return nil, fmt.Errorf("the table does not contain at least one of the following columns: `%v`", cols)
	}
	return ixc.AddIndexByColTags(indexName, tags, prefixLengths, props)
}

func (ixc *indexCollectionImpl) AddIndexByColTags(indexName string, tags []uint64, prefixLengths []uint16, props IndexProperties) (Index, error) {
	lowerName := strings.ToLower(indexName)
	if strings.HasPrefix(lowerName, "dolt_") && !strings.HasPrefix(lowerName, "dolt_ci_") {
		return nil, fmt.Errorf("indexes cannot be prefixed with `dolt_`")
	}
	if ixc.Contains(lowerName) {
		return nil, sql.ErrDuplicateKey.New(lowerName)
	}
	if !ixc.tagsExist(tags...) {
		return nil, fmt.Errorf("tags %v do not exist on this table", tags)
	}

	for _, tag := range tags {
		// we already validated the tag exists
		c, _ := ixc.colColl.GetByTag(tag)
		err := validateColumnIndexable(c)
		if err != nil {
			return nil, err
		}
	}

	index := &indexImpl{
		indexColl:        ixc,
		name:             indexName,
		tags:             tags,
		allTags:          combineAllTags(tags, ixc.pks),
		isUnique:         props.IsUnique,
		isSpatial:        props.IsSpatial,
		isFullText:       props.IsFullText,
		isVector:         props.IsVector,
		isUserDefined:    props.IsUserDefined,
		comment:          props.Comment,
		prefixLengths:    prefixLengths,
		fullTextProps:    props.FullTextProperties,
		vectorProperties: props.VectorProperties,
	}
	ixc.indexes[lowerName] = index
	for _, tag := range tags {
		ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
	}
	return index, nil
}

// validateColumnIndexable returns an error if the column given cannot be used in an index
func validateColumnIndexable(c Column) error {
	return nil
}

func (ixc *indexCollectionImpl) UnsafeAddIndexByColTags(indexName string, tags []uint64, prefixLengths []uint16, props IndexProperties) (Index, error) {
	index := &indexImpl{
		indexColl:     ixc,
		name:          indexName,
		tags:          tags,
		allTags:       combineAllTags(tags, ixc.pks),
		isUnique:      props.IsUnique,
		isSpatial:     props.IsSpatial,
		isFullText:    props.IsFullText,
		isVector:      props.IsVector,
		isUserDefined: props.IsUserDefined,
		comment:       props.Comment,
		prefixLengths: prefixLengths,
		fullTextProps: props.FullTextProperties,
	}
	ixc.indexes[strings.ToLower(indexName)] = index
	for _, tag := range tags {
		ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
	}
	return index, nil
}

func (ixc *indexCollectionImpl) AllIndexes() []Index {
	indexes := make([]Index, len(ixc.indexes))
	i := 0
	for _, index := range ixc.indexes {
		indexes[i] = index
		i++
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name() < indexes[j].Name()
	})
	return indexes
}

func (ixc *indexCollectionImpl) Contains(indexName string) bool {
	_, ok := ixc.indexes[strings.ToLower(indexName)]
	return ok
}

func (ixc *indexCollectionImpl) Count() int {
	return len(ixc.indexes)
}

func (ixc *indexCollectionImpl) Equals(other IndexCollection) bool {
	otherIxc, ok := other.(*indexCollectionImpl)
	if !ok || len(ixc.indexes) != len(otherIxc.indexes) {
		// if the lengths don't match then we can quickly return
		return false
	}

	// if named indexes are all equal, then we can skip the checks which compare indexes by their tags.
	nameCmp := true
	for name, i1 := range ixc.indexes {
		i2, ok := otherIxc.indexes[name]
		if !ok {
			nameCmp = false
			break
		}

		if !i1.Equals(i2) {
			nameCmp = false
			break
		}
	}
	if nameCmp {
		return true
	}

	for _, index := range ixc.indexes {
		otherIndex := otherIxc.containsColumnTagCollection(index.tags...)
		if otherIndex == nil || !index.Equals(otherIndex) {
			return false
		}
	}
	return true
}

func (ixc *indexCollectionImpl) GetByName(indexName string) Index {
	ix, ok := ixc.indexes[strings.ToLower(indexName)]
	if ok {
		return ix
	}
	return nil
}

func (ixc *indexCollectionImpl) GetByNameCaseInsensitive(indexName string) (Index, bool) {
	for name, ix := range ixc.indexes {
		if strings.EqualFold(name, indexName) {
			return ix, true
		}
	}
	return nil, false
}

func (ixc *indexCollectionImpl) hasIndexOnColumns(cols ...string) bool {
	tags := make([]uint64, len(cols))
	for i, col := range cols {
		col, ok := ixc.colColl.NameToCol[col]
		if !ok {
			return false
		}
		tags[i] = col.Tag
	}
	return ixc.hasIndexOnTags(tags...)
}

func (ixc *indexCollectionImpl) GetIndexByColumnNames(cols ...string) (Index, bool) {
	tags := make([]uint64, len(cols))
	for i, col := range cols {
		col, ok := ixc.colColl.NameToCol[col]
		if !ok {
			return nil, false
		}
		tags[i] = col.Tag
	}
	return ixc.GetIndexByTags(tags...)
}

func (ixc *indexCollectionImpl) GetIndexByTags(tags ...uint64) (Index, bool) {
	idx := ixc.containsColumnTagCollection(tags...)
	if idx == nil {
		return nil, false
	}
	return idx, true
}

// GetIndexesByTags implements the schema.Index interface
func (ixc *indexCollectionImpl) GetIndexesByTags(tags ...uint64) []Index {
	var result []Index

	tagCount := len(tags)
	for _, idx := range ixc.indexes {
		if tagCount != len(idx.tags) {
			continue
		}

		allMatch := true
		for i, idxTag := range idx.tags {
			if tags[i] != idxTag {
				allMatch = false
				break
			}
		}
		if allMatch {
			result = append(result, idx)
		}
	}
	return result
}

func (ixc *indexCollectionImpl) hasIndexOnTags(tags ...uint64) bool {
	_, ok := ixc.GetIndexByTags(tags...)
	return ok
}

func (ixc *indexCollectionImpl) IndexesWithColumn(columnName string) []Index {
	col, ok := ixc.colColl.NameToCol[columnName]
	if !ok {
		return nil
	}
	return ixc.IndexesWithTag(col.Tag)
}

func (ixc *indexCollectionImpl) IndexesWithTag(tag uint64) []Index {
	indexImpls := ixc.colTagToIndex[tag]
	indexes := make([]Index, len(indexImpls))
	for i, idx := range indexImpls {
		indexes[i] = idx
	}
	return indexes
}

func (ixc *indexCollectionImpl) Iter(cb func(index Index) (stop bool, err error)) error {
	for _, ind := range ixc.indexes {
		stop, err := cb(ind)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func (ixc *indexCollectionImpl) Merge(indexes ...Index) {
	for _, index := range indexes {
		if tags, ok := ixc.columnNamesToTags(index.ColumnNames()); ok && !ixc.Contains(index.Name()) {
			newIndex := &indexImpl{
				name:          index.Name(),
				tags:          tags,
				indexColl:     ixc,
				isUnique:      index.IsUnique(),
				isSpatial:     index.IsSpatial(),
				isFullText:    index.IsFullText(),
				isVector:      index.IsVector(),
				isUserDefined: index.IsUserDefined(),
				comment:       index.Comment(),
				prefixLengths: index.PrefixLengths(),
				fullTextProps: index.FullTextProperties(),
			}
			ixc.AddIndex(newIndex)
		}
	}
}

func (ixc *indexCollectionImpl) RemoveIndex(indexName string) (Index, error) {
	lowerName := strings.ToLower(indexName)
	if !ixc.Contains(lowerName) {
		return nil, sql.ErrIndexNotFound.New(indexName)
	}
	index := ixc.indexes[lowerName]
	delete(ixc.indexes, lowerName)
	for _, tag := range index.tags {
		indexesRefThisCol := ixc.colTagToIndex[tag]
		for i, comparisonIndex := range indexesRefThisCol {
			if comparisonIndex == index {
				ixc.colTagToIndex[tag] = append(indexesRefThisCol[:i], indexesRefThisCol[i+1:]...)
				break
			}
		}
	}
	return index, nil
}

func (ixc *indexCollectionImpl) RenameIndex(oldName, newName string) (Index, error) {
	lowerCaseOldName := strings.ToLower(oldName)
	lowerCaseNewName := strings.ToLower(newName)
	if !ixc.Contains(lowerCaseOldName) {
		return nil, sql.ErrIndexNotFound.New(oldName)
	}
	if ixc.Contains(lowerCaseNewName) {
		return nil, sql.ErrDuplicateKey.New(newName)
	}
	index := ixc.indexes[lowerCaseOldName]
	delete(ixc.indexes, lowerCaseOldName)
	index.name = newName
	ixc.indexes[lowerCaseNewName] = index
	return index, nil
}

func (ixc *indexCollectionImpl) columnNamesToTags(cols []string) ([]uint64, bool) {
	tags := make([]uint64, len(cols))
	for i, colName := range cols {
		col, ok := ixc.colColl.NameToCol[colName]
		if !ok {
			return nil, false
		}
		tags[i] = col.Tag
	}
	return tags, true
}

func (ixc *indexCollectionImpl) containsColumnTagCollection(tags ...uint64) *indexImpl {
	tagCount := len(tags)
	for _, idx := range ixc.indexes {
		if tagCount == len(idx.tags) {
			allMatch := true
			for i, idxTag := range idx.tags {
				if tags[i] != idxTag {
					allMatch = false
					break
				}
			}
			if allMatch {
				return idx
			}
		}
	}
	return nil
}

func (ixc *indexCollectionImpl) removeIndex(index *indexImpl) {
	delete(ixc.indexes, strings.ToLower(index.name))
	for _, tag := range index.tags {
		var newReferences []*indexImpl
		for _, referencedIndex := range ixc.colTagToIndex[tag] {
			if referencedIndex != index {
				newReferences = append(newReferences, referencedIndex)
			}
		}
		ixc.colTagToIndex[tag] = newReferences
	}
}

func (ixc *indexCollectionImpl) tagsExist(tags ...uint64) bool {
	if len(tags) == 0 {
		return false
	}
	tagToCol := ixc.colColl.TagToCol
	for _, tag := range tags {
		if _, ok := tagToCol[tag]; !ok {
			return false
		}
	}
	return true
}

func (ixc *indexCollectionImpl) SetPks(tags []uint64) error {
	if len(tags) != len(ixc.pks) {
		return ErrInvalidPkOrdinals
	}
	ixc.pks = tags
	return nil
}

func (ixc *indexCollectionImpl) ContainsFullTextIndex() bool {
	for _, idx := range ixc.indexes {
		if idx.isFullText {
			return true
		}
	}
	return false
}

// TableNameSlice returns the table names as a slice, which may be used to easily grab all of the tables using a for loop.
func (props FullTextProperties) TableNameSlice() []string {
	return []string{
		props.ConfigTable,
		props.PositionTable,
		props.DocCountTable,
		props.GlobalCountTable,
		props.RowCountTable,
	}
}

func combineAllTags(tags []uint64, pks []uint64) []uint64 {
	allTags := make([]uint64, len(tags))
	_ = copy(allTags, tags)
	foundCols := make(map[uint64]struct{})
	for _, tag := range tags {
		foundCols[tag] = struct{}{}
	}
	for _, pk := range pks {
		if _, ok := foundCols[pk]; !ok {
			allTags = append(allTags, pk)
		}
	}
	return allTags
}
