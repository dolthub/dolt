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
)

type IndexCollection interface {
	// AddIndex adds the given index, overwriting any current indexes with the same name or columns.
	// It does not perform any kind of checking, and is intended for schema modifications.
	AddIndex(indexes ...Index)
	// AddIndexByColNames adds an index with the given name and columns (in index order).
	AddIndexByColNames(indexName string, cols []string, props IndexProperties) (Index, error)
	// AddIndexByColTags adds an index with the given name and column tags (in index order).
	AddIndexByColTags(indexName string, tags []uint64, props IndexProperties) (Index, error)
	// todo: this method is trash, clean up this interface
	UnsafeAddIndexByColTags(indexName string, tags []uint64, props IndexProperties) (Index, error)
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
	// GetByName returns the index with a matching case-insensitive name, the bool return value indicates if a match was found.
	GetByNameCaseInsensitive(indexName string) (Index, bool)
	// GetIndexByColumnNames returns whether the collection contains an index that has this exact collection and ordering of columns.
	GetIndexByColumnNames(cols ...string) (Index, bool)
	// GetIndexByTags returns whether the collection contains an index that has this exact collection and ordering of columns.
	GetIndexByTags(tags ...uint64) (Index, bool)
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
}

type IndexProperties struct {
	IsUnique      bool
	IsUserDefined bool
	Comment       string
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

func (ixc *indexCollectionImpl) AddIndex(indexes ...Index) {
	for _, indexInterface := range indexes {
		index, ok := indexInterface.(*indexImpl)
		if !ok {
			panic(fmt.Errorf("unknown index type: %T", indexInterface))
		}
		index = index.copy()
		index.indexColl = ixc
		index.allTags = combineAllTags(index.tags, ixc.pks)
		oldNamedIndex, ok := ixc.indexes[index.name]
		if ok {
			ixc.removeIndex(oldNamedIndex)
		}
		oldTaggedIndex := ixc.containsColumnTagCollection(index.tags...)
		if oldTaggedIndex != nil {
			ixc.removeIndex(oldTaggedIndex)
		}
		ixc.indexes[index.name] = index
		for _, tag := range index.tags {
			ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
		}
	}
}

func (ixc *indexCollectionImpl) AddIndexByColNames(indexName string, cols []string, props IndexProperties) (Index, error) {
	tags, ok := ixc.columnNamesToTags(cols)
	if !ok {
		return nil, fmt.Errorf("the table does not contain at least one of the following columns: `%v`", cols)
	}
	return ixc.AddIndexByColTags(indexName, tags, props)
}

func (ixc *indexCollectionImpl) AddIndexByColTags(indexName string, tags []uint64, props IndexProperties) (Index, error) {
	if strings.HasPrefix(indexName, "dolt_") {
		return nil, fmt.Errorf("indexes cannot be prefixed with `dolt_`")
	}
	if ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", indexName)
	}
	if !ixc.tagsExist(tags...) {
		return nil, fmt.Errorf("tags %v do not exist on this table", tags)
	}
	if ixc.hasIndexOnTags(tags...) {
		return nil, fmt.Errorf("cannot create a duplicate index on this table")
	}
	for _, c := range ixc.colColl.cols {
		if IsColSpatialType(c) {
			return nil, fmt.Errorf("cannot create an index over spatial type columns")
		}
	}
	index := &indexImpl{
		indexColl:     ixc,
		name:          indexName,
		tags:          tags,
		allTags:       combineAllTags(tags, ixc.pks),
		isUnique:      props.IsUnique,
		isUserDefined: props.IsUserDefined,
		comment:       props.Comment,
	}
	ixc.indexes[indexName] = index
	for _, tag := range tags {
		ixc.colTagToIndex[tag] = append(ixc.colTagToIndex[tag], index)
	}
	return index, nil
}

func (ixc *indexCollectionImpl) UnsafeAddIndexByColTags(indexName string, tags []uint64, props IndexProperties) (Index, error) {
	index := &indexImpl{
		indexColl:     ixc,
		name:          indexName,
		tags:          tags,
		allTags:       combineAllTags(tags, ixc.pks),
		isUnique:      props.IsUnique,
		isUserDefined: props.IsUserDefined,
		comment:       props.Comment,
	}
	ixc.indexes[indexName] = index
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
	_, ok := ixc.indexes[indexName]
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
	for _, index := range ixc.indexes {
		otherIndex := otherIxc.containsColumnTagCollection(index.tags...)
		if otherIndex == nil || !index.Equals(otherIndex) {
			return false
		}
	}
	return true
}

func (ixc *indexCollectionImpl) GetByName(indexName string) Index {
	ix, ok := ixc.indexes[indexName]
	if ok {
		return ix
	}
	return nil
}

func (ixc *indexCollectionImpl) GetByNameCaseInsensitive(indexName string) (Index, bool) {
	for name, ix := range ixc.indexes {
		if strings.ToLower(name) == strings.ToLower(indexName) {
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
				isUserDefined: index.IsUserDefined(),
				comment:       index.Comment(),
			}
			ixc.AddIndex(newIndex)
		}
	}
}

func (ixc *indexCollectionImpl) RemoveIndex(indexName string) (Index, error) {
	if !ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` does not exist as an index for this table", indexName)
	}
	index := ixc.indexes[indexName]
	delete(ixc.indexes, indexName)
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
	if !ixc.Contains(oldName) {
		return nil, fmt.Errorf("`%s` does not exist as an index for this table", oldName)
	}
	if ixc.Contains(newName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", newName)
	}
	index := ixc.indexes[oldName]
	delete(ixc.indexes, oldName)
	index.name = newName
	ixc.indexes[newName] = index
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
	delete(ixc.indexes, index.name)
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
