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

package schema

import (
	"fmt"
	"sort"
)

type IndexCollection interface {
	// AddIndex adds the given index, overwriting any current indexes with the same name or columns.
	// It does not perform any kind of checking, and is intended for schema modifications.
	AddIndex(indexes ...Index)
	// AddIndexByColNames adds an index with the given name and columns (in index order).
	AddIndexByColNames(indexName string, cols []string, isUnique bool, comment string) (Index, error)
	// AddIndexByColTags adds an index with the given name and column tags (in index order).
	AddIndexByColTags(indexName string, tags []uint64, isUnique bool, comment string) (Index, error)
	// AllIndexes returns a slice containing all of the indexes in this collection.
	AllIndexes() []Index
	// Contains returns whether the given index name already exists for this table.
	Contains(indexName string) bool
	// Count returns the number of indexes in this collection.
	Count() int
	// Get returns the index with the given name, or nil if it does not exist.
	Get(indexName string) Index
	// HasIndexes returns whether this collection has any indexes.
	HasIndexes() bool
	// HasIndexOnColumns returns whether the collection contains an index that has this exact collection and ordering of columns.
	HasIndexOnColumns(cols ...string) bool
	// HasIndexOnTags returns whether the collection contains an index that has this exact collection and ordering of columns.
	HasIndexOnTags(tags ...uint64) bool
	// IndexesWithColumn returns all indexes that index the given column.
	IndexesWithColumn(columnName string) []Index
	// IndexesWithTag returns all indexes that index the given tag.
	IndexesWithTag(tag uint64) []Index
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

type indexCollectionImpl struct {
	colColl       *ColCollection
	indexes       map[string]*indexImpl
	colTagToIndex map[uint64][]*indexImpl
	pks           []uint64
}

func NewIndexCollection(cols *ColCollection) IndexCollection {
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

func (ixc *indexCollectionImpl) AddIndexByColNames(indexName string, cols []string, isUnique bool, comment string) (Index, error) {
	tags, ok := ixc.columnNamesToTags(cols)
	if !ok {
		return nil, fmt.Errorf("the table does not contain at least one of the following columns: `%v`", cols)
	}
	return ixc.AddIndexByColTags(indexName, tags, isUnique, comment)
}

func (ixc *indexCollectionImpl) AddIndexByColTags(indexName string, tags []uint64, isUnique bool, comment string) (Index, error) {
	if ixc.Contains(indexName) {
		return nil, fmt.Errorf("`%s` already exists as an index for this table", indexName)
	}
	if !ixc.tagsExist(tags...) {
		return nil, fmt.Errorf("tags %v do not exist on this table", tags)
	}
	if ixc.HasIndexOnTags(tags...) {
		return nil, fmt.Errorf("cannot create a duplicate index on this table")
	}
	index := &indexImpl{
		indexColl: ixc,
		name:      indexName,
		tags:      tags,
		allTags:   combineAllTags(tags, ixc.pks),
		isUnique:  isUnique,
		comment:   comment,
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

func (ixc *indexCollectionImpl) Get(indexName string) Index {
	ix, ok := ixc.indexes[indexName]
	if ok {
		return ix
	}
	return nil
}

func (ixc *indexCollectionImpl) HasIndexes() bool {
	if len(ixc.indexes) > 0 {
		return true
	}
	return false
}

func (ixc *indexCollectionImpl) HasIndexOnColumns(cols ...string) bool {
	tags := make([]uint64, len(cols))
	for i, col := range cols {
		col, ok := ixc.colColl.NameToCol[col]
		if !ok {
			return false
		}
		tags[i] = col.Tag
	}
	return ixc.HasIndexOnTags(tags...)
}

func (ixc *indexCollectionImpl) HasIndexOnTags(tags ...uint64) bool {
	idx := ixc.containsColumnTagCollection(tags...)
	if idx == nil {
		return false
	}
	return true
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

func (ixc *indexCollectionImpl) Merge(indexes ...Index) {
	for _, index := range indexes {
		if tags, ok := ixc.columnNamesToTags(index.ColumnNames()); ok && !ixc.Contains(index.Name()) {
			newIndex := &indexImpl{
				name:      index.Name(),
				tags:      tags,
				indexColl: ixc,
				isUnique:  index.IsUnique(),
				comment:   index.Comment(),
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
