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

type Index interface {
	// AllTags returns the tags of the columns in the entire index, including the primary keys.
	// The result is equivalent to a schema check on the associated dolt index table.
	AllTags() []uint64
	// Columns returns the names of the columns in the index.
	Columns() []string
	// Comment returns the comment that was provided upon index creation.
	Comment() string
	// GetColumn returns the column for the given tag and whether the column was found or not.
	GetColumn(tag uint64) (Column, bool)
	// InnerIndexes returns all inner indexes
	InnerIndexes() []InnerIndex
	// IsUnique returns whether the index enforces the UNIQUE constraint.
	IsUnique() bool
	// Name returns the name of the index.
	Name() string
	// PrimaryKeys returns the primary keys of the indexed table, in the order that they're stored for that table.
	PrimaryKeys() []uint64
	// Schema returns the schema for the internal index map. Can be used for table operations.
	Schema() Schema
	// Tags returns the tags of the columns in the index.
	Tags() []uint64
}

var _ Index = (*indexImpl)(nil)

type indexImpl struct {
	name      string
	tags      []uint64
	allTags   []uint64
	indexColl *indexCollectionImpl
	isUnique  bool
	comment   string
}

func (ix *indexImpl) AllTags() []uint64 {
	return ix.allTags
}

func (ix *indexImpl) Columns() []string {
	colNames := make([]string, len(ix.tags))
	for i, tag := range ix.tags {
		colNames[i] = ix.indexColl.colColl.TagToCol[tag].Name
	}
	return colNames
}

func (ix *indexImpl) Comment() string {
	return ix.comment
}

func (ix *indexImpl) GetColumn(tag uint64) (Column, bool) {
	return ix.indexColl.colColl.GetByTag(tag)
}

func (ix *indexImpl) InnerIndexes() []InnerIndex {
	tagLength := len(ix.tags)
	innerIndexes := make([]InnerIndex, tagLength)
	for i := 1; i <= tagLength; i++ {
		innerIndexes[i-1] = &innerIndexImpl{
			index:     ix,
			tagLength: i,
		}
	}
	return innerIndexes
}

func (ix *indexImpl) IsUnique() bool {
	return ix.isUnique
}

func (ix *indexImpl) Name() string {
	return ix.name
}

func (ix *indexImpl) PrimaryKeys() []uint64 {
	return ix.indexColl.pks
}

func (ix *indexImpl) Schema() Schema {
	cols := make([]Column, len(ix.allTags))
	for i, tag := range ix.allTags {
		col := ix.indexColl.colColl.TagToCol[tag]
		cols[i] = Column{
			Name:        col.Name,
			Tag:         tag,
			Kind:        col.Kind,
			IsPartOfPK:  true,
			TypeInfo:    col.TypeInfo,
			Constraints: nil,
		}
	}
	allCols, _ := NewColCollection(cols...)
	nonPkCols, _ := NewColCollection()
	return &schemaImpl{
		pkCols:          allCols,
		nonPKCols:       nonPkCols,
		allCols:         allCols,
		indexCollection: NewIndexCollection(nil),
	}
}

func (ix *indexImpl) Tags() []uint64 {
	return ix.tags
}

func (ix *indexImpl) copy() *indexImpl {
	tags := make([]uint64, len(ix.tags))
	_ = copy(tags, ix.tags)
	allTags := make([]uint64, len(ix.allTags))
	_ = copy(allTags, ix.allTags)
	return &indexImpl{
		name:      ix.name,
		tags:      tags,
		allTags:   allTags,
		indexColl: ix.indexColl,
		isUnique:  ix.isUnique,
		comment:   ix.comment,
	}
}
