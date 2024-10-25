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
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/types"
)

type Index interface {
	// AllTags returns the tags of the columns in the entire index, including the primary keys.
	// If we imagined a dolt index as being a standard dolt table, then the tags would represent the schema columns.
	AllTags() []uint64
	// ColumnNames returns the names of the columns in the index.
	ColumnNames() []string
	// Comment returns the comment that was provided upon index creation.
	Comment() string
	// Count returns the number of indexed columns in this index.
	Count() int
	// DeepEquals returns whether this Index is equivalent to another. This function is similar to Equals, however it
	// does take the table's primary keys into consideration.
	DeepEquals(other Index) bool
	// Equals returns whether this Index is equivalent to another. This does not check for column names, thus those may
	// be renamed and the index equivalence will be preserved. It also does not depend on the table's primary keys.
	Equals(other Index) bool
	// GetColumn returns the column for the given tag and whether the column was found or not.
	GetColumn(tag uint64) (Column, bool)
	// IndexedColumnTags returns the tags of the columns in the index.
	IndexedColumnTags() []uint64
	// IsUnique returns whether the given index has the UNIQUE constraint.
	IsUnique() bool
	// IsSpatial returns whether the given index has the SPATIAL constraint.
	IsSpatial() bool
	// IsFullText returns whether the given index has the FULLTEXT constraint.
	IsFullText() bool
	// IsVector returns whether the given index has the VECTOR constraint.
	IsVector() bool
	// IsUserDefined returns whether the given index was created by a user or automatically generated.
	IsUserDefined() bool
	// Name returns the name of the index.
	Name() string
	// PrimaryKeyTags returns the primary keys of the indexed table, in the order that they're stored for that table.
	PrimaryKeyTags() []uint64
	// Schema returns the schema for the internal index map. Can be used for table operations.
	Schema() Schema
	// ToTableTuple returns a tuple that may be used to retrieve the original row from the indexed table when given
	// a full index key (and not a partial index key).
	ToTableTuple(ctx context.Context, fullKey types.Tuple, format *types.NomsBinFormat) (types.Tuple, error)
	// PrefixLengths returns the prefix lengths for the index
	PrefixLengths() []uint16
	// FullTextProperties returns all properties belonging to a Full-Text index.
	FullTextProperties() FullTextProperties
	// VectorProperties returns all properties belonging to a vector index.
	VectorProperties() VectorProperties
}

var _ Index = (*indexImpl)(nil)

type indexImpl struct {
	name             string
	tags             []uint64
	allTags          []uint64
	indexColl        *indexCollectionImpl
	isUnique         bool
	isSpatial        bool
	isFullText       bool
	isVector         bool
	isUserDefined    bool
	comment          string
	prefixLengths    []uint16
	fullTextProps    FullTextProperties
	vectorProperties VectorProperties
}

func NewIndex(name string, tags, allTags []uint64, indexColl IndexCollection, props IndexProperties) Index {
	var indexCollImpl *indexCollectionImpl
	if indexColl != nil {
		indexCollImpl = indexColl.(*indexCollectionImpl)
	}

	return &indexImpl{
		name:             name,
		tags:             tags,
		allTags:          allTags,
		indexColl:        indexCollImpl,
		isUnique:         props.IsUnique,
		isSpatial:        props.IsSpatial,
		isFullText:       props.IsFullText,
		isVector:         props.IsVector,
		isUserDefined:    props.IsUserDefined,
		comment:          props.Comment,
		fullTextProps:    props.FullTextProperties,
		vectorProperties: props.VectorProperties,
	}
}

// AllTags implements Index.
func (ix *indexImpl) AllTags() []uint64 {
	return ix.allTags
}

// ColumnNames implements Index.
func (ix *indexImpl) ColumnNames() []string {
	colNames := make([]string, len(ix.tags))
	for i, tag := range ix.tags {
		colNames[i] = ix.indexColl.colColl.TagToCol[tag].Name
	}
	return colNames
}

// Comment implements Index.
func (ix *indexImpl) Comment() string {
	return ix.comment
}

// Count implements Index.
func (ix *indexImpl) Count() int {
	return len(ix.tags)
}

// Equals implements Index.
func (ix *indexImpl) Equals(other Index) bool {
	if ix.Count() != other.Count() {
		return false
	}

	// we're only interested in columns the index is defined over, not the table's primary keys
	tt := ix.IndexedColumnTags()
	ot := other.IndexedColumnTags()
	for i := range tt {
		if tt[i] != ot[i] {
			return false
		}
	}

	return ix.IsUnique() == other.IsUnique() &&
		ix.IsSpatial() == other.IsSpatial() &&
		compareUint16Slices(ix.PrefixLengths(), other.PrefixLengths()) &&
		ix.Comment() == other.Comment() &&
		ix.Name() == other.Name()
}

// DeepEquals implements Index.
func (ix *indexImpl) DeepEquals(other Index) bool {
	if ix.Count() != other.Count() {
		return false
	}

	// DeepEquals compares all tags used in this index, as well as the tags from the table's primary key
	tt := ix.AllTags()
	ot := other.AllTags()
	for i := range tt {
		if tt[i] != ot[i] {
			return false
		}
	}

	return ix.IsUnique() == other.IsUnique() &&
		ix.IsSpatial() == other.IsSpatial() &&
		compareUint16Slices(ix.PrefixLengths(), other.PrefixLengths()) &&
		ix.Comment() == other.Comment() &&
		ix.Name() == other.Name()
}

// compareUint16Slices returns true if |a| and |b| contain the exact same uint16 values, in the same order; otherwise
// it returns false.
func compareUint16Slices(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// GetColumn implements Index.
func (ix *indexImpl) GetColumn(tag uint64) (Column, bool) {
	return ix.indexColl.colColl.GetByTag(tag)
}

// IndexedColumnTags implements Index.
func (ix *indexImpl) IndexedColumnTags() []uint64 {
	return ix.tags
}

// IsUnique implements Index.
func (ix *indexImpl) IsUnique() bool {
	return ix.isUnique
}

// IsSpatial implements Index.
func (ix *indexImpl) IsSpatial() bool {
	return ix.isSpatial
}

// IsFullText implements Index.
func (ix *indexImpl) IsFullText() bool {
	return ix.isFullText
}

// IsVector implements Index.
func (ix *indexImpl) IsVector() bool {
	return ix.isVector
}

// IsUserDefined implements Index.
func (ix *indexImpl) IsUserDefined() bool {
	return ix.isUserDefined
}

// Name implements Index.
func (ix *indexImpl) Name() string {
	return ix.name
}

// PrimaryKeyTags implements Index.
func (ix *indexImpl) PrimaryKeyTags() []uint64 {
	return ix.indexColl.pks
}

// Schema implements Index.
func (ix *indexImpl) Schema() Schema {
	contentHashedFields := make([]uint64, 0)
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

		// contentHashedFields is the collection of column tags for columns in a unique index that do
		// not have a prefix length specified and should be stored as a content hash. This information
		// is needed to later identify that an index is using content-hashed encoding.
		prefixLength := uint16(0)
		if len(ix.PrefixLengths()) > i {
			prefixLength = ix.PrefixLengths()[i]
		}
		if ix.IsUnique() && prefixLength == 0 {
			contentHashedFields = append(contentHashedFields, tag)
		}
	}
	allCols := NewColCollection(cols...)
	nonPkCols := NewColCollection()
	return &schemaImpl{
		pkCols:              allCols,
		nonPKCols:           nonPkCols,
		allCols:             allCols,
		indexCollection:     NewIndexCollection(nil, nil),
		checkCollection:     NewCheckCollection(),
		contentHashedFields: contentHashedFields,
	}
}

// ToTableTuple implements Index.
func (ix *indexImpl) ToTableTuple(ctx context.Context, fullKey types.Tuple, format *types.NomsBinFormat) (types.Tuple, error) {
	pkTags := make(map[uint64]int)
	for i, tag := range ix.indexColl.pks {
		pkTags[tag] = i
	}
	tplItr, err := fullKey.Iterator()
	if err != nil {
		return types.Tuple{}, err
	}
	resVals := make([]types.Value, len(pkTags)*2)
	for {
		_, tag, err := tplItr.NextUint64()
		if err != nil {
			if err == io.EOF {
				break
			}
			return types.Tuple{}, err
		}
		idx, inPK := pkTags[tag]
		if inPK {
			_, valVal, err := tplItr.Next()
			if err != nil {
				return types.Tuple{}, err
			}
			resVals[idx*2] = types.Uint(tag)
			resVals[idx*2+1] = valVal
		} else {
			err := tplItr.Skip()
			if err != nil {
				return types.Tuple{}, err
			}
		}
	}
	return types.NewTuple(format, resVals...)
}

// PrefixLengths implements Index.
func (ix *indexImpl) PrefixLengths() []uint16 {
	return ix.prefixLengths
}

// FullTextProperties implements Index.
func (ix *indexImpl) FullTextProperties() FullTextProperties {
	return ix.fullTextProps
}

// VectorProperties implements Index.
func (ix *indexImpl) VectorProperties() VectorProperties {
	return ix.vectorProperties
}

// copy returns an exact copy of the calling index.
func (ix *indexImpl) copy() *indexImpl {
	newIx := *ix
	newIx.tags = make([]uint64, len(ix.tags))
	_ = copy(newIx.tags, ix.tags)
	newIx.allTags = make([]uint64, len(ix.allTags))
	_ = copy(newIx.allTags, ix.allTags)
	if len(ix.prefixLengths) > 0 {
		newIx.prefixLengths = make([]uint16, len(ix.prefixLengths))
		_ = copy(newIx.prefixLengths, ix.prefixLengths)
	}
	if len(newIx.fullTextProps.KeyPositions) > 0 {
		newIx.fullTextProps.KeyPositions = make([]uint16, len(ix.fullTextProps.KeyPositions))
		_ = copy(newIx.fullTextProps.KeyPositions, ix.fullTextProps.KeyPositions)
	}
	return &newIx
}
