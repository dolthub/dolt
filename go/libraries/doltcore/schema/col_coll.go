package schema

import (
	"errors"
	"sort"
)

// ErrColTagCollision is an error that is returned when two columns within a ColCollection have the same tag
// but a different name or type
var ErrColTagCollision = errors.New("two different columns with the same tag.")

// ErrColNotFound is an error that is returned when attempting an operation on a column that does not exist
var ErrColNotFound = errors.New("column not found")

// ErrColNameCollision is an error that is returned when two columns within a ColCollection have the same name
// but a different type or tag
var ErrColNameCollision = errors.New("two different columns with the same name exist")

var EmptyColColl = &ColCollection{
	[]Column{},
	[]uint64{},
	[]uint64{},
	map[uint64]Column{},
	map[string]Column{},
}

// ColCollection is a collection of columns.
type ColCollection struct {
	cols []Column
	// Tags is a list of all the tags in the ColCollection in their original order.
	Tags []uint64
	// SortedTags is a list of all the tags in the ColCollection in sorted order.
	SortedTags []uint64
	// TagToCol is a map of tag to column
	TagToCol map[uint64]Column
	// NameToCol is a map from name to column
	NameToCol map[string]Column
}

// NewColCollectionFromMap creates a column collection from a map.  The keys are ignored and the columns are extracted.
func NewColCollectionFromMap(colMap map[string]Column) (*ColCollection, error) {
	cols := make([]Column, len(colMap))

	i := 0
	for _, col := range colMap {
		cols[i] = col
		i++
	}

	return NewColCollection(cols...)
}

// NewColCollection creates a new collection from a list of columns
func NewColCollection(cols ...Column) (*ColCollection, error) {
	var tags []uint64
	var sortedTags []uint64

	tagToCol := make(map[uint64]Column, len(cols))
	nameToCol := make(map[string]Column, len(cols))

	var uniqueCols []Column
	for i, col := range cols {
		if val, ok := tagToCol[col.Tag]; !ok {
			uniqueCols = append(uniqueCols, col)
			tagToCol[col.Tag] = col
			tags = append(tags, col.Tag)
			sortedTags = append(sortedTags, col.Tag)
			nameToCol[col.Name] = cols[i]
		} else if !val.Equals(col) {
			return nil, ErrColTagCollision
		}
	}

	sort.Slice(sortedTags, func(i, j int) bool { return sortedTags[i] < sortedTags[j] })

	return &ColCollection{uniqueCols, tags, sortedTags, tagToCol, nameToCol}, nil
}

// GetColumns returns the underlying list of columns. The list returned is a copy.
func (cc *ColCollection) GetColumns() []Column {
	colsCopy := make([]Column, len(cc.cols))
	copy(colsCopy, cc.cols)
	return colsCopy
}

// AppendColl returns a new collection with the additional ColCollection's columns appended
func (cc *ColCollection) AppendColl(colColl *ColCollection) (*ColCollection, error) {
	return cc.Append(colColl.cols...)
}

// Append returns a new collection with the additional columns appended
func (cc *ColCollection) Append(cols ...Column) (*ColCollection, error) {
	allCols := make([]Column, 0, len(cols)+len(cc.cols))
	allCols = append(allCols, cc.cols...)
	allCols = append(allCols, cols...)

	return NewColCollection(allCols...)
}

// Iter iterates over all the columns in the supplied ordering
func (cc *ColCollection) Iter(cb func(tag uint64, col Column) (stop bool)) {
	for _, col := range cc.cols {
		stop := cb(col.Tag, col)

		if stop {
			break
		}
	}
}

// IterInSortOrder iterates over all the columns from lowest tag to highest tag.
func (cc *ColCollection) IterInSortedOrder(cb func(tag uint64, col Column) (stop bool)) {
	for _, tag := range cc.SortedTags {
		val := cc.TagToCol[tag]
		stop := cb(tag, val)

		if stop {
			break
		}
	}
}

// GetByName takes the name of a column and returns the column and true if found, otherwise InvalidCol and false are
// returned
func (cc *ColCollection) GetByName(name string) (Column, bool) {
	val, ok := cc.NameToCol[name]

	if ok {
		return val, true
	}

	return InvalidCol, false
}

// GetByTag takes a tag and returns the corresponding column and true if found, otherwise InvalidCol and false are
// returned
func (cc *ColCollection) GetByTag(tag uint64) (Column, bool) {
	val, ok := cc.TagToCol[tag]

	if ok {
		return val, true
	}

	return InvalidCol, false
}

// GetByIndex returns a column with a given index
func (cc *ColCollection) GetByIndex(idx int) Column {
	return cc.cols[idx]
}

// Size returns the number of columns in the collection.
func (cc *ColCollection) Size() int {
	return len(cc.cols)
}