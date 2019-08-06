// Copyright 2019 Liquidata, Inc.
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
	"errors"
	"sort"
	"strings"
)

// ErrColTagCollision is an error that is returned when two columns within a ColCollection have the same tag
// but a different name or type
var ErrColTagCollision = errors.New("two different columns with the same tag")

// ErrColNotFound is an error that is returned when attempting an operation on a column that does not exist
var ErrColNotFound = errors.New("column not found")

// ErrColNameCollision is an error that is returned when two columns within a ColCollection have the same name but a
// different type or tag
var ErrColNameCollision = errors.New("two different columns with the same name exist")

// ErrNoPrimaryKeyColumns is an error that is returned when wo
var ErrNoPrimaryKeyColumns = errors.New("no primary key columns")

var EmptyColColl = &ColCollection{
	[]Column{},
	[]uint64{},
	[]uint64{},
	map[uint64]Column{},
	map[string]Column{},
	map[string]Column{},
}

// ColCollection is a collection of columns. As a stand-alone collection, all columns in the collection must have unique
// tags. To be instantiated as a schema for writing to the database, names must also be unique.
// See schema.ValidateForInsert for details.
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
	// LowerNameToCol is a map from lower-cased name to column
	LowerNameToCol map[string]Column
}

// NewColCollection creates a new collection from a list of columns. All columns must have unique tags or this method
// returns an error. If any columns have the same name, by-name lookups from this collection will not function
// correctly, and this column collection cannot be used to create a schema. If any columns have the same case-
// insensitive name, case-insensitive lookups will be unable to return the correct column in all cases.
func NewColCollection(cols ...Column) (*ColCollection, error) {
	var tags []uint64
	var sortedTags []uint64

	tagToCol := make(map[uint64]Column, len(cols))
	nameToCol := make(map[string]Column, len(cols))
	lowerNameToCol := make(map[string]Column, len(cols))

	var uniqueCols []Column
	for i, col := range cols {
		if val, ok := tagToCol[col.Tag]; !ok {
			uniqueCols = append(uniqueCols, col)
			tagToCol[col.Tag] = col
			tags = append(tags, col.Tag)
			sortedTags = append(sortedTags, col.Tag)
			nameToCol[col.Name] = cols[i]

			// If multiple columns have the same lower case name, the first one is used for case-insensitive matching.
			lowerCaseName := strings.ToLower(col.Name)
			if _, ok := lowerNameToCol[lowerCaseName]; !ok {
				lowerNameToCol[lowerCaseName] = cols[i]
			}
		} else if !val.Equals(col) {
			return nil, ErrColTagCollision
		}
	}

	sort.Slice(sortedTags, func(i, j int) bool { return sortedTags[i] < sortedTags[j] })

	return &ColCollection{
		cols:           uniqueCols,
		Tags:           tags,
		SortedTags:     sortedTags,
		TagToCol:       tagToCol,
		NameToCol:      nameToCol,
		LowerNameToCol: lowerNameToCol,
	}, nil
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
func (cc *ColCollection) Iter(cb func(tag uint64, col Column) (stop bool, err error)) error {
	for _, col := range cc.cols {
		if stop, err := cb(col.Tag, col); err != nil {
			return err
		} else if stop {
			break
		}
	}

	return nil
}

// IterInSortOrder iterates over all the columns from lowest tag to highest tag.
func (cc *ColCollection) IterInSortedOrder(cb func(tag uint64, col Column) (stop bool)) {
	for _, tag := range cc.SortedTags {
		val := cc.TagToCol[tag]
		if stop := cb(tag, val); stop {
			break
		}
	}
}

// GetByName takes the name of a column and returns the column and true if found. Otherwise InvalidCol and false are
// returned.
func (cc *ColCollection) GetByName(name string) (Column, bool) {
	val, ok := cc.NameToCol[name]

	if ok {
		return val, true
	}

	return InvalidCol, false
}

// GetByNameCaseInensitive takes the name of a column and returns the column and true if there is a column with that
// name ignoring case. Otherwise InvalidCol and false are returned. If multiple columns have the same case-insensitive
// name, the first declared one is returned.
func (cc *ColCollection) GetByNameCaseInsensitive(name string) (Column, bool) {
	val, ok := cc.LowerNameToCol[strings.ToLower(name)]

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
