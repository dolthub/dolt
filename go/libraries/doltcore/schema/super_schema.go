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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// SuperSchema is the union of all Schemas over the history of a table
// the tagNames map tracks all names corresponding to a column tag
type SuperSchema struct {
	// All columns that have existed in the history of the corresponding schema.
	// Names of the columns are not stored in this collection as they can change
	// over time.
	// Constraints are not tracked in this collection or anywhere in SuperSchema
	allCols *ColCollection

	// All names in each column's history, keyed by tag.
	// The columns latest name is index 0
	tagNames map[uint64][]string
}

// NewSuperSchema creates a SuperSchema from the columns of schemas.
func NewSuperSchema(schemas ...Schema) (*SuperSchema, error) {
	cc := NewColCollection()
	tn := make(map[uint64][]string)
	ss := SuperSchema{cc, tn}

	for _, sch := range schemas {
		err := ss.AddSchemas(sch)
		if err != nil {
			return nil, err
		}
	}

	return &ss, nil
}

// UnmarshalSuperSchema creates a SuperSchema, it is only used by the encoding package.
func UnmarshalSuperSchema(allCols *ColCollection, tagNames map[uint64][]string) *SuperSchema {
	return &SuperSchema{allCols, tagNames}
}

// AddColumn adds a column and its name to the SuperSchema
func (ss *SuperSchema) AddColumn(col Column) (err error) {
	existingCol, found := ss.allCols.GetByTag(col.Tag)
	if found {
		// TODO: We need to rethink the nature of column compatibility with primary key changes being allowed.
		// We can't necessary say compatibility == diffability as primary key set changes cannot be diffed
		if existingCol.Kind != col.Kind {
			ecName := ss.tagNames[col.Tag][0]
			return fmt.Errorf("tag collision for columns %s and %s, different definitions (tag: %d)",
				ecName, col.Name, col.Tag)
		}
	}

	names, found := ss.tagNames[col.Tag]
	if found {
		for _, nm := range names {
			if nm == col.Name {
				return nil
			}
		}
		// we haven't seen this name for this column before
		ss.tagNames[col.Tag] = append([]string{col.Name}, names...)
		return nil
	}

	// we haven't seen this column before
	ss.tagNames[col.Tag] = append(names, col.Name)
	ss.allCols = ss.allCols.Append(simpleColumn(col))

	return nil
}

// AddSchemas adds all names and columns of each schema to the SuperSchema
func (ss *SuperSchema) AddSchemas(schemas ...Schema) error {
	for _, sch := range schemas {
		err := sch.GetAllCols().Iter(func(_ uint64, col Column) (stop bool, err error) {
			err = ss.AddColumn(col)
			stop = err != nil
			return stop, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// GetByTag returns the corresponding column and true if found, returns InvalidCol and false otherwise
func (ss *SuperSchema) GetByTag(tag uint64) (Column, bool) {
	return ss.allCols.GetByTag(tag)
}

// Iter processes each column in the SuperSchema with the specified function
func (ss *SuperSchema) Iter(cb func(tag uint64, col Column) (stop bool, err error)) error {
	return ss.allCols.Iter(cb)
}

// AllColumnNames returns all names of the column corresponding to tag
func (ss *SuperSchema) AllColumnNames(tag uint64) []string {
	return ss.tagNames[tag]
}

// AllTags returns a slice of all tags contained in the SuperSchema
func (ss *SuperSchema) AllTags() []uint64 {
	return ss.allCols.Tags
}

// LatestColumnName returns the latest name of the column corresponding to tag
func (ss *SuperSchema) LatestColumnName(tag uint64) string {
	return ss.tagNames[tag][0]
}

// Size returns the number of columns in the SuperSchema
func (ss *SuperSchema) Size() int {
	return ss.allCols.Size()
}

// Equals returns true iff the SuperSchemas have the same ColCollections and tagNames maps
func (ss *SuperSchema) Equals(oss *SuperSchema) bool {
	// check equality of column collections
	if ss.Size() != oss.Size() {
		return false
	}

	ssEqual := true
	_ = ss.Iter(func(tag uint64, col Column) (stop bool, err error) {
		otherCol, found := oss.allCols.GetByTag(tag)

		if !found {
			ssEqual = false
		}

		if !col.Equals(otherCol) {
			ssEqual = false
		}

		return !ssEqual, nil
	})

	if !ssEqual {
		return false
	}

	// check equality of column name lists
	if len(ss.tagNames) != len(oss.tagNames) {
		return false
	}

	for colTag, colNames := range ss.tagNames {
		otherColNames, found := oss.tagNames[colTag]

		if !found {
			return false
		}

		if !set.NewStrSet(colNames).Equals(set.NewStrSet(otherColNames)) {
			return false
		}
	}
	return true
}

func (ss *SuperSchema) nameColumns() map[uint64]string {
	// create a unique name for each column
	collisions := make(map[string][]uint64)
	uniqNames := make(map[uint64]string)
	for tag, names := range ss.tagNames {
		n := names[0]
		uniqNames[tag] = n
		collisions[n] = append(collisions[n], tag)
	}
	for name, tags := range collisions {
		// if a name is used by more than one column, concat its tag
		if len(tags) > 1 {
			for _, t := range tags {
				uniqNames[t] = fmt.Sprintf("%s_%d", name, t)
			}
		}
	}
	return uniqNames
}

// GenerateColCollection creates a ColCollection from all the columns in the SuperSchema.
// Each column is assigned its latest name from its name history.
func (ss *SuperSchema) GenerateColCollection() (*ColCollection, error) {
	uniqNames := ss.nameColumns()
	cc := NewColCollection()
	err := ss.Iter(func(tag uint64, col Column) (stop bool, err error) {
		col.Name = uniqNames[tag]
		cc = cc.Append(col)
		stop = err != nil
		return stop, err
	})

	if err != nil {
		return nil, err
	}

	return cc, nil
}

// GenerateSchema creates a Schema from all the columns in the SuperSchema.
// Each column is assigned its latest name from its name history.
func (ss *SuperSchema) GenerateSchema() (Schema, error) {
	cc, err := ss.GenerateColCollection()
	if err != nil {
		return nil, err
	}
	return SchemaFromCols(cc)
}

// NameMapForSchema creates a field name mapping needed to construct a rowconv.RowConverter
// sch columns are mapped by tag to the corresponding SuperSchema columns
func (ss *SuperSchema) NameMapForSchema(sch Schema) (map[string]string, error) {
	inNameToOutName := make(map[string]string)
	uniqNames := ss.nameColumns()
	allCols := sch.GetAllCols()
	err := allCols.Iter(func(tag uint64, col Column) (stop bool, err error) {
		_, ok := uniqNames[tag]
		if !ok {
			return true, errors.New("failed to map columns")
		}
		inNameToOutName[col.Name] = uniqNames[tag]
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return inNameToOutName, nil
}

// SuperSchemaUnion combines multiple SuperSchemas.
func SuperSchemaUnion(superSchemas ...*SuperSchema) (*SuperSchema, error) {
	cc := NewColCollection()
	tagNameSets := make(map[uint64]*set.StrSet)
	latestNames := make(map[uint64]string)
	for _, ss := range superSchemas {
		err := ss.Iter(func(tag uint64, col Column) (stop bool, err error) {
			_, found := cc.GetByTag(tag)

			if !found {
				tagNameSets[tag] = set.NewStrSet(ss.AllColumnNames(tag))
				cc = cc.Append(simpleColumn(col))
			} else {
				tagNameSets[tag].Add(ss.AllColumnNames(tag)...)
			}
			latestNames[tag] = ss.AllColumnNames(tag)[0]

			stop = err != nil
			return stop, err
		})

		if err != nil {
			return nil, err
		}
	}

	tn := make(map[uint64][]string)
	for tag, nameSet := range tagNameSets {
		nn := []string{latestNames[tag]}
		nameSet.Remove(latestNames[tag])
		tn[tag] = append(nn, nameSet.AsSlice()...)
	}

	return &SuperSchema{cc, tn}, nil
}

// SuperSchema only retains basic info about the column def
func simpleColumn(col Column) Column {
	return Column{
		// column names are tracked in SuperSchema.tagNames
		Name:       "",
		Tag:        col.Tag,
		Kind:       col.Kind,
		IsPartOfPK: col.IsPartOfPK,
		TypeInfo:   col.TypeInfo,
	}
}
