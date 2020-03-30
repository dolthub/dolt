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
	"errors"
	"fmt"
	"sort"

	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
)

// TODO: track latest name for each column
const arbitraryIdx = 0

// SuperSchema is the union of all Schemas over the history of a table
// the tagNames map tracks all names corresponding to a column tag
type SuperSchema struct {
	// All columns that have existed in the history of the corresponding schema.
	// Names of the columns are not stored in this collection as they can change
	// over time.
	// Constraints are not tracked in this collection or anywhere in SuperSchema
	allCols *ColCollection

	// All names in each column's history, keyed by tag. No order is guaranteed
	tagNames map[uint64][]string
}

// NewSuperSchema creates a SuperSchema from the columns of schemas.
func NewSuperSchema(schemas ...Schema) (*SuperSchema, error) {
	cc, _ := NewColCollection()
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

// TODO: take a variadic param
// AddColumn adds a column and its name to the SuperSchema
func (ss *SuperSchema) AddColumn(col Column) (err error) {
	ct := col.Tag
	ac := ss.allCols
	existingCol, found := ac.GetByTag(ct)
	if found {
		if col.IsPartOfPK != existingCol.IsPartOfPK ||
			col.Kind != existingCol.Kind ||
			!col.TypeInfo.Equals(existingCol.TypeInfo) {
			ecName := ss.tagNames[col.Tag][arbitraryIdx]
			panic(fmt.Sprintf(
				"tag collision for columns %s and %s, different definitions (tag: %d)",
				ecName, col.Name, col.Tag))
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
		ss.tagNames[col.Tag] = append(names, col.Name)
		return nil
	}

	// we haven't seen this column before
	ss.tagNames[col.Tag] = append(names, col.Name)
	ss.allCols, err = ss.allCols.Append(stripColNameAndConstraints(col))

	return err
}

// TODO: make this functional
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

		if len(colNames) != len(otherColNames) {
			return false
		}

		sort.Strings(colNames)
		sort.Strings(otherColNames)
		for i := range colNames {
			if colNames[i] != otherColNames[i] {
				return false
			}
		}
	}
	return true
}

// IsSuperSetOfSchema returns true iff all cols in sch are in the SuperSchema
func (ss *SuperSchema) IsSuperSetOfSchema(sch Schema) bool {
	isSuperSet := true
	_ = sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		ssCol, found := ss.GetByTag(tag)
		if !found {
			isSuperSet = false
		}

		if !ssCol.TypeInfo.Equals(col.TypeInfo) {
			isSuperSet = false
		}

		if ssCol.IsPartOfPK != col.IsPartOfPK {
			isSuperSet = false
		}

		if ssCol.Kind != col.Kind {
			isSuperSet = false
		}

		hasName := false
		for _, nm := range ss.tagNames[tag] {
			if col.Name == nm {
				hasName = true
			}
		}

		if !hasName {
			isSuperSet = false
		}

		return !isSuperSet, nil
	})
	return isSuperSet
}

func (ss *SuperSchema) nameColumns() map[uint64]string {
	// create a unique name for each column
	collisions := make(map[string][]uint64)
	uniqNames := make(map[uint64]string)
	for tag, names := range ss.tagNames {
		n := names[arbitraryIdx]
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

// Creates a Schema by choosing an arbitrary name for each column in the SuperSchema
func (ss *SuperSchema) GenerateColCollection() (*ColCollection, error) {
	// TODO: track latest name for each column
	uniqNames := ss.nameColumns()
	cc, _ := NewColCollection()
	err := ss.Iter(func(tag uint64, col Column) (stop bool, err error) {
		col.Name = uniqNames[tag]
		cc, err = cc.Append(col)
		stop = err != nil
		return stop, err
	})

	if err != nil {
		return nil, err
	}

	return cc, nil
}

// Creates a Schema by choosing an arbitrary name for each column in the SuperSchema
func (ss *SuperSchema) GenerateSchema() (Schema, error) {
	cc, err := ss.GenerateColCollection()
	if err != nil {
		return nil, err
	}
	return SchemaFromCols(cc), nil
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

// RebaseTag changes the tag of a column from oldTag to newTag.
func (ss *SuperSchema) RebaseTag(tagMapping map[uint64]uint64) (*SuperSchema, error) {
	tn := make(map[uint64][]string)
	var cc []Column
	err := ss.allCols.Iter(func(tag uint64, col Column) (stop bool, err error) {
		if newTag, found := tagMapping[tag]; found {
			col.Tag = newTag
			tn[newTag] = ss.tagNames[tag]
		} else {
			tn[tag] = ss.tagNames[tag]
		}
		cc = append(cc, col)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	ac, err := NewColCollection(cc...)

	if err != nil {
		return nil, err
	}

	return &SuperSchema{ac, tn}, nil
}

// SuperSchemaUnion combines multiple SuperSchemas.
func SuperSchemaUnion(superSchemas ...*SuperSchema) (*SuperSchema, error) {
	cc, _ := NewColCollection()
	tagNameSets := make(map[uint64]*set.StrSet)
	for _, ss := range superSchemas {
		err := ss.Iter(func(tag uint64, col Column) (stop bool, err error) {
			_, found := cc.GetByTag(tag)

			if !found {
				tagNameSets[tag] = set.NewStrSet(ss.AllColumnNames(tag))
				cc, err = cc.Append(stripColNameAndConstraints(col))
			} else {
				tagNameSets[tag].Add(ss.AllColumnNames(tag)...)
			}

			stop = err != nil
			return stop, err
		})

		if err != nil {
			return nil, err
		}
	}

	tn := make(map[uint64][]string)
	for tag, nameSet := range tagNameSets {
		tn[tag] = nameSet.AsSlice()
	}

	return &SuperSchema{cc, tn}, nil
}

// SuperSchemaSubtract returns the logical set difference of left and right.
func SuperSchemaSubtract(left, right *SuperSchema) *SuperSchema {
	cc, _ := NewColCollection()
	tn := make(map[uint64][]string)
	_ = left.Iter(func(tag uint64, col Column) (stop bool, err error) {
		_, found := right.GetByTag(tag)
		if !found {
			cc, _ = cc.Append(col)
			tn[tag] = left.AllColumnNames(tag)
		}
		return false, nil
	})
	return &SuperSchema{cc, tn}
}

// SuperSchemaIntersection returns the logical set intersection of the columns of ss1 and ss2, along with
// the union of each column's name history.
func SuperSchemaIntersection(ss1, ss2 *SuperSchema) *SuperSchema {
	cc, _ := NewColCollection()
	tn := make(map[uint64][]string)
	_ = ss1.Iter(func(tag uint64, col Column) (stop bool, err error) {
		_, found := ss2.GetByTag(tag)
		if found {
			cc, _ = cc.Append(col)
			ss := set.NewStrSet(ss1.AllColumnNames(tag))
			ss.Add(ss2.AllColumnNames(tag)...)
			tn[tag] = ss.AsSlice()
		}
		return false, nil
	})
	return &SuperSchema{cc, tn}
}

func stripColNameAndConstraints(col Column) Column {
	// track column names in SuperSchema.tagNames
	col.Name = ""
	// don't track constraints
	col.Constraints = []ColConstraint(nil)
	return col
}
