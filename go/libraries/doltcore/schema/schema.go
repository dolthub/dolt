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

// Schema is an interface for retrieving the columns that make up a schema
type Schema interface {
	// GetPKCols gets the collection of columns which make the primary key.
	GetPKCols() *ColCollection

	// GetNonPKCols gets the collection of columns which are not part of the primary key.
	GetNonPKCols() *ColCollection

	// GetAllCols gets the collection of all columns (pk and non-pk)
	GetAllCols() *ColCollection

	// Indexes returns a collection of all indexes on the table that this schema belongs to.
	Indexes() IndexCollection
}

// ColFromTag returns a schema.Column from a schema and a tag
func ColFromTag(sch Schema, tag uint64) (Column, bool) {
	return sch.GetAllCols().GetByTag(tag)
}

// ColFromName returns a schema.Column from a schema from it's name
func ColFromName(sch Schema, name string) (Column, bool) {
	return sch.GetAllCols().GetByName(name)
}

// ExtractAllColNames returns a map of tag to column name, with one map entry for every column in the schema.
func ExtractAllColNames(sch Schema) (map[uint64]string, error) {
	colNames := make(map[uint64]string)
	err := sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		colNames[tag] = col.Name
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return colNames, nil
}

// TODO: this function never returns an error
// SchemasAreEqual tests equality of two schemas.
func SchemasAreEqual(sch1, sch2 Schema) (bool, error) {
	all1 := sch1.GetAllCols()
	all2 := sch2.GetAllCols()

	if all1.Size() != all2.Size() {
		return false, nil
	}

	areEqual := true
	err := all1.Iter(func(tag uint64, col1 Column) (stop bool, err error) {
		col2, ok := all2.GetByTag(tag)

		if !ok || !col1.Equals(col2) {
			areEqual = false
			return true, nil
		}

		return false, nil
	})

	if err != nil {
		return false, err
	}

	return areEqual, nil
}

// TODO: this function never returns an error
// VerifyInSchema tests that the incoming schema matches the schema from the original table
// based on the presence of the column name in the original schema.
func VerifyInSchema(inSch, outSch Schema) (bool, error) {
	inSchCols := inSch.GetAllCols()
	outSchCols := outSch.GetAllCols()

	if inSchCols.Size() != outSchCols.Size() {
		return false, nil
	}

	match := true
	err := inSchCols.Iter(func(tag uint64, inCol Column) (stop bool, err error) {
		_, isValid := outSchCols.GetByNameCaseInsensitive(inCol.Name)

		if !isValid {
			match = false
			return true, nil
		}

		return false, nil
	})

	if err != nil {
		return false, err
	}

	return match, nil
}
