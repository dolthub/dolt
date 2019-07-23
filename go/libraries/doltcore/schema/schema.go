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
	"math/rand"
	"time"
)

// Schema is an interface for retrieving the columns that make up a schema
type Schema interface {
	// GetPKCols gets the collection of columns which make the primary key.
	GetPKCols() *ColCollection

	// GetNonPKCols gets the collection of columns which are not part of the primary key.
	GetNonPKCols() *ColCollection

	// GetAllCols gets the collection of all columns (pk and non-pk)
	GetAllCols() *ColCollection
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
func ExtractAllColNames(sch Schema) map[uint64]string {
	colNames := make(map[uint64]string)
	sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool) {
		colNames[tag] = col.Name
		return false
	})

	return colNames
}

// SchemasArEqual tests equality of two schemas.
func SchemasAreEqual(sch1, sch2 Schema) bool {
	all1 := sch1.GetAllCols()
	all2 := sch2.GetAllCols()

	if all1.Size() != all2.Size() {
		return false
	}

	areEqual := true
	all1.Iter(func(tag uint64, col1 Column) (stop bool) {
		col2, ok := all2.GetByTag(tag)

		if !ok || !col1.Equals(col2) {
			areEqual = false
			return true
		}

		return false
	})

	return areEqual
}

var randGen = rand.New(rand.NewSource(time.Now().UnixNano()))

func AutoGenerateTag(sch Schema) uint64 {
	var maxTagVal uint64 = 128 * 128

	allCols := sch.GetAllCols()
	for maxTagVal/2 < uint64(allCols.Size()) {
		if maxTagVal == ReservedTagMin - 1 {
			panic("There is no way anyone should ever have this many columns.  You are a bad person if you hit this panic.")
		} else if maxTagVal*128 < maxTagVal {
			maxTagVal = ReservedTagMin - 1
		} else {
			maxTagVal = maxTagVal * 128
		}
	}

	var randTag uint64
	for {
		randTag = uint64(randGen.Int63n(int64(maxTagVal)))

		if _, ok := allCols.GetByTag(randTag); !ok {
			break
		}
	}

	return randTag
}
