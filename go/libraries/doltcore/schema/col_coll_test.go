// Copyright 2019 Dolthub, Inc.
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
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

var firstNameCol = Column{"first", 0, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil}
var lastNameCol = Column{"last", 1, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil}
var firstNameCapsCol = Column{"FiRsT", 2, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil}
var lastNameCapsCol = Column{"LAST", 3, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil}

func TestGetByNameAndTag(t *testing.T) {
	cols := []Column{firstNameCol, lastNameCol, firstNameCapsCol, lastNameCapsCol}
	colColl := NewColCollection(cols...)

	tests := []struct {
		name       string
		tag        uint64
		expected   Column
		shouldBeOk bool
	}{
		{firstNameCol.Name, firstNameCol.Tag, firstNameCol, true},
		{lastNameCol.Name, lastNameCol.Tag, lastNameCol, true},
		{firstNameCapsCol.Name, firstNameCapsCol.Tag, firstNameCapsCol, true},
		{lastNameCapsCol.Name, lastNameCapsCol.Tag, lastNameCapsCol, true},
		{"FIRST", InvalidTag, InvalidCol, false},
		{"missing", InvalidTag, InvalidCol, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, ok := colColl.GetByName(test.name)

			if ok != test.shouldBeOk {
				t.Errorf("name - shouldBeOk: %v, ok: %v", test.shouldBeOk, ok)
			} else if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("name - %v != %v", actual, test.expected)
			}

			actual, ok = colColl.GetByTag(test.tag)

			if ok != test.shouldBeOk {
				t.Errorf("tag - shouldBeOk: %v, ok: %v", test.shouldBeOk, ok)
			} else if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("tag - %v != %v", actual, test.expected)
			}
		})
	}
}

func TestGetByNameCaseInsensitive(t *testing.T) {
	cols := []Column{firstNameCol, lastNameCol, firstNameCapsCol, lastNameCapsCol}
	colColl := NewColCollection(cols...)

	tests := []struct {
		name       string
		expected   Column
		shouldBeOk bool
	}{
		{firstNameCol.Name, firstNameCol, true},
		{lastNameCol.Name, lastNameCol, true},
		{firstNameCapsCol.Name, firstNameCol, true},
		{lastNameCapsCol.Name, lastNameCol, true},
		{"missing", InvalidCol, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			actual, ok := colColl.GetByNameCaseInsensitive(test.name)

			if ok != test.shouldBeOk {
				t.Errorf("name - shouldBeOk: %v, ok: %v", test.shouldBeOk, ok)
			} else if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("name - %v != %v", actual, test.expected)
			}

		})
	}
}

func TestAppendAndItrInSortOrder(t *testing.T) {
	cols := []Column{
		{"0", 0, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"2", 2, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"4", 4, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"3", 3, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"1", 1, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
	}
	cols2 := []Column{
		{"7", 7, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"9", 9, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"5", 5, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"8", 8, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
		{"6", 6, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
	}

	colColl := NewColCollection(cols...)
	validateIter(len(cols), colColl, t)
	colColl2 := colColl.Append(cols2...)
	validateIter(len(cols), colColl, t) //validate immutability
	validateIter(len(cols)+len(cols2), colColl2, t)
}

func validateIter(numCols int, colColl *ColCollection, t *testing.T) {
	if numCols != colColl.Size() {
		t.Error("missing data")
	}

	err := colColl.Iter(func(tag uint64, col Column) (stop bool, err error) {
		if col.Name != strconv.FormatUint(tag, 10) || col.Tag != tag {
			t.Errorf("tag:%d - %v", tag, col)
		}

		return false, nil
	})

	assert.NoError(t, err)
}
