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

	typeinfo "github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

var firstNameCol = Column{Name: "first", Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType}
var lastNameCol = Column{Name: "last", Tag: 1, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType}
var firstNameCapsCol = Column{Name: "FiRsT", Tag: 2, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType}
var lastNameCapsCol = Column{Name: "LAST", Tag: 3, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType}

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
		{Name: "0", Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "2", Tag: 2, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "4", Tag: 4, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "3", Tag: 3, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "1", Tag: 1, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
	}
	cols2 := []Column{
		{Name: "7", Tag: 7, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "9", Tag: 9, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "5", Tag: 5, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "8", Tag: 8, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
		{Name: "6", Tag: 6, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
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
