package schema

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"strconv"
	"testing"
)

var firstNameCol = Column{"first", 0, types.StringKind, false, nil}
var lastNameCol = Column{"last", 1, types.StringKind, false, nil}
var firstNameCapsCol = Column{"FiRsT", 2, types.StringKind, false, nil}
var lastNameCapsCol = Column{"LAST", 3, types.StringKind, false, nil}

func TestGetByNameAndTag(t *testing.T) {
	cols := []Column{firstNameCol, lastNameCol, firstNameCapsCol, lastNameCapsCol}
	colColl, err := NewColCollection(cols...)
	require.NoError(t, err)

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
	colColl, err := NewColCollection(cols...)
	require.NoError(t, err)

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

func TestNewColCollectionErrorHandling(t *testing.T) {
	tests := []struct {
		name       string
		cols       []Column
		expectedErr error
	}{
		{
			name:        "tag collision",
			cols:        []Column{firstNameCol, lastNameCol, {"collision", 0, types.StringKind, false, nil}},
			expectedErr: ErrColTagCollision,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewColCollection(test.cols...)
			assert.Error(t, err)
			assert.Equal(t, err, test.expectedErr)
		})
	}
}

func TestAppendAndItrInSortOrder(t *testing.T) {
	cols := []Column{
		{"0", 0, types.StringKind, false, nil},
		{"2", 2, types.StringKind, false, nil},
		{"4", 4, types.StringKind, false, nil},
		{"3", 3, types.StringKind, false, nil},
		{"1", 1, types.StringKind, false, nil},
	}
	cols2 := []Column{
		{"7", 7, types.StringKind, false, nil},
		{"9", 9, types.StringKind, false, nil},
		{"5", 5, types.StringKind, false, nil},
		{"8", 8, types.StringKind, false, nil},
		{"6", 6, types.StringKind, false, nil},
	}

	colColl, _ := NewColCollection(cols...)
	validateIter(len(cols), colColl, t)
	colColl2, _ := colColl.Append(cols2...)
	validateIter(len(cols), colColl, t) //validate immutability
	validateIter(len(cols)+len(cols2), colColl2, t)
}

func validateIter(numCols int, colColl *ColCollection, t *testing.T) {
	if numCols != colColl.Size() {
		t.Error("missing data")
	}

	colColl.Iter(func(tag uint64, col Column) (stop bool) {
		if col.Name != strconv.FormatUint(tag, 10) || col.Tag != tag {
			t.Errorf("tag:%d - %v", tag, col)
		}

		return false
	})
}
