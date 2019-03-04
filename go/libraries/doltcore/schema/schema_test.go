package schema

import (
	"github.com/attic-labs/noms/go/types"
	"reflect"
	"testing"
)

const (
	lnColName       = "last"
	fnColName       = "first"
	addrColName     = "address"
	ageColName      = "age"
	titleColName    = "title"
	reservedColName = "reserved"
	lnColTag        = 1
	fnColTag        = 0
	addrColTag      = 6
	ageColTag       = 4
	titleColTag     = 40
	reservedColTag  = 50
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

var testKeyCols = []Column{
	{lnColName, lnColTag, types.StringKind, true, nil},
	{fnColName, fnColTag, types.StringKind, true, nil},
}
var testCols = []Column{
	{addrColName, addrColTag, types.StringKind, false, nil},
	{ageColName, ageColTag, types.UintKind, false, nil},
	{titleColName, titleColTag, types.StringKind, false, nil},
	{reservedColName, reservedColTag, types.StringKind, false, nil},
}

var allCols = append(append([]Column(nil), testKeyCols...), testCols...)

func TestSchemaFromCols(t *testing.T) {
	colColl, _ := NewColCollection(allCols...)
	sch := SchemaFromCols(colColl)

	validateCols(t, allCols, sch.GetAllCols(), "SchemaFromCols::GetAllCols")
	validateCols(t, testKeyCols, sch.GetPKCols(), "SchemaFromCols::GetPKCols")
	validateCols(t, testCols, sch.GetNonPKCols(), "SchemaFromCols::GetNonPKCols")
}

func TestSchemaFromPKAndNonPKCols(t *testing.T) {
	testKeyColColl, _ := NewColCollection(testKeyCols...)
	testNonKeyColsColl, _ := NewColCollection(testCols...)
	sch, _ := SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColsColl)

	validateCols(t, allCols, sch.GetAllCols(), "SchemaFromPKAndNonPKCols::GetAllCols")
	validateCols(t, testKeyCols, sch.GetPKCols(), "SchemaFromPKAndNonPKCols::GetPKCols")
	validateCols(t, testCols, sch.GetNonPKCols(), "SchemaFromPKAndNonPKCols::GetNonPKCols")
}

func validateCols(t *testing.T, cols []Column, colColl *ColCollection, msg string) {
	if !reflect.DeepEqual(cols, colColl.cols) {
		t.Error()
	}
}
