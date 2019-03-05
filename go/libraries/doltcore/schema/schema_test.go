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

func TestSchema(t *testing.T) {
	colColl, _ := NewColCollection(allCols...)
	schFromCols := SchemaFromCols(colColl)

	testSchema("SchemaFromCols", schFromCols, t)

	testKeyColColl, _ := NewColCollection(testKeyCols...)
	testNonKeyColsColl, _ := NewColCollection(testCols...)
	schFromPKAndNonPKCols, _ := SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColsColl)

	testSchema("SchemaFromPKAndNonPKCols", schFromPKAndNonPKCols, t)

	if !SchemasAreEqual(schFromCols, schFromPKAndNonPKCols) {
		t.Error("schemas should be equal")
	}
}

func testSchema(method string, sch Schema, t *testing.T) {
	validateCols(t, allCols, sch.GetAllCols(), method+"GetAllCols")
	validateCols(t, testKeyCols, sch.GetPKCols(), method+"GetPKCols")
	validateCols(t, testCols, sch.GetNonPKCols(), method+"GetNonPKCols")

	extracted := ExtractAllColNames(sch)
	expExt := map[uint64]string{
		lnColTag: lnColName, fnColTag: fnColName, ageColTag: ageColName, addrColTag: addrColName, titleColTag: titleColName, reservedColTag: reservedColName}

	if !reflect.DeepEqual(extracted, expExt) {
		t.Error("extracted columns did not match expectation")
	}

	if col, ok := ColFromName(sch, titleColName); !ok {
		t.Error("Failed to get by name")
	} else if col.Tag != titleColTag {
		t.Error("Unexpected tag")
	}

	if col, ok := ColFromTag(sch, titleColTag); !ok {
		t.Error("Failed to get by name")
	} else if col.Name != titleColName {
		t.Error("Unexpected tag")
	}
}

func validateCols(t *testing.T, cols []Column, colColl *ColCollection, msg string) {
	if !reflect.DeepEqual(cols, colColl.cols) {
		t.Error()
	}
}

/*
func TestAutoGenerateTag(t *testing.T) {
	colColl, _ := NewColCollection()
	sch := SchemaFromCols(colColl)

	var max uint64 = 128
	for i := uint64(0); i < 128*128; i++ {
		if i > 8192 {
			max = 2097152
		} else if i > 64 {
			max = 16384
		}

		tag := AutoGenerateTag(sch)

		if tag >= max {
			t.Fatal("auto generated tag out of range")
		} else {
			var err error
			colColl, err = colColl.Append(NewColumn(strconv.FormatUint(i, 10), tag, types.StringKind, false))

			if err != nil {
				t.Fatal(err)
			}

			sch = SchemaFromCols(colColl)
		}
	}
}*/
