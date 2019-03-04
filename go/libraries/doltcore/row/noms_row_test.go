package row

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
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

var testKeyCols = []schema.Column{
	{lnColName, lnColTag, types.StringKind, true, []schema.ColConstraint{schema.NotNullConstraint{}}},
	{fnColName, fnColTag, types.StringKind, true, []schema.ColConstraint{schema.NotNullConstraint{}}},
}
var testCols = []schema.Column{
	{addrColName, addrColTag, types.StringKind, false, nil},
	{ageColName, ageColTag, types.UintKind, false, nil},
	{titleColName, titleColTag, types.StringKind, false, nil},
	{reservedColName, reservedColTag, types.StringKind, false, nil},
}
var testKeyColColl, _ = schema.NewColCollection(testKeyCols...)
var testNonKeyColColl, _ = schema.NewColCollection(testCols...)
var sch, _ = schema.SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColColl)

func newTestRow() nomsRow {
	key := TaggedValues{
		fnColTag: fnVal,
		lnColTag: lnVal,
	}

	val := TaggedValues{
		addrColTag:  addrVal,
		ageColTag:   ageVal,
		titleColTag: titleVal,
	}

	return nomsRow{key, val}
}

func TestItrRowCols(t *testing.T) {
	r := newTestRow()

	itrVals := make(TaggedValues)
	r.IterCols(func(tag uint64, val types.Value) (stop bool) {
		itrVals[tag] = val
		return false
	})

	matchesExpectation := reflect.DeepEqual(itrVals, TaggedValues{
		lnColTag:    lnVal,
		fnColTag:    fnVal,
		ageColTag:   ageVal,
		addrColTag:  addrVal,
		titleColTag: titleVal,
	})

	if !matchesExpectation {
		t.Error("Unexpected iteration results")
	}
}

func validateRow(t *testing.T, r Row, expected TaggedValues) {
	for expTag, expVal := range expected {
		val, ok := r.GetColVal(expTag)

		if !ok {
			t.Error("missing value")
		} else if val != nil && !val.Equals(expVal) {
			t.Error(types.EncodedValue(val), "!=", types.EncodedValue(expVal))
		}
	}

	val, ok := r.GetColVal(45667456)

	if ok {
		t.Error("Should not be ok")
	} else if val != nil {
		t.Error("missing value should be nil")
	}
}

func TestRowSet(t *testing.T) {
	updatedVal := types.String("sanchez")

	expected := map[uint64]types.Value{
		lnColTag:    lnVal,
		fnColTag:    fnVal,
		ageColTag:   ageVal,
		addrColTag:  addrVal,
		titleColTag: titleVal}

	r := newTestRow()

	validateRow(t, r, expected)

	updated, err := r.SetColVal(lnColTag, updatedVal, sch)

	if err != nil {
		t.Error("failed to update:", err)
	}

	// validate calling set does not mutate the original row
	validateRow(t, r, expected)

	expected[lnColTag] = updatedVal
	validateRow(t, updated, expected)
}
