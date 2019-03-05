package row

import (
	"github.com/attic-labs/noms/go/types"
	"testing"
)

func TestGetFieldByName(t *testing.T) {
	r := newTestRow()

	val, ok := GetFieldByName(lnColName, r, sch)

	if !ok {
		t.Error("Expected to find value")
	} else if !val.Equals(lnVal) {
		t.Error("Unexpected value")
	}

	val, ok = GetFieldByName(reservedColName, r, sch)

	if ok {
		t.Error("should not find missing key")
	} else if val != nil {
		t.Error("missing key should return null value")
	}
}

func TestGetFieldByNameWithDefault(t *testing.T) {
	r := newTestRow()
	defVal := types.String("default")

	val := GetFieldByNameWithDefault(lnColName, defVal, r, sch)

	if !val.Equals(lnVal) {
		t.Error("expected:", lnVal, "actual", val)
	}

	val = GetFieldByNameWithDefault(reservedColName, defVal, r, sch)

	if !val.Equals(defVal) {
		t.Error("expected:", defVal, "actual", val)
	}
}

func TestIsValid(t *testing.T) {
	r := newTestRow()

	if !IsValid(r, sch) {
		t.Error("Not valid")
	}

	invalidRow, _ := r.SetColVal(lnColTag, nil, sch)

	if IsValid(invalidRow, sch) {
		t.Error("This row should not be valid")
	}

	col := GetInvalidCol(invalidRow, sch)

	if col.Tag != lnColTag {
		t.Error("Unexpected column returned by GetInvalidCol")
	}

	if !AreEqual(r, r, sch) {
		t.Error("Row should definitely be equal to itself")
	}

	if AreEqual(r, invalidRow, sch) {
		t.Error("Row should not be equal to invalidRow")
	}
}
