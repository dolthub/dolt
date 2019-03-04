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
}
