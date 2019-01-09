package untyped

import (
	"github.com/attic-labs/noms/go/types"
	"testing"
)

func TestNewUntypedSchema(t *testing.T) {
	fieldNames := []string{"name", "city", "blurb"}
	sch := NewUntypedSchema(fieldNames)

	if sch.NumFields() != 3 {
		t.Error("Wrong field count")
	}

	for i := 0; i < sch.NumFields(); i++ {
		fld := sch.GetField(i)

		if fld.NameStr() != fieldNames[i] {
			t.Error("Unexpected name")
		}

		if fld.NomsKind() != types.StringKind {
			t.Error("Unexpected kind")
		}

		if fld.IsRequired() {
			t.Error("Nothing should be required")
		}
	}

	name := "Billy Bob"
	city := "Fargo"
	blurb := "Billy Bob is a scholar."
	r := NewRowFromStrings(sch, []string{name, city, blurb})

	nameVal, nameFld := r.CurrData().GetFieldByName("name")

	if nameFld.NomsKind() != types.StringKind || string(nameVal.(types.String)) != name {
		t.Error("Unexpected name")
	}

	cityVal, cityFld := r.CurrData().GetFieldByName("city")

	if cityFld.NomsKind() != types.StringKind || string(cityVal.(types.String)) != city {
		t.Error("Unexpected city")
	}

	blurbVal, blurbFld := r.CurrData().GetFieldByName("blurb")

	if blurbFld.NomsKind() != types.StringKind || string(blurbVal.(types.String)) != blurb {
		t.Error("Unexpected blurb")
	}
}
