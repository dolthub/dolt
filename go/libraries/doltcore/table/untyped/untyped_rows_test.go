package untyped

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

func TestNewUntypedSchema(t *testing.T) {
	colNames := []string{"name", "city", "blurb"}
	nameToTag, sch := NewUntypedSchema(colNames...)

	if sch.GetAllCols().Size() != 3 {
		t.Error("Wrong column count")
	}

	i := 0
	sch.GetNonPKCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name != colNames[i] {
			t.Error("Unexpected name")
		}

		if col.Kind != types.StringKind {
			t.Error("Unexpected kind")
		}

		if col.Constraints != nil {
			t.Error("Nothing should be required")
		}

		if col.IsPartOfPK {
			t.Error("non pk cols should not be part of the pk")
		}

		i++
		return false
	})

	name := "Billy Bob"
	city := "Fargo"
	blurb := "Billy Bob is a scholar."
	r := NewRowFromStrings(sch, []string{name, city, blurb})

	nameVal, _ := r.GetColVal(nameToTag["name"])

	if nameVal.Kind() != types.StringKind || string(nameVal.(types.String)) != name {
		t.Error("Unexpected name")
	}

	cityVal, _ := r.GetColVal(nameToTag["city"])

	if cityVal.Kind() != types.StringKind || string(cityVal.(types.String)) != city {
		t.Error("Unexpected city")
	}

	blurbVal, _ := r.GetColVal(nameToTag["blurb"])

	if blurbVal.Kind() != types.StringKind || string(blurbVal.(types.String)) != blurb {
		t.Error("Unexpected blurb")
	}
}
