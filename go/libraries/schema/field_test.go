package schema

import (
	"github.com/attic-labs/noms/go/types"
	"strings"
	"testing"
)

func TestField(t *testing.T) {
	const fName = "F1"
	const fKind = types.StringKind
	const fRequired = true
	f1 := NewField(fName, fKind, fRequired)

	if f1.NameStr() != strings.ToLower(fName) {
		t.Error("All field names should be lowercase")
	}

	if f1.NomsKind() != fKind {
		t.Error("Unexpected kind for field")
	}

	if f1.IsRequired() != fRequired {
		t.Error("Unexpected required flag value for field")
	}

	if !f1.Equals(f1) {
		t.Error("Field should definitely be equal to itself")
	}

	f2 := NewField(fName, fKind, !fRequired)
	if f1.Equals(f2) {
		t.Error("Should not be equal")
	}
}
