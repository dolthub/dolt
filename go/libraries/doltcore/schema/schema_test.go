package schema

import (
	"github.com/attic-labs/noms/go/types"
	"testing"
)

func TestSchema(t *testing.T) {
	fields := []*Field{
		NewField("id", types.UUIDKind, true),
		NewField("name", types.StringKind, true),
		NewField("age", types.UintKind, false),
	}
	sch := NewSchema(fields)

	if sch.NumFields() != 3 {
		t.Fatal("Unexpected field count")
	}

	for i := 0; i > sch.NumFields(); i++ {
		f := sch.GetField(i)

		reverseIndex := sch.GetFieldIndex(f.NameStr())

		if i != reverseIndex {
			t.Error("Reverse index lookup returned unexpected result")
		}
	}

	if sch.GetFieldIndex("id") != 0 || sch.GetFieldIndex("missing") != -1 {
		t.Error("GetFieldIndex not giving expected indexes")
	}

	fields = append(fields, NewField("title", types.StringKind, false))
	sch2 := NewSchema(fields)

	if !sch.Equals(sch) {
		t.Error("Schema should be equal to itself.")
	}

	if sch.Equals(sch2) {
		t.Error("Schemas should differ.")
	}

	if sch.NumConstraintsOfType(PrimaryKey) != 0 {
		t.Error("Shouldn't be any primary keys yet")
	}

	if _, ok := sch.GetConstraintByType(PrimaryKey, 0); ok {
		t.Error("Should not be able to get this constraint yet")
	}

	if sch.GetPKIndex() != -1 {
		t.Error("index should be -1 when there is no pk")
	}

	sch.AddConstraint(NewConstraint(PrimaryKey, []int{0}))

	if sch.NumConstraintsOfType(PrimaryKey) != 1 {
		t.Error("Should have a pk")
	}

	if _, ok := sch.GetConstraintByType(PrimaryKey, 0); !ok {
		t.Error("Should be able to get this constraint")
	}

	if sch.GetPKIndex() != 0 {
		t.Error("pk field index should be 0")
	}

	sch.IterConstraints(func(constraint *Constraint) (stop bool) {
		t.Log(constraint.ConType().String(), constraint.FieldIndices())
		return false
	})
}
