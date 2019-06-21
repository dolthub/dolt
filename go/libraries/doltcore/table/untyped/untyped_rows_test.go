package untyped

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewUntypedSchema(t *testing.T) {
	colNames := []string{"name", "city", "blurb"}
	nameToTag, sch := NewUntypedSchema(colNames...)

	if sch.GetAllCols().Size() != 3 {
		t.Error("Wrong column count")
	}

	i := 0
	sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name != colNames[i] {
			t.Error("Unexpected name")
		}

		if col.Kind != types.StringKind {
			t.Error("Unexpected kind")
		}

		if col.Constraints != nil {
			t.Error("Nothing should be required")
		}

		if !col.IsPartOfPK {
			t.Error("pk cols should be part of the pk")
		}

		i++
		return false
	})
	assert.Equal(t, 1, i, "Exactly one PK column expected")

	sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name != colNames[i] {
			t.Error("Unexpected name")
		}

		if col.Kind != types.StringKind {
			t.Error("Unexpected kind")
		}

		if col.Constraints != nil {
			t.Error("Nothing should be required")
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

func TestUntypedSchemaUnion(t *testing.T) {
	cols := []schema.Column{
		schema.NewColumn("a", 0, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 1, types.IntKind, true),
		schema.NewColumn("c", 2, types.UintKind, true),
		schema.NewColumn("d", 3, types.StringKind, false),
		schema.NewColumn("e", 4, types.BoolKind, false),
	}

	untypedColColl, _ := schema.NewColCollection(
		schema.NewColumn("a", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("b", 1, types.StringKind, true),
		schema.NewColumn("c", 2, types.StringKind, true),
		schema.NewColumn("d", 3, types.StringKind, false),
		schema.NewColumn("e", 4, types.StringKind, false))

	unequalColCollumn := cols[1]
	unequalColCollumn.Name = "bad"

	untypedSch := schema.SchemaFromCols(untypedColColl)

	tests := []struct {
		colsA     []schema.Column
		colsB     []schema.Column
		expectErr bool
	}{
		{cols[:2], cols[2:], false},
		{cols[:2], cols[1:], false},
		{cols[:2], []schema.Column{unequalColCollumn}, true},
	}

	for i, test := range tests {
		colCollA, _ := schema.NewColCollection(test.colsA...)
		colCollB, _ := schema.NewColCollection(test.colsB...)
		schA := schema.SchemaFromCols(colCollA)
		schB := schema.SchemaFromCols(colCollB)

		union, err := UntypedSchemaUnion(schA, schB)

		if (err != nil) != test.expectErr {
			t.Error(i, "expected err:", test.expectErr, "received err:", err != nil)
		} else if err == nil && !schema.SchemasAreEqual(union, untypedSch) {
			actualJson, _ := encoding.MarshalAsJson(untypedSch)
			expectedJson, _ := encoding.MarshalAsJson(union)
			t.Error(i, "\nexpected:\n", expectedJson, "\nactual:\n", actualJson)
		}
	}
}
