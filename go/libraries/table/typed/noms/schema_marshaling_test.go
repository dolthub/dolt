package noms

import (
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"testing"
)

func createTestSchema() *schema.Schema {
	fields := []*schema.Field{
		schema.NewField("id", types.UUIDKind, true),
		schema.NewField("first", types.StringKind, true),
		schema.NewField("last", types.StringKind, true),
		schema.NewField("age", types.UintKind, false),
	}

	sch := schema.NewSchema(fields)
	sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))

	return sch
}

func TestNomsMarshalling(t *testing.T) {
	tSchema := createTestSchema()
	dbSpec, err := spec.ForDatabase("mem")

	if err != nil {
		t.Fatal("Could not create in mem noms db.")
	}

	db := dbSpec.GetDatabase()
	val, err := MarshalAsNomsValue(db, tSchema)

	if err != nil {
		t.Fatal("Failed to marshal Schema as a types.Value.")
	}

	unMarshalled, err := UnmarshalNomsValue(val)

	if err != nil {
		t.Fatal("Failed to unmarshal types.Value as Schema")
	}

	if !tSchema.Equals(unMarshalled) {
		t.Error("Value different after marshalling and unmarshalling.")
	}
}
