package jsonenc

import (
	"encoding/json"
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
	err :=sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))

	if err != nil {
		panic(err)
	}

	return sch
}

func TestFieldDataJson(t *testing.T) {
	fld := schema.NewField("id", types.UUIDKind, true)
	jfd := newJsonFieldData(fld)
	jsonData, err := json.Marshal(jfd)

	if err != nil {
		t.Fatal("Error marshalling", err)
	}

	t.Log("json serialized as:", string(jsonData))

	var unmarshalled jsonFieldData
	err = json.Unmarshal(jsonData, &unmarshalled)

	if err != nil {
		t.Fatal("Error unmarshalling", string(jsonData), err)
	}

	result := unmarshalled.toField()
	if !fld.Equals(result) {
		t.Fatal("original != unmarshalled")
	}

	if fld.NameStr() != "id" || fld.NomsKind() != types.UUIDKind || fld.KindString() != "uuid" || !fld.IsRequired() {
		t.Error("Accessors not returning expected results")
	}
}

func TestSchemaJson(t *testing.T) {
	tSchema := createTestSchema()

	data, err := SchemaToJSON(tSchema)

	if err != nil {
		t.Fatal("Unable to marshal as json.", err)
	}

	unmarshalled, err := SchemaFromJSON(data)

	if err != nil {
		t.Fatal("Unable to deserialize schema from json.", err)
	}

	if !tSchema.Equals(unmarshalled) {
		t.Errorf("Value changed after marshalling and unmarshalling.")
	}

	if tSchema.NumFields() != 4 {
		t.Error("Unexpected column count in schema")
	}

	pkIndex := tSchema.GetFieldIndex("id")

	unMarshalledID := tSchema.GetField(pkIndex)
	if unMarshalledID.NameStr() != "id" || unMarshalledID.NomsKind() != types.UUIDKind || !unMarshalledID.IsRequired() {
		t.Error("Unmarshalled PK does not match the initial PK")
	}
}
