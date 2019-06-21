package encoding

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"reflect"
	"testing"
)

const expectedSQL = `CREATE TABLE %s (
  first String comment 'tag:1',
  last String not null comment 'tag:2',
  age Uint comment 'tag:3',
  id UUID not null comment 'tag:4',
  primary key (id)
);`

func createTestSchema() schema.Schema {
	columns := []schema.Column{
		schema.NewColumn("id", 4, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", 1, types.StringKind, false),
		schema.NewColumn("last", 2, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", 3, types.UintKind, false),
	}

	colColl, _ := schema.NewColCollection(columns...)
	sch := schema.SchemaFromCols(colColl)

	return sch
}

func TestNomsMarshalling(t *testing.T) {
	tSchema := createTestSchema()
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), nil, nil)

	if err != nil {
		t.Fatal("Could not create in mem noms db.")
	}

	val, err := MarshalAsNomsValue(context.Background(), db, tSchema)

	if err != nil {
		t.Fatal("Failed to marshal Schema as a types.Value.")
	}

	unMarshalled, err := UnmarshalNomsValue(context.Background(), val)

	if err != nil {
		t.Fatal("Failed to unmarshal types.Value as Schema")
	}

	if !reflect.DeepEqual(tSchema, unMarshalled) {
		t.Error("Value different after marshalling and unmarshalling.")
	}
}

func TestJSONMarshalling(t *testing.T) {
	tSchema := createTestSchema()
	jsonStr, err := MarshalAsJson(tSchema)

	if err != nil {
		t.Fatal("Failed to marshal Schema as a types.Value.")
	}

	jsonUnmarshalled, err := UnmarshalJson(jsonStr)

	if err != nil {
		t.Fatal("Failed to unmarshal types.Value as Schema")
	}

	if !reflect.DeepEqual(tSchema, jsonUnmarshalled) {
		t.Error("Value different after marshalling and unmarshalling.")
	}
}
