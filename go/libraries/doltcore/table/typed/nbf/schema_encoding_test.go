package nbf

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
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

func TestNBFEncoding(t *testing.T) {
	tSchema := createTestSchema()

	schemaPath := "schema.nbf"
	fs := filesys.NewInMemFS([]string{"/"}, nil, "/")
	nbfOut, err := fs.OpenForWrite(schemaPath)

	if err != nil {
		t.Fatal("Failed to open file.", err)
	}

	func() {
		defer nbfOut.Close()
		err = WriteBinarySchema(tSchema, nbfOut)
	}()

	if err != nil {
		t.Fatal("Failed to write file.", err)
	}

	nbfIn, err := fs.OpenForRead(schemaPath)

	if err != nil {
		t.Fatal("Unable to open file.", err)
	}

	var resultSch *schema.Schema
	func() {
		defer nbfIn.Close()
		resultSch, err = ReadBinarySchema(nbfIn)
	}()

	if err != nil {
		t.Fatal("Failed to read schema.")
	}

	if !tSchema.Equals(resultSch) {
		t.Errorf("Value changed between serializing and deserializing.")
	}

	if tSchema.NumFields() != 4 {
		t.Error("Unexpected column count in schema")
	}

	idIdx := tSchema.GetFieldIndex("id")

	unMarshalledPK := tSchema.GetField(idIdx)
	if unMarshalledPK.NameStr() != "id" || unMarshalledPK.NomsKind() != types.UUIDKind || !unMarshalledPK.IsRequired() {
		t.Error("Unmarshalled PK does not match the initial PK")
	}
}
