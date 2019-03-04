package rowconv

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

func TestRowConverter(t *testing.T) {
	srcCols, _ := schema.NewColCollection(
		schema.NewColumn("uuidtostr", 0, types.UUIDKind, true),
		schema.NewColumn("floattostr", 1, types.FloatKind, false),
		schema.NewColumn("uinttostr", 2, types.UintKind, false),
		schema.NewColumn("booltostr", 3, types.BoolKind, false),
		schema.NewColumn("inttostr", 4, types.IntKind, false),
		schema.NewColumn("stringtostr", 5, types.StringKind, false),
		schema.NewColumn("nulltostr", 6, types.NullKind, false),
	)

	destCols, _ := schema.NewColCollection(
		schema.NewColumn("uuidToStr", 0, types.StringKind, true),
		schema.NewColumn("floatToStr", 1, types.StringKind, false),
		schema.NewColumn("uintToStr", 2, types.StringKind, false),
		schema.NewColumn("boolToStr", 3, types.StringKind, false),
		schema.NewColumn("intToStr", 4, types.StringKind, false),
		schema.NewColumn("stringToStr", 5, types.StringKind, false),
		schema.NewColumn("nullToStr", 6, types.StringKind, false),
	)

	srcSch := schema.SchemaFromCols(srcCols)
	destSch := schema.SchemaFromCols(destCols)
	mapping, err := NewInferredMapping(srcSch, destSch)

	if err != nil {
		t.Fatal("Err creating field oldNameToSchema2Name")
	}

	rConv, err := NewRowConverter(mapping)

	if err != nil {
		t.Fatal("Error creating row converter")
	}

	id, _ := uuid.NewRandom()
	inRow := row.New(srcSch, row.TaggedValues{
		0: types.UUID(id),
		1: types.Float(1.25),
		2: types.Uint(12345678),
		3: types.Bool(true),
		4: types.Int(-1234),
		5: types.String("string string string"),
		6: types.NullValue,
	})

	outData, err := rConv.Convert(inRow)

	if err != nil {
		t.Fatal(err)
	}

	expected := row.New(destSch, row.TaggedValues{
		0: types.String(id.String()),
		1: types.String("1.25"),
		2: types.String("12345678"),
		3: types.String("true"),
		4: types.String("-1234"),
		5: types.String("string string string"),
		6: types.NullValue,
	})

	if !row.AreEqual(outData, expected, destSch) {
		t.Error("\n", row.Fmt(expected, destSch), "!=\n", row.Fmt(outData, destSch))
	}
}
