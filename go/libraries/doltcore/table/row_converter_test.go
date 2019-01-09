package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

func TestRowConverter(t *testing.T) {
	srcSch := schema.NewSchema([]*schema.Field{
		schema.NewField("uuidtostr", types.UUIDKind, true),
		schema.NewField("floattostr", types.FloatKind, true),
		schema.NewField("uinttostr", types.UintKind, true),
		schema.NewField("booltostr", types.BoolKind, true),
		schema.NewField("inttostr", types.IntKind, true),
		schema.NewField("stringtostr", types.StringKind, true),
		schema.NewField("nulltostr", types.NullKind, true),
	})

	destSch := schema.NewSchema([]*schema.Field{
		schema.NewField("uuidToStr", types.StringKind, true),
		schema.NewField("floatToStr", types.StringKind, true),
		schema.NewField("uintToStr", types.StringKind, true),
		schema.NewField("boolToStr", types.StringKind, true),
		schema.NewField("intToStr", types.StringKind, true),
		schema.NewField("stringToStr", types.StringKind, true),
		schema.NewField("nullToStr", types.StringKind, true),
	})

	mapping, err := schema.NewInferredMapping(srcSch, destSch)

	if err != nil {
		t.Fatal("Err creating field oldNameToSchema2Name")
	}

	rConv, err := NewRowConverter(mapping)

	if err != nil {
		t.Fatal("Error creating row converter")
	}

	id, _ := uuid.NewRandom()
	inRow := NewRow(RowDataFromValMap(srcSch, map[string]types.Value{
		"uuidtostr":   types.UUID(id),
		"floattostr":  types.Float(1.25),
		"uinttostr":   types.Uint(12345678),
		"booltostr":   types.Bool(true),
		"inttostr":    types.Int(-1234),
		"stringtostr": types.String("string string string"),
		"nulltostr":   types.NullValue,
	}))

	outData, _ := rConv.Convert(inRow)

	expected := RowDataFromValMap(destSch, map[string]types.Value{
		"uuidtostr":   types.String(id.String()),
		"floattostr":  types.String("1.25"),
		"uinttostr":   types.String("12345678"),
		"booltostr":   types.String("true"),
		"inttostr":    types.String("-1234"),
		"stringtostr": types.String("string string string"),
		"nulltostr":   types.NullValue,
	})

	if !RowDataEqualIgnoringSchema(expected, outData) {
		t.Error("\n", RowDataFmt(expected), "!=\n", RowDataFmt(outData))
	}
}
