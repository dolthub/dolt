package rowconv

import (
	"context"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

var srcCols, _ = schema.NewColCollection(
	schema.NewColumn("uuidtostr", 0, types.UUIDKind, true),
	schema.NewColumn("floattostr", 1, types.FloatKind, false),
	schema.NewColumn("uinttostr", 2, types.UintKind, false),
	schema.NewColumn("booltostr", 3, types.BoolKind, false),
	schema.NewColumn("inttostr", 4, types.IntKind, false),
	schema.NewColumn("stringtostr", 5, types.StringKind, false),
	schema.NewColumn("nulltostr", 6, types.NullKind, false),
)

var srcSch = schema.SchemaFromCols(srcCols)

func TestRowConverter(t *testing.T) {
	mapping := TypedToUntypedMapping(srcSch)

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

	results, _ := GetRowConvTransformFunc(rConv)(inRow, pipeline.ImmutableProperties{})
	outData := results[0].RowData

	destSch := mapping.DestSch
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
		t.Error("\n", row.Fmt(context.Background(), expected, destSch), "!=\n", row.Fmt(context.Background(), outData, destSch))
	}
}

func TestUnneccessaryConversion(t *testing.T) {
	mapping, err := TagMapping(srcSch, srcSch)

	if err != nil {
		t.Error(err)
	}

	rconv, err := NewRowConverter(mapping)

	if !rconv.IdentityConverter {
		t.Error("expected identity converter")
	}
}
