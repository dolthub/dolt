package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"strconv"
	"testing"
)

var initialSchema = schema.NewSchema([]*schema.Field{
	schema.NewField("id_str", types.StringKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("title", types.StringKind, false),
	schema.NewField("age", types.StringKind, true),
	schema.NewField("is_married", types.StringKind, true),
})

var mappedSchema1 = schema.NewSchema([]*schema.Field{
	schema.NewField("id_str", types.StringKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("title", types.StringKind, false),
	schema.NewField("age", types.FloatKind, true),
	schema.NewField("is_married", types.BoolKind, true),
})

var mappedSchema2 = schema.NewSchema([]*schema.Field{
	schema.NewField("id", types.UUIDKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("age", types.UintKind, true),
	schema.NewField("title", types.StringKind, false),
})

var oldNameToSchema2Name = map[string]string{
	"id_str": "id",
	"name":   "name",
	"age":    "age",
	"title":  "title",
}

var uuids = []uuid.UUID{uuid.Must(uuid.NewRandom()), uuid.Must(uuid.NewRandom()), uuid.Must(uuid.NewRandom())}
var names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var ages = []uint64{32, 25, 21}
var titles = []string{"Senior Dufus", "Dufus", ""}
var maritalStatus = []bool{true, false, false}

func tableWithInitialState() *InMemTable {
	imt := NewInMemTable(initialSchema)

	for i := 0; i < len(uuids); i++ {
		marriedStr := "true"
		if !maritalStatus[i] {
			marriedStr = "false"
		}

		valsMap := map[string]types.Value{
			"id_str":     types.String(uuids[i].String()),
			"name":       types.String(names[i]),
			"age":        types.String(strconv.FormatUint(ages[i], 10)),
			"title":      types.String(titles[i]),
			"is_married": types.String(marriedStr),
		}
		imt.AppendRow(NewRow(RowDataFromValMap(initialSchema, valsMap)))
	}

	return imt
}

func TestMappingReader(t *testing.T) {
	initialTable := tableWithInitialState()
	resultTable := NewInMemTable(mappedSchema2)

	rd := NewInMemTableReader(initialTable)

	mapping, _ := schema.NewInferredMapping(initialTable.sch, mappedSchema1)
	rconv, _ := NewRowConverter(mapping)
	tr := NewRowTransformer("mapping transform", rconv.TransformRow)

	mapping2, _ := schema.NewFieldMappingFromNameMap(mappedSchema1, mappedSchema2, oldNameToSchema2Name)
	rconv2, _ := NewRowConverter(mapping2)
	tr2 := NewRowTransformer("mapping transform 2", rconv2.TransformRow)

	func() {
		imttWr := NewInMemTableWriter(resultTable)
		defer imttWr.Close()

		transforms := NewTransformCollection(NamedTransform{"t1", tr}, NamedTransform{"t2", tr2})

		p, start := NewAsyncPipeline(rd, transforms, imttWr, func(_ *TransformRowFailure) (quit bool) {
			t.Fatal("Bad Transform")
			return true
		})

		start()

		err := p.Wait()

		if err != nil {
			t.Fatal("Failed to move data.")
		}
	}()

	resultRd := NewInMemTableReader(resultTable)
	rows, _, _ := ReadAllRows(resultRd, true)

	testMappingRead(mappedSchema2, rows, t)
}

func testMappingRead(typedSchema *schema.Schema, rows []*Row, t *testing.T) {
	expectedRows := make([]*Row, len(names))
	for i := 0; i < len(names); i++ {
		rowValMap := map[string]types.Value{
			"id":    types.UUID(uuids[i]),
			"name":  types.String(names[i]),
			"age":   types.Uint(ages[i]),
			"title": types.String(titles[i]),
		}
		expectedRows[i] = NewRow(RowDataFromValMap(typedSchema, rowValMap))
	}

	if len(expectedRows) != len(rows) {
		t.Error("Unexpected row count")
	}

	for i, row := range rows {
		if !RowsEqualIgnoringSchema(row, expectedRows[i]) {
			t.Error("\n", RowFmt(row), "!=\n", RowFmt(expectedRows[i]))
		}
	}
}
