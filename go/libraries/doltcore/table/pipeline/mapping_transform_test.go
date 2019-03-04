package pipeline

/*
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

func tableWithInitialState() *table.InMemTable {
	imt := table.NewInMemTable(initialSchema)

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
		imt.AppendRow(table.NewRow(table.RowDataFromValMap(initialSchema, valsMap)))
	}

	return imt
}

func TestMappingReader(t *testing.T) {
	initialTable := tableWithInitialState()
	resultTable := table.NewInMemTable(mappedSchema2)

	rd := table.NewInMemTableReader(initialTable)

	mapping, _ := schema.NewInferredMapping(initialTable.GetSchema(), mappedSchema1)
	rconv, _ := table.NewRowConverter(mapping)
	tr := NewRowTransformer("mapping transform", GetRowConvTransformFunc(rconv))

	mapping2, _ := schema.NewFieldMappingFromNameMap(mappedSchema1, mappedSchema2, oldNameToSchema2Name)
	rconv2, _ := table.NewRowConverter(mapping2)
	tr2 := NewRowTransformer("mapping transform 2", GetRowConvTransformFunc(rconv2))

	func() {
		imttWr := table.NewInMemTableWriter(resultTable)
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

	resultRd := table.NewInMemTableReader(resultTable)
	rows, _, _ := table.ReadAllRows(resultRd, true)

	testMappingRead(mappedSchema2, rows, t)
}

func testMappingRead(typedSchema *schema.Schema, rows []*table.Row, t *testing.T) {
	expectedRows := make([]*table.Row, len(names))
	for i := 0; i < len(names); i++ {
		rowValMap := map[string]types.Value{
			"id":    types.UUID(uuids[i]),
			"name":  types.String(names[i]),
			"age":   types.Uint(ages[i]),
			"title": types.String(titles[i]),
		}
		expectedRows[i] = table.NewRow(table.RowDataFromValMap(typedSchema, rowValMap))
	}

	if len(expectedRows) != len(rows) {
		t.Error("Unexpected Row count")
	}

	for i, Row := range rows {
		if !table.RowsEqualIgnoringSchema(Row, expectedRows[i]) {
			t.Error("\n", table.RowFmt(Row), "!=\n", table.RowFmt(expectedRows[i]))
		}
	}
}
*/
